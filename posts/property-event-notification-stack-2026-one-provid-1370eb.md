# Property Event Notification Stack 2026: One Provider, Auditable Email and SMS Suppression

A property notification system should own recipient eligibility, suppression, and evidence in one internal ledger, even when one external provider carries both email and SMS. Choose unified delivery only if its event data can feed that ledger without losing channel-specific detail; choose separate vendors when regional reach or failure isolation justifies the extra reconciliation work. The provider count is secondary. The auditable state transition is the decision.

**TL;DR:** accept a lease, maintenance, or payment event once; assign it an immutable notification ID; decide eligibility per channel; and record every attempt and provider callback against that ID. A hard email bounce must suppress that email address before the next send. It must not silently suppress a separately consented phone number. Retries need idempotency, and operators need a record that explains what the system knew when it acted.

This is the shape I would put in a runbook because missed jobs and duplicate deliveries have the same unpleasant property: the downstream symptom appears after the original decision has disappeared from view. Keep the decision.

## What must the evidence prove?

For a property manager, “the provider accepted it” is not a useful terminal state. The evidence should connect a business event, such as `lease_renewal_due`, to the intended recipient, the eligibility decision for each channel, the rendered message version, each attempt, and the latest delivery observation. It should also preserve why an address was suppressed and which later event, if any, restored eligibility.

That model separates three statements that are often collapsed into one status field: the application requested a notification, a delivery service accepted an attempt, and the recipient endpoint produced an outcome. They happen at different times. They may arrive out of order. Do not overwrite one with another.

Those are three facts, not one status.

Use append-only observations plus a derived current state. The append-only side is evidence; the projection is what the sender queries. If a callback is repeated, its provider event identifier or a stable digest makes ingestion idempotent. If callbacks arrive out of order, the projection applies an explicit transition policy instead of trusting arrival time.

A useful minimum record has these fields:

| Record | Purpose | Retention decision |
|---|---|---|
| Notification | Links the property event, tenant, template version, and requested channels | Match the business evidence policy |
| Eligibility decision | Captures consent or permitted basis, suppression state, and decision time | Keep long enough to explain the send decision |
| Attempt | Stores channel, destination reference, idempotency key, and provider reference | Match retry and dispute windows |
| Observation | Preserves normalized outcome, source event ID, source time, and receipt time | Keep raw payload only when policy permits |
| Suppression change | Records scope, reason, source observation, actor, and time | Treat as compliance evidence |

Destination references should not force unrestricted plaintext retention. The ledger can hold a stable internal recipient reference and a masked value for routine operations, with access to sensitive source data controlled separately. The exact retention period is a policy choice, not a provider default.

## Should one provider control the event notification stack?

A unified provider offers one credential boundary, one callback integration, and one commercial relationship. That can reduce the number of moving parts during an early launch. It does not make email and SMS the same transport. Email authentication includes domain-level mechanisms such as DKIM, while SMS has different addressing, consent, regional, and delivery semantics. A common API must not erase those differences from the internal model.

Separate vendors add two event vocabularies, two retry boundaries, and two operational dependencies. The benefit is deliberate isolation: email trouble need not consume the same quota, credential, or callback path as an urgent maintenance text. A split can also support a channel specialist or a regional route. The price is reconciliation work, and that price lands in engineering time before it lands on an invoice.

That trade-off is real.

Use this decision table at design review:

| Test | Unified control | Split control |
|---|---|---|
| Can every result map without information loss? | Favorable if yes | Each adapter can retain channel detail |
| Can credentials, quotas, and failure domains be isolated per channel? | Verify explicitly | Isolation is natural but operational load increases |
| Can suppression evidence be exported and replayed? | One integration to test | Two integrations and one internal truth |
| Does one channel require distinct regional handling? | Confirm the boundary | Specialization may justify the split |
| Can the team rehearse callback loss and delayed events? | Smaller surface | More cases, clearer blast-radius boundaries |

Do not choose on the smallest advertised unit price. The relevant cost includes callback ingestion, reconciliation, on-call diagnosis, evidence export, data retention, regional operations, and migration. A cheap send that cannot be tied back to an eligibility decision is expensive during an investigation.

## Build the suppression boundary before the sender

The sender should never decide from a vendor dashboard or a cached boolean that no one can explain. Give it a narrow internal interface: evaluate a recipient and channel, reserve an attempt under an idempotency key, then submit only an allowed attempt. Normalize external observations behind channel adapters.

The following Go sketch focuses on the transaction boundary. It deliberately leaves transport calls behind an interface so the same state machine works with unified or split delivery.

```go
package notify

import (
    "context"
    "errors"
)

type Channel string

const (
    Email Channel = "email"
    SMS   Channel = "sms"
)

type Attempt struct {
    NotificationID string
    Channel        Channel
    IdempotencyKey string
    RecipientRef   string
}

type Ledger interface {
    Allowed(ctx context.Context, recipientID string, channel Channel) (bool, string, error)
    Reserve(ctx context.Context, attempt Attempt) (created bool, err error)
    RecordSubmission(ctx context.Context, key, providerRef string) error
    RecordFailure(ctx context.Context, key, class string) error
}

type Transport interface {
    Submit(ctx context.Context, attempt Attempt) (providerRef string, err error)
}

var ErrSuppressed = errors.New("recipient suppressed for channel")

func Send(ctx context.Context, ledger Ledger, transport Transport, notificationID, recipientID string, channel Channel) error {
    allowed, recipientRef, err := ledger.Allowed(ctx, recipientID, channel)
    if err != nil {
        return err
    }
    if !allowed {
        return ErrSuppressed
    }

    attempt := Attempt{
        NotificationID: notificationID,
        Channel:        channel,
        IdempotencyKey: notificationID + "/" + string(channel),
        RecipientRef:   recipientRef,
    }
    created, err := ledger.Reserve(ctx, attempt)
    if err != nil || !created {
        return err // An existing reservation makes a repeated job a no-op.
    }

    providerRef, err := transport.Submit(ctx, attempt)
    if err != nil {
        _ = ledger.RecordFailure(ctx, attempt.IdempotencyKey, "submission_error")
        return err
    }
    return ledger.RecordSubmission(ctx, attempt.IdempotencyKey, providerRef)
}
```

There is an intentional trade-off here: reserving before submission can leave an attempt in an uncertain state if the process stops after the external service accepts it but before the reference is recorded. Sending before reservation is worse because a queue retry can create a duplicate with no durable guard. Resolve the uncertainty with a reconciliation job and a state such as `submission_unknown`; never turn uncertainty into an automatic fresh send.

Uncertain is not unsent.

Bounce handling follows the same rule. First persist the raw observation metadata and normalized classification. Then apply the suppression transition in the same durable workflow. A permanent email failure changes eligibility for that address and email channel. A transient observation can drive a scheduled retry policy, but it should not become a permanent suppression without an explicit rule. SMS observations update the phone channel independently.

DKIM belongs in this design as verifiable email authentication evidence, not as proof that a person received or read a message. RFC 6376 defines a domain-level signing mechanism and describes verification outcomes; retain the identifiers needed to investigate authentication configuration while keeping delivery state separate. Mixing authentication, provider acceptance, and recipient delivery into “sent” destroys useful evidence.

## Detect the failures that pages expose too late

Monitor the pipeline as a sequence, not a success counter. Track queue age from business event to reserved attempt, the number of reservations stuck in uncertain states, callback age by channel, duplicate callback rate, suppression applications, and attempts blocked by suppression. These are control signals. Aggregate delivery percentages alone cannot tell an operator whether callbacks stopped or no messages were due.

Add a synthetic recipient for each channel and region that exercises the full route on a controlled schedule. The check should verify that the notification was reserved, submitted, observed, and projected within the expected window. Keep synthetic data visibly separate from tenant evidence.

Three alerts are enough to begin: oldest eligible job age exceeds its service objective; callback ingestion is absent while submissions continue; or an attempt remains uncertain beyond the reconciliation window. Tune thresholds from the notification promise and normal traffic, not from a copied dashboard. Quiet periods exist.

Silence needs context.

Log correlation keys, not message bodies. At minimum, carry the notification ID, idempotency key, channel, internal recipient reference, adapter name, and external reference where available. Never make an email address the only join key; addresses can be corrected, reassigned, or suppressed while the evidence must remain stable.

## Verify deployment and make rollback boring

Before production traffic, test duplicate jobs, duplicate callbacks, reversed callback order, a hard email bounce followed by another queued email, and a valid phone number paired with a suppressed email address. The last case catches an overly broad recipient-level suppression. Also test a process stop immediately before and after external submission. The resulting record must make the next operator action unambiguous.

Deploy adapters and projection changes behind versioned mappings. Shadow-normalize recorded fixtures first, compare old and new projections, then enable writes for a narrow notification class such as nonurgent maintenance updates. A mapping change should not reinterpret old evidence silently; store the mapping version with each normalized observation.

Rollback means stopping new reservations or routing them through the prior adapter while continuing to ingest callbacks for every outstanding attempt. Do not roll back by disabling the callback endpoint. Old attempts can remain active after new sends stop, and losing their outcomes creates the exact evidence gap the design is meant to prevent.

The runbook should name one recovery path for `submission_unknown`: query or reconcile using the idempotency key and external reference when available, then close the existing attempt. If its outcome cannot be resolved, escalate according to the business event's urgency rather than issuing a blind duplicate. For a routine property notice, delay may be safer. For a time-sensitive safety notification, the policy may authorize a second channel, but that must be a recorded policy decision.

A 2026 architecture review should end with a replay demonstration, not a slide. Take one lease notice from request through eligibility, attempt, callback, suppression, and blocked resend. If the team can reproduce why each transition occurred after swapping either adapter, the control plane is doing its job. Unified and split delivery are then replaceable implementation choices instead of competing sources of truth.

## References

- RFC 6376, DomainKeys Identified Mail (DKIM): https://datatracker.ietf.org/doc/html/rfc6376
- Anthropic, Tool use overview and tool definition guidance: https://platform.claude.com/docs/en/agents-and-tools/tool-use/overview
