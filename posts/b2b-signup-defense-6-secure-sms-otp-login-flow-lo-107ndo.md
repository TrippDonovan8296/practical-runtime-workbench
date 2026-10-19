# B2B Signup Defense: 6 Secure SMS OTP Login Flow Lockout Decisions

TL;DR: Page on failed signup verification outcomes, not merely SMS send errors. Keep template ownership explicit, rate-limit identities and network sources, make verification a single atomic consume, and alert on a sustained loss of successful challenges. For B2B SaaS signup, this is the least complex design that covers retries, lockouts, and replay without pretending SMS is a phishing-resistant authenticator.

The page says new accounts cannot complete verification. On-call sees accepted sends, a normal queue, and no obvious transport failure. The page arrived late because the system measured message handoff while users were failing downstream.

A signup verification link and an SMS OTP have different user interfaces, but share an operational path: issuance, delivery, presentation, validation, and one-time consumption. NIST treats the public switched telephone network as restricted for out-of-band authentication, requires rate limiting for short secrets, and requires a valid secret to be accepted only once [1].

## 1. How should a secure SMS OTP login flow fail?

Transport acceptance does not prove receipt, a valid link, a matching code, or a verified account. Define `signup_verified` as the terminal service event. Join it to `challenge_issued`, `dispatch_accepted`, `verification_attempted`, and `challenge_consumed` with an opaque challenge ID. Never log the OTP, token, or full phone number.

Compare mature issued cohorts with consumed challenges, separated by a controlled region dimension. Challenge detail belongs in logs, not unbounded metric labels. **Measure verification completion as the outcome and transport acceptance as a diagnostic stage.**

Start there.

The trade-off is detection delay. Waiting for a cohort to mature avoids paging while a person is still switching apps and typing, but it also postpones notice of a real regional failure. A useful alert therefore carries two clocks: dispatch failures can page quickly because the system already has a terminal rejection, while completion loss waits for the agreed user-action interval. Write that distinction into the runbook. Otherwise, an engineer under pressure can shorten the window, watch the graph recover as cohorts mature, and close a real issue as noise.

## 2. Make template ownership a security boundary

Authentication owners should control message purpose, approved destination hosts, expiry wording, and challenge semantics. Communications owners can control reviewed prose and localization. The delivery layer renders a versioned template but cannot extend validity or change the callback host.

Reject unknown variables and hosts before enqueueing. Record the template version on issuance for rollback and cohort analysis. If email is a signup fallback, DKIM signs selected headers and the body under defined canonicalization rules [2]; an email template or sending-domain change can therefore affect authentication. DKIM does not secure SMS.

## 3. Rate-limit more than the phone number, then define retry state

Apply expiring counters to the signup identity, normalized destination, network source, and total service issuance. One key is insufficient: attackers distribute requests, while legitimate users can share office networks.

Exact values are policy. Three sends per destination in 15 minutes, five attempts per challenge, and a progressive reissue delay are reasonable test inputs, not universal facts. Validate them against abuse evidence, support load, and regional latency. Return the same user-facing response when a key trips, but retain distinct internal reason codes.

Do not erase abuse history after success. Close the challenge.

The fourth decision belongs in the same state machine. Reissue creates or re-delivers a challenge; retry presents a candidate secret. They need separate budgets. Either reissue delivers the same unexpired challenge without extending its life, or it invalidates the prior challenge. The second rule is easier to explain if the UI says only the newest code works. Reissue must never reset attempts.

Lock a challenge temporarily rather than permanently disabling an account. NIST requires rate limiting when the secret has less than 64 bits of entropy [1]; a six-digit decimal OTP is in that short-secret category. Model `pending` transitions to `consumed`, `expired`, or `locked`, with no transition back.

## 4. Consume once, atomically

A read followed by a write permits two concurrent requests to validate the same code. Compare the stored keyed digest, check purpose, expiry, and budget, then change state in one conditional operation. Bind each challenge to its subject and purpose so a signup code cannot authorize password recovery.

```go
package otp

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

var ErrRejected = errors.New("verification rejected")

func Consume(ctx context.Context, db *sql.DB, id string, digest []byte, now time.Time) error {
	r, err := db.ExecContext(ctx, `
		UPDATE otp_challenges SET state = 'consumed', consumed_at = ?
		WHERE id = ? AND purpose = 'signup' AND state = 'pending'
		AND expires_at > ? AND attempts_used < attempts_max AND otp_digest = ?`,
		now, id, now, digest)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrRejected
	}
	return nil
}
```

Attempt accounting needs its own atomic rejection path, and digest construction must suit the datastore. The invariant is simple: one request changes one pending challenge to consumed. Use the challenge ID as the idempotency key for account creation. Test concurrent submissions, expiry, an old code after resend, a correct code after lockout, and a consumed code presented again.

## 5. Tune the page against user harm

Alert on a sustained change in consumed challenges divided by eligible issued challenges, corroborated by absolute volume. A ratio is noisy at low traffic; a raw count pages during healthy growth. Let cohorts mature beyond ordinary delivery and user-action time.

Transport rejection routes to the delivery runbook. More incorrect attempts routes to abuse review. Normal dispatch with lower consumption routes first to application and template evidence: destination binding, link construction, template version, and verifier decisions.

Record the alert owner, cohort definition, minimum volume, and duration. The false-positive cost is real: repeated pages teach responders to distrust the signal, while an automated lockout driven by the same noisy threshold denies legitimate signup. Paging and enforcement may share evidence, but they need separate actions.

SMS also has a hard product boundary. It is a practical reach channel for many signup flows, but its restricted status and exposure to telephone-network risks make it unsuitable as the only factor for higher-assurance administration or recovery. A stronger authenticator reduces that channel risk, though it adds enrollment, device-loss, and support work. The decision is not “SMS everywhere” or “SMS nowhere.” Use the lowest-friction method that meets the action's assurance requirement, and preserve an independently reviewed recovery path.

Own challenge semantics in authentication, constrain templates to presentation, consume each challenge once, and page on verified outcomes. Higher-assurance actions need an authenticator chosen for that assurance level, not stronger wording in an SMS.

## Further reading

- [1] NIST SP 800-63B, Digital Identity Guidelines: https://pages.nist.gov/800-63-3/sp800-63b.html
- [2] RFC 6376, DomainKeys Identified Mail (DKIM): https://datatracker.ietf.org/doc/html/rfc6376
