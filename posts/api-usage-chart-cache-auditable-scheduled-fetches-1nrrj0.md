# API Usage Chart Cache: Auditable Scheduled Fetches During Credential Rotation

Rotate the production API key with an overlap window, and make every scheduled usage snapshot prove which credential generation fetched it. The deciding constraint is auditability: a chart that looks current but cannot tie access to a known principal is not operational evidence.

TL;DR: keep collection server-side, issue a second credential before revoking the first, tag each attempt with a non-secret key ID, and publish a snapshot only after its data and source timestamp commit together. During rotation, accept either credential for collection but allow only one scheduler lease to advance the cursor. Show both `source_updated_at` and `fetched_at` in the dashboard. If verification fails, keep the last complete snapshot visible, mark it stale, and roll the collector back to the still-valid credential.

## How should a scheduled API fetch cache a usage chart?

A successful cron invocation proves very little. The process may have fetched the same upstream interval twice, written points without advancing the watermark, or advanced the watermark before the points became durable. Key rotation adds another ambiguous state: one replica may use the new credential while another retries with the old one. If the store records only a final HTTP status and `updated_at`, an operator cannot distinguish delayed source data from a broken collector or a partial deployment.

Treat freshness as two clocks. `source_updated_at` is the newest timestamp represented by the usage data; `fetched_at` is when the collector completed the request. Their gap describes upstream data age. The gap between now and `fetched_at` describes collector age. Do not replace either with the dashboard render time.

This matters during an API-key rotation because authentication success is not the end condition. The new key must fetch an expected account scope, the resulting snapshot must land in the store, and the audit event must identify the new key generation without recording the secret itself. Only then is revocation justified.

Green is not fresh.

## Make the write atomic and the retry boring

Use a stable interval as the idempotency boundary, such as account plus period start and period end. Upsert that interval, then update the collector cursor in the same database transaction. A retry can repeat work, but it cannot create a second logical interval or move the cursor past missing data.

The secret belongs in a secrets manager or an injected runtime value, never in the job payload, logs, or dashboard store. OWASP recommends centralizing secrets management, applying least privilege, automating rotation, and maintaining audit logs. For this workflow, the stored identifier should be an opaque generation label such as `usage-reader-2026-09-b`, not a prefix or hash intended to help reconstruct the credential.

```go
package collector

import (
    "context"
    "database/sql"
    "time"
)

type Snapshot struct {
    AccountID     string
    PeriodStart   time.Time
    PeriodEnd     time.Time
    SourceUpdated time.Time
    Payload       []byte
}

func CommitSnapshot(ctx context.Context, db *sql.DB, s Snapshot, fetchedAt time.Time, keyID string) error {
    tx, err := db.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer tx.Rollback()

    _, err = tx.ExecContext(ctx, `
        INSERT INTO usage_snapshots
            (account_id, period_start, period_end, source_updated_at, fetched_at, payload)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (account_id, period_start, period_end) DO UPDATE SET
            source_updated_at = EXCLUDED.source_updated_at,
            fetched_at = EXCLUDED.fetched_at,
            payload = EXCLUDED.payload`,
        s.AccountID, s.PeriodStart, s.PeriodEnd, s.SourceUpdated, fetchedAt, s.Payload)
    if err != nil {
        return err
    }

    _, err = tx.ExecContext(ctx, `
        INSERT INTO collection_audit
            (account_id, period_end, fetched_at, credential_generation, outcome)
        VALUES ($1, $2, $3, $4, 'committed')`,
        s.AccountID, s.PeriodEnd, fetchedAt, keyID)
    if err != nil {
        return err
    }

    return tx.Commit()
}
```

The example deliberately excludes secret values and request headers. It also commits the snapshot and audit record together. In a real schema, put a uniqueness constraint on the logical interval and constrain audit access separately from dashboard read access. The dashboard needs aggregates and freshness metadata; it does not need credential identifiers.

One writer. Many processes may wake up, especially after a deployment or scheduler recovery, but a database advisory lock, leased queue message, or compare-and-swap lease should select the writer. Keep the lease shorter than the job timeout and renew it only while useful progress continues. The exact mechanism depends on the store; the invariant does not.

This design has a real trade-off. A local snapshot store adds schema ownership, migrations, retention work, and another read path to test; it is a poor fit when the upstream data is already fast, highly available, safe for direct client access, and allowed to receive every dashboard read. Direct fetching removes the replication delay, while the scheduled cache protects the production credential, absorbs upstream outages, preserves a reviewable history, and decouples chart latency from the source. Choose it only when those operational properties justify owning the copy. The overlap window has a boundary too: it temporarily leaves two valid credentials, so keep it bounded by the runbook and end it as soon as a new-generation snapshot commits. There is no honest zero-risk handoff between two independently validated credentials; the useful decision is which risk is visible, limited, and reversible.

## Rotate by evidence, not elapsed time

Start by creating the replacement credential with the same narrow read scope required by the collector. Record who initiated the change, the intended account, and the new opaque generation ID in the change record. Do not revoke the active credential yet.

Deploy the new secret reference to the collector, then observe a complete scheduled cycle. The acceptance evidence is specific: the scheduler acquired one writer lease; authentication succeeded under the new generation; the returned account and interval matched the request; the snapshot and audit row committed; and the next dashboard read exposed the new `fetched_at` and expected `source_updated_at`. A health check that merely reaches the upstream endpoint is insufficient because it bypasses the storage path users depend on.

Keep the old credential available only for the bounded overlap defined by the runbook. Once a complete cycle has been attributed to the new generation, revoke the old credential and verify that no later successful audit records name it. Access to the audit stream should itself be controlled and retained according to the organization's policy; the public source supports logging and auditing as controls, but it does not prescribe one universal retention duration.

Do not test revocation by placing the old secret in a command line. Shell histories, process listings, and copied incident notes turn a validation step into a second secret-distribution path. Exercise the normal secret reference through a controlled canary or collector instance, and log only the generation ID and outcome.

## What should the dashboard admit?

Display the time represented by the data and the time it was fetched. If either exceeds the service's declared freshness objective, show a stale state beside the chart rather than silently stretching the last point to the present. A last-known-good snapshot is useful during an outage, provided the UI does not imply it is live.

The alert should follow the same semantics. Alert on a missing committed snapshot or a freshness threshold breach, not merely on one failed attempt; retries are expected, while absence of fresh durable data is the user-facing failure. Include account ID, interval, scheduler run ID, and credential generation ID in the operational event. Exclude the credential, authorization header, and raw response if it may contain sensitive account data.

Three words for the runbook: trust durable evidence.

Stale must look stale.

## Verification and rollback

Before closing the change, query the stored snapshot through the same read path as the dashboard. Confirm that the displayed source time equals the committed source timestamp, that the fetch time advances after the scheduled run, and that exactly one logical interval exists. Then inspect the restricted audit trail for a committed event from the replacement generation and no successful events from the revoked generation after its cutoff.

Rollback means restoring the previous secret reference only while that credential remains valid, restarting or redeploying the collector through the normal path, and waiting for one committed cycle before declaring recovery. Never roll the database cursor backward by hand just to make the chart move. If the prior credential has already been revoked, issue a new replacement under the normal approval path; resurrecting revoked material weakens the audit story.

If data was fetched but the transaction did not commit, retry the same interval. If the snapshot committed but the dashboard cache did not refresh, invalidate or regenerate that presentation cache from the durable snapshot rather than calling the upstream usage API from the browser. The browser should hold neither the production credential nor authority to mutate collection state.

The completion rule is compact: one active read credential, one durable snapshot per logical interval, two honest freshness timestamps, and an audit trail that connects the change to the access without containing the secret.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
