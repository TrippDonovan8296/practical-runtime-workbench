# At-Least-Once Consumers: Idempotency Keys for Duplicate Jobs in a Rate-Limited Queue

Short answer: let the background job queue deliver at least once, give each business operation a stable idempotency key, and commit that key with the local side effect before acknowledging the message. Limit concurrency separately. This keeps duplicate processing from becoming duplicate work, while making the latency-versus-cost choice explicit for a rate-limited worker pool.

I have been paged for both halves of this failure: a missed job because a worker acknowledged too early, and a duplicate delivery because a worker finished the work but lost its acknowledgement. They look like different incidents in the queue dashboard. They are the same boundary problem. Delivery is retried; the handler must decide whether the intended operation has already been committed.

The least complicated fix is an application-owned ledger, not a more aggressive consumer loop. A queue cannot make an email, database write, or webhook exactly once across a process crash and an external system. The consumer can make its own committed state idempotent, then retry work that has not reached that state.

## What should a background job queue consumer do about at-least-once duplicate processing?

Start by drawing the acknowledgement boundary. A message is leased to a worker. The worker reads it, performs the operation, and acknowledges it only after the durable local commit succeeds. If the worker exits after the commit but before the acknowledgement, the queue may deliver the message again. That is normal at-least-once behavior. If it acknowledges before the commit, a process exit can turn a temporary failure into a missed job.

The idempotency key must identify the business intent, not the delivery. For example, `account:8421:monthly-invoice:2026-08` can identify one invoice operation, while a new random UUID generated on every retry identifies every attempt as a new operation. A unique constraint on that intent key gives the handler a durable answer to “have I already committed this?”

This ordering matters more than the queue brand or the language. A Node.js consumer and a Go consumer face the same crash window. The rate limit changes how many messages can be in flight; it does not change the need for idempotency.

There is one important boundary. A database transaction cannot atomically commit with a third-party HTTP request. For an external effect, record an outbox event in the same transaction as the business state, publish that event with the same idempotency key, and make the receiving operation idempotent when its contract allows it. If the receiver has no idempotency facility, a reconciliation record and a business-specific deduplication rule are safer than pretending the network call is exactly once.

## Test the crash windows before increasing concurrency

The first useful test is deliberately unglamorous: terminate a worker after the database commit and before the acknowledgement, then deliver the same message again. The second is to terminate it before the commit. The first run must become a ledger conflict with no second local effect; the second must be safe to retry. Add a third test for two consumers racing on the same key. The unique index, rather than timing luck, must choose one winner.

This is where I keep finding the real defect. Teams often test a handler's happy path, then test a timeout, but skip the narrow interval between durable work and acknowledgement. That interval is where an at-least-once queue earns its name. A small fault-injection test is cheaper than explaining a duplicate invoice during an incident, and it gives deployment reviews a concrete invariant to preserve.

Keep the test in CI.

## The incident lesson: separate retries from concurrency

When a rate-limited worker pool falls behind, the tempting response is to increase workers and requeue every error immediately. That can lower queue age for a moment, then produce HTTP 429 responses, more retries, and a larger bill. It also makes duplicate delivery harder to distinguish from duplicate publication.

I use three states in the runbook: committed, retryable, and poison. A committed message is acknowledged after the ledger and local work are durable. A retryable error leaves the message available for the queue's retry policy. A poison message is isolated for inspection, usually in a dead-letter queue, instead of being requeued forever. A 429 belongs in the retryable path: honor `Retry-After` when it is present, and use backoff when it is not.

Slow down.

The worker pool should have a bounded in-flight count, a deadline for each attempt, and a backoff policy with jitter. Measure queue age, attempt count, acknowledgement latency, handler latency, 429 count, dead-letter volume, and the number of ledger conflicts. Those signals tell you whether latency is caused by too few workers, a downstream rate limit, or duplicate work.

The following Go sketch shows the safe local ordering. The unique constraint is part of the design; the application check alone is subject to a race between two consumers.

```go
package main

import (
	"context"
	"database/sql"
	"errors"
)

type Job struct {
	ID             string
	IdempotencyKey string
	AccountID      string
}

// handle commits one local business operation at most once.
// Ack the queue message only after this function returns nil.
func handle(ctx context.Context, db *sql.DB, job Job) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var inserted bool
	err = tx.QueryRowContext(ctx, `
		INSERT INTO job_ledger (idempotency_key, message_id)
		VALUES ($1, $2)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING true`, job.IdempotencyKey, job.ID).Scan(&inserted)
	if errors.Is(err, sql.ErrNoRows) {
		// A redelivery found a committed business intent.
		return tx.Commit()
	}
	if err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE accounts SET invoice_pending = true WHERE id = $1`, job.AccountID); err != nil {
		return err
	}

	return tx.Commit()
}
```

The `message_id` is useful for tracing, but it is not the deduplication key. Store the business key with a retention period that covers the maximum retry and replay window you have chosen. The exact duration is a policy decision; it should be longer than the period in which repeating the operation would be harmful.

## How do retries and dead-letter queues change the latency and cost decision?

Retries trade extra attempts for recovery from transient failure. More concurrent workers can reduce queue latency until the downstream service becomes the bottleneck. After that point, more concurrency raises contention and 429s without increasing useful throughput. A smaller pool may cost less and protect the dependency, but it increases queue age. Neither choice repairs a non-idempotent handler.

Use a dead-letter queue for messages that need a different human or code path. AWS documents dead-letter queues as a way to handle messages that cannot be successfully processed after repeated attempts; the retention and redrive policy still need to match the business operation's recovery window. Do not automatically redrive a poison payload at full speed. That just recreates the same incident. I don't treat a dead-letter count of zero as proof that the system is healthy: a consumer that acknowledges too early can hide failure entirely, so the success metric has to include business-state reconciliation.

The useful comparison is operational, not a leaderboard:

| Design choice | Latency benefit | Cost or failure trade-off |
| --- | --- | --- |
| Increase worker concurrency | Lower queue age while the dependency has spare capacity | More in-flight work, more rate-limit pressure, and more duplicate overlap during recovery |
| Add exponential backoff with jitter | Fewer synchronized retry bursts | A single attempt may finish later, so queue age must be monitored |
| Keep a durable idempotency ledger | Safe redelivery and simpler recovery | Database storage, retention cleanup, and a unique-index write per operation |
| Use a dead-letter queue | Stops poison messages from consuming worker capacity | Someone must inspect, repair, and deliberately redrive messages |

Your mileage may vary. I am not sure a single concurrency number can be chosen from queue depth alone; the downstream quota, handler duration, and acceptable recovery time are the missing measurements. Start with a load test that includes a lost acknowledgement and a repeated delivery, then tune concurrency against the 429 rate and queue-age objective.

## When is this design the wrong fit?

The ledger pattern is not suitable when the operation is intentionally repeatable and the storage cost of keys is greater than the risk of a duplicate. It is also the wrong abstraction for a workflow that needs durable timers, joins, or a dependency graph; use a workflow-oriented system when those are first-class requirements. Stick with the queue's native retry and dead-letter features when the team already operates them well and the job is an independent unit of work.

Do not hide a missing capability behind a retry loop. A queue consumer cannot infer whether sending two notifications is acceptable, and a worker pool cannot manufacture a receiver-side idempotency contract. Make that decision with the owner of the side effect, document the key format, and test the crash points in deployment.

The durable rule is short: commit the intended operation, then acknowledge delivery. Tune the pool for the downstream limit and the latency objective. Treat every other duplicate-job fix as an implementation of those two decisions.

## References

- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429
- https://docs.bullmq.io/
- https://docs.temporal.io/
