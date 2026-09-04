# Weekly SaaS Digests: Retry Failed Webhook Jobs with a Delayed Queue or Cron

Short answer: use a delayed queue for the normal retry clock, and use cron to redrive a deliberately bounded recovery set. For a B2B SaaS team sending a weekly digest to active customers, this keeps each failed delivery observable without making every retry wait for the next sweep.

The least complex design is a worker that owns one delivery attempt at a time. It records an idempotency decision, calls the customer-facing webhook over a public HTTPS endpoint, and schedules the next attempt only when the failure is retryable. Cron remains useful for inspection and operator-directed recovery.

## Start with the delivery record, not the scheduler

The failure mode is familiar: the digest batch is generated on schedule, the partner accepts some deliveries, and a timeout leaves the rest marked as failed. A five-minute cron scan then finds those records together. If the partner is still slow, the scan creates another burst; if the earlier request actually arrived, the next attempt may duplicate a customer-visible event.

I have been paged for both sides of this. The missed job gets attention first. The duplicate delivery is the quieter postmortem finding, because it often appears as a customer complaint rather than an alarm.

The invariant I want is simple: one delivery has one retry state, one stable identifier, and one auditable attempt history. A delayed queue expresses that state directly. A retry message waits independently, so a failed webhook does not hold unrelated customers behind a scheduler tick.

That is the whole alarm.

Before comparing mechanisms, define what the record means. For a weekly digest, the outbox row should identify the customer, digest period, destination, payload version, attempt count, last outcome, next eligible time, and parked reason. The destination should be treated as untrusted input: authenticate requests at the public HTTPS boundary, enforce a timeout, and record the response class without storing more customer data than the incident requires. If a worker dies after the remote service accepts the request but before the local acknowledgment, the record must make a repeat attempt safe. If the digest generator crashes after the business transaction commits, the pending outbox record must still be visible to a publisher. This is why I start with the record rather than the schedule. The queue and cron are wake-up mechanisms around an existing contract; neither can reconstruct a lost delivery decision after the fact.

## How should a SaaS team retry failed webhook jobs with a delayed queue or cron?

Use a delayed queue for per-delivery backoff. The worker should acknowledge a successful attempt, or publish the same delivery with its incremented attempt number and a bounded delay. It should stop retrying after the policy limit and move the record to a dead-letter or parked state that an operator can inspect.

Use cron for the exception path. A scheduled handler can select a limited set of parked records, publish them for workers, and return. It should not perform a large webhook backlog inline. That separation matters when a public HTTP invocation has a fixed execution window or when one slow customer endpoint would consume the whole run.

Here is the decision boundary in Go. The delivery store and queue are intentionally generic; the important part is the order of the state transition and the external call.

```go
package delivery

import "context"

type Delivery struct {
	ID      string
	Attempt int
	Body    []byte
}

type Result struct {
	RetryAfterSeconds int
	Retryable         bool
}

func Process(ctx context.Context, d Delivery, store Store, sender Sender, queue Queue) error {
	claimed, err := store.ClaimAttempt(ctx, d.ID, d.Attempt)
	if err != nil {
		return err
	}
	if !claimed {
		return nil // Another worker already owns this attempt.
	}

	result, err := sender.Send(ctx, d.Body, d.ID)
	if err == nil {
		return store.MarkDelivered(ctx, d.ID, d.Attempt)
	}
	if !result.Retryable || d.Attempt >= store.MaxAttempts() {
		return store.Park(ctx, d.ID, err.Error())
	}

	next := d
	next.Attempt++
	return queue.PublishAfter(ctx, next, result.RetryAfterSeconds)
}
```

The claim must be durable, and the receiver must treat the delivery ID as an idempotency key. At-least-once delivery means the worker can see the same message again after a timeout. A five-minute deduplication window, if one exists in the chosen queue, is not an application-level guarantee for a partner outage lasting longer than five minutes. A `429` belongs on the retry path too; honor `Retry-After` when supplied instead of adding pressure to a rate-limited endpoint.

## What does a cron redrive actually buy you?

Cron is cheap in operational complexity when the work is periodic and bounded. It gives an explicit time to run a reconciliation query, report its count, and hand a selected batch to the queue. That makes it a good repair tool after a deployment, a credential rotation, or an operator-approved replay.

It is a poor substitute for per-delivery timing. A sweep must rediscover state, claim rows safely, and decide whether a previous timeout caused a remote side effect. Every one of those decisions needs the same idempotency and lease rules as a queue worker. The schedule only supplies a wake-up signal.

The easiest public HTTPS endpoint is also not automatically the safest architecture. A push target must be reachable from the delivery system and must authenticate the request, bound its body size, and return a useful status quickly. A private worker can instead pull work outbound, leaving the customer webhook as the only public boundary.

Compare the options by failure behavior, not sticker price. The table belongs in the design review after the delivery record and retry policy are clear:

| Option | Fits this weekly digest when | Cost or complexity to accept |
|---|---|---|
| Delayed queue | Individual failures need independent backoff and inspection | The consumer must be idempotent and tolerate duplicate delivery |
| Cron redrive | Parked work needs a bounded, reviewable repair run | Polling can create bursts and requires safe row claiming |
| Queue plus public HTTPS push | A worker can acknowledge quickly at a reachable endpoint | Endpoint authentication, timeout, and replay handling become part of the service |
| Queue plus pull worker | The worker network is private or outbound-only | The worker must maintain polling and visibility-timeout behavior |

The cheapest option is the one that does not turn a missed digest into a manual data repair project. The easiest option is the one whose retry state can be explained from one delivery record during an incident. Those are engineering costs, not a claim about a provider's current price.

## Where this pattern is not suitable

The catch is that a delayed queue plus cron redrive is a delivery mechanism, not a workflow engine. It is not suitable when recovery requires a dependency graph, a human approval step, compensation across several services, or a durable event history for independent consumers. Choose a workflow system or event log for those requirements and accept the additional operating model.

It is also not suitable to keep a retry message alive forever. Set a maximum age or attempt count, park the delivery, and make redrive an intentional action. Your mileage may vary when customer contracts require a specific retry window; the contract should decide the backoff and retention policy before implementation, not after the first incident.

For the weekly digest, the practical runbook is short: generate a stable delivery ID, persist the outbox record with the business change, let workers perform attempts, and use cron to report or redrive a bounded parked set. The transactional outbox pattern is useful here because it keeps the digest decision and its pending delivery from being separated by a process crash.

## References

- https://en.wikipedia.org/wiki/Cron
- https://microservices.io/patterns/data/transactional-outbox.html
