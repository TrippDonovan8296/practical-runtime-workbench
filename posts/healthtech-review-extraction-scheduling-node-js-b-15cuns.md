# Healthtech Review Extraction: Scheduling Node.js Batch LLM Jobs for Cost and Correctness

Short answer: move summarization, tagging, and extraction to asynchronous batch LLM jobs only when the result can miss the request path and still meet a declared completion deadline. Compare the cost per accepted structured finding, not the advertised cost per token. Keep realtime calls for work that blocks a reviewer, and fail closed when a batch result does not match the schema.

For a healthtech code-review service, the unit of success is not a cheap response. It is a valid finding attached once to the right repository, commit, tenant, and policy version. A malformed severity or a duplicate patient-data warning can create more review work than the inference call removed.

This is a scheduling problem first.

## Govern the output contract before the queue

The useful signal is queue age relative to the review deadline. If the oldest eligible change is 18 minutes old and the service objective allows findings within two hours, there is room to form a batch. If the finding must appear before a merge button is enabled, there isn't. Those numbers are an example policy, not a universal target; the repository owner has to set the actual deadline.

The failure mode is easy to miss because transport success looks healthy. A worker can receive a syntactically valid model response that names an unknown severity, omits a file path, or returns the same finding twice after a retry. The API call completed, but the job did not. Count schema rejection, identity mismatch, and duplicate suppression separately from provider request failures. Otherwise a cheaper asynchronous lane can appear successful while its usable yield falls.

I've been paged by missed jobs and duplicate deliveries. That changes the runbook: every queued item gets a stable job ID, every attempt is recorded, and publishing findings is an idempotent state transition. Don't let a retry invent a second review.

In a system that handles electronic protected health information, the data path also belongs in the decision. The HIPAA Security Rule establishes safeguards for electronic protected health information, while the Privacy Rule addresses protected health information. A team should have its privacy and security owners decide what code, prompts, metadata, and outputs may enter a given processing path; batching does not relax those obligations.

## How should batch LLM jobs replace realtime API extraction?

Split the workload by deadline and consequence. Interactive review assistance stays in a realtime lane. Repository-wide summarization, nightly policy tagging, and extraction from already-queued changes can enter a deferred lane when users do not wait on the result. Both lanes should call the same validator and publisher, so changing the schedule cannot quietly change the acceptance standard.

A small decision table is enough for the first runbook:

| Condition | Lane | Operator action |
| --- | --- | --- |
| A reviewer is blocked on the finding | Realtime | Enforce the request deadline and return an explicit incomplete state |
| Work has deadline slack and stable input | Batch | Queue with an immutable input reference and deadline |
| Input can change before execution | Neither yet | Snapshot the commit and policy version first |
| Output fails schema or identity checks | Quarantine | Do not publish; retain the validation reason |
| The same job is delivered again | Existing lane | Reuse the idempotency key and suppress a second publish |

Estimate economics from completed work. For each lane, record input units, output units, storage and queue expense, attempts, accepted findings, and operator time. Then compare total lane cost divided by accepted findings over the same workload mix. This catches a common accounting error: counting a malformed extraction as a completed bargain. It also keeps the comparison valid when summarization produces long output but tagging produces very little.

Provider capabilities and commercial terms vary. AWS describes Amazon Bedrock as a service for building generative AI applications and documents its available capabilities on the linked product page, but a production decision still needs the current contract and the precise model and region. I'm not sure a batch lane will reduce a particular team's bill until those terms and its observed acceptance rate are put into the same worksheet. Your mileage may vary.

## Go code for the acceptance gate

The application may be Node.js at the request edge; the worker example is Go because the contract is plain JSON and should not depend on an SDK. The important part is the boundary — validate identity and every enum before publishing anything.

```go
package review

import (
    "context"
    "errors"
    "fmt"
    "time"
)

type Job struct {
    ID            string
    TenantID      string
    Repository    string
    CommitSHA     string
    PolicyVersion string
    Deadline      time.Time
}

type Finding struct {
    RuleID   string
    Severity string
    File     string
    Line     int
    Summary  string
}

type Result struct {
    JobID         string
    CommitSHA     string
    PolicyVersion string
    Findings      []Finding
}

type Store interface {
    PublishOnce(ctx context.Context, idempotencyKey string, result Result) error
    Quarantine(ctx context.Context, job Job, reason string) error
}

func Accept(ctx context.Context, store Store, job Job, result Result) error {
    if time.Now().After(job.Deadline) {
        return store.Quarantine(ctx, job, "completion deadline exceeded")
    }
    if result.JobID != job.ID || result.CommitSHA != job.CommitSHA {
        return store.Quarantine(ctx, job, "result identity mismatch")
    }
    if result.PolicyVersion != job.PolicyVersion {
        return store.Quarantine(ctx, job, "policy version mismatch")
    }

    seen := make(map[string]struct{}, len(result.Findings))
    for _, finding := range result.Findings {
        if err := validateFinding(finding); err != nil {
            return store.Quarantine(ctx, job, err.Error())
        }
        key := fmt.Sprintf("%s:%s:%d", finding.RuleID, finding.File, finding.Line)
        if _, exists := seen[key]; exists {
            return store.Quarantine(ctx, job, "duplicate finding in result")
        }
        seen[key] = struct{}{}
    }

    publishKey := fmt.Sprintf("%s:%s:%s", job.TenantID, job.Repository, job.ID)
    return store.PublishOnce(ctx, publishKey, result)
}

func validateFinding(f Finding) error {
    if f.RuleID == "" || f.File == "" || f.Line < 1 || f.Summary == "" {
        return errors.New("finding has a missing required field")
    }
    switch f.Severity {
    case "low", "medium", "high", "critical":
        return nil
    default:
        return fmt.Errorf("unknown severity %q", f.Severity)
    }
}
```

`PublishOnce` needs a database uniqueness constraint behind it; an in-memory check is not enough when two workers race. Use the tenant, repository, and stable job ID in the key. The exact database is an implementation choice, but the invariant is not: one logical job may produce at most one visible result set. The queue record should also carry an immutable input reference. For code review, that means a commit SHA rather than a branch name. Policy versions matter for the same reason: if a replay uses today's policy against yesterday's commit, the output may be internally valid and still impossible to compare with the original run. Keep raw model output away from the publishing transaction as well. Decode it into a typed result, validate it, then perform one atomic publish. No partial findings. A ten-item result with one invalid item is a rejected result unless the product contract explicitly defines partial acceptance and exposes that state to reviewers; otherwise an operator cannot tell whether nine findings means success, truncation, or a policy breach.

## Rollout starts with accepted-result yield

Start with shadow execution on a fixed, de-identified corpus approved for the environment. Send the same immutable inputs through realtime and batch lanes, but publish neither shadow result. Compare schema acceptance, finding identity, duplicate rate, completion before deadline, and input/output usage. This is a compatibility check, not a claim that the two lanes must produce identical prose. Structured fields and policy outcomes are the hard gate.

Then canary a narrow job class, such as nightly repository summaries, while retaining the realtime path. The dashboard should separate queued, leased, completed, quarantined, expired, and published states. Page on deadline risk and a sustained fall in accepted-result yield; do not page merely because a batch is still inside its allowed window.

Rollback is a scheduler change. Stop admitting new work to the batch lane, allow already-completed results to pass through the same validator, and route eligible new jobs to realtime capacity. Preserve job IDs across the move so replay cannot double-publish. If realtime capacity cannot absorb the backlog, apply admission control and communicate delay rather than creating an unbounded retry wave.

Test the ugly sequence: a worker completes, loses its acknowledgement, and receives the same job again. Also test a result arriving after its deadline, a policy version mismatch, two workers publishing concurrently, and a commit that no longer matches the queued reference. These are deterministic tests. Run them before comparing spend.

## Operational risks set the boundary

The catch is latency and operational surface area. Batch processing is not suitable when a clinician or reviewer needs a finding in the current interaction, when the provider's data-handling terms do not satisfy the approved control set, or when the workload is too small to justify queue, storage, reconciliation, and on-call ownership. Stick with realtime processing for merge-blocking checks and low-volume work whose deadline leaves no batching window.

Async bulk execution also creates correlated failure domains: one bad policy version can affect a large set before anyone inspects the first result. Limit batch size by blast radius, canary policy changes, and retain a kill switch at admission. Big batches are not automatically better.

The decision rule is deliberately plain: choose the deferred lane only when it meets the same structured-output acceptance gate, finishes inside the business deadline, satisfies the approved data controls, and lowers observed cost per accepted finding after queue and operations overhead. Otherwise, keep the realtime lane.

## References

- AWS, Amazon Bedrock: https://aws.amazon.com/bedrock/
- Electronic Code of Federal Regulations, 45 CFR Part 164: https://www.ecfr.gov/current/title-45/subtitle-A/subchapter-C/part-164

## Further reading

The two primary sources above are the starting points for current service capability review and the applicable US health-information rules. Confirm provider terms, model availability, regional controls, and organizational policy during each production review.
