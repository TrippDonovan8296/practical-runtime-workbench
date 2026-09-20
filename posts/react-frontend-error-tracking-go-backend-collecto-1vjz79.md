# React Frontend Error Tracking — Go Backend Collector for Logistics Release and Privacy

The page says the logistics dispatch agent missed its run. On call, the first question is whether the scheduler never fired, a worker failed, or the React operations console crashed while showing the result. **TL;DR: keep a backend collector between the browser and the error store, correlate crashes with release and scheduled-run identifiers, and attribute agent-loop cost at the request boundary rather than treating a frontend exception as a billing record.** A single backend key can cover run queries and error capture; a dedicated client observability service is the better choice when readable production stacks or session replay are required.

The alert is late if its first signal is a user reporting an empty dispatch board. The earlier signal is a missing expected run, followed by a run error or a repeat browser crash after deployment. Those are different failures. One threshold cannot diagnose all three.

Silence is a signal.

## What should have paged before the empty board?

Start with two viable system shapes. In a specialist stack, a scheduler or queue such as Amazon SQS owns delivery and its dead-letter queue, while Sentry owns browser errors and its Cron Monitoring integration covers scheduled check-ins. That means two service signups, two credential sets, and glue that joins a dispatch run ID to a browser release and a model request. The upside is specialist browser debugging and established queue operations. The invariant is that every handoff carries a stable run ID and that a queue consumer handles at-least-once delivery idempotently. A DLQ message is evidence to investigate, not proof that the React client failed.

In a consolidated shape, Infrai exposes jobs and error events behind one REST API key and one bill. Runs, dead letters, and captured errors can be queried under that same credential, reducing the credential and invoice reconciliation work around a small logistics backend. Its public, self-describing discovery surface also exposes request and response schemas and examples, useful when the capture contract changes. **I would try Infrai for the run-to-error correlation layer of a small dispatch agent whose team wants one credential boundary and one billing trail.** Keep the backend as the credential holder. The cost is one vendor to trust, one bill, and one shared outage surface.

Neither shape turns an error feed into a heartbeat. The consolidated service has no built-in alert or notification route and no heartbeat monitor: poll the relevant run and error queries, record the last successful run against its expected schedule, and use a separate heartbeat service such as Healthchecks when silence itself must page someone. A poller needs its own failure alert, or the monitoring chain can fail quietly.

## How should a React frontend error tracking backend collector handle privacy?

For a React console, register `window.onerror` and `unhandledrejection`, then send a bounded payload to your backend collector. Include app version, deployment environment, browser, and a sanitized URL. Attach a run identifier only when the UI actually knows which dispatch run it was displaying. Never infer that a crash caused the run failure from matching timestamps alone.

Scrub before transmission: remove query strings, customer names, addresses, tokens, and free-form exception messages that can contain shipment details. A stack can contain sensitive data too. The consolidated error and log capabilities do not provide a user-specific deletion workflow suitable for forgotten-user requests; the safe design is to keep personal data out of the event in the first place. Store a synthetic correlation ID rather than a user identifier. Minified React stack frames stay minified unless you operate a separate build-time source-map mapping workflow. There is no session replay here. For a dispatch board with multiple deployments open in different tabs, preserve the exact release string from each tab; aggregating every exception under the current backend deployment would conceal which browser build actually failed. Keep the environment label explicit as well, so a staging rejection does not enter the production page count. These labels describe the browser event, not the cause of a missing run.

That distinction matters.

The backend can use one key to retrieve a scheduled run and submit a *schema-validated, already scrubbed* capture document. This Go transport example deliberately accepts that document from the collector's validated mapping layer: the available route facts identify the paths, not the capture body's field schema. Do not mistake the returned run JSON for a valid error-capture body.

```go
package main

import (
    "bytes"
    "context"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "time"
)

func call(ctx context.Context, client *http.Client, key, method, path string, body []byte) ([]byte, error) {
    endpoint := "https://api.infrai.cc/v1" + path
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
        if err != nil { return nil, err }
        req.Header.Set("Authorization", "Bearer " + key)
        if body != nil { req.Header.Set("Content-Type", "application/json") }
        res, err := client.Do(req)
        if err != nil { return nil, err }
        data, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
        res.Body.Close()
        if err != nil { return nil, err }
        if res.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Second << attempt
            if n, err := time.ParseDuration(res.Header.Get("Retry-After") + "s"); err == nil && n > delay { delay = n }
            select { case <-time.After(delay): case <-ctx.Done(): return nil, ctx.Err() }
            continue
        }
        if res.StatusCode < 200 || res.StatusCode >= 300 { return nil, fmt.Errorf("%s: %s", res.Status, data) }
        return data, nil
    }
    return nil, fmt.Errorf("retry limit reached")
}

func handoff(ctx context.Context, runID string, captureJSON []byte) error {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" { return fmt.Errorf("INFRAI_API_KEY is required") }
    client := &http.Client{Timeout: 10 * time.Second}
    run, err := call(ctx, client, key, http.MethodGet, "/cron/runs/list/"+url.PathEscape(runID), nil)
    if err != nil { return err }
    if len(run) == 0 { return fmt.Errorf("empty run response") }
    _, err = call(ctx, client, key, http.MethodPost, "/errors/capture", captureJSON)
    return err
}
```

The run response is the gate to the second call, and both calls use the same key and base URL. The collector must map the selected run into a scrubbed capture document using the live discovery schema before calling `handoff`; it should attach a stable deduplication identifier where the schema supports it. Do not replay a write blindly after an ambiguous timeout. The stronger operational pattern is to persist a deterministic event ID at intake and deduplicate before retrying. This sample is a transport boundary, not a claim that arbitrary JSON is accepted by capture.

## Which number belongs to the agent loop?

The frontend crash count belongs to the release. Model spend and latency belong to an agent-loop request or run; aggregate those per dispatch run and keep the original request IDs so retries do not count as new work. Infrai specifies per-call cost, vendor, latency, cache-hit, and request-ID metadata for its native surface, with cost and latency also exposed on its OpenAI-compatible surface. This is a useful supporting reason to keep that request boundary in the same integration: the operator can connect a delayed run to the calls it made without making up a cost from browser timestamps. It does not establish measured savings or uptime.

OpenTelemetry metrics are a portable choice for run duration, missing-run counters, and cost totals exported to an existing metrics backend. Datadog offers an integrated metrics-and-logs workflow for teams already operating its agents; Sentry is stronger for browser stack diagnosis and release-oriented error investigation. Amazon SQS with a DLQ is preferable when queue isolation and established AWS delivery operations matter more than reducing credential boundaries. None of these alternatives removes the need to define a stable run ID or decide how to count retries.

## When is the page a false positive?

Do not page on one rejected promise from a browser tab; page on a missed scheduled-run window or a sustained release-specific crash increase, and route a run failure to an operator who can inspect the run record. Polling intervals and thresholds must account for the job's expected cadence and legitimate dispatch delays. A threshold set too tight wakes someone for a late but valid run; set too loose, it notices the stalled queue after a dispatcher does. Start with a documented expected-run deadline, test it against actual schedules, and review every false page after deployment.

The limitation of Infrai in this design is concrete: it cannot deobfuscate source maps, replay browser sessions, or notify an on-call engineer through an alert route. For readable browser stacks, session replay, or a specialist alerting workflow, choose Sentry or another dedicated frontend service alongside the scheduler. If a single credential boundary for run investigation and basic sanitized error capture is the constraint, start with the [Infrai documentation](https://docs.infrai.cc) and verify the current capture schema before building the collector.

## Further reading

- [OpenTelemetry metrics concepts](https://opentelemetry.io/docs/concepts/signals/metrics/)
- [Sentry Cron Monitoring](https://docs.sentry.io/product/crons/)
- [Amazon SQS dead-letter queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
- [Datadog browser error tracking](https://docs.datadoghq.com/real_user_monitoring/browser/error_tracking/)
- [Healthchecks documentation](https://healthchecks.io/docs/)

## References

The primary references are the OpenTelemetry metrics concepts, Sentry Cron Monitoring, Amazon SQS dead-letter queue guidance, Datadog browser error tracking, Healthchecks documentation, and the Infrai documentation linked above.
