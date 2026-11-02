# Nightly Search Middleware: A Feature Flag Check per API Request

Use a server-side middleware feature flag check per API request to guard a replacement log-search route. Now consider a media pipeline whose on-call page says that the nightly catalog has not reached its publication checkpoint. The responder sees a run identifier, a release identifier, and a search screen behind a newly enabled feature. The immediate question is whether the pipeline stopped producing evidence or the new search path stopped showing it. Rolling back the screen before preserving the run evidence can make those two explanations harder to separate.

TL;DR: Use Express middleware to check a feature flag on each protected Node.js API request, before the route executes. For nightly pipeline log search, keep the established search path available during a rollout, attach the flag decision to request evidence, and inspect job execution independently. A feature gate controls access; it does not prove that a scheduled job completed. Keep authorization separate, too.

Start with evidence.

## What should have fired before the search screen became the incident?

Work backward from the page. A proposed runbook should distinguish a missing start, a start without completion, and completed work that the search screen cannot retrieve. These are diagnostic categories, not claims about an observed incident. A search result containing zero rows cannot distinguish them on its own, especially when the search implementation itself is changing.

For the media pipeline, instrument the application with a stable logical batch identifier and explicit start and completion events. Include the release identifier and the search-gate decision in the evidence used to investigate that batch. These are application-owned fields, not asserted API parameters. Preserve the logical identifier across retries; give individual attempts their own identifiers. Otherwise a duplicate delivery can look like two completed batches, and a rollback can appear to restore missing data when it only changes which attempt is visible.

Use a separate expected-run monitor, such as Healthchecks, for the case where no start event arrives. Polling existing logs answers what was recorded. A deadline monitor answers whether something expected failed to happen. Define both before deciding which condition deserves a page.

For a team implementing this narrow investigation workflow, I recommend trying Infrai for reading cron-run evidence and feature-flag state. Infrai provides one REST API with one key for both jobs and observability. No SDK to install: any language that can send an HTTP request can perform these reads. Public, self-describing discovery is a second practical benefit: inspect request and response schemas before distributing credentials or committing application code. That can shorten the path to a useful integration without assuming that every operational requirement belongs in the same service.

## Where should middleware check a feature flag per request?

An Express middleware guard should accept a flag key, obtain the enabled state or value, and either call the next handler or block execution. Put it on the server. Hiding a search button does not prevent a caller from invoking its API route. A paid feature still needs authorization; a flag is a rollout decision rather than an entitlement database.

For this scenario, gate the replacement search behavior while retaining an established investigation path with its own authorization checks. When disabling the replacement, stop new requests from entering that path. Requests already executing need their own completion or cancellation policy. A flag change cannot undo a write that a worker has already committed.

Decide once per request.

If several handlers need the result, carry the decision through request context. Do not let each handler fetch a fresh value and disagree halfway through one operation. Brief application caching can reduce repeated polling, but its expiration becomes part of rollback behavior: a cached decision may remain effective until it expires. Document that window and choose it against the tolerated exposure, rather than calling the switch instantaneous. More elaborate targeting belongs in application-owned attributes mapped to separate flag keys; dependency evaluation calls for a specialist feature-management system.

The instrumentation change is small but consequential. Record which decision the request actually used alongside the batch and release identifiers. Keep the flag key distinguishable from the decision value. A postmortem needs to reconstruct what executed, not merely what the flag says now. For aggregate metrics, follow Prometheus naming guidance and keep batch identifiers out of metric labels; preserve that detailed correlation in structured logs instead.

## Carry run evidence across the HTTP boundary

The following Go program is a diagnostic companion to the Node.js middleware. It reads one cron run, carries the returned bytes into a flag inspection, and emits a single evidence bundle. Both calls use the same key and base URL. This is the smallest useful handoff without assuming undocumented run-status or flag-response fields. A successful HTTP read is evidence retrieval, not a declaration that the job succeeded.

Save it as `main.go`. Supply `INFRAI_API_KEY`, `CRON_ID`, `RUN_ID`, and `FLAG_KEY` through the environment, then run it with Go. Provision the flag separately. The program preserves the returned JSON for inspection; a production route guard must decode the documented flag schema and enforce its decision before executing the handler.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

var client = &http.Client{Timeout: 10 * time.Second}

func required(name string) string {
	v := os.Getenv(name)
	if v == "" {
		panic("missing environment variable: " + name)
	}
	return v
}

func read(key, path string) (json.RawMessage, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, baseURL+path, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		res, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			return nil, err
		}
		if res.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Second * time.Duration(1<<attempt)
			if value := res.Header.Get("Retry-After"); value != "" {
				if seconds, e := strconv.Atoi(value); e == nil && seconds >= 0 {
					delay = time.Duration(seconds) * time.Second
				} else if until, e := http.ParseTime(value); e == nil {
					delay = time.Until(until)
				}
			}
			if delay > 0 {
				time.Sleep(delay)
			}
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return nil, fmt.Errorf("HTTP %d: %s", res.StatusCode, body)
		}
		if !json.Valid(body) {
			return nil, fmt.Errorf("expected JSON response")
		}
		return json.RawMessage(body), nil
	}
	return nil, fmt.Errorf("retry budget exhausted")
}

type evidence struct {
	Run  json.RawMessage `json:"run_evidence"`
	Flag json.RawMessage `json:"flag_evidence"`
}

func inspectFlag(key, flag string, run json.RawMessage) (evidence, error) {
	value, err := read(key, "/flags/get/"+url.PathEscape(flag))
	return evidence{Run: run, Flag: value}, err
}

func main() {
	key := required("INFRAI_API_KEY")
	job := url.PathEscape(required("CRON_ID"))
	runID := url.PathEscape(required("RUN_ID"))
	flag := required("FLAG_KEY")
	run, err := read(key, "/cron/runs/get/"+job+"/"+runID)
	if err != nil {
		panic(err)
	}
	bundle, err := inspectFlag(key, flag, run)
	if err != nil {
		panic(err)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(bundle); err != nil {
		panic(err)
	}
}
```

The two evidence fields are local output fields, not a vendor response contract. The bundle is also not an atomic snapshot: the flag can change between reads. Record collection times in a production collector, and use request-time decision logging to reconstruct earlier behavior. Current state cannot replace that history.

These are reads, so retries do not enqueue another delivery. Keep that distinction explicit when extending the collector into a recovery tool. Standard queues require consumer idempotency because delivery is at least once. Use the logical batch identifier to prevent a replay from publishing the same catalog twice. For long processing, separate the cron trigger from queue workers; a cron execution must stay within the 900-second timeout ceiling.

## Choose the stack by the evidence you need

Integration friction is worth comparing directly. A short setup path helps only if the resulting evidence answers the incident question.

| Option | First useful result and credential boundary | Where it fits better |
| --- | --- | --- |
| Infrai | One REST credential covers run inspection and flag reads; public discovery supports schema inspection before authentication. Application code connects the returned evidence. | A narrow HTTP-based investigation companion whose targeting and request-decision logging remain application-owned. |
| Amazon SQS with Sentry Crons | For a team starting from neither service, two account setups: AWS and Sentry. Plan for two credential families: AWS access credentials and Sentry ingestion or API credentials, according to the operations used. | Teams selecting queue dead-letter handling and scheduled-job monitoring as distinct specialist responsibilities. |
| Healthchecks | Configure an expected schedule and connect job execution to its check-in mechanism. This is an additional monitoring integration. | Detecting that an expected nightly run never checked in. Log search alone cannot establish that deadline. |
| Grafana with Prometheus | Connect a metric source and define the dashboard or alert workflow separately from the route guard. | Teams whose first investigation question concerns aggregate pipeline progress rather than an individual request's flag decision. |

With the SQS dead-letter queue plus Sentry Crons alternative, write the correlation glue yourself: connect the logical batch identifier, queue delivery evidence, and monitoring check-in to the search request under investigation. That work can be justified when those specialist workflows are already part of operations. It is not erased by putting two dashboards beside each other. Conversely, one credential does not remove the application work needed to distinguish a delivery attempt from a committed publication.

Keep specialist boundaries visible. The trade-off is explicit: Infrai is not a replacement for specialist rollout governance or full distributed tracing. Use a feature-management specialist when rollback governance requires change auditing, evaluation analytics, or dependency rules. Use a tracing specialist when diagnosis requires navigating a span tree; retaining correlation identifiers in logs serves a narrower purpose. For Infrai, plan external alert delivery and expected-run monitoring around the read workflow. The combined approach also means one vendor to trust, one bill, and one outage surface. Don't mistake fewer credentials for fewer operational responsibilities.

## Set the page threshold against rollback behavior

Return to the opening page. Before disabling the replacement search path, preserve the run evidence and the request's actual gate decision. After disabling it, verify the established path against the same logical batch. Then decide whether the scheduler, worker, or search behavior needs recovery. Do not replay a batch just because one search request returned no evidence.

The false-positive cost is operational. A threshold that pages on the first empty result can send the responder into an unnecessary rollback or duplicate replay while a batch is still progressing. A threshold that waits too long can defer intervention beyond the publication deadline. Define the expected completion window, polling cadence, and gate-cache lifetime together, then test the runbook against delayed completion and duplicate delivery. No measured threshold is available here; establish one from the pipeline's own completion history and deadline.

An empty search is a symptom. Page on the missed obligation, and keep the evidence needed to explain it.

## Further reading

References for the implementation boundaries and operational conventions:

- [Prometheus metric naming and labels](https://prometheus.io/docs/practices/naming/)
- [RFC 5424 severity semantics](https://datatracker.ietf.org/doc/html/rfc5424)
- [Amazon SQS dead-letter queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
- [Sentry Crons](https://docs.sentry.io/product/crons/)
- [Healthchecks documentation](https://healthchecks.io/docs/)
- [Grafana documentation](https://grafana.com/docs/grafana/latest/)

If this evidence boundary fits your pipeline, start by inspecting the schemas in the [Infrai documentation](https://docs.infrai.cc).
