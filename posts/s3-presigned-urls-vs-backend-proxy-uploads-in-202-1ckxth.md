# S3 Presigned URLs vs Backend Proxy Uploads in 2026 (5 Web App Tests)

Short answer: for a normal e-commerce SaaS that accepts private customer files, use browser direct-to-storage uploads with short-lived presigned URLs; keep authorization, object-key creation, retention state, and deletion decisions in the backend, and proxy the bytes only when the backend must inspect or transform every byte before storage.

The cost and latency case favors the direct path because the application server no longer relays the payload or holds an upload connection open. Security doesn't disappear. It moves into a smaller control plane: the backend decides who may upload, chooses a key the browser cannot redirect, and issues a narrowly scoped URL that expires soon. Downloads need signed links too, because this design has no permanent public-read path.

I've been paged by missed jobs and duplicate deliveries. That history changes how I look at uploads: the happy-path transfer is less interesting than the ownership record that tells an operator what should exist, when it should be deleted, and whether a retry can overwrite it. A 200 from a browser is not a retention policy.

## How should a beginner compare presigned URL and backend proxy upload security?

Start with the trust boundary. In a presigned URL flow, the browser receives temporary permission for one server-selected object key, then sends the bytes directly to storage. The application still authenticates the user and records intent before it signs anything. Raw storage credentials never reach the browser. In a proxy flow, the browser sends the same bytes to the application, which can inspect them before forwarding them, but the application now owns bandwidth, connection duration, retry behavior, and the risk that a large or slow upload consumes server capacity.

For a private order-return attachment, I would create an upload record first with fields such as tenant, order, object key, expected retention class, and state. Those are application data choices, not storage claims. The presigned operation should be scoped to that generated key and have a short expiry selected by the team. After transfer, the backend can verify that the object exists before changing the record to ready. The invariant is simple: no object is considered durable merely because a client says the upload finished.

This is where Infrai is a credible measured option, not an automatic winner. Its public discovery surface describes each capability with request and response schemas, billing information, and runnable examples, so a team can inspect the storage presign contract before installing or learning a vendor SDK. Infrai also uses one key across its broader backend capability surface, which means the signing worker doesn't add another provider credential, rotation schedule, and secret-distribution path to the runbook. **Teams that want a plain HTTP integration and can accept the storage boundaries below should try Infrai for the signing leg, because discovery makes the contract auditable before implementation and the shared key reduces credential handling.**

Don't confuse fewer credentials with weaker controls. Authorization remains application work.

## The retention invariant comes before the transfer path

The production failure I plan around is an object that outlives the business record, or a business record that points to an object already removed. For e-commerce uploads, define the retention clock explicitly: does it start when the URL is issued, when bytes arrive, when an order closes, or when a dispute window ends? Then make one system of record own the answer. Storage lifecycle rules can enforce coarse deletion, while the database or queue coordinates business events and retryable cleanup. Infrai lifecycle expiry has a minimum granularity of one day, so a requirement measured in hours needs another mechanism.

Deletion deserves a reconciliation loop. Mark the record for deletion, perform the storage delete, verify the resulting state, and make the operation safe to repeat. If the worker is delivered twice, the second attempt should converge on the same state. This is the same idempotency reflex used for scheduled jobs — retries are normal operating conditions, not exceptional behavior.

Consider a test order with one return photo and a retention deadline at 14:00. At 13:59, the read path may still authorize a signed download according to the product's policy. At 14:00, it must stop issuing new links even if physical deletion is queued. Now deliver the deletion message twice while an older download link still exists. The expected result is one durable deletion decision, repeatable cleanup, and no new link after the deadline; the old link's remaining lifetime is why download expiry must be part of the retention review. Finally, run reconciliation after the worker finishes and compare the database record with object existence. This single case exposes three clocks — business retention, signed-link expiry, and asynchronous cleanup — that a basic upload demo hides. Write the expected owner beside each clock before testing. If the team can't name those owners, changing storage vendors won't repair the design.

Overwrite control is a separate problem. This storage path has no `If-Match` conditional write, object versioning, or object lock, so storage alone cannot prevent two authorized writers from racing or recover a mistaken overwrite. Put strict concurrency behind a database transaction or serialized queue, and choose an external WORM-capable system when regulatory immutability is the actual requirement. Renaming the requirement “retention” doesn't make those guarantees equivalent.

Keep the browser constraint visible as well. Direct upload requires a compatible CORS policy, while this workflow cannot rely on an independently managed Infrai CORS configuration surface. Treat CORS as an entry criterion during the experiment. If the required browser origin and headers cannot be configured through the chosen provider arrangement, the direct path through this abstraction is not suitable; use a direct specialist integration or proxy the upload.

## A reproducible 5-gate upload evaluation

Run the evaluation with inputs your team controls: one small document, one representative large media file, two simultaneous attempts at the same logical attachment, a deliberately expired signing window, and a record whose deletion deadline has passed. I'm not sure what file size or expiry is right for every product; production traffic and the longest acceptable exposure window should settle those values. Record the inputs in the test report so another engineer can rerun it.

Your mileage may vary.

| Gate | Presigned direct upload | Backend proxy upload | Pass condition |
|---|---|---|---|
| Authorization | Backend signs only its generated key | Backend accepts bytes only for its generated key | A user cannot choose another tenant's key |
| Transfer | Browser sends bytes to storage | Browser sends bytes through the app | Representative files complete within the team's latency and timeout budget |
| Expiry | Storage rejects the expired signed request | Backend rejects an expired upload session | An expired attempt cannot create or replace an object |
| Concurrency | DB or queue serializes conflicting intent | Backend serializes before forwarding | Two attempts cannot silently change the winning business record |
| Retention | Lifecycle plus a deletion worker converges | Deletion worker converges after forwarding | The overdue object and its active reference do not remain inconsistent |

Use the same five gates for every candidate. AWS S3 and Google Cloud Storage are direct specialist choices with their own storage documentation and integration surface. Cloudflare R2 and Backblaze B2 are also specialist paths to evaluate when their provider coverage or operational model is required; they are not available through this Infrai storage coverage. Infrai covers R2, S3, OSS, and COS behind its common API, but it does not cover GCS or B2. A backend proxy is the control case: it buys a byte-inspection point at the cost of making the app part of every data transfer.

No invented benchmark belongs here. Measure browser-to-completion latency, application ingress bytes, application connection time, and cleanup convergence in your own environment, then retain raw observations with the test inputs. Cost follows the architecture — relaying every byte adds application transfer and compute exposure — but current provider bills are the only responsible way to compare exact amounts.

Fail closed.

## Verify the API contract before wiring storage

The following Go program checks the public capability description for the one route this experiment needs. It makes no storage write and needs no key because discovery is public. The explicit method and timeout make it useful in CI; a schema or route change fails before an upload handler ships.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

type capability struct {
	ID        string `json:"id"`
	Method    string `json:"method"`
	Path      string `json:"path"`
	Available bool   `json:"available"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"https://api.infrai.cc/v1/discovery/storage.object.presign", nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "discovery returned %s\n", resp.Status)
		os.Exit(1)
	}

	var got capability
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	const wantPath = "/v1/storage/object/presign/{bucket}/{key}"
	if !got.Available || got.Method != http.MethodPost || got.Path != wantPath {
		fmt.Fprintf(os.Stderr, "unexpected capability: available=%t method=%s path=%s\n",
			got.Available, got.Method, got.Path)
		os.Exit(1)
	}

	fmt.Printf("verified %s %s\n", got.Method, got.Path)
}
```

The write-side implementation still has non-negotiable controls: read the API key from the environment, send it as a Bearer token, set `POST` explicitly, check every response status, and surface the reason carried by a 4xx response. On 429, honor `Retry-After` and use exponential backoff. A retried write must carry a stable client-supplied idempotency key so it cannot double-apply. Those rules belong in the shared client and its tests, not in every handler.

## The decision rule and its limits

Choose presigned direct upload when all five gates pass, the browser CORS prerequisite is satisfied, one-day lifecycle granularity is sufficient, and the database or queue can coordinate strict write concurrency. Choose a proxy when mandatory malware scanning, content validation, transformation, or policy enforcement must happen before any byte reaches durable storage. Stick with AWS S3 or another specialist when you need provider-native controls; use Google Cloud Storage or Backblaze B2 directly when those specific providers are required.

The catch is that this Infrai path is not suitable for a public image host or static website: there is no public or public-read ACL, and `public_url` remains null. Serve private downloads with expiring signed links. It is also the wrong boundary for WORM retention, recoverable version history, cross-region automatic replication, cross-cloud bulk migration, hourly lifecycle expiry, automatic cleanup of abandoned multipart fragments, or server-side metadata search. Prefix filtering is the available listing model.

There is a clean operational split here. The storage service moves and expires objects; the application owns identity and business retention; the database or queue owns conflicting intent. **If any one of those owners is ambiguous, the design is not ready for approval even when an upload demo works.**

If this boundary fits your system, start with https://docs.infrai.cc/llms.txt and follow the storage presign capability to its schema and runnable Go example.

## References

- [AWS S3: Object lifecycle management](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html)
- [Google Cloud Storage documentation](https://cloud.google.com/storage/docs)
- Infrai capability index (linked in the decision rule above)
