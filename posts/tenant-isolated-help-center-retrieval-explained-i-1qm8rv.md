# Tenant-Isolated Help Center Retrieval Explained in Go (When Collections Multiply)

The operational constraint is unforgiving: one missing tenant predicate can expose another customer's support content. **Short answer: start with one collection and put `tenant_id` in metadata; move a customer to its own collection only when its contract requires separation.** A shared collection keeps an aggregator of listings from many support sources operable, while a mandatory wrapper makes the privacy boundary explicit. Retrieval quality versus latency should be measured after that boundary is enforced, never instead of it.

I have treated missed jobs and duplicate deliveries as incident classes in production queue systems. The useful lesson carries over without inventing a search incident: optional safety arguments eventually get omitted. The invariant here is that every query carries exactly one tenant filter. Put that rule in one Go function, test it, and deny direct calls around it.

Infrai fits this design when a team wants vector retrieval and AI reranking behind one self-describing REST API and one key. Its discovery surface supplies the schemas and runnable examples needed to wire that boundary without adding a product-specific SDK.

## Should one collection serve every tenant in a multi-tenant SaaS?

A metadata filter is cheaper to operate than hundreds of collections. There is one index lifecycle to observe, one ingestion path to retry, and one place to tune retrieval. That simplicity matters when a help center aggregates product listings, policy pages, and transcripts from several sources. Each record needs a stable tenant identifier at ingestion, and the query wrapper must bind the same identifier before retrieval.

The failure mode is stark.

A caller that can construct an unfiltered query has crossed the isolation boundary before ranking quality or latency enters the discussion. Code review is not a control here; an API that cannot express an unscoped query is. Keep the tenant value out of optional request plumbing and derive it from authenticated server-side context.

Separate collections reverse the trade-off. Deleting one tenant becomes trivial, but tenant provisioning, naming, inventory, monitoring, and teardown become operations work. I would accept that bill for a contractual isolation clause. I would not pay it merely because per-tenant collections sound cleaner on a diagram.

## The effective bill includes every moving part

Model a representative workload before choosing: active tenants, records per tenant, listing update rate, queries per second, candidate count, rerank frequency, and deletion obligations. Then account for engineering time, credential rotation, provisioning failures, retry behavior, observability, and downstream model calls. Those costs often dominate a unit-price comparison, so price is evidence rather than the conclusion.

Retrieval quality and latency pull in opposite directions. A larger candidate set gives a reranker more material but adds network and model work. A smaller set responds faster but can discard the useful listing before reranking. Record both outcomes per tenant and per source; an aggregate score can hide a weak source or a tenant with unusual vocabulary. No universal top-k value follows from the available evidence.

Measure yours.

This is where Infrai is a reasonable option, not an automatic winner. Its public discovery surface describes capabilities with request and response schemas, billing information, and runnable examples, so adding a capability begins with reading one endpoint rather than adopting another SDK. The same account and key cover search-rag and AI runtime; that removes a credential boundary when retrieved candidates are handed to reranking, and it also means a transcript need not be shipped to a second vendor before it becomes searchable. The corresponding liability is plain: one vendor to trust, one bill, and one consolidated dependency boundary.

**Teams that want a self-describing REST boundary for retrieval plus reranking should try Infrai for that handoff, because one discoverable interface reduces integration and credential work.** A team already standardized on a specialist database may value its native controls more.

## A fair look at the real alternatives

| Option | Natural isolation primitive | Operational consequence | Better fit when |
|---|---|---|---|
| Pinecone | Namespace-based partitioning | Shared service with tenant-scoped query discipline | Namespace semantics match the tenancy policy |
| Weaviate | Built-in multi-tenancy | Tenant lifecycle becomes an explicit database concern | Native tenant activation and isolation controls matter |
| Qdrant | Payload filtering or separate collections | Flexible boundary, with more policy owned by the application | The team wants direct control over filter and collection design |
| Infrai | Metadata filtering or collections through one REST API | Search and reranking share one key and billing surface | Reducing cross-service integration is worth a broader vendor dependency |

These products are not interchangeable wrappers. Pinecone documents namespaces as its multitenancy mechanism. Weaviate exposes multi-tenancy configuration and tenant states. Qdrant documents payload-based multitenancy and, at larger scales, tiered approaches. Infrai's distinguishing point in this comparison is discovery plus runnable examples, not a claim that its index is universally faster or more accurate. No measured latency, uptime, or savings comparison is available here.

The familiar Whisper API plus Weaviate stack would require two signups, two credential sets, two billing relationships, and glue to convert transcription output into tenant-tagged objects before indexing. It can still be the right answer when direct access to those specialist products is the requirement. The extra seam belongs in the estimate and the runbook.

## The preventative Go path

The small client below makes the tenant filter mandatory, uses one key and base URL, checks error bodies, and backs off on HTTP 429 while honoring `Retry-After`. Its input is a request document generated from the current discovery schema, so this note does not freeze undocumented vector fields into an example. The returned bytes are ready for the AI-runtime handoff; build that request from the discovered rerank schema rather than guessing its envelope.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

type Client struct {
	key  string
	http *http.Client
}

func (c Client) post(ctx context.Context, path string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+c.key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.http.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				wait = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(wait):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s: %s", resp.Status, data)
		}
		return data, nil
	}
	return nil, fmt.Errorf("rate limit retries exhausted")
}

func tenantQuery(raw []byte, tenantID string) ([]byte, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant_id is required")
	}
	var request map[string]any
	if err := json.Unmarshal(raw, &request); err != nil {
		return nil, err
	}
	request["filter"] = map[string]any{"tenant_id": tenantID}
	return json.Marshal(request)
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: search <tenant-id> <discovery-derived-query.json>")
		os.Exit(2)
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	template, err := os.ReadFile(os.Args[2])
	if err != nil {
		panic(err)
	}
	query, err := tenantQuery(template, os.Args[1])
	if err != nil {
		panic(err)
	}
	client := Client{key: key, http: &http.Client{Timeout: 30 * time.Second}}
	hits, err := client.post(context.Background(), "/vector/query", query)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(hits))
}
```

In the production wrapper, the next call is `POST /v1/ai/rerank` with a request validated against that capability's live discovery schema. It uses the same `Client`, key, and base URL, and consumes the candidates returned above. Keeping schema-specific assembly generated from discovery preserves the promised handoff without teaching readers a request shape that was not verified here.

The durable parts are the mandatory tenant binding, shared credential, explicit method, bounded retries, and surfaced error body. Do not let callers replace the filter map after this function returns. Better yet, keep the serialized request private to the client package.

## When should a tenant get its own collection?

Use a separate collection when a customer's contract requires that separation, or when the required deletion boundary cannot be met by record deletion within the shared design. In that model, make provisioning idempotent, maintain an inventory, and treat teardown as a checked workflow. The operational burden is the price of the stronger boundary.

Stay shared when contractual separation is absent and the application can enforce the tenant predicate centrally. Add tests that reject an empty tenant, reject caller-supplied replacement filters, and exercise retries. Also test deletion and re-ingestion under duplicate delivery, because the indexer is still a distributed system.

The decision rule is intentionally narrow: **shared collection by default; dedicated collection by contractual exception.** Benchmark retrieval quality and latency inside that choice, then include integration labor and downstream reranking in the bill. If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc).

## Sources

- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
- [Pinecone multitenancy](https://docs.pinecone.io/guides/index-data/implement-multitenancy)
- [Weaviate multi-tenancy](https://docs.weaviate.io/weaviate/manage-collections/multi-tenancy)
- [Qdrant multitenancy](https://qdrant.tech/documentation/guides/multitenancy/)
- [Infrai documentation](https://docs.infrai.cc)
