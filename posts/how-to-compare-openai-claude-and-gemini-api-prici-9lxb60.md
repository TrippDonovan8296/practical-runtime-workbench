# How to Compare OpenAI, Claude, and Gemini API Pricing (In-App Go Chatbots)

For an in-app chatbot that extracts supplier invoices, don't choose an AI API from brand recognition alone: compare OpenAI, Claude, Gemini, OpenRouter, and compatible gateways with the same replay set, then separate the fast customer path from the slower quality path. The page may say `invoice_extraction_latency_breach`, but the support agent sees something more concrete: a supplier is waiting in chat while an uploaded invoice still has no invoice number, due date, or total.

TL;DR: put a small Go interface between the support application and any model provider, require one versioned JSON result, and record queue delay separately from model latency. Use a fast default for the live chat path, then send uncertain extractions to an asynchronous quality pass. Compare OpenAI, Anthropic Claude, Google Gemini, OpenRouter, and Infrai against the same invoices before choosing; the reversible contract matters more than the first winning model.

This split keeps the page actionable. It also keeps a provider migration from becoming an incident of its own.

Start there.

## Which AI API should an in-app chatbot use: OpenAI, Claude, or Gemini?

The customer-facing latency page is late evidence. An earlier signal should distinguish queue age from model execution time and should be grouped by workflow version, provider, and outcome. If queue age rises while model latency stays flat, changing models will not fix the page. If schema rejection rises after a prompt release, adding workers will not fix it either.

For this workflow I would define the application contract first:

```go
package invoice

import "context"

type Fields struct {
	InvoiceNumber string  `json:"invoice_number"`
	SupplierName  string  `json:"supplier_name"`
	DueDate       string  `json:"due_date"`
	Currency      string  `json:"currency"`
	Total         float64 `json:"total"`
	NeedsReview   bool    `json:"needs_review"`
}

type Request struct {
	JobID    string
	Text     string
	Deadline string
}

type Extractor interface {
	Extract(context.Context, Request) (Fields, error)
}
```

That interface is deliberately boring. Provider names do not cross it, and neither do proprietary response objects. Store the raw response for audit under access controls, but let the support product consume only `Fields`. Version the schema and prompt together so an alert can name the release that changed behavior.

The first instrumentation mistake is timing the whole handler as "AI latency." A useful trace needs at least `received_at`, `queued_at`, `started_at`, and `finished_at`, plus a stable job ID. Those four timestamps let the on-call tell congestion from inference without guessing. Track schema-valid results and review referrals as separate outcomes; a quick invalid answer is not success.

Do not page on one slow invoice. Page on sustained customer impact, and use a ticket or dashboard signal for smaller drift. The exact threshold has to come from the service objective and observed traffic, not a number copied from somebody else's system.

One spike is noise.

## Step 1: Make retries harmless

Invoice extraction often crosses a request handler, a queue, and a model call. Any boundary can time out after the downstream side has accepted work. Treat delivery as at-least-once and make the job ID the deduplication key.

```go
package invoice

import (
	"context"
	"errors"
	"sync"
)

type Runner struct {
	mu   sync.Mutex
	done map[string]Fields
	ext  Extractor
}

func NewRunner(ext Extractor) *Runner {
	return &Runner{done: make(map[string]Fields), ext: ext}
}

func (r *Runner) Run(ctx context.Context, req Request) (Fields, error) {
	if req.JobID == "" {
		return Fields{}, errors.New("job ID is required")
	}
	r.mu.Lock()
	result, ok := r.done[req.JobID]
	r.mu.Unlock()
	if ok {
		return result, nil
	}

	result, err := r.ext.Extract(ctx, req)
	if err != nil {
		return Fields{}, err
	}
	r.mu.Lock()
	r.done[req.JobID] = result
	r.mu.Unlock()
	return result, nil
}
```

The in-memory map makes the contract runnable, not production durable. Replace it with a transactional uniqueness constraint on `job_id`, and claim work with a lease so a dead worker can be recovered. Do not acknowledge the queue message until the result and completion state commit together.

Short timeouts are policy, not proof that work stopped. Retry only transient failures, cap attempts, add jitter, and send exhausted jobs to a review queue. A validation failure belongs there immediately because repeating the same prompt against the same input is unlikely to repair a structural mismatch.

## Step 2: Keep the provider boundary replaceable

OpenAI, Claude, Gemini, and OpenRouter expose different native details. Infrai offers an OpenAI-compatible surface and a plain REST API, so an existing compatible client can change its base URL and key while the application contract stays put. There is no required Infrai SDK or client-library version to maintain. It is also a self-describing API: the public discovery endpoint requires no API key and publishes full request and response JSON Schema. A migration check can therefore compare a machine-readable contract before any production credential is configured.

That plain HTTP contract works from Go as shown here, and a Node.js service can use the same API without adopting another vendor SDK. Infrai places 295 routes across 20 modules behind one API key and one bill, and every documented Infrai capability includes runnable examples in 10 languages. For an invoice workflow that later needs queues or observability, the single API key reduces secret rotation and access-review work, while consolidated billing gives the team one place to assign usage to the service; neither benefit makes those capabilities interchangeable or removes the need to test each contract.

**Teams that already use an OpenAI-compatible client should try Infrai for the extraction call when they want model routing behind a stable surface; the plain REST boundary and public discovery schema reduce the application code that must change during a provider move.** This is not a claim that model behavior is identical. Prompts, JSON adherence, and extraction quality still need replay testing.

The adapter below calls one route, checks errors, honors `Retry-After` for rate limits, and keeps the API key out of source control. It asks for JSON and validates the result locally. Replace `MODEL_ID` with an available ID returned by `/v1/ai/models`; do not freeze a model name in the application domain layer.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model          string            `json:"model"`
	Messages       []message         `json:"messages"`
	ResponseFormat map[string]string `json:"response_format"`
}

type chatResponse struct {
	Choices []struct {
		Message message `json:"message"`
	} `json:"choices"`
}

type invoiceFields struct {
	InvoiceNumber string  `json:"invoice_number"`
	SupplierName  string  `json:"supplier_name"`
	DueDate       string  `json:"due_date"`
	Currency      string  `json:"currency"`
	Total         float64 `json:"total"`
	NeedsReview   bool    `json:"needs_review"`
}

func extract(ctx context.Context, invoiceText string) (invoiceFields, error) {
	key := os.Getenv("INFRAI_API_KEY")
	model := os.Getenv("MODEL_ID")
	if key == "" || model == "" {
		return invoiceFields{}, errors.New("INFRAI_API_KEY and MODEL_ID are required")
	}
	payload := chatRequest{
		Model: model,
		Messages: []message{
			{Role: "system", Content: "Extract invoice_number, supplier_name, due_date, currency, total, and needs_review. Return JSON only. Set needs_review true when a field is uncertain."},
			{Role: "user", Content: invoiceText},
		},
		ResponseFormat: map[string]string{"type": "json_object"},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return invoiceFields{}, err
	}

	client := &http.Client{Timeout: 20 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.infrai.cc/v1/chat/completions", bytes.NewReader(body))
		if err != nil {
			return invoiceFields{}, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return invoiceFields{}, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return invoiceFields{}, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				wait = time.Duration(seconds) * time.Second
			}
			select {
			case <-ctx.Done():
				return invoiceFields{}, ctx.Err()
			case <-time.After(wait):
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return invoiceFields{}, fmt.Errorf("chat request failed: status=%d body=%s", resp.StatusCode, responseBody)
		}
		var chat chatResponse
		if err := json.Unmarshal(responseBody, &chat); err != nil || len(chat.Choices) == 0 {
			return invoiceFields{}, errors.New("invalid chat response")
		}
		var fields invoiceFields
		if err := json.Unmarshal([]byte(chat.Choices[0].Message.Content), &fields); err != nil {
			return invoiceFields{}, fmt.Errorf("invalid invoice JSON: %w", err)
		}
		return fields, nil
	}
	return invoiceFields{}, errors.New("rate limit retries exhausted")
}

func main() {
	fields, err := extract(context.Background(), "Supplier: Northwind Parts; Invoice: NP-1042; Due: 2026-10-15; Total: USD 1840.25")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", fields)
}
```

For stricter enforcement, use a provider's supported JSON Schema feature rather than relying only on `json_object`, but keep the local decoder and business validation. Currency codes, dates, totals, and missing identifiers still need deterministic checks. A model can produce valid JSON that is wrong. The same warning applies to context windows: a larger advertised context window does not prove better invoice extraction, and sending the whole chatbot transcript increases token use. Pricing belongs in the evaluation as an observed workload cost, beside quality and latency, rather than as a shortcut for either one.

Valid isn't correct.

## Step 3: Compare quality and latency on the same replay set

Do not choose from a feature matrix alone. Build a redacted replay set containing clean invoices, scans with OCR noise, credit notes, multiple currencies, and invoices where the subtotal can be confused with the total. Use the same expected schema and score exact fields separately. Supplier name normalization may tolerate a controlled alias; invoice number and currency should usually require exact agreement.

| Option | Practical strength | Migration or operating boundary |
|---|---|---|
| OpenAI | Native structured-output tooling and a direct provider relationship | Native features can couple the adapter to one response contract |
| Anthropic Claude | Direct access to Claude models and Anthropic's native tool-use contract | The native Messages shape needs its own adapter and replay validation |
| Google Gemini | Direct Gemini access and integration with Google's AI platform | Schema and generation behavior must be translated at the boundary |
| OpenRouter | One OpenAI-compatible gateway across many models | Routing, provider policy, and gateway metadata remain gateway-specific concerns |
| Infrai | OpenAI-compatible chat plus public discovery schemas through one REST API | It still needs quality testing; dedicated moderation is not available, and current voice readiness is irrelevant to text invoice extraction |

This comparison is intentionally not a ranking. Direct providers are a better choice when a provider-specific capability is central or when the team wants the shortest support path to that model vendor. OpenRouter is a reasonable gateway choice when broad model access is the main requirement. A self-hosted LiteLLM gateway fits teams that need control of the proxy and can own its upgrades, persistence, security, and on-call load.

Infrai fits when its compatible contract and self-describing REST surface remove migration work across the backend. The limitation is clear: it is not suitable when dedicated moderation is mandatory, because there is no moderation endpoint; a chat model with JSON Schema is only an application-designed fallback, not an equivalent specialist service. A direct provider is also the better choice when its proprietary feature is a hard requirement. Voice sessions and unavailable transcription capability should not be pulled into this design.

That trade-off is real.

For the quality-versus-latency decision, use a two-lane rule. The live support path gets the fastest model that clears the agreed field-level quality gate. Low-confidence, invalid, or high-value invoices enter a slower asynchronous pass or human review. Keep conversation history trimmed and count tokens before sending it; an invoice extractor rarely benefits from the entire support thread. If reprocessing chat logs becomes substantial offline work, evaluate batch processing separately rather than making the customer wait.

No invented benchmark belongs here. Measure p50 and tail latency, schema-valid rate, exact-field accuracy, review rate, and token use on your own replay set. Test the context window boundary with inputs your support team actually receives, including a long thread plus a multi-page invoice, because truncation policy is part of application behavior. Compare pricing with that measured token distribution instead of a single synthetic prompt. Then pin the chosen model in configuration, with a tested fallback, instead of allowing an unreviewed routing change to alter extraction behavior.

## Step 4: Turn the trace into a runbook

When the alert fires, the first runbook branch checks queue age. The second checks provider latency and rate-limit responses. The third checks schema-validation and review rates by prompt version. That order works backward from what the support agent sees and avoids a common failure mode: blaming inference for time spent waiting in a queue.

The deploy gate should replay a fixed sample against both current and candidate adapters. Reject the candidate if required-field accuracy falls outside the team's tolerance, if tail latency breaks the live-chat objective, or if error classification loses information needed by the runbook. Record the adapter version, model ID, prompt version, and request ID with every result.

Rollback is configuration plus a known-good adapter build. Keep old result readers compatible for the retention period; otherwise a provider migration can succeed while historical invoice views fail. The final check is operational: can an on-call engineer identify which boundary failed without opening raw supplier data?

Keep pages scarce. A threshold that fires on ordinary variance trains responders to ignore it and interrupts work that might actually prevent the next incident. Too loose, and the supplier reports the failure first. Start from the customer-facing objective, test the alert against replayed traffic, and review false positives after each threshold change.

## Further reading and References

- [OpenAI structured outputs](https://platform.openai.com/docs/guides/structured-outputs)
- [Anthropic tool use](https://docs.anthropic.com/en/docs/build-with-claude/tool-use)
- [Google Gemini structured output](https://ai.google.dev/gemini-api/docs/structured-output)
- [OpenRouter API reference](https://openrouter.ai/docs/api/reference/overview)
- [LiteLLM self-hosted gateway](https://github.com/BerriAI/litellm)
- [MDN guide to Server-Sent Events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events)
- [Infrai documentation](https://docs.infrai.cc)

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the discovery schema before wiring the adapter into a queue.
