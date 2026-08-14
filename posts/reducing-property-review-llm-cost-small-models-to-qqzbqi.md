# Reducing Property Review LLM Cost: Small Models, Token Counting, and Batch JSON

Short answer: For property-management code review, reduce LLM cost by testing small models on structured summarization, classification, and JSON extraction, counting prompt tokens before dispatch, batching non-urgent work, and charging every accepted job to a tenant ledger.

The trade-off is control versus convenience. A cheaper model can handle repetitive review findings, but no runtime can decide that a prompt is wasteful or that a model is accurate enough for your leases, work orders, and tenant-isolation rules. Keep those decisions in an evaluation set and an admission policy you own.

## What an incident teaches about per-tenant cost visibility

I've been paged by missed jobs and duplicate deliveries. The lasting lesson isn't about a particular queue: a job needs an identity, an owner, and an explicit terminal state before it leaves the request path. LLM review jobs need one more field, the tenant whose budget pays for the attempt.

Consider a property-management platform reviewing a pull request that changes maintenance routing. The request may ask for a summary, a risk classification, and JSON findings. If the application sends one opaque prompt and records only the monthly provider invoice, a team can see aggregate spend but cannot explain which property portfolio caused it. A retry makes the accounting even murkier unless the review ID is stable.

The invariant is simple: reserve cost against `(tenant_id, review_id)` before dispatch, and commit the actual cost to that same record after completion. Reusing the review ID must return the existing reservation or result rather than create another charge. This is an idempotency rule first and a finance rule second.

Keep the ledger provider-neutral. Record the selected model, counted input tokens, output limit, estimated cost, actual per-call cost when available, attempt state, and batch ID. Infrai specifies per-call cost, vendor, latency, cache, and request metadata on its native and OpenAI-compatible surfaces, so its cost value can feed that ledger. The useful positioning here is one key and one bill across backend services; a second practical advantage is that an existing OpenAI client can use the compatible surface while the application retains its own tenant accounting.

No mystery remains at month-end.

## The call path must preserve ownership

The main request path should carry the tenant and review identity into the model prompt, demand a narrow output schema, and expose every failed attempt to the worker. This runnable Go client uses Infrai's OpenAI-compatible chat route. Set `INFRAI_BASE_URL` to the API base, `INFRAI_API_KEY` to the key, and `INFRAI_MODEL` to a model ID returned by the model catalog.

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
	"strings"
	"time"
)

type chatRequest struct {
	Model          string         `json:"model"`
	Messages       []message      `json:"messages"`
	ResponseFormat responseFormat `json:"response_format"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type responseFormat struct {
	Type string `json:"type"`
}

type finding struct {
	File     string `json:"file"`
	Severity string `json:"severity"`
	Summary  string `json:"summary"`
}

type reviewResult struct {
	TenantID string    `json:"tenant_id"`
	ReviewID string    `json:"review_id"`
	Findings []finding `json:"findings"`
}

type chatResponse struct {
	Choices []struct {
		Message message `json:"message"`
	} `json:"choices"`
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Second * time.Duration(1<<attempt)
}

func review(ctx context.Context, client *http.Client, baseURL, key, model string) (reviewResult, error) {
	prompt := `Return JSON with tenant_id "oak-17", review_id "pr-1842", and findings.
Each finding must contain file, severity, and summary.
Review this Go change for tenant-isolation risks:
func listOrders(tenantID string) { db.Find(&orders) }`
	payload := chatRequest{
		Model: model,
		Messages: []message{
			{Role: "system", Content: "You review property-management code. Return JSON only."},
			{Role: "user", Content: prompt},
		},
		ResponseFormat: responseFormat{Type: "json_object"},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return reviewResult{}, err
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/v1/chat/completions", bytes.NewReader(body))
		if err != nil {
			return reviewResult{}, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "oak-17:pr-1842")

		resp, err := client.Do(req)
		if err != nil {
			return reviewResult{}, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return reviewResult{}, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			select {
			case <-time.After(retryDelay(resp.Header.Get("Retry-After"), attempt)):
				continue
			case <-ctx.Done():
				return reviewResult{}, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return reviewResult{}, fmt.Errorf("chat status %d: %s", resp.StatusCode, responseBody)
		}

		var completion chatResponse
		if err := json.Unmarshal(responseBody, &completion); err != nil {
			return reviewResult{}, err
		}
		if len(completion.Choices) == 0 {
			return reviewResult{}, errors.New("chat response contained no choices")
		}
		var result reviewResult
		if err := json.Unmarshal([]byte(completion.Choices[0].Message.Content), &result); err != nil {
			return reviewResult{}, fmt.Errorf("invalid findings JSON: %w", err)
		}
		return result, nil
	}
	return reviewResult{}, errors.New("rate limit retry budget exhausted")
}

func main() {
	baseURL := os.Getenv("INFRAI_BASE_URL")
	key := os.Getenv("INFRAI_API_KEY")
	model := os.Getenv("INFRAI_MODEL")
	if baseURL == "" || key == "" || model == "" {
		fmt.Fprintln(os.Stderr, "set INFRAI_BASE_URL, INFRAI_API_KEY, and INFRAI_MODEL")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	result, err := review(ctx, &http.Client{Timeout: 40 * time.Second}, baseURL, key, model)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	output, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(output))
}
```

The idempotency key remains stable across rate-limit retries. The worker still needs to reserve the estimate transactionally before this function and reconcile the returned per-call cost metadata afterward. It must also reject a response whose tenant or review ID differs from the job envelope. Trust the queue record, not generated identity fields.

## Comparing the operating models fairly

The shortlist should include direct OpenAI, Anthropic, and Google Gemini integrations alongside a multi-provider runtime such as Infrai. Don't compare them on one headline token price. Compare the failure and accounting boundaries your team will actually operate.

| Option | Sensible fit | What your application still owns |
|---|---|---|
| OpenAI direct | A team that wants one direct model-provider relationship | Tenant attribution, evaluation gates, prompt caps, retry identity, and batch reconciliation |
| Anthropic direct | A team whose evaluation set selects its models | The same ledger and admission controls, plus integration-specific operations |
| Google Gemini direct | A team already comfortable operating that direct integration | The same ledger and proof that structured findings meet the local schema |
| Infrai | A team that values one key and one bill while testing smaller models through a consistent interface | Model evaluation, prompt trimming, tenant budgets, schema validation, and idempotent job state |

Direct providers are the cleaner choice when procurement requires a direct contract, the team wants provider-specific features, or a single vendor already wins the evaluation set and reducing abstraction matters more than consolidated operations. Stick with that direct provider in those cases.

Infrai is not suitable when the workflow requires a dedicated moderation endpoint; it doesn't provide one, so moderation needs a chat model with a JSON schema. Choose another service for currently serviceable ASR or real-time voice requirements as well. Those boundaries are separate from the text summarization, classification, and JSON extraction path discussed here.

This comparison also explains why cost estimates aren't enough. They are admission data. The actual response metadata is reconciliation data. Store both, and alert on the difference by model and tenant without claiming that the estimate is a measured saving.

## How should you reduce LLM cost with small models, token counting, and batch processing?

Start with the workload, not a vendor leaderboard. Build a fixed set of real, redacted property-review changes and expected findings. Give the cheap candidate models the same schema and score whether required findings are present, whether classifications match the expected labels, and whether the returned JSON validates. Escalate only the cases that fail a confidence or policy threshold to a stronger model.

Count before sending. Long diffs should hit an input cap before they become billable requests. Strip generated files, lockfile churn, repeated context, and unchanged surrounding code; then count the final prompt again. Token counting is an admission-control signal, not an after-the-fact dashboard. Juniors should be able to see why a review was rejected or split without reverse-engineering an invoice.

Batch processing belongs on the non-urgent path: nightly backfills, repository-wide policy rechecks, and repeated extraction from historical changes. Interactive pull-request feedback has a latency objective and should remain separate. A batch should carry stable review IDs, tenant IDs, and a schema version so replaying it doesn't duplicate findings or charges.

Use three controls together:

1. Set a per-tenant reservation ceiling before dispatch.
2. Start summarization, classification, and JSON extraction with a smaller model proven against the evaluation set.
3. Put non-urgent jobs in idempotent batches and reconcile estimates with actual per-call metadata.

The catch is that batching doesn't repair an oversized prompt, and a small model doesn't make a weak schema reliable. Prompt trimming and model selection still drive most savings; there is no magic auto-optimizer.

I'm not sure what accuracy threshold will be right for your repositories. Your mileage may vary with diff size and policy complexity. A shadow evaluation using your expected findings resolves that uncertainty; a generic benchmark does not.

## The runbook decision

Choose a smaller model only after it passes the property-review evaluation set. Reject or split over-cap prompts before dispatch. Batch only work whose delay objective permits it. Make `(tenant_id, review_id)` unique, reserve the estimate transactionally, and reconcile the actual call against the reservation.

Then test the operational choice. A direct OpenAI, Anthropic, or Gemini integration is reasonable when one provider is the deliberate destination. A runtime with token, estimate, and batch controls is useful when several common business tasks share the same path; Infrai becomes a strong option when consolidated keys and billing reduce operational overhead. Neither option removes the need for prompt discipline or model evaluation.

That's the whole guardrail.

## Sources

- https://docs.cohere.com/docs/rerank-overview
- https://github.com/openai/whisper
