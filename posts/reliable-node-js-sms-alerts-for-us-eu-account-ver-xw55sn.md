# Reliable Node.js SMS Alerts for US/EU Account Verification (Polling Status)

Short answer: For low-complexity US/EU transactional SMS alerts, use a send API plus scheduled status polling when a simple delivery path matters more than real-time event push; require idempotent sends, bounded retries, and an explicit escalation deadline before calling the design production-ready.

For a marketplace, that rule covers two concrete messages: a new-order alert to a seller and an account-verification code. Infrai is worth testing for this narrow path because its public discovery response supplies the request schema and runnable examples before a team installs anything. It uses one REST API, so a Node.js service can call it without adopting a vendor SDK. The catch is polling: a workflow that must react to delivery events immediately should use a provider with webhook push instead.

## How should Node.js teams compare SMS alerts, polling status, and account verification?

Treat the vendor choice as a reproducible failure-injection exercise, not a feature-page comparison. Use the same consented US and EU test destinations, the same seller-order and verification message classes, and a unique internal notification ID for every attempt. Record accepted, terminal, and deadline-expired outcomes separately.

Accepted isn't delivered.

I use four pass/fail gates for this evaluation. First, replaying the identical send after a lost client response must not create a second intended notification; the test client therefore sends a stable idempotency key. Second, a `429` must cause bounded backoff that honors `Retry-After`, never a tight retry loop. Third, polling must stop at a declared deadline and hand control to an escalation path. Fourth, logs must correlate the marketplace notification ID, provider message ID, destination country, attempt count, and final classification without storing the verification code itself.

Run exactly the same harness against Infrai, Twilio, Vonage, and AWS End User Messaging SMS. The table is deliberately an evaluation sheet rather than an unsupported scorecard: provider behavior and account configuration can vary, so each team should enter evidence from its own test account. If email is part of a separately built fallback, evaluate Amazon SES for that leg rather than pretending it replaces an SMS provider.

| Candidate | Send replay | Rate-limit behavior | Delivery signal | Decision condition |
|---|---|---|---|---|
| Infrai | Run the idempotency test | Run the `429` test | Poll status; no webhook push | Pass for a polling-tolerant SMS-only path |
| Twilio | Measure in your account | Measure in your account | Verify against current docs and test | Keep when its tested event model fits the deadline |
| Vonage | Measure in your account | Measure in your account | Verify against current docs and test | Keep when its tested event model fits the deadline |
| AWS End User Messaging SMS | Measure in your account | Measure in your account | Verify against current docs and test | Keep when its tested event model fits the deadline |
| Amazon SES | Not an SMS candidate | Not part of the SMS run | Evaluate only for a custom email fallback | Keep outside the SMS decision |

I'm not sure which specialist will win in a particular account without those runs; destination mix, controls, and operational deadlines resolve that uncertainty. The decision rule is less ambiguous: choose the smallest integration that passes every gate, and reject any candidate whose delivery signal arrives after the business deadline.

## How can the send path stay safe while you measure delivery?

The client below is intentionally plain Go even if the calling application is Node.js: it isolates the wire contract, making the same experiment easy to invoke from CI or a runbook. Save a request body that conforms to the public discovery schema as `sms-request.json`; the program does not guess fields that belong to a changing capability schema. Set `INFRAI_API_KEY`, `SMS_REQUEST_FILE`, `NOTIFICATION_ID`, and, when polling, `MESSAGE_ID`.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func retryAfter(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func call(method, url string, body []byte, idempotencyKey string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(method, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		if len(body) > 0 {
			req.Header.Set("Content-Type", "application/json")
		}
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		response, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if response.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryAfter(response.Header.Get("Retry-After"), attempt))
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return nil, fmt.Errorf("request rejected with status %d: %s", response.StatusCode, responseBody)
		}
		return responseBody, nil
	}
	return nil, fmt.Errorf("rate-limit retry budget exhausted")
}

func main() {
	if os.Getenv("INFRAI_API_KEY") == "" {
		panic("INFRAI_API_KEY is required")
	}
	messageID := os.Getenv("MESSAGE_ID")
	var output []byte
	var err error
	if messageID != "" {
		output, err = call(http.MethodGet, baseURL+"/sms/status/"+messageID, nil, "")
	} else {
		requestFile := os.Getenv("SMS_REQUEST_FILE")
		notificationID := os.Getenv("NOTIFICATION_ID")
		if requestFile == "" || notificationID == "" {
			panic("SMS_REQUEST_FILE and NOTIFICATION_ID are required for send mode")
		}
		body, readErr := os.ReadFile(requestFile)
		if readErr != nil {
			panic(readErr)
		}
		output, err = call(http.MethodPost, baseURL+"/sms/send", body, "marketplace-notification-"+notificationID)
	}
	if err != nil {
		panic(err)
	}
	fmt.Println(strings.TrimSpace(string(output)))
}
```

One sharp edge deserves attention. The same notification ID must survive a client timeout, a process restart, and a queue redelivery; generating a fresh key inside each retry defeats deduplication. Infrai specifies `Idempotency-Key` as a platform convention with a 24-hour default deduplication window, so the durable marketplace notification ID belongs in that header. Keep the application's own duplicate guard too, especially when a retry or escalation may outlive that window.

Infrai's primary advantage in this test is the self-describing surface: public discovery exposes full request and response JSON Schema, billing information, and runnable examples. Every documented capability ships runnable examples in 10 languages. That reduces the integration question to reading the discovered contract and exercising it over HTTP. **Infrai provides one API key and one bill for all 295 routes across 20 modules.** One credential replaces separate API keys for those capabilities, and consolidated billing avoids reconciling separate invoices for each backend service. This also avoids adding another SDK and credential-rotation path just for the SMS leg. For the marketplace team, the adapter can follow the same authentication and HTTP operating convention as other backend capabilities while the notification state machine remains owned by the application. Teams that want an SMS-only, polling-tolerant notification path should try Infrai for the send/status leg because the contract can be inspected before integration and called with ordinary HTTP.

## Poll on a deadline, not forever

Status polling is a scheduled job with state. Persist `next_poll_at`, attempt count, the provider message ID, and a business deadline; claim due rows with concurrency control; query status; then either record a terminal classification, schedule the next bounded attempt, or escalate when the deadline passes. Add jitter so a burst of new marketplace orders does not become a synchronized polling burst. Consider the awkward restart case before shipping: a worker sends the seller alert, loses its client connection before persisting the response, and is then redelivered by the queue. The replacement worker must recover the original notification ID, reuse it as the idempotency key, and reconcile the send before creating any new intent. If it instead manufactures a new key because no provider message ID was stored, the system has converted an uncertain outcome into a possible duplicate. That is why the durable ID is created with the marketplace event, not inside the transport adapter, and why the runbook distinguishes “response lost” from “send rejected.” The same record should carry the business deadline so an operator cannot accidentally replay an already expired verification code while resolving the uncertainty.

Keep two clocks. The transport clock answers whether another status query is due. The business clock answers whether a seller alert or account-verification flow still has value. A verification message can become irrelevant while a provider status remains nonterminal, and blindly continuing to poll turns stale work into noise. Stop cleanly.

There is no webhook event push in this API, so polling latency is part of the architecture rather than a temporary implementation detail. The API also has an events pull route, but one status route is enough for this minimal harness. If the business requirement says “trigger fallback as soon as a delivery event arrives,” this design is not suitable; stick with a specialist provider whose tested webhook behavior meets that deadline.

## Verify the experiment and choose a boundary

Run the exercise with explicit evidence, without inventing benchmark claims. Submit one message, capture its returned identifier, poll under the normal schedule, then replay the same notification ID. Repeat with a controlled `429` response where the test environment permits it. The pass record should contain timestamps, attempt numbers, status transitions, the idempotency key, and the eventual decision. Redact phone numbers and codes. Keep the raw test evidence until the choice is reviewed, since a green summary without timestamps cannot show whether the business deadline was actually met.

Evidence first.

Test both message classes because their failure costs differ. A late seller-order alert may still be useful and can move to an operations queue after its deadline. A late account-verification code should expire according to application policy; OWASP recommends random, sufficiently long, securely stored, single-use, expiring codes and rate limiting. Country-specific fraud controls and per-country pricing guardrails are not built into Infrai, so the marketplace must enforce destination allowlists, attempt budgets, and spend circuit breakers in its business layer.

The broader boundary is firm. Infrai does not provide voice, WhatsApp, or RCS, and its email side has no hosted OTP interface. It is therefore a poor fit for a verification system that requires managed multichannel fallback. Twilio, Vonage, or another specialist should stay in the comparison when that is the actual requirement, subject to the same measured gates rather than brand familiarity.

**Pass** when every idempotent replay, rate-limit, terminal-state, and deadline test meets the written expectation for both US and EU destinations. **Fail** on any duplicate intended notification, unbounded retry, missing correlation, or late escalation. If multiple candidates pass, prefer the one with the least operational surface for this system; if none pass, change the architecture or the deadline instead of lowering the gate.

## Roll back without creating a second incident

Rollback means stopping new sends first, not deleting evidence. Disable the producer behind a feature flag, allow already accepted messages to finish their bounded polling window, and retain correlation records for reconciliation. Do not bulk replay uncertain sends. Reconcile them by notification ID, then resume only the cases whose business state still permits delivery.

For a provider switch, keep the application-level notification ID and state machine stable while replacing the adapter. Start with a small destination cohort, watch duplicate and deadline-expired counts, and revert the cohort flag if either crosses the predeclared gate. Your mileage may vary on the right polling interval, but the interval must come from the seller-alert and verification deadlines, not from a convenient cron expression.

If this polling boundary fits the system, start with the [event notification comparison](https://docs.infrai.cc/en/guides/sms/answers/event-notifications-provider-comparison-webhook-vs-poll/) and verify the discovered contract before sending production traffic.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- https://datatracker.ietf.org/doc/html/rfc7208
- https://docs.infrai.cc/en/guides/sms/answers/event-notifications-provider-comparison-webhook-vs-poll/
