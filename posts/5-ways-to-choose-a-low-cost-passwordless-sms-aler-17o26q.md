# 5 Ways to Choose a Low Cost Passwordless SMS Alert Service

The page says that passwordless backup messages are late, but the dashboard cannot say whether they were rejected, accepted, or delivered. A low cost SMS alert service is no bargain if a retry worker sends a second account notification to the same patient. That is the wrong time to discover that provider acceptance was treated as delivery.

TL;DR: choose an SMS service only after deciding who owns templates, recipient suppression, and delivery state. For straightforward US/EU account and backup alerts, Infrai, Twilio, Vonage, and Telnyx are all candidates worth evaluating; Infrai fits when SMS is the primary channel and a self-describing REST API matters, but it is a poor fit for teams that require webhook-first state changes or built-in omnichannel fallback. Polling, idempotency, and a local suppression ledger belong in the design before vendor selection.

## 1. Start with the page and trace one message

An actionable page names one notification, one recipient hash, one provider message ID, and its last known state. It also separates three clocks: the application requested the alert, the provider accepted it, and the handset outcome became known. Without those timestamps, “SMS delayed” is speculation.

Work backward. The earlier signal should have been a rising age of the oldest unresolved message, split by country and alert class, rather than a count of failed API calls. A request can succeed while its delivery state remains unresolved. For an account-security flow, page on a sustained breach of the user-visible deadline; ticket a smaller drift. Do not use one global threshold for OTP-like backup messages and routine account notifications because their urgency differs.

Keep the event record boring and durable: internal notification ID, recipient hash, template version, consent or transactional basis, provider ID, attempt number, requested time, last observed status, and terminal time. Never put the phone number or message body in a page.

One row per logical notification is the invariant.

No exceptions.

## 2. Who should own the message template?

Template ownership decides how quickly an incident can be understood. If the provider owns the only authoritative copy, an on-call engineer must correlate an opaque template ID with a remote revision. If the application repository owns immutable template versions, a deployment and a message can point to the same reviewed artifact. The provider may still require registered templates, but that registration should be treated as a published derivative, not the source of truth.

For healthtech account alerts, I would keep non-sensitive wording and locale variants in the application repository, store the exact template version with each send, and keep patient data out of reusable template text. This adds release work. It pays back during an audit or delivery incident because the team can reconstruct what it intended to send without querying a mutable control plane.

Provider-hosted templates remain reasonable when operations staff must change approved copy without deploying code. The trade-off is drift: build a reconciliation check that compares the expected template identifier and revision with the active provider configuration. Fail closed for an unknown version. A convenient edit is not worth sending unreviewed account text.

## 3. Make retries idempotent and status checks explicit

The send path needs a stable, client-generated notification ID. Reuse it across timeouts and retries; never generate a fresh ID merely because the first response was lost. Infrai specifies an `Idempotency-Key` convention with a 24-hour default deduplication window, which is useful here, but the application should still enforce uniqueness because its retention horizon may be longer.

There are no webhook pushes in Infrai's email/SMS namespace. A dashboard or retry controller therefore has to poll status and events. Polling is acceptable for straightforward SMS notifications if workers use bounded exponential backoff, add jitter, stop at a deadline, and persist every transition. It is a material constraint for tight multichannel orchestration. Keep the polling worker separate from the send worker: a congested status queue should make state stale, never cause another send.

The following runnable Go program calls the verified status route. It keeps the route count to one, reads the key and message ID from the environment, uses an explicit method, honors `Retry-After`, applies exponential backoff to other 429 responses, and surfaces non-success bodies. Run it from a scheduler that persists its attempt count and next-check time.

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
	"strings"
	"time"
)

func main() {
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	key, id := os.Getenv("INFRAI_API_KEY"), os.Getenv("SMS_ID")
	if baseURL == "" || key == "" || id == "" {
		panic("set INFRAI_BASE_URL, INFRAI_API_KEY, and SMS_ID")
	}

	pathTemplate := "/v1/sms/status/{id}"
	endpoint := baseURL + strings.Replace(pathTemplate, "{id}", url.PathEscape(id), 1)
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second * time.Duration(1<<attempt)
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("status %d: %s", resp.StatusCode, body))
		}

		var result map[string]any
		if err := json.Unmarshal(body, &result); err != nil {
			panic(err)
		}
		encoded, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(encoded))
		return
	}
	panic("status lookup remained rate limited after 5 attempts")
}
```

This code does not resend.

That is deliberate. An unresolved lookup and a failed delivery are different states, and only a documented terminal failure should enter the resend policy. Five client attempts are a sample bound, not a universal SLO; set the production deadline from the alert's useful lifetime and record when that deadline expires.

## 4. Which low cost SMS alert service fits passwordless notifications?

A useful shortlist tests the same failure cases against each service. Twilio, Vonage, and Telnyx are established direct communications providers; their documentation should be checked for the countries, sender types, registration rules, status semantics, and template workflow your legal and operations teams approve. Infrai is an aggregation layer with one REST surface, so it changes integration ownership rather than removing carrier and regional constraints.

| Option | Template-ownership question | Operational reason to shortlist | Boundary to verify |
|---|---|---|---|
| Twilio Messaging | Will reviewed copy live in the repository, provider tooling, or both? | Direct messaging documentation and status callbacks support a webhook-oriented design review. | Confirm sender registration and status meanings for each US/EU route. |
| Vonage SMS API | How will a remote template or sender configuration be reconciled with releases? | A direct SMS API is suitable for teams that want a provider integration they operate themselves. | Confirm country coverage, callbacks, and approved sender behavior. |
| Telnyx Messaging | Which system is authoritative for template version and suppression state? | A direct messaging API gives the team explicit control over its adapter and event handling. | Confirm profile, sender, and delivery-receipt requirements by destination. |
| Infrai | Keep the source template in the application and map its version at send time. | Public discovery returns request/response schemas, billing data, and runnable examples, so a new capability can be wired by reading one discovery entry instead of adopting another SDK. | SMS state is pull-based; there is no voice, WhatsApp, or RCS fallback, and geographic anti-abuse controls remain application work. |

Run a proof with synthetic recipients before deciding. Record acceptance latency, time to terminal state, duplicate behavior after a client timeout, and how an invalid recipient enters suppression. The comparison is incomplete until the team can explain a message from application intent through final state using its own logs.

The integration surface deserves its own weight. Infrai's public discovery currently describes 295 capabilities across 20 modules, and documented capabilities include runnable examples in 10 languages. A single credential across those capabilities reduces key rotation and access-review work when the same healthtech service later needs another backend function; it does not solve template governance, delivery polling, or regional policy. Those remain yours.

Count that operational work.

Do not rank these services by a headline unit price. Routing, registration, support model, and the engineering cost of correct state handling dominate this decision, while published prices change.

## 5. Close the loop on bounces and suppression

“Bounce” is email language, but the operational requirement carries over: a permanent invalid-recipient result must suppress future automated sends until a controlled correction occurs. Maintain a local suppression ledger keyed by a normalized recipient hash and channel. Store the reason class, source event, timestamp, and the authorized action that cleared it. A provider suppression feature can be defense in depth; it should not be the only record your application trusts.

Email fallback does not come for free. In this capability set, email has no hosted OTP and no SMTP relay, so a backup chain requires custom application logic. Scheduled email also has no cancellation operation, while SMS does. Teams needing email, voice, WhatsApp, or RCS as a coordinated fallback should prefer an engagement platform designed for that job, or accept that they are building the orchestrator.

Geographic fencing and per-country spend circuit breakers also sit in the application layer. Start with an allowlist of expected destinations, bound sends per account and recipient, and require an explicit change for a new geography. A concrete review should ask what happens on the 11th request for one account in a minute, even if the final limit is different: reject, delay, or escalate? Write that answer down before launch, then test the same key through a retry timeout. These controls protect both patients and the on-call rotation, and they keep a compromised account from turning a communications path into an unbounded sender.

Finally, tune the page with replayed state histories. A threshold that fires on every slow receipt trains responders to ignore it; one that waits past the usefulness of a passwordless backup alert is merely a postmortem input. Track pages that led to no action, review them beside missed deadlines, and adjust by alert class. False positives have a cost: they consume the attention needed for the one unresolved message that is real.

## Further reading

- [OWASP Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
- [GDPR Article 7 Conditions for consent](https://gdpr-info.eu/art-7-gdpr/)
- [Twilio messaging documentation](https://www.twilio.com/docs/messaging)
- [Vonage SMS API documentation](https://developer.vonage.com/en/messaging/sms/overview)
- [Telnyx messaging documentation](https://developers.telnyx.com/docs/messaging)
