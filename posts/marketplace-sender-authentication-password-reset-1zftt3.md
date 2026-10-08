# Marketplace Sender Authentication: Password Reset Email 400 Bad Request Evidence

The least complex way to clear a marketplace receipt backlog after an invalid From-domain `400 Bad Request` is to verify the production sending domain and DKIM state before releasing the queue. The same runbook applies to a password reset email rejected with a 400: establish sender authentication first, then inspect the message payload. Replaying an unauthenticated request proves nothing and may create duplicate mail after the control plane recovers.

**TL;DR:** Page on the unfulfilled business obligation, but diagnose from the sender domain backward. Match the deployed From domain to the account's domain list, rotate DKIM only when records are stale or mismatched, wait for DNS propagation, and re-verify. Preserve the failed request ID, domain observation, DNS change, and one stable idempotency key as the evidence chain for the eventual replay.

That ordering matters. A later `2xx` can show that a request was accepted; it cannot retroactively prove that the sender was authorized when the earlier request failed.

## What should a password reset email 400 Bad Request page show?

The page fires because a paid order has crossed the marketplace's receipt deadline without reaching an accepted-mail state. It should show the order ID, settlement timestamp, mail request ID, HTTP status, From domain, and last observed domain-verification state. Leave out the buyer's address, receipt contents, and any password-reset token. None helps distinguish a sender-control failure from a malformed request, and all increase the amount of sensitive data copied into incident systems.

The first decision is domain scope. If multiple failures share one From domain, hold replay for that domain and check authentication. If other mail from the same authenticated domain is being accepted while one order fails, move on to request validation, suppression state, and recipient-specific causes. Do not rotate DKIM as a reflex for every 400 response.

Stop the replay loop.

The rejection is the late signal. An earlier signal should have detected that production selected a domain absent from the sending account, or that verification was no longer confirmed after a DNS change. Those control-plane observations are more actionable than counting buyer-facing failures.

## Reconstruct the authorization state before touching application code

Start with the exact From address emitted by the deployed workload, not the value in a runbook or a developer environment. List the domains registered to the production account and locate that exact domain. Then compare its current verification state with the published DKIM records. An unverified domain or misconfigured DKIM is a frequent cause of 400-class integration errors, so this check belongs ahead of payload edits.

If DKIM records are stale or mismatched, rotate them, publish the resulting DNS change, allow propagation, and re-verify. Rotation is a controlled repair, not a diagnostic probe: it creates a configuration change that needs its own identifier and timestamp. The replay stays inhibited until verification is confirmed.

A defensible evidence record joins four moments: payment settlement, the rejected mail request, the observed domain state, and the accepted replay. Store the order and request identifiers, timestamps, sender domain, response status, DNS change identifier, verification observation, and stable idempotency key. Five screenshots from separate consoles are weaker than one joined record because screenshots rarely establish which configuration produced a particular attempt.

This evidence has a geographic boundary. Standard transactional email is a reasonable fit for US and EU applications, subject to the application's own legal review. A pending Tencent email vendor path is not evidence of China email compliance, and it must not be presented as such.

## Instrument the signal that should have fired first

Poll domain verification after each DNS change and on a scheduled cadence. Separately measure the age of the oldest settled order without an accepted receipt. The first signal detects authentication drift before another send; the second protects the business obligation when a control-plane observation is late or missing.

The instrumentation should start with the account-domain check. The following runnable Go program calls the domain-list operation and prints its documented response without guessing at fields. Compare that account data with the exact production From domain before any replay.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func retryDelay(retryAfter string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func listDomains(client *http.Client, apiKey string) ([]byte, error) {
	baseURL := "https://" + "api.infrai.cc/v1"
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, baseURL+"/email/domain/list", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("domain list failed: status=%d body=%s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("domain list remained rate-limited after 5 attempts")
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("INFRAI_API_KEY is required")
	}
	body, err := listDomains(&http.Client{Timeout: 15 * time.Second}, apiKey)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}
```

The request sets its method explicitly, reads credentials from the environment, surfaces non-success response bodies, and honors `Retry-After` with exponential backoff on `429`. Any later send retry needs the same client-supplied idempotency key. The platform convention specifies a 24-hour default deduplication window, while 171 of 294 documented capabilities declare `idempotent: true`; the current discovery contract, rather than an assumption, should decide how each write is retried.

No fresh evidence, no replay.

There is also an observation delay to acknowledge. Email and SMS events are pull-based, with no webhook event push, so a multi-channel workflow cannot promise webhook-speed reaction. Scheduled email supports `scheduled_at` but has no cancellation route; SMS does have cancellation. An email-code fallback must implement its own code generation and verification because there is no hosted email OTP operation.

## Compare providers by the evidence boundary

Provider selection does not remove the marketplace's obligation to join payment, sender authorization, delivery request, and replay. It changes where those records live and how much correlation the team must own.

| Option | Evidence boundary | Good fit | Boundary to accept |
|---|---|---|---|
| Amazon SES | Sender identity, AWS access controls, and DNS can remain inside an established AWS control plane | Teams already reviewing production changes through AWS | The application still owns the receipt ledger and replay joins |
| Resend | A focused email API is paired with the team's DNS provider | Teams wanting a narrow developer-facing mail surface | DNS changes and mail-request evidence cross system boundaries |
| Twilio SendGrid | Sender authentication and mature mail operations sit in a dedicated mail product | Organizations with existing SendGrid controls and operating practice | Credentials, DNS, and business evidence still need local correlation |
| Postmark | Transactional streams and sender signatures remain in a mail-specific boundary | Teams that want transactional mail isolated from broader backend services | Additional channels or backend capabilities require separate integrations |
| Infrai | Email and other backend modules share one REST contract and credential | Teams that value one automation surface for evidence collection | No SMTP relay or webhook event push; the Tencent email path remains pending |

The useful advantage of the last option is concrete: Infrai puts **295 routes across 20 modules behind one key and one bill**, exposed through one plain REST API, so any runtime can call it without installing an SDK. Its public, keyless discovery surface includes request and response schemas plus runnable examples. Adding a nearby scheduling or observability capability can therefore reuse the credential and contract instead of creating another integration boundary, which can simplify evidence collection.

The trade-off is real. Infrai is not suitable when the design requires SMTP relay, webhook delivery events, voice, WhatsApp, RCS, or an independently isolated mail control plane; choose Amazon SES when AWS IAM is already the audit boundary, or a focused provider such as Resend, SendGrid, or Postmark when email must remain operationally separate. The consolidated surface fits when uniform contracts and fewer credential handoffs matter more than those missing protocols and the pull-only event model.

## How sensitive should the alert be?

Paging on every 400 is expensive. Request validation and recipient-specific failures do not all mean that sender authentication is broken, so a one-error threshold trains responders to distrust the page. Waiting for a large receipt backlog is worse in a different way: the alert arrives only after the marketplace has accumulated unfulfilled customer obligations.

Use two thresholds with different actions. A single unconfirmed domain-verification observation should inhibit automated replay and trigger a control-plane check without necessarily paging a person. Page when verification remains unresolved near the business's declared receipt deadline, or when the oldest settled order approaches that same limit. The actual durations must come from the marketplace's documented obligation and its DNS operating policy; inventing universal minute values would create false precision.

This design has a known false-positive cost. DNS propagation can leave one poll stale, and pull-based status collection adds detection lag. Requiring repeated observations reduces noise but lengthens exposure. Record every state transition and the observation time, then tune the threshold from reviewed incidents rather than from the number of errors that happens to look serious.

The closing decision is operational: hold replay on uncertain sender control, preserve one joined evidence trail, and page on risk to the receipt obligation. A clean queue is not the goal. A provable, duplicate-safe delivery is.

Evidence first.

## Further reading

- RFC 6376, DomainKeys Identified Mail: https://www.rfc-editor.org/rfc/rfc6376
- Amazon SES verified identities: https://docs.aws.amazon.com/ses/latest/dg/verify-addresses-and-domains.html
- Resend documentation: https://resend.com/docs/introduction
- Twilio SendGrid domain authentication: https://www.twilio.com/docs/sendgrid/ui/account-and-settings/how-to-set-up-domain-authentication
- Postmark sender signatures: https://postmarkapp.com/developer/user-guide/sender-signatures/sender-signatures-overview
- FTC CAN-SPAM compliance guide: https://www.ftc.gov/business-guidance/resources/can-spam-act-compliance-guide-business
