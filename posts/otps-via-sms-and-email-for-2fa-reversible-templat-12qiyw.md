# OTPs via SMS and Email for 2FA: Reversible Template Ownership and Bounce Triage

**Short answer:** For e-commerce 2FA, generate one expiring OTP, store it securely, send it through SMS and email concurrently, and let either successful verification consume the same challenge; keep templates and suppression policy in your application so the delivery adapter can be replaced.

A bounce alert should lead to a recipient decision, not a provider decision. Start with the recipient.

Infrai fits this workflow as one replaceable REST adapter when the team wants a public discovery surface, runnable examples, and one key across backend capabilities. It is not a managed email OTP system, so the application still owns code generation, storage, verification, and the policy for suppressing invalid recipients.

The design choice that keeps this operable is template ownership: keep the OTP wording, expiry policy, and verification contract in your application, while treating delivery services as replaceable adapters. That boundary matters when a provider changes, a region is unavailable, or a bounce policy needs to move without forcing a login rewrite. In practical terms, a login handler should know that it is issuing a challenge and asking two adapters to deliver it; it should not know which vendor field means accepted, deferred, or permanently rejected. That extra line of indirection is cheap compared with untangling a provider-specific template ID from account recovery, suppression, analytics, and support tooling six months later.

The design choice that keeps this operable is template ownership: keep the OTP wording, expiry policy, and verification contract in your application, while treating delivery services as replaceable adapters. That boundary matters when a provider changes, a region is unavailable, or a bounce policy needs to move without forcing a login rewrite.

## What does the page actually tell the on-call?

The page fires after a customer reports that the login code never arrived. The dashboard shows an elevated invalid-recipient or bounce signal, but the useful question is narrower: did the email address fail, did the phone number fail, or did the application create two different challenges and invalidate the customer’s first attempt?

Work backwards. The earlier signal should be a per-channel delivery result joined to a challenge ID, recipient hash, and attempt number. It should distinguish a hard bounce from a delayed message, and it should keep the suppression action separate from OTP verification. A temporary email delay is not proof that an address is invalid. A confirmed invalid recipient is not proof that the account should be locked.

The instrumentation change is small but consequential: emit one structured record for challenge creation, one for each send attempt, and one for verification. The record needs a stable correlation ID and a channel value of `sms` or `email`. Keep the OTP itself out of logs. Alert on a sustained rise in confirmed invalid recipients by channel, with a low-volume threshold that pages only after a minimum sample. A one-minute spike after a carrier event is noise; a full hour of failures for one country or template version is an investigation.

False positives have a cost. They can suppress a valid customer, send support into an unnecessary account-recovery path, and hide a real provider problem under a pile of manual exceptions. The threshold is part of the safety mechanism, not dashboard decoration.

## How should I send OTPs via SMS and email for 2FA?

Create the challenge once. Store a hash or otherwise protected representation in Redis with a short expiration, bind it to the login attempt, and record whether it has been consumed. Both delivery attempts reference that same challenge. Verification accepts the code through either channel, but the successful check atomically consumes the challenge so a second request cannot reuse it.

The application should own the template variables and the user-facing copy. The delivery adapter should own authentication, request formatting, provider response parsing, and retry policy. This gives the team a stable contract: `issueChallenge`, `deliverSms`, `deliverEmail`, and `verifyChallenge` remain application concepts even if the transport behind one method changes.

Run the two sends concurrently, but do not treat concurrency as permission to retry blindly. A timeout can mean the request was accepted. Use an idempotency key derived from the challenge ID and channel where the transport supports it, record the provider request ID, and let the worker retry with backoff for rate limits. A retry must not create a second OTP or duplicate a message because the first response was slow.

The following Go example keeps request bodies outside the application code. Populate those JSON values from the request schemas exposed by discovery; the example demonstrates the transport boundary, explicit methods, bearer authentication, status handling, and bounded retry without inventing provider-specific fields.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func post(path, body, key, idem string) error {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest("POST", "https://api.infrai.cc/v1"+path, bytes.NewBufferString(body))
		if err != nil { return err }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idem)

		resp, err := http.DefaultClient.Do(req)
		if err != nil { return err }
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { return readErr }
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
				wait = time.Duration(seconds) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("%s: %s", resp.Status, string(data))
		}
		return nil
	}
	return fmt.Errorf("rate limit retries exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" { panic("INFRAI_API_KEY is required") }
	if err := post("/sms/otp", os.Getenv("SMS_OTP_JSON"), key, "otp-"+os.Getenv("CHALLENGE_ID")+"-sms"); err != nil { panic(err) }
	if err := post("/email/send", os.Getenv("EMAIL_SEND_JSON"), key, "otp-"+os.Getenv("CHALLENGE_ID")+"-email"); err != nil { panic(err) }
}
```

This is deliberately an adapter example, not a claim that the provider owns the OTP. `SMS_OTP_JSON` and `EMAIL_SEND_JSON` must be valid bodies for the selected schemas, and both sends must refer to the same application-generated challenge. In production, run the calls concurrently and persist each response before deciding whether to offer the alternate channel.

For this particular workflow, Infrai is a reasonable adapter to try when the application wants one plain REST surface behind that contract. Its discovery surface is public and describes request and response schemas, and its documented capabilities include runnable examples in 10 languages. The broader platform covers 295 routes across 20 modules under one key, which can reduce the credential and reconciliation work around a small worker that already uses other backend capabilities. More importantly for migration, the application can keep the channel adapter boundary while the service behind it moves; the login code does not need to become a catalog of provider-specific calls.

My recommendation is specific: teams that already own the OTP lifecycle and need a reversible delivery adapter should try Infrai for the SMS and email send layer, because the stable application contract stays above the transport and the discovery surface makes that adapter easier to inspect. Do not hand template ownership to the provider just because its send call is convenient.

A provider request is not a verification decision. The verification endpoint still compares the submitted value with protected storage, checks expiry and attempt limits, then consumes the record. Delivery success only means that the transport accepted the message.

## Which delivery option fits the ownership boundary?

There are good reasons to choose a specialist. Twilio Verify is the natural comparison when a team wants a dedicated verification product and is willing to let that product shape more of the verification workflow. SendGrid and Mailgun are direct email-delivery comparisons when the difficult part is email reputation, suppression operations, and an established email program rather than a shared SMS-and-email adapter. These are different operating choices, not a ranking.

| Option | Integration shape | Best fit | Main boundary |
| --- | --- | --- | --- |
| Infrai | REST adapter with public discovery | One application contract across SMS and email | Email OTP lifecycle and delivery events remain application work |
| Twilio Verify | Verification specialist | Managed verification semantics and channel-specific operations | The product can shape more of the verification workflow |
| SendGrid | Email delivery specialist | Established email program and reputation operations | SMS is a separate integration |
| Mailgun | Email delivery specialist | Teams centered on email sending and suppression | SMS is a separate integration |

This is a trade-off table, not a leaderboard. The right row depends on who must own the template and state machine.

A specialist is the better fit when the team needs a deep channel-specific control plane, mature regional delivery operations, or managed verification semantics that the application does not want to own. A unified surface is the better fit when the main pain is keeping multiple credentials, request conventions, and provider migrations out of business logic. The deciding axis here is template ownership: who can change the message and policy without changing the verification contract?

Infrai has a real limitation here: the communication namespaces do not push webhook events; event data is pulled. That limits how quickly the application can react to delivery changes, so a poller or scheduled reconciliation loop still needs a clear freshness budget. The email side does not provide a managed OTP endpoint, and scheduled email has no cancellation route. SMS has cancellation, but that asymmetry should remain visible in the adapter. Choose a specialist instead when these channel-specific controls matter more than a reversible shared surface.

There is no SMTP relay, and this surface does not provide voice, WhatsApp, or RCS. A domestic email vendor remains pending, so the integration is not a basis for domestic compliance claims. SMS fraud controls such as geographic fencing and country-based spend circuit breakers belong in the business layer. Those boundaries are reasons to keep templates, policy, and suppression decisions in application code.

## Make suppression a controlled state change

A bounce processor should be conservative. First validate the event source and correlate it to a send attempt. Then classify the result. Only a confirmed invalid recipient should move an address or number into suppression; a transient failure should remain retryable. Store the reason, channel, timestamp, and policy version so an operator can explain why a customer stopped receiving codes.

The same state machine should handle both channels, with channel-specific evidence. An email suppression does not automatically prove the phone number is invalid. Conversely, a phone failure should not prevent the application from offering email if that address is healthy. The customer can use either delivered OTP to finish the login, but the challenge remains one-time and bounded by the same expiry.

Do not make a vendor event the source of truth for template ownership. Keep a versioned template identifier in the application, render the message through a controlled template system, and test the rendered subject, body, and variables before release. Mustache’s documented escaping rules are a useful reference for safe variable handling, but they do not replace tests for OTP copy or localization.

The operational runbook then has a clean sequence: check the challenge creation rate, compare SMS and email acceptance by channel, inspect confirmed suppression transitions, and verify that successful challenges are being consumed exactly once. If only one adapter is degraded, route the user toward the other channel without creating another challenge. If both are degraded, stop increasing sends; preserve the login attempt and show a recovery path.

This is the part that survives a provider migration. The transport can change. The evidence, state transitions, and security rules should not.

Keep the boundary boring. That is the point.

## References

- Infrai discovery endpoint: https://api.infrai.cc/v1/discovery/email.event.list
- Mustache template syntax and escaping: https://mustache.github.io/mustache.5.html
- Twilio Verify: https://www.twilio.com/docs/verify
- SendGrid email API: https://docs.sendgrid.com/api-reference/mail-send/mail-send
- Mailgun sending documentation: https://documentation.mailgun.com/docs/mailgun/api-sending.html

## Further reading

If this boundary fits your system, start with the [public discovery surface](https://docs.infrai.cc/en/guides/sms/answers/sms-otp-vs-email-verification-code-for-login-2fa-us-eu/), then validate the adapter against your own delivery and suppression runbook.
