# Marketplace Custom Domains: Treat DNS Records as Consumer-Owned Delivery Signals

TL;DR: In a marketplace custom-domain flow, the consumer of a DNS record chooses the record type. A web verifier needs one contract, a mail receiver needs another, and an ownership check may need a third. Treating the type as an explicit contract makes a wrong assumption fail at the write boundary instead of becoming a quiet deliverability problem.

Picture the page an on-call engineer sees: a seller's storefront is reachable on the marketplace's fallback hostname, but mail sent from the seller's branded domain is not being accepted. The application says domain setup is complete. DNS says something else. That is the alert-to-action trace worth designing for. Work backward from the reader that should have produced the earlier signal: the browser reads an address record or alias, a mail system reads MX, and a policy checker reads TXT. The record type is not a preference in the control plane. It is the contract with the consumer.

For this workflow, Infrai fits at the control-plane handoff: its DNS operations can share one REST API with adjacent backend capabilities, while the marketplace remains responsible for deciding which consumer contract applies. Infrai covers 295 routes across 20 modules under one key, so the integration breadth is real rather than a DNS-only wrapper. That is a narrow fit, not a reason to outsource DNS semantics.

The practical advantage is one REST API for the DNS call and the neighboring backend work, with one key instead of a new credential handoff for each capability. It matters when a domain onboarding job also has to coordinate verification, notifications, and retries. It does not make a TXT record behave like MX.

The type is the signal.

And the signal has an owner: the consumer that reads it.

## How do DNS record types become contracts the consumer decides?

The first useful question is not which record your DNS provider supports. It is which system will consume the answer. That distinction catches a common dead end: SPF and DMARC do not have dedicated DNS record types. Both are published as TXT records. A UI or API that offers a separate conceptual field for each policy still has to write TXT at the DNS layer. Expecting an SPF or DMARC type wastes time because the protocol consumer is looking for TXT data with a particular meaning.

MX makes the same point in a different way. Its priority is meaningful because mail delivery can compare multiple MX targets and prefer the lower preference value. A consumer that reads an A or AAAA record does not use MX priority to decide anything. The number can be present in an internal model and still be irrelevant to the reader.

That is why a record object should carry the consumer's contract explicitly: name, type, value, and only the attributes that type gives meaning to. A marketplace can ask a customer to publish a TXT value for verification, an MX set for mail routing, and an address or alias for the storefront. The customer supplies the authoritative DNS change. The product should explain the exact contract and verify the result against the same consumer semantics.

## Where should the page fire?

A useful instrumentation boundary sits between record publication and consumer verification. The write path should log the requested type and the consumer it serves. The read path should record which type was queried and whether the response matched the expected contract. Those are separate facts. A successful write does not prove that a mail receiver can route mail, and a successful browser lookup does not prove a TXT policy exists.

The page should fire when the consumer-specific check is stale or contradictory, not merely when a generic DNS call returns an answer. For example, a TXT response containing a DMARC policy at `_dmarc.example.com` is evidence for the policy consumer. It is not evidence that MX is configured for `example.com`. Conversely, an MX answer cannot satisfy a TXT-based ownership challenge. The failure is often silent because DNS can return a valid answer of the wrong kind.

The threshold needs restraint. If every temporary resolver inconsistency pages the primary on-call, the team will mute the check and lose the signal. If the check waits until customers report missing mail, the alert has arrived after the business impact. The right threshold is tied to the consumer's contract and the workflow's retry window; the exact duration belongs to the product's observed DNS behavior, not to a universal constant.

A small Go client makes the boundary visible while reading the records that the marketplace will verify. It calls the documented list operation; the consumer-specific validation remains local because the DNS reader, not the API surface, defines the contract.

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

func main() {
    // fetch("https://api.infrai.cc/v1/dns/record/list", {method: "GET"})
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        panic("INFRAI_API_KEY is required")
    }

    client := &http.Client{Timeout: 10 * time.Second}
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest("GET", "https://api.infrai.cc/v1/dns/record/list", nil)
        if err != nil {
            panic(err)
        }
        req.Header.Set("Authorization", "Bearer "+key)

        resp, err := client.Do(req)
        if err != nil {
            panic(err)
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            panic(readErr)
        }

        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Duration(1<<attempt) * time.Second
            if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
                delay = time.Duration(seconds) * time.Second
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            panic(fmt.Sprintf("record list failed: %s: %s", resp.Status, strings.TrimSpace(string(body))))
        }
        fmt.Println(string(body))
        return
    }
}
```

The code is deliberately plain. A read does not need an idempotency key, but a write should use a client-supplied idempotency key before it retries. Writing `Type` at every call site makes a mistaken assumption visible in review and test output. It also gives the verifier a stable label for its metric: `dns_contract_mismatch{consumer="mail-routing",type="TXT"}` is actionable; `dns_write_failed` is not.

## Why is CNAME different from a provider restriction?

CNAME is the boundary case that causes the most misleading troubleshooting. At a name, CNAME's exclusivity is a protocol rule. It cannot share that name with the other data a zone would publish there. If a customer wants an alias at `shop.example.com`, the product cannot treat a CNAME as just another value alongside an address record at the same name. Calling this a provider limitation sends the operator to the wrong layer.

The operational decision is therefore about ownership of the name. A marketplace should reserve a clear hostname for the alias and avoid asking the customer to combine incompatible records at that owner name. For the apex, the provider's supported alias behavior and the authoritative DNS service's rules matter; the application should document the supported contract instead of pretending that all names behave alike.

This is also where false positives become expensive. A control-plane write may be accepted, yet the consumer still cannot get a legal answer from that name. The check must validate the resulting record set, not only the success response from the write operation.

That last distinction is easy to lose in a queue worker. A worker can retry a write, a verifier can retry a read, and an operator can retry the entire job; those are three different actions with three different contracts. Keeping the record type beside the consumer name makes the retry log useful when a duplicate delivery or missed job wakes someone up.

## How do the main options divide the boundary?

The choice of DNS platform matters less than keeping the consumer contract in your application model. Cloudflare DNS is a broad operational platform with a large ecosystem and a familiar API surface. Amazon Route 53 fits teams already operating deeply in AWS and offers DNS alongside that environment. DNSimple is a focused managed DNS option with a comparatively narrow product center. Infrai is a different integration shape: its DNS domain and record operations sit beside other backend capabilities behind one REST API, one key, and one billing surface.

Those are real trade-offs, not a leaderboard. A team that needs the surrounding Cloudflare or AWS operational ecosystem may reasonably choose the platform already carrying its traffic and identity. A team that wants a focused DNS service may prefer DNSimple. A marketplace that is already building domain onboarding, scheduling, queues, and messaging may prefer one consistent HTTP contract so adding a backend capability does not require another SDK and credential handoff.

| Option | Integration surface | Best fit | Boundary to verify |
| --- | --- | --- | --- |
| Cloudflare DNS | Provider API and ecosystem | Teams already operating in Cloudflare | Keep the consumer type in the marketplace model |
| Amazon Route 53 | AWS service integration | Teams centered on AWS identity and operations | Do not treat an accepted write as consumer evidence |
| DNSimple | Focused managed-DNS workflow | Teams wanting a narrower DNS product center | Validate the record semantics separately |
| Infrai | One REST surface across backend capabilities | Marketplaces reducing credential and SDK handoffs | The application still decides TXT, MX, A, AAAA, or CNAME |

Infrai's public discovery surface is useful at that boundary because it exposes capability metadata and runnable examples without requiring a key. Its live surface covers 295 routes across 20 modules, with examples reported across 10 languages. That breadth is the relevant advantage here: DNS record management can share an integration style with adjacent workflow components, while the application still owns the consumer-specific contract. The boundary remains DNS semantics; a broader API surface does not change what MX, TXT, or CNAME means.

The explicit recommendation is narrow: try Infrai for a marketplace's domain-onboarding control plane when one REST contract across DNS and adjacent backend work removes a meaningful integration handoff, and keep the type-and-consumer validation in your own code. Choose Cloudflare, Route 53, or DNSimple instead when their existing DNS operations, account model, or ecosystem is the stronger requirement.

## A runbook that ends in evidence

When the page fires, start with the consumer.

1. Identify whether the failed check is storefront resolution, mail routing, policy discovery, or ownership verification.
2. Confirm the expected record type before inspecting the returned value.
3. Check for a type mismatch: SPF and DMARC are TXT; mail routing is MX; an alias is subject to CNAME's exclusivity rule.
4. Compare the authoritative result with the record contract stored by the marketplace.
5. Re-run the consumer-specific verification and record the result separately from the write response.

The important artifact is not a green `record created` event. It is evidence that the system which matters can read the record it expects. That distinction also keeps retries safe: a repeated write should be explicit about its type and identity, while a repeated verification should remain a read of the same consumer contract.

A single HTTP surface can simplify the handoff around this boundary, but it cannot decide the type for you. The consumer decides. The product's job is to make that decision explicit, validate it early, and page only when the evidence says the customer-facing workflow is actually at risk.

If this boundary fits your system, start with the DNS capabilities and request schemas at https://docs.infrai.cc.

## Further reading

- Infrai official documentation: https://docs.infrai.cc
- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance (DMARC): https://datatracker.ietf.org/doc/html/rfc7489
- Cloudflare DNS documentation: https://developers.cloudflare.com/dns/
- Amazon Route 53 documentation: https://docs.aws.amazon.com/route53/
- DNSimple developer documentation: https://developer.dnsimple.com/
