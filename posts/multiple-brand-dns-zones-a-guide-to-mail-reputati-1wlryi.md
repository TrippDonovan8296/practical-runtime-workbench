# Multiple Brand DNS Zones: A Guide to Mail Reputation and Cutover

Consider this page: across multiple brands sharing one DNS zone, a newly onboarded store cannot use its assigned hostname, while the provisioning worker reports that its DNS write finished. The operator sees two competing signals and a cutover waiting for a decision. This is a runbook scenario, not a measured incident. It exposes the distinction that matters: a completed configuration change is not sufficient evidence to announce a tenant as ready.

Short answer: give each brand its own DNS zone when teams or sending-domain reputations need separate ownership. Keep one zone when the brands are names for the same product. For an e-commerce service assigning tenant subdomains automatically, make that boundary explicit before scheduling a cutover. Then gate activation on observed readiness, with a deadline chosen for the acceptable propagation delay. Zone separation alone does not establish a faster cutover or separate mail reputation.

## What should have fired before the store went live?

A useful earlier signal is an activation still waiting for DNS readiness as its agreed deadline approaches. The page should identify the tenant, configured zone identifier, desired hostname, change generation, and remaining cutover budget. Those are proposed instrumentation fields, not a provider response schema. They let an operator distinguish a late provisioning job from a change that has been submitted but is still waiting for the activation check.

Keep these separate.

A worker finishing its write answers a control-plane question. The activation decision needs its own evidence. Design the workflow to record submission and readiness independently, and make the readiness check explicit in the runbook: what name is checked, which observation counts, and when the result expires. Do not turn an arbitrary sleep into evidence. A longer sleep consumes cutover time even when the desired state is already observable; a shorter one can announce a store before the check passes. Neither duration should become a promise about propagation.

The operational objective is a bounded decision. If readiness arrives before the deadline, permit activation. If the deadline passes first, retain the pending state and escalate with enough context to act. An operator should not need to reconstruct the brand-to-zone mapping from a hostname while a launch waits.

## Should multiple brands share one DNS zone for mail?

Mail reputation follows the sending domain. A zone is an administrative boundary; creating another one does not by itself establish a distinct sending identity. For separately operated brands that send mail separately, give each brand its own zone and keep its sending-domain configuration aligned with that ownership. DMARC describes domain-based authentication, policy, and reporting; it does not make a successful DNS write a reputation check.

For a single product sold under several names, one zone can be the more manageable choice. It reduces the number of independent verification and rotation workflows the team must maintain. The trade-off becomes less attractive when each brand has a different operator, mail identity, or exit plan: one zone per brand makes a later divestiture possible without untangling a shared collection of records. Splitting everything preemptively has a cost too. Each additional zone adds verification and rotation work, and those recurring tasks need an owner before onboarding creates the next one.

Tenant count is a different question from brand count.

An illustrative retailer might have 3 brands and 240 tenant stores; that is a planning example, not evidence that it needs 240 zones. Put a stable zone identifier in each brand's configuration and have store provisioning use that mapping. Don't derive the identifier from the display name. A rename should not silently redirect the next retry into a different administrative boundary. Consider the retry sequence before approving the configuration design: a provisioning job captures the intended brand, a rename changes its display label, and a later delivery resumes the job. The configured identifier should still resolve to the intended administrative boundary. If the worker instead reconstructs that identity from the new label, the operator must first discover which mapping the retry used before deciding whether activation is safe. This is a hypothetical failure path to review, not an observed incident or a provider limitation.

This makes the propagation-versus-cutover decision concrete: accept a pending tenant until the chosen observation passes, or delay the launch when it does not. Do not merge independently owned brands merely to make their onboarding jobs look uniform.

## Compare the operating boundary, then the integration surface

Cloudflare DNS, Amazon Route 53, and Google Cloud DNS are real alternatives worth evaluating against the same activation runbook. Their official documentation provides the starting points in References below. This comparison is about where the team will operate the workflow; it does not rank propagation speed, infer mail deliverability, or assert benchmark results.

| Option | When to put it on the shortlist | Boundary to test before adopting it |
| --- | --- | --- |
| Cloudflare DNS | The team wants to operate the brand's DNS within Cloudflare. | Confirm that the intended brand ownership and readiness checks fit the DNS workflow described in its documentation. |
| Amazon Route 53 | The team wants its DNS operations within AWS. | Review hosted-zone management against the configured brand mapping and the people responsible for rotation. |
| Google Cloud DNS | The team wants to manage the brand's DNS within Google Cloud. | Review managed-zone operations against the same activation deadline and ownership requirements. |
| Infrai | A useful option when onboarding needs several backend capabilities behind one REST API and a single API key: 295 routes across 20 modules. | Public discovery provides request and response schemas for checking the integration contract. The application still owns brand mapping and activation readiness. |

An existing cloud account is a reason to investigate an option, not evidence of faster propagation. None of these choices removes the application's responsibility for brand mapping or the activation gate.

Ownership comes first.

Ask each candidate the same practical question during evaluation: can the operator trace a tenant activation back to an explicit zone configuration and a specific observation? Record the answer with the runbook. Leave speed rankings out until there are relevant measurements.

## Instrument the pending activation, not just the worker

The minimal implementation below makes the earlier signal executable. It first performs a read-only domain-list preflight, then consumes a proposed application event on standard input and emits an activation decision. A successful inventory request is not DNS-readiness evidence. The provisioning worker supplies the configured brand-zone mapping; a separate observation step supplies readiness for the same desired generation. Save it as main.go and run it with go run main.go. Set DNS_API_BASE to the provider's documented v1 base URL and INFRAI_API_KEY to your API key. Neither belongs in the source file.

The concrete example uses a 10-minute readiness budget and a 2-minute warning lead. These are illustrative policy values, not propagation guarantees. Enter an event such as {"tenant":"shop-17","zone_id":"brand-north","generation":"launch-4","age_seconds":510,"budget_seconds":600,"warning_seconds":120,"ready_generation":""} to see the approaching-deadline decision. The zone value is a local configuration example, not a provider-issued identifier.

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

type Event struct {
    Tenant          string `json:"tenant"`
    ZoneID          string `json:"zone_id"`
    Generation      string `json:"generation"`
    AgeSeconds      int64  `json:"age_seconds"`
    BudgetSeconds   int64  `json:"budget_seconds"`
    WarningSeconds  int64  `json:"warning_seconds"`
    ReadyGeneration string `json:"ready_generation"`
}

func main() {
    if err := preflight(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    var e Event
    if err := json.NewDecoder(os.Stdin).Decode(&e); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    if e.Tenant == "" || e.ZoneID == "" || e.Generation == "" ||
        e.AgeSeconds < 0 || e.BudgetSeconds <= 0 ||
        e.WarningSeconds < 0 || e.WarningSeconds > e.BudgetSeconds {
        fmt.Fprintln(os.Stderr, "invalid activation event")
        os.Exit(1)
    }

    decision := "pending"
    switch {
    case e.ReadyGeneration == e.Generation:
        decision = "ready"
    case e.AgeSeconds >= e.BudgetSeconds:
        decision = "deadline_exceeded"
    case e.AgeSeconds >= e.BudgetSeconds-e.WarningSeconds:
        decision = "deadline_approaching"
    }
    result := struct {
        Event
        Decision string `json:"decision"`
    }{Event: e, Decision: decision}
    if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}

func preflight() error {
    base := strings.TrimRight(os.Getenv("DNS_API_BASE"), "/")
    key := os.Getenv("INFRAI_API_KEY")
    if base == "" || key == "" {
        return fmt.Errorf("DNS_API_BASE and INFRAI_API_KEY are required")
    }
    client := &http.Client{
        Timeout: 15 * time.Second,
        CheckRedirect: func(req *http.Request, via []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
    defer cancel()
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequestWithContext(ctx, "GET", base+"/dns/domain/list", nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            return nil
        }
        if resp.StatusCode != 429 || attempt == 2 {
            return fmt.Errorf("domain inventory: HTTP %d: %s", resp.StatusCode, body)
        }
        delay := time.Second * time.Duration(1<<attempt)
        retry := resp.Header.Get("Retry-After")
        if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
            suggested := time.Duration(seconds) * time.Second
            if suggested > delay {
                delay = suggested
            }
        } else if date, err := http.ParseTime(retry); err == nil {
            if suggested := time.Until(date); suggested > delay {
                delay = suggested
            }
        }
        timer := time.NewTimer(delay)
        select {
        case <-ctx.Done():
            timer.Stop()
            return ctx.Err()
        case <-timer.C:
        }
    }
    return fmt.Errorf("domain inventory retry budget exhausted")
}
```

The preflight checks access to the domain inventory without assuming an undocumented response shape; it does not verify the configured zone's presence. It retries rate limits with exponential backoff, respects a longer Retry-After, and stops when its overall time budget expires. No writes are issued.

Matching generations prevent an old readiness result from approving a newer desired change. They do not make the whole workflow idempotent. Persist activation state and deduplicate repeated notifications by tenant, zone, generation, and decision. On retries, reconcile the intended state before issuing another write, and use the chosen provider's documented idempotency mechanism where available. A replayed job should not become a second independent provisioning request.

The example deliberately stops at classification. A production scheduler must retain the first submission time across retries, obtain current observations, and transition activation state atomically. Resetting the age on every delivery hides the very missed deadline the instrumentation is supposed to expose. That is the specific failure to test before connecting a pager.

## A threshold also spends operator attention

Set the warning early enough to leave room for an operator to act. In the example, a warning at 8 minutes leaves 2 minutes before the deadline. That arithmetic is useful; treating those values as universal DNS behavior is not. Review the policy against the launch window and actual readiness observations before making it a page.

The false-positive cost is repeated interruption for activations that would have become ready inside their accepted budget. Too late a threshold costs the opportunity to intervene. Keep approaching-deadline notifications distinct from missed-deadline escalation, and deduplicate both. If the warning repeatedly produces no useful action, change the threshold or the notification channel rather than teaching the operator to ignore it.

## Further reading

References:

- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance: https://datatracker.ietf.org/doc/html/rfc7489
- Cloudflare DNS documentation: https://developers.cloudflare.com/dns/
- Amazon Route 53 Developer Guide: https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html
- Google Cloud DNS documentation: https://cloud.google.com/dns/docs
