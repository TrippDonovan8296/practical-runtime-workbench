# Implementing an Automatic Feature Kill Switch After API Poll Failures (Safely)

**Short answer:** Shut off the new pricing rule only after multiple complete observation windows breach an explicit error threshold, and preserve the evidence used to make that decision. A single failed request, or even one ugly poll, is not enough: the kill switch needs minimum traffic, consecutive bad windows, a revision check, and an alert that records why automation acted.

For an e-commerce rollout, the practical target is narrower than "disable on errors." Measure requests that actually evaluated the new pricing rule, compare their failure rate with a declared threshold, and disable only that rule when the evidence repeats. Keep serving the established price path. This limits customer impact without turning a noisy monitor into a second source of incidents.

## Start with the incident record, not the threshold

The disable event is an operational record, not a celebratory log line. Capture the flag key, old and requested state, observed revision, threshold policy version, each contributing window, evaluated and failed counts, controller identity, decision timestamp, and a correlation identifier that connects the action to the alert. Avoid customer payloads; identifiers and aggregate counts are normally enough for this decision trail.

This is where I apply the same reflex learned from missed jobs and duplicate deliveries: a retried action must be safe, and its record must distinguish "requested" from "confirmed." The flag API should accept an idempotency key or compare-and-set revision. The controller should emit confirmation only after the update succeeds, while a duplicate request with the same evidence should leave the flag disabled rather than toggle it back on.

Alert on controller errors and on successful automatic disablement, but route them differently. A poll failure means the safety mechanism has lost visibility and needs attention. A confirmed disable means the guardrail acted; the on-call response is to validate customer recovery, preserve the evidence, and stop the rollout from being re-enabled casually. Google SRE's four golden signals provide a useful review frame here: errors are the trigger, while traffic and latency help explain whether the sample is representative and whether customer behavior improved after rollback. Saturation can expose a shared dependency problem that a feature-specific shutdown will not cure.

One line is too little.

For the pricing scenario, log the treatment identifier and rule version alongside the computed-price outcome, but keep the automatic predicate narrow. If checkout failures rise everywhere, disabling one pricing flag may be harmless yet ineffective. The controller's evidence should make that distinction visible before an operator draws a causal conclusion.

## How should the kill switch poll API errors before disabling a feature flag?

Poll an aggregate produced by the request path, not raw logs from whichever instance happens to answer. Each sample should cover a closed time window and contain at least the window end, evaluated request count, failed request count, and current flag revision. The controller can then make the same decision after a restart and an operator can reconstruct it later.

Use four gates:

1. The window is newer than the last processed window.
2. Enough requests evaluated the pricing rule to make the ratio meaningful.
3. The failure ratio crossed the configured threshold for several consecutive windows.
4. The flag still has the revision observed by the controller when it requests the change.

The third gate absorbs a brief spike. The fourth prevents a stale controller from overwriting a human decision made during the same incident. Keep the threshold and required streak in configuration, but review them as an operational policy; I'm not sure there is a universal ratio that fits both a high-volume catalog page and a low-volume administrative checkout flow. Traffic shape and the cost of a false shutdown decide it.

Don't count every bad response. A timeout or rejected dependency call attributable to the new pricing path belongs in its failure signal. A malformed request that both old and new paths reject usually does not. If attribution is missing, automatic action is premature — alert first and add the label that distinguishes treatment from control.

## Implement the controller as a small state machine

The following program polls a generic aggregate endpoint. It expects JSON shaped as `window_end`, `evaluated`, `failed`, and `flag_revision`, then sends a compare-and-set disable request containing the observed revision and an evidence record. The endpoint locations and bearer token come from environment variables, so the example does not assume a vendor or route convention.

The sample policy is deliberately explicit: at least 100 evaluated requests, a 5% failure ratio, and three consecutive bad windows. Those are example operating values, not universal recommendations. Start by replaying your own rollout traffic and choose values that bound both detection delay and false trips.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Sample struct {
	WindowEnd   time.Time `json:"window_end"`
	Evaluated   int64     `json:"evaluated"`
	Failed      int64     `json:"failed"`
	FlagRevision string   `json:"flag_revision"`
}

type DisableRequest struct {
	ExpectedRevision string    `json:"expected_revision"`
	Reason           string    `json:"reason"`
	WindowEnd        time.Time `json:"window_end"`
	Evaluated        int64     `json:"evaluated"`
	Failed           int64     `json:"failed"`
}

type Controller struct {
	client      *http.Client
	metricsURL  string
	flagURL     string
	token       string
	minRequests int64
	maxRatio    float64
	requiredBad int
	badStreak   int
	lastWindow  time.Time
}

func (c *Controller) poll(ctx context.Context) error {
	sample, err := c.fetchSample(ctx)
	if err != nil {
		return fmt.Errorf("poll aggregate: %w", err)
	}
	if !sample.WindowEnd.After(c.lastWindow) {
		return nil
	}
	c.lastWindow = sample.WindowEnd

	if sample.Evaluated < c.minRequests {
		c.badStreak = 0
		log.Printf("decision=hold reason=low_volume window_end=%s evaluated=%d",
			sample.WindowEnd.Format(time.RFC3339), sample.Evaluated)
		return nil
	}

	ratio := float64(sample.Failed) / float64(sample.Evaluated)
	if ratio < c.maxRatio {
		c.badStreak = 0
		log.Printf("decision=hold reason=below_threshold failure_ratio=%.4f", ratio)
		return nil
	}

	c.badStreak++
	log.Printf("decision=observe reason=threshold_breached failure_ratio=%.4f streak=%d",
		ratio, c.badStreak)
	if c.badStreak < c.requiredBad {
		return nil
	}

	req := DisableRequest{
		ExpectedRevision: sample.FlagRevision,
		Reason:           "pricing_rule_repeated_error_threshold",
		WindowEnd:        sample.WindowEnd,
		Evaluated:        sample.Evaluated,
		Failed:           sample.Failed,
	}
	if err := c.disable(ctx, req); err != nil {
		return fmt.Errorf("disable pricing flag: %w", err)
	}
	log.Printf("decision=disable revision=%s window_end=%s evaluated=%d failed=%d",
		sample.FlagRevision, sample.WindowEnd.Format(time.RFC3339), sample.Evaluated, sample.Failed)
	return nil
}

func (c *Controller) fetchSample(ctx context.Context) (Sample, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.metricsURL, nil)
	if err != nil {
		return Sample{}, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.client.Do(req)
	if err != nil {
		return Sample{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Sample{}, fmt.Errorf("unexpected poll status %d", resp.StatusCode)
	}
	var sample Sample
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&sample); err != nil {
		return Sample{}, err
	}
	if sample.Evaluated < 0 || sample.Failed < 0 || sample.Failed > sample.Evaluated {
		return Sample{}, errors.New("invalid aggregate counts")
	}
	return sample, nil
}

func (c *Controller) disable(ctx context.Context, body DisableRequest) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.flagURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected disable status %d", resp.StatusCode)
	}
	return nil
}

func mustInt64(name string) int64 {
	v, err := strconv.ParseInt(os.Getenv(name), 10, 64)
	if err != nil {
		log.Fatalf("%s must be an integer: %v", name, err)
	}
	return v
}

func mustFloat(name string) float64 {
	v, err := strconv.ParseFloat(os.Getenv(name), 64)
	if err != nil {
		log.Fatalf("%s must be a number: %v", name, err)
	}
	return v
}

func main() {
	c := &Controller{
		client:      &http.Client{Timeout: 5 * time.Second},
		metricsURL:  os.Getenv("METRICS_URL"),
		flagURL:     os.Getenv("FLAG_URL"),
		token:       os.Getenv("API_TOKEN"),
		minRequests: mustInt64("MIN_REQUESTS"),
		maxRatio:    mustFloat("MAX_FAILURE_RATIO"),
		requiredBad: 3,
	}

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := c.poll(ctx)
		cancel()
		if err != nil {
			log.Printf("alert=kill_switch_controller_error error=%q", err)
		}
		<-ticker.C
	}
}
```

There is an idempotency wrinkle. This in-memory streak resets when the process restarts, which delays a shutdown but does not create an unsafe one. For high-availability controllers, persist the last processed window and streak in a store that supports conditional updates, or elect a single active evaluator. Do not let three replicas turn one bad window into a three-window streak.

Also separate two error classes in the runbook. A pricing-path breach advances the kill-switch state machine. Failure to poll the aggregate or update the flag alerts the operator but must not be counted as evidence that the pricing rule itself is bad. Mixing them creates a feedback loop: a monitoring outage looks like a product regression, so the monitor changes production while blind.

## Verify, alert, and rehearse rollback before rollout

Test the state machine with recorded aggregates before granting it write access. The minimum useful matrix includes a bad window below minimum volume, two bad windows followed by a good one, three distinct bad windows, the same bad window delivered three times, a stale flag revision, a controller restart, and two controller instances seeing the same sample. Verify both state and emitted evidence for every case.

Then run it in observe-only mode. It should calculate and alert on the decision it would make without changing the flag. Compare those decisions with the rollout owner's judgment, tune the policy, and only then enable write access for the single pricing flag. This phase is not proof of future correctness, but it catches attribution mistakes and thresholds that chatter on ordinary traffic.

Rollback has two parts. Automatic disable returns evaluation to the established pricing path. Re-enabling the new rule is a separate, human-approved rollout after the triggering condition is understood and a fresh revision is deployed. Don't implement automatic re-enable from a few good windows; those windows occurred while the suspect rule was off and say nothing about its behavior.

The catch is that automatic shutdown is not suitable when failures cannot be attributed to treatment traffic, when the fallback path cannot handle full load, or when disabling the rule would violate a business or safety invariant. In those cases, keep the flag under human control, alert with the same evidence bundle, and use a runbook that names the decision owner. A manual process is slower, but an automatic decision based on ambiguous data is merely fast.

During the rehearsal, confirm the alert reaches a staffed destination, the evidence link resolves without privileged guesswork, the flag state matches the event, and dashboards show the control path taking over. Page only on conditions that require immediate action. Ticket lower-urgency drift such as repeated low-volume holds, because paging on every non-decision teaches responders to ignore the controller.

Stop there.

## References

- Google SRE Book, "Monitoring Distributed Systems": https://sre.google/sre-book/monitoring-distributed-systems/
- Logback Manual, "Appenders": https://logback.qos.ch/manual/appenders.html
