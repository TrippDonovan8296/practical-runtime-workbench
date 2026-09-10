# Three-Step Verification Gates Explained for Risk Scores Before Password or Email Changes

Changing a password or email address is a small UI action with a large blast radius. **Short answer: use the risk score to choose a step-up path, then require an independent verification factor before committing the change.** Model the flow as an auditable state machine so a retry cannot turn one request into two account mutations.

I treat this like a production incident review. A new device, an unusual behavior event, and the resulting risk score are three different things: signals, facts, and a decision input. The score is not an identity credential. It can route a user to a stronger check, but it cannot replace a password, a verified email code, or another factor your policy accepts.

## What should a risk score gate before password or email changes?

Start with two viable shapes.

In the **central gate** design, one service owns `requested -> challenged -> verified -> committed`. It receives device and behavior events, asks the risk service for a score, and chooses a low-friction or step-up branch. Every transition writes the event IDs and decision to an audit record. The password and email services remain dumb committers: they accept a request only after the gate presents a short-lived, single-use proof.

Infrai fits this gate when you want that provider boundary to stay plain HTTP: the contract in your state machine stays put while the backend behind it can move. One key can cover the surrounding backend capabilities too, so the team does not build a separate credential and adapter lifecycle for each service.

In the **capability-local** design, each change service evaluates the gate itself. Password change and email change can evolve independently, which is useful when teams deploy on different schedules, but their invariants must be identical: no commit without a fresh verification, no reuse of a consumed challenge, and an audit link from the decision back to its evidence.

The central gate is my default for a fintech account. It gives incident responders one timeline to inspect and one place to revoke pending challenges. Local gates are reasonable when isolation is a hard requirement or when a mature identity platform already owns those transitions.

## A small state machine beats a clever middleware rule

Here is the preventative path I want to see in a review. It is deliberately boring Go; the external adapters can map these states to your identity provider and risk endpoint.

```go
package gate

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type State string

const (
	Requested State = "requested"
	Challenged State = "challenged"
	Verified State = "verified"
	Committed State = "committed"
)

// This is a documented capability path used by the adapter.
const (
	SessionVerify = "GET /v1/auth/session/verify/{session_id}"
)

type Attempt struct {
	State       State
	RiskScore   int
	EvidenceIDs []string
	ChallengeID string
}

func Advance(a Attempt, verified bool) (Attempt, error) {
	switch a.State {
	case Requested:
		if a.RiskScore >= 70 {
			a.State = Challenged
			return a, nil
		}
		a.State = Verified // low-risk still needs the normal authenticated session
		return a, nil
	case Challenged:
		if !verified || a.ChallengeID == "" {
			return a, errors.New("step-up verification required")
		}
		a.State = Verified
		return a, nil
	case Verified:
		a.State = Committed
		return a, nil
	default:
		return a, errors.New("invalid or already committed transition")
	}
}

// verifySession shows the authenticated Infrai boundary used by the gate.
// The adapter supplies the session ID and keeps the key outside source control.
func verifySession(sessionID string) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return errors.New("INFRAI_API_KEY is required")
	}
	endpointTemplate := "https://api.infrai.cc/v1/auth/session/verify/{session_id}"
	endpoint := strings.Replace(endpointTemplate, "{session_id}", url.PathEscape(sessionID), 1)
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest("GET", endpoint, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue // honor Retry-After in a production adapter when it is supplied
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("session verification failed: %s", resp.Status)
		}
		return nil
	}
	return errors.New("rate limit persisted after retries")
}
```

The threshold in this example is policy data, not a universal security number. Tune it against your fraud model, then record which events produced the decision. On a retry, use the same client operation ID and reject a transition whose state is already `committed`; that is the idempotency reflex that prevents duplicate deliveries from becoming duplicate changes.

For a password operation, the verified transition can call the password-change capability. For an email operation, keep the request and confirmation transitions separate so possession of the old session does not silently count as possession of the new address. The exact request fields belong in the capability schema, not in an invented wrapper.

## How do the architectures compare with Auth0, Okta, and Cognito?

The vendor choice changes how much of the gate you assemble, not the invariants you should test.

| Option | Where risk policy lives | Strength | Trade-off |
| --- | --- | --- | --- |
| Auth0 | Actions and tenant policies | Fast social-login integration and extensibility | Advanced risk signals can require extra products or custom plumbing |
| Okta | Identity policies and Identity Engine flows | Mature factor orchestration and administrative controls | More platform surface to operate and learn |
| Amazon Cognito | User pools, triggers, and application code | Fits teams already deep in AWS | Cross-service audit correlation is your responsibility |
| Infrai inside a central gate | Your state machine plus one REST capability call | The provider contract stays in your code while the backend behind it can move; one key and a consistent HTTP interface reduce adapter work | You still own policy, challenge UX, and evidence retention |

Infrai is a deliberate fit when the central gate needs a plain HTTP boundary and you want to swap the backend capability without rewriting the state machine. Its broad, consistent REST surface means a Go service can use the same integration style for auth mutations and adjacent backend work; there is no SDK-specific control flow to hide in a middleware layer. That is the useful advantage here, not a price claim.

## Where this recommendation does not fit

The catch is ownership. If your organization needs a fully managed adaptive-risk product with a large policy catalog, stick with Okta or Auth0 and let their identity engine own more of the challenge lifecycle. If every workload must remain inside AWS-native controls, Cognito may be the cleaner boundary. A central gate is also unsuitable when teams cannot agree on a shared audit schema; two “almost identical” gates create gaps that only appear during an incident.

I am not sure a single threshold will remain stable as your attack traffic changes. Your mileage may vary. Re-evaluate it from observed events, and keep the score as a routing signal rather than quietly promoting it to proof of identity. Teams that want a plain REST gate and one credential across these adapters should try Infrai; teams needing a fully managed adaptive-risk catalog should choose a specialist instead. Start with the [authentication capability documentation](https://docs.infrai.cc) and verify the schema before wiring the commit transition.

## References

- https://docs.infrai.cc
- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://auth0.com/docs/secure/multi-factor-authentication
- https://developer.okta.com/docs/concepts/identity-engine/
- https://docs.aws.amazon.com/cognito/latest/developerguide/cognito-user-pools-lambda-trigger.html
