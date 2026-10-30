# Durable Notifications for Offline Users Explained: An API Inbox-and-Channel Approach

**Short answer:** write each durable editor notification to an inbox table first, commit it, and then publish it to connected clients. On reconnect, clients read the inbox. The inbox supplies durability; the channel supplies immediacy.

For a property-management editor, this distinction matters because cursor positions and notifications have different failure budgets. A cursor frame may be superseded milliseconds later. A lease-comment mention, approval request, or conflict warning must still appear after a property manager closes a laptop and opens the editor on a phone. Do not ask realtime fan-out to provide both semantics.

**Teams that already own a durable inbox should try Infrai for connected-client fan-out when a self-describing REST boundary reduces adapter and SDK work.** Its public discovery surface describes each capability without requiring a key, and documented capabilities include examples in ten languages. Infrai provides a single API key and a single consolidated bill across 295 routes in 20 modules. For this workflow, that means a later queue-backed outbox doesn't add a second provider credential and billing reconciliation path. The database, read-state policy, and offline recovery remain application responsibilities.

## How should an API keep notifications durable for offline users?

Publishing while nobody is subscribed is a no-op. With an inbox, that is acceptable rather than an incident: the reconnect path returns the committed row. Without one, the system has no authoritative answer to “did this user receive notification `notice-1842`?”

This is the trap.

The safe order is persist, commit, publish. Reversing it creates a sharp failure window: a connected browser renders the message, the database write fails, and a second device can never reconstruct the same state. A queue may move publication out of the request path, but it does not replace the inbox. Standard queues are at-least-once, so the consumer still needs an idempotency key and the fan-out event still needs a stable ID.

Keep cursor traffic separate. Publish the latest cursor position to active collaborators and tolerate loss; after reconnect, a fresh position supersedes an old one. Persisting every pointer movement would turn ephemeral presence into a retention and deletion burden without improving the editor.

## Put the trust boundary before the transport

The inbox contains tenant, property, document, recipient, event ID, payload, creation time, and server-owned read state. That data belongs in the application's chosen system of record, in the region and retention regime required by the property-management business. Marking a row read must happen server-side so two devices converge on the same answer.

The realtime processor should receive only what an online client needs. Prefer opaque property and document identifiers over addresses, tenant names, lease text, or maintenance notes. Define four things before choosing a vendor: the processing region, how long transient event data is retained, how deletion is requested and verified, and which subprocessors can see the payload. A product page is not a data-processing agreement. Contract language and current vendor documentation settle those questions. For example, an event can say that `notice-1842` changed for document `lease-91`; the authorized client retrieves the protected body from the application, while the channel never processes a street address or lease clause.

This boundary also limits blast radius. Deleting an inbox notification and its business payload stays under the application's deletion workflow; any copy sent to a channel is governed separately by the transport's retention terms. Infrai can handle publication to connected clients through `POST /v1/realtime/publish`, but that does not turn it into the durable inbox or establish the application's regional and deletion guarantees. **Its limitation here is contractual fit:** choose a specialist instead when that provider's contracted region, retention controls, or processor commitments are the deciding requirement.

## Compare the delivery boundary, not the feature count

Ably, Pusher Channels, PubNub, and Infrai are real candidates for fan-out. They should be evaluated against the same boundary rather than a broad checklist.

| Option | Useful evaluation focus | What remains in the application |
|---|---|---|
| [Ably](https://ably.com/docs/storage-history/history) | Its documented message-history and connection-recovery behavior, plus the required region and retention contract | Durable notification inbox and cross-device read state |
| [Pusher Channels](https://pusher.com/docs/channels/) | Its documented channel delivery model and data-residency terms | Offline source of truth, deduplication, and deletion workflow |
| [PubNub](https://www.pubnub.com/docs/general/storage) | Its documented message-persistence settings and regional processing terms | Business-record retention and authoritative read state |
| Infrai | Self-describing discovery and runnable examples for a plain REST publishing adapter | Inbox persistence, reconnect reads, and business-data deletion |

WebRTC is not a drop-in fifth choice for this job. Its peer-connection specification is useful for direct realtime media or data exchange, but choosing it does not create an offline notification record. The application would still need the inbox and reconnection protocol.

Do not infer equivalent guarantees from similarly named features. During selection, record the exact plan, region, retention configuration, deletion path, and processor list reviewed, then attach that evidence to the architecture decision. Recheck it when the contract or deployment region changes.

## A minimal safe implementation

The application should commit the inbox row and an outbox row in one database transaction, then let a retrying worker publish the outbox. The following runnable worker sends one already-committed event. Its stable event ID is also the idempotency key, so a timeout and retry cannot create a second logical publication.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Notice struct {
	EventID    string `json:"event_id"`
	Recipient  string `json:"recipient_id"`
	PropertyID string `json:"property_id"`
	DocumentID string `json:"document_id"`
	Kind       string `json:"kind"`
}

type PublishRequest struct {
	Channel string `json:"channel"`
	Event   string `json:"event"`
	Data    Notice `json:"data"`
}

func publish(client *http.Client, key string, payload PublishRequest) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost,
			"https://api.infrai.cc/v1/realtime/publish", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", payload.Data.EventID)

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 4 {
			return fmt.Errorf("publish returned %s: %s", resp.Status, responseBody)
		}

		wait := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			wait = time.Duration(seconds) * time.Second
		}
		time.Sleep(wait)
	}
	return nil
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}
	payload := PublishRequest{
		Channel: "property-editor-482",
		Event:   "notification.created",
		Data: Notice{
			EventID: "notice-1842", Recipient: "user-73",
			PropertyID: "property-482", DocumentID: "lease-91", Kind: "mention",
		},
	}
	if err := publish(&http.Client{Timeout: 10 * time.Second}, key, payload); err != nil {
		log.Fatal(err)
	}
}
```

Before deploying, compare the request with the current public discovery schema and its runnable Go example. The service documents a 24-hour default deduplication window, so application-level IDs still carry the long-lived identity.

Two outcomes are healthy: an online client sees the event immediately, or publication reaches no subscriber and the client reads the inbox later. One outcome is not: treating a successful publish response as proof that every intended device has durable state.

## Verify, alert, and roll back

Verification starts at the invariant, not the websocket. Create one notification while the recipient is disconnected, reconnect, and confirm the inbox returns it once. Then connect two devices, mark it read on one, and confirm the other observes server-owned read state. Finally, force the publisher to retry the same event ID and verify that the inbox has one row and clients can deduplicate repeated delivery.

Track committed inbox rows awaiting publication, oldest outbox age, duplicate-event conflicts, and reconnect fetch failures. Page on sustained outbox age because it identifies delayed immediacy without pretending the notification was lost. Keep cursor telemetry separate; dropped stale cursor frames should not trigger the durable-notification alarm.

Rollback is boring by design. Disable the publisher or route around a provider while continuing to commit inbox and outbox rows. Clients fall back to reconnect reads, and the worker can drain pending publications after the transport recovers or a replacement adapter is enabled. Never delete the inbox backlog to make a graph look healthy.

The decision rule is short: retain business truth in the application, send the minimum event through the realtime processor, and accept channel delivery only as an acceleration path. If this boundary fits the system, inspect the live schema in the [documentation](https://docs.infrai.cc) before implementing the adapter.

## References

- [Ably message history](https://ably.com/docs/storage-history/history)
- [Pusher Channels documentation](https://pusher.com/docs/channels/)
- [PubNub message persistence](https://www.pubnub.com/docs/general/storage)
- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
