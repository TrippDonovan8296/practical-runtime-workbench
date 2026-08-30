# When files must be deleted on a deadline: signed URL delivery or app-server downloads?

In short: keep exactly one copy of each document in private object storage, let the Node.js app server decide who may read it, and hand the browser a signed URL whose expiry can never outlive that document's deletion deadline. Streaming the bytes through the app tier is the simpler access-control story — the session is already in hand, the audit write is one line — and that simplicity is what breaks the deadline eight months later, because every hop that touches the bytes is another place a copy can survive.

The marketplace version of this problem is specific enough to design against: signed seller agreements and dispute settlements, retained for a fixed window, then deleted on a date the contract actually names. Not "we delete old data periodically". A date.

I run cron and queue infrastructure, so I read every delivery design through the sweep that has to clean up after it.

## The invariant hiding behind a deletion deadline

I've been paged for missed jobs and I've been paged for duplicate deliveries, and a retention sweep is exactly where those two failure modes meet. It must run. It must be safe to run twice. Most teams get the second property by accident and the first one never, because a job that quietly stops doing anything produces no errors at all.

Here is the sequence I plan for, and it is depressingly ordinary. The export feature ships in week one: a worker renders the agreement, the app server writes it to local disk, and an authenticated route streams it back. In month three someone adds a second instance, so the file moves to a shared volume. In month six a caching layer lands in front of the download route because the reports are large and the same buyer refreshes twice. In month nine legal signs a commitment with a deletion deadline, an engineer writes a sweep that deletes the database row and the object, ticks the box, and nobody asks the only question that matters: how many copies existed at the moment the sweep ran? The shared volume had one. The cache had one, keyed by a URL nobody was tracking. The nightly instance snapshot had both.

The invariant is narrower than "we deleted the file". A deletion deadline is enforceable only when one authoritative copy exists and every delivery path reads that copy instead of duplicating it.

Count your copies before you promise a date. That is the whole lesson.

## Should the app server stream the download, or hand out a signed URL for the file?

Both are defensible, and the axis is access control versus delivery simplicity — not cost, whatever the storage line item suggests.

Proxying gives you per-request policy. You can check that this buyer is still party to this dispute, refuse a download after an account suspension, watermark the PDF, and log the exact object served, all in one handler with the session in scope. What you pay is copies and event-loop time: a Node.js process holding a 40 MB stream open is a process not answering API calls, and every temp file, cache entry and snapshot along that path becomes something your retention sweep has to find.

A signed URL inverts both properties. One copy, no bandwidth through your tier, and the authorization decision collapses into a single moment — the moment you sign. After that the link is a bearer capability. Anyone holding it can fetch until it expires, and you cannot revoke it without deleting the object or rotating the signing credential, which nukes every other outstanding link too.

| Delivery path | Where access control lives | What retention has to chase |
| --- | --- | --- |
| App server streams the bytes | In the handler, on every request | The object, plus temp files, caches, instance disks and their backups |
| Signed URL, expiry clamped to the deadline | In the issuing endpoint, once per link | The object, plus any link still inside its TTL |
| Public bucket with an unguessable key | Nowhere | Everything, forever, including whatever crawled it |

The third row exists because it keeps showing up in incident reviews. An unguessable key is not access control; it is a password you email to people and then index in server logs.

## Clamping the link to the deadline

The interesting part of signed-URL delivery is not the signature, it's the expiry arithmetic. A ten-minute link issued four minutes before the deletion deadline is a promise you can't keep, and the failure is invisible until an auditor asks. So the issuing path clamps.

```go
package retention

import (
	"context"
	"errors"
	"time"
)

// Store is all the delivery path needs from object storage. Keeping the
// interface this narrow is what lets the sweep stay honest: one key in,
// one deletion out, and nowhere to stash a second copy.
type Store interface {
	PresignGet(ctx context.Context, key string, ttl time.Duration) (string, error)
	Delete(ctx context.Context, key string) error
}

type Document struct {
	ID       string
	TenantID string
	Key      string
	DeleteAt time.Time
	PurgedAt *time.Time
}

var ErrGone = errors.New("document is past its retention deadline")

const maxLinkTTL = 10 * time.Minute

// DownloadLink assumes the caller has already authorized this tenant for
// this document. It issues a link that cannot outlive the deadline.
func DownloadLink(ctx context.Context, s Store, d Document, now time.Time) (string, error) {
	if d.PurgedAt != nil || !now.Before(d.DeleteAt) {
		return "", ErrGone
	}
	ttl := maxLinkTTL
	if remaining := d.DeleteAt.Sub(now); remaining < ttl {
		ttl = remaining
	}
	return s.PresignGet(ctx, d.Key, ttl)
}

// Purge is safe to run twice. Deleting a key that is already gone is not
// an error, and PurgedAt is stamped only after the object is confirmed
// deleted, so a crash between the two leaves work to redo, never to skip.
func Purge(ctx context.Context, s Store, d Document) error {
	if d.PurgedAt != nil {
		return nil
	}
	return s.Delete(ctx, d.Key)
}
```

One detail that bites teams moving off a proxy: the browser's save-versus-render behavior comes from `Content-Disposition`, and with a signed URL you no longer control response headers at request time. You set them when you sign, through the response-header override parameters, or you accept that a PDF opens in a tab.

## What the sweep has to survive in production

Storage lifecycle rules look like the obvious mechanism, and they are a useful floor, but read their semantics before you build a commitment on them. Lifecycle expiration is expressed in days, so an hour-precision deadline has nowhere to live. Deletion is asynchronous: S3 documents that objects are queued for removal and may still be listed and billed briefly after a rule fires, and Google Cloud Storage warns that a lifecycle configuration change can take up to 24 hours to take effect. If your contract says a document is unreachable at a named hour, the enforceable answer is an application sweep that revokes access on time, with the storage rule behind it as a backstop that catches whatever the sweep missed.

Then make the sweep observable, because this is the failure I get paged for. A run that deletes nothing is byte-identical, from the outside, to a run that never started. Emit the candidate count, the deleted count and the age of the oldest unpurged document on every execution, and alert on the last one rather than on errors — errors are the easy case.

Test it with an injected clock and run the sweep twice against the same fixture. If the second pass throws because a key is already absent, the sweep is not idempotent, and a queue redelivery will page someone at 3 a.m.

Cost lands where you'd expect: duplicate copies cost storage forever while the requests are one-time, so the cleanup you skip is the bill you keep paying. And the honest team trade-off is that signed URLs move a debugging problem from a place you own to a place you don't. When a customer says a download failed, a proxy gives you a log line. A signed URL gives you a shrug and a request ID from someone else's edge.

## When this advice doesn't hold

A single-instance internal tool with files that never leave the box does not need any of it — put reports on disk, let the app serve them, and skip the whole subsystem. The catch with object storage plus signing is real operational surface: credential rotation, clock skew on expiry checks, CORS for browser fetches, and one more system in the retention runbook.

If every download must be individually authorized, watermarked, or logged with the exact bytes served, stick with proxying and accept that you now own a copy inventory. Regulated immutability inverts the problem entirely: when the requirement is that a document cannot be deleted early, you want object lock and legal hold, and a helpful cleanup job becomes the threat model rather than the control. For a few thousand small contracts, encrypted blobs in the primary database remove an entire moving part, and I'm not sure that's wrong at that size — your mileage may vary once the median file is a 50 MB scan bundle.

Whichever path a SaaS picks, the same question decides it. When the deletion deadline arrives, how many copies of the file exist, and does the sweep know about all of them?

## Sources

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
- https://cloud.google.com/storage/docs
- https://cloud.google.com/storage/docs/lifecycle
- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Disposition
