package viewsync

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/channel"
)

// ApplyOutcome is the result of one Receiver.Apply call. It mirrors the
// branches of L1 §8.4 server apply rule.
type ApplyOutcome int

const (
	// ApplyOutcomeContiguous — the frame's seq advanced the cursor (it
	// was last_received_seq + 1, optionally followed by buffered
	// contiguous frames).
	ApplyOutcomeContiguous ApplyOutcome = iota

	// ApplyOutcomeDuplicate — the frame's seq <= last_received_seq;
	// INSERT OR IGNORE has run, cursor unchanged.
	ApplyOutcomeDuplicate

	// ApplyOutcomeGap — the frame's seq > last_received_seq + 1; the
	// row was persisted but cursor did NOT advance. A Resync should be
	// scheduled per L1 §8.5.
	ApplyOutcomeGap
)

// String returns a stable label for logging / tests.
func (o ApplyOutcome) String() string {
	switch o {
	case ApplyOutcomeContiguous:
		return "contiguous"
	case ApplyOutcomeDuplicate:
		return "duplicate"
	case ApplyOutcomeGap:
		return "gap"
	default:
		return "unknown"
	}
}

// ApplyResult is the full server-side apply outcome (kernel-typed so
// runtime/server callers can switch on it).
//
// DrainedMessages carries the messages whose cursor-acked state flipped
// during THIS Apply call when a gap closed (L1 §8.4 — "advance cursor
// and drain buffered contiguous frames"). Semantics:
//
//   - Outcome != ApplyOutcomeContiguous   → DrainedMessages == nil
//   - Plain contiguous (no buffered drain) → DrainedMessages == nil
//   - Gap-fill (≥1 buffered seq drained)   → DrainedMessages = [current
//     frame, ...newly drained buffered frames] in seq ASC order
//
// Callers fan-out as follows (gateway OnPush):
//
//	if len(res.DrainedMessages) > 0:        fan-out every drained
//	elif Outcome == ApplyOutcomeContiguous: fan-out current frame
//	else:                                   do nothing
//
// Ack still carries only the contiguous cursor (LastReceivedSeq).
type ApplyResult struct {
	Outcome         ApplyOutcome
	LastReceivedSeq LastReceivedSeq // post-apply cursor; what the ack frame should carry

	// DrainedMessages is populated only when a buffered gap closed in
	// this Apply call. Empty for plain contiguous / duplicate / gap.
	DrainedMessages []PushFrame
}

// Pusher is the daemon-side view-sync push contract. runtime/transit
// implements it on top of the persistent outbox + daemonbus client.
//
// Push is at-least-once: the implementation MUST persist the envelope
// to view_sync_outbox **inside the same transaction** as the messages
// row write (L1 §8.6). The kernel package only declares the contract.
type Pusher interface {
	// EnqueuePush hands one envelope+seq to the outbox for asynchronous
	// push to server. Returns immediately; failures are surfaced via
	// system.event (L1 §8.1.5 view_sync_failed monitoring) — caller
	// MUST NOT block on push completion.
	EnqueuePush(ctx context.Context, frame PushFrame) error

	// AckReceived is called by the daemonbus client when an ack frame
	// arrives from server. The implementation advances LastAckedSeq +
	// GCs outbox rows with seq <= ack.LastReceivedSeq (L1 §8.6).
	AckReceived(ctx context.Context, ack AckFrame) error
}

// Receiver is the server-side view-sync apply contract. server/viewcache
// implements it.
//
// Apply is the entry point for inbound `viewsync.push` frames. It MUST
// run the full L1 §8.4 transaction (INSERT OR IGNORE + cursor advance
// for contiguous arrivals + buffer drain) and only emit the ack AFTER
// COMMIT (L1 §8.4 + L1 §8.8 transactional boundary).
type Receiver interface {
	Apply(ctx context.Context, frame PushFrame) (ApplyResult, error)
}

// Resyncer pairs the two sides of the closed-interval Resync RPC (L1
// §8.5).
//
// ServeResync runs on the daemon side: server requests an interval,
// daemon returns the closed-interval messages from outbox + messages
// table.
//
// RequestResync runs on the server side: server-only call to request a
// resync from the daemon for a known gap.
type Resyncer interface {
	// ServeResync is invoked on the daemon side when a
	// viewsync.resync_request arrives via daemonbus.
	ServeResync(ctx context.Context, req ResyncRequest) (ResyncResponse, error)

	// RequestResync is invoked on the server side when the apply path
	// detects a gap (L1 §8.5 first row: server detects gap via
	// reconcile loop) or during cold start. The implementation sends a
	// viewsync.resync_request frame to the daemon and applies the
	// returned messages via Receiver.Apply.
	RequestResync(ctx context.Context, channelID channel.ID, since, until Seq) error
}
