// Package viewsync declares the daemon→server view-sync protocol
// contract — frame schema, cursor types, and the Pusher / Receiver /
// Resyncer interfaces that runtime/transit (daemon side) and
// server/viewcache (server side) implement.
//
// Authoritative spec: L1 §8 (launch view sync complete state machine —
// outbox + cursor + ack + gap + resync).
package viewsync

import (
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/log"
)

// Seq is the per-channel monotonic sequence carried in viewsync frames.
// Identical to log.Seq — view-sync seq IS messages.seq (L1 §8.3).
type Seq = log.Seq

// LastPushedSeq is the daemon-side cursor tracking the highest seq
// that has been written into a `viewsync.push` frame but not yet acked
// (L1 §8.2). Aliased to Seq for type-safe call sites.
type LastPushedSeq = Seq

// LastAckedSeq is the daemon-side cursor tracking the highest seq for
// which the server has returned a `viewsync.ack` (L1 §8.2). It bounds
// the daemon outbox GC: rows with seq <= LastAckedSeq are safe to
// delete (L1 §8.6).
type LastAckedSeq = Seq

// LastReceivedSeq is the server-side cursor tracking the highest
// **contiguous** seq that has been durably applied + cursor-advanced
// (L1 §8.2 + §8.4 transactional apply). Returned to daemon as the
// ack water-mark.
type LastReceivedSeq = Seq

// Cursors bundles the three cursor values for one channel. Convenience
// type for transit-layer book-keeping; protocol does not require the
// three values to be passed together (each frame carries only what it
// needs — see frame.go).
type Cursors struct {
	ChannelID       channel.ID
	LastPushedSeq   LastPushedSeq
	LastAckedSeq    LastAckedSeq
	LastReceivedSeq LastReceivedSeq
}
