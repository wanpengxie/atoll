package actorbase

import (
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// Hooks is the engine's out-of-band, per-host configuration (spec §3's
// out-generation matrix) — the two questions a call ledger cannot answer
// from inside the closed capability face alone. Both fields are OPTIONAL and
// nil means an honest, documented degrade, never a silent one.
type Hooks struct {
	// ResolveTarget expands a one-, two-, or three-segment actor target against
	// this channel's active roster before a request is written.
	ResolveTarget func(string) (actor.ActorID, error)

	// Canceller reaches the protocol-level cancel signal for one in-flight
	// outbound request (pending.Cancel's "经 Canceller 投递" half, spec
	// §1.5). Both production assembly paths fill it: server bodies wire it to
	// Home.CancelRequest, and daemon bodies use the current DaemonOutbound
	// bundle to forward a cancel-upstream frame over the actor's exact stream.
	// nil only occurs in tests or an unassembled stage; there Cancel still
	// commits the caller's own unanswered_timeout terminal and only the signal
	// to the receiver's in-station account is skipped.
	Canceller func(target actor.ActorID, requestID message.ID)

	// TimeoutResolver supplies the per-(target, request type) closure
	// deadline (author#2's ExpiresAt) for a Call/Submit that leaves its own
	// deadline unset. nil, or a false ok, falls back to DefaultTimeout — the
	// injection-point contract vs implementation-fill split (a describe
	// cache/catalog layer fills this once it exists; the engine itself
	// carries none).
	TimeoutResolver func(target actor.ActorID, reqType string) (time.Duration, bool)
}

// DefaultTimeout is the closure deadline (ExpiresAt) used when neither the
// caller nor Hooks.TimeoutResolver supplies one.
//
// A deadline is a SLIDING window, one semantic everywhere: span = expires_at −
// ts, and the request is unanswered when span elapses after its latest
// activity (the request itself, or the most recent provisional response
// written for it). Every progress the receiver writes restarts the window.
// The caller's out-station timer, the receiver's in-station timer and the
// substrate reaper all evaluate exactly this rule — the reaper from truth (the
// progress rows are on the ledger), the two timers from memory as its
// low-latency observers. So 5 minutes means "5 minutes of silence", not "5
// minutes of work": a turn that keeps reporting progress never times out.
const DefaultTimeout = 5 * time.Minute
