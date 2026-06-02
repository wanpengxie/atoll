package behavior

import (
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// CorrelationKey is the lookup key adapter framework uses to find an
// in-flight request — by L2 §8.2 it is the request envelope.id (the
// "request_id" wire form).
type CorrelationKey message.ID

// String returns the wire form.
func (k CorrelationKey) String() string { return string(k) }

// CorrelationEntry is a snapshot of one in-flight request inside the
// CorrelationTracker. Pure data — no methods so backends (sqlite /
// in-memory) stay free to choose row representation.
type CorrelationEntry struct {
	RequestID     CorrelationKey
	CorrelationID message.ID // envelope.correlation_id (informational)
	ChannelID     channel.ID
	AudienceActor actor.ActorID // sender actor of the response (the adapter actor id)
	ParentID      message.ID    // == RequestID (response.parent_id should equal RequestID)
	EnqueuedAt    int64         // ms epoch
	ExpiresAt     int64         // ms epoch (derived from MaxPendingMs). IMMUTABLE
	// after Reserve: it mirrors the append-only request envelope.expires_at and
	// is the tamper anchor several framework validation paths assert against, so
	// it is NEVER rewritten by a heartbeat.
	//
	// RearmedExpiresAt is the latest provisional-heartbeat-extended F3 deadline
	// (ms epoch), 0 when the request has never been re-armed. A provisional
	// liveness heartbeat re-arms the in-memory F3 timer (which is lost on daemon
	// restart); persisting the extended deadline here lets crash recovery re-arm
	// against the LIVE deadline instead of the stale original ExpiresAt, so a
	// still-alive long-running receiver is not force-failed at 1µs after a
	// restart (temporal R1 / temporal-termination-consistency.md §6.2). Kept
	// separate from ExpiresAt precisely so the immutable tamper anchor above
	// stays untouched. Always clamped to EnqueuedAt + ScheduleToCloseCeiling by
	// the writer, so it can never push total lifetime past the hard ceiling.
	RearmedExpiresAt int64 // ms epoch; 0 = never re-armed
	State            CorrelationState
}

// CorrelationState is the closed set tracking an in-flight request's
// lifecycle inside the framework (L2 §8.2).
type CorrelationState string

const (
	CorrelationPending  CorrelationState = "pending"
	CorrelationDone     CorrelationState = "done"     // terminal response emitted
	CorrelationExpired  CorrelationState = "expired"  // F3 timer fired terminal_failure
	CorrelationRejected CorrelationState = "rejected" // harness rejected the response
)

// The F2 CorrelationTracker interface (sqlite-backed pending table) is retired:
// adapterhost inlines correlation as a plain map owned by the cell goroutine
// (dismantle §1 — the mailbox IS the serialization, no lock, no store round
// trip). CorrelationEntry/State remain as the in-cell data shape.
