package adapter

import (
	"context"

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
	ExpiresAt     int64         // ms epoch (derived from MaxPendingMs)
	State         CorrelationState
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

// CorrelationTracker is the F2 interface (L2 §8.2). adapters/framework
// implements it on top of an `adapter_correlation` sqlite table; tests
// can swap in an in-memory implementation.
type CorrelationTracker interface {
	// Reserve creates a pending entry for an inbound request. Idempotent
	// by RequestID — if the entry already exists, returns it unchanged
	// (race-safe on duplicate Handle invocation). MUST run inside the
	// adapter's local transaction so reserve survives crash recovery.
	Reserve(ctx context.Context, e CorrelationEntry) (CorrelationEntry, error)

	// Get returns the entry by request_id (ok=false when absent).
	Get(ctx context.Context, requestID CorrelationKey) (CorrelationEntry, bool, error)

	// MarkDone advances an entry's state from pending to done after the
	// adapter emits the terminal response via Respond. Idempotent.
	MarkDone(ctx context.Context, requestID CorrelationKey) error

	// MarkExpired advances pending → expired when the F3 timer fires
	// the unanswered_timeout terminal. Idempotent.
	MarkExpired(ctx context.Context, requestID CorrelationKey) error

	// MarkRejected advances pending → rejected when harness returns a
	// reject for the adapter's response (e.g. terminal_duplicate
	// because another Respond won the race). Idempotent.
	MarkRejected(ctx context.Context, requestID CorrelationKey, reason string) error

	// ListPending returns every entry still in pending state. Used by
	// boot recovery (L2 §8.6 — re-arm timers) and operator
	// introspection (`adapter.<name>.pending.list`).
	ListPending(ctx context.Context) ([]CorrelationEntry, error)
}
