package timerspec

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/protocol/actor"
)

var (
	ErrScheduleQuota = errors.New("timerspec: schedule quota exceeded")
)

type DeathClass string

const (
	DeathFireRejected DeathClass = "fire_rejected"
)

// TimerID names one pending timer. It is a RUNTIME-level name (control-plane),
// deliberately NOT a protocol type: pending intent is mutable control-plane
// state, and the proto ontology {channel, actor, resource, message, access} is
// closed — a timer that fires becomes an ordinary message; one that is
// cancelled never existed as truth.
//
// MINTED BY THE ENGINE, never caller-supplied, never reused: the fire
// message ID derives deterministically from TimerID, so a reused TimerID
// would let an old fire's messages.id UNIQUE swallow a NEW
// timer's legitimate fire. ScheduleReq has no ID field (caller-supply is
// unrepresentable at compile time); the engine mints a fresh uuid per
// Schedule, so ID uniqueness is by construction, not by table constraint
// (the PRIMARY KEY only guards concurrently-pending rows).
type TimerID string

// TimerRow is one pending timer in the Durable Scheduler home — control-plane
// intent, NEVER truth. It is keyed by author identity and survives Scheduler
// process restarts until fire/cancel/rejection. An author may become inactive
// while an admitted Schedule is in flight; that ordinary stale intent is
// rejected and reaped at fire time. Memory-home timers are kept only in the
// current Channel/Scheduler instance and therefore have no row here. This
// storage distinction is unrelated to actor AttemptKey/Incarnation.
//
// author_id is the identity that scheduled it AND the welded author of the
// fired message (self-targeted: there is no target field, structurally — a
// timer can only ever produce a message authored by the actor that scheduled
// it). No actor-generation coordinate belongs here: every timer is
// ActorID-owned by construction.
type TimerRow struct {
	ID            TimerID
	AuthorID      actor.ActorID
	FireAt        int64  // UnixMilli, per repo-wide timestamp convention
	Type          string // fire envelope's type (domain vocabulary, opaque)
	Payload       []byte // fire envelope's payload (opaque)
	CorrelationID string // causal coordinate captured at schedule time; inherited by the fire envelope
	CreatedAt     int64
}

// TimerStore is the durable pending table. It trusts its caller (the schedule
// engine welds author; mirrors storespec's store-not-validate discipline) and
// is CONFINED to the runtime tree: a raw TimerStore reachable downstream would
// let anyone insert a row with a forged author_id — a delayed forged-sender
// write path around the pen.
type TimerStore interface {
	Insert(ctx context.Context, row TimerRow) error
	// Delete removes one pending row (fire completion / cancel / drop);
	// deleting an absent row reports existed=false, not an error (Cancel
	// after fire is a no-op — fired truth is not retractable).
	Delete(ctx context.Context, id TimerID) (existed bool, err error)
	// Due returns rows with FireAt <= now, fairly partitioned by author.
	Due(ctx context.Context, now int64) ([]TimerRow, error)
	MoveToDead(ctx context.Context, id TimerID, class DeathClass, reason, detail string, diedAt int64) (moved bool, evicted int, err error)
	// NextFireAt returns the earliest pending FireAt (ok=false when empty) —
	// the poll/wake loop's sleep-until target.
	NextFireAt(ctx context.Context) (fireAt int64, ok bool, err error)
	// ListOwned returns author's pending rows, earliest first — the durable
	// half of "what alarms do I have set". Author is the WHERE clause, the
	// same non-ambient posture as CancelOwned: a caller can only ever see the
	// rows it asked about, and an author with none gets an empty slice rather
	// than an error.
	ListOwned(ctx context.Context, author actor.ActorID) ([]TimerRow, error)
	// CancelOwned deletes id IFF its author matches — Cancel's non-ambient
	// check lives in the same statement (author is the WHERE clause, so a
	// handle can only ever cancel its own timers; a foreign/absent id is the
	// same existed=false, no existence leak).
	CancelOwned(ctx context.Context, id TimerID, author actor.ActorID) (existed bool, err error)
	// MarkFired closes one durable fire after the deterministic fire message
	// has passed the ordinary Harness path. Missing/already-fired rows are
	// idempotent success: Cancel may win after the due snapshot, and a crash
	// may leave the message committed before this marker is advanced.
	MarkFired(context.Context, TimerID) error
	AckOwned(context.Context, TimerID, actor.ActorID) (bool, error)
}
