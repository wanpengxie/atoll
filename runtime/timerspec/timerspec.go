package timerspec

import (
	"context"

	"github.com/wanpengxie/ActOS/protocol/actor"
)

// TimerID names one pending timer. It is a RUNTIME-level name (control-plane),
// deliberately NOT a protocol type: pending intent is mutable control-plane
// state, and the proto ontology {channel, actor, resource, message, access} is
// closed — a timer that fires becomes an ordinary message; one that is
// cancelled never existed as truth.
//
// MINTED BY THE ENGINE, never caller-supplied, never reused (v1.2 双线审
// blocker): the fire message ID derives deterministically from TimerID, so a
// reused TimerID would let an old fire's messages.id UNIQUE swallow a NEW
// timer's legitimate fire. ScheduleReq has no ID field (caller-supply is
// unrepresentable at compile time); the engine mints a fresh uuid per
// Schedule, so ID uniqueness is by construction, not by table constraint
// (the PRIMARY KEY only guards concurrently-pending rows).
type TimerID string

// TimerRow is one pending IDENTITY-level timer — control-plane intent, NEVER
// truth. This store holds ONLY the durable half of the time axis: intent keyed
// by a durable name (author identity), surviving restarts until deregister.
// Incarnation-bind timers are NOT rows and never will be — they live in the
// schedule engine's memory, welded to the live embodiment, and vanish with the
// process (v1.1 历史校准: BEAM in-VM timers / Orleans in-activation Timers /
// POSIX timers on task_struct — ephemeral intent lives in ephemeral memory;
// a durable account for a must-die thing is half a token, 已拔).
//
// author_id is the identity that scheduled it AND the welded author of the
// fired message (self-targeted: there is no target field, structurally — a
// timer can only ever produce a message authored by the actor that scheduled
// it). Incarnation is NOT here (§5.2: not serialisable) — and with v1.1 there
// is no bind column either: everything in this table is identity-bind by
// construction (structure IS the bind, 同 §12.9 scope-由结构表达 的手法).
type TimerRow struct {
	ID            TimerID
	AuthorID      actor.ActorID
	FireAt        int64  // UnixMilli(仓库时戳纪律)
	Type          string // fire envelope 的 type(domain 词汇,opaque)
	Payload       []byte // fire envelope 的 payload(opaque)
	CorrelationID string // schedule 时捕获的因果坐标;fire envelope 继承(红线❺)
	CreatedAt     int64
}

// TimerStore is the durable pending table. It trusts its caller (the schedule
// engine welds author; mirrors storespec's store-not-validate discipline) and
// is CONFINED to the runtime tree: a raw TimerStore reachable downstream would
// let anyone insert a row with a forged author_id — a delayed forged-sender
// write path around the pen (红线❻).
type TimerStore interface {
	Insert(ctx context.Context, row TimerRow) error
	// Delete removes one pending row (fire completion / cancel / drop);
	// deleting an absent row reports existed=false, not an error (Cancel
	// after fire is a no-op — fired truth is not retractable).
	Delete(ctx context.Context, id TimerID) (existed bool, err error)
	// Due returns rows with FireAt <= now, ordered by FireAt, capped at limit.
	Due(ctx context.Context, now int64, limit int) ([]TimerRow, error)
	// NextFireAt returns the earliest pending FireAt (ok=false when empty) —
	// the poll/wake loop's sleep-until target.
	NextFireAt(ctx context.Context) (fireAt int64, ok bool, err error)
	// CancelOwned deletes id IFF its author matches — Cancel's non-ambient
	// check lives in the same statement (author is the WHERE clause, so a
	// handle can only ever cancel its own timers; a foreign/absent id is the
	// same existed=false, no existence leak).
	CancelOwned(ctx context.Context, id TimerID, author actor.ActorID) (existed bool, err error)
}
