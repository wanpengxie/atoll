package schedule

import (
	"context"
	"errors"
	"log/slog"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/timerspec"
)

// TimerID is schedule's re-export of the engine-minted timer name — canonical
// definition lives in timerspec (a kernel-only leaf), downstream code names it
// through THIS package (the public face) without ever importing timerspec
// directly (archtest confines the raw contract leaf to the runtime tree,
// timer-build-spec.md §2 表). A Go type alias, not a new type: the two names
// are structurally identical, so a timerspec.TimerID and a schedule.TimerID
// are interchangeable at every call site.
type TimerID = timerspec.TimerID

// Bind is the closed set of lifecycle levels a timer's PRODUCT belongs to
// (§7.3 归属判准: "崩溃重启后这意图该不该还在"). It is a ROUTING choice, not a
// persisted tag: identity routes to the durable TimerStore row; incarnation
// routes to the engine's in-memory due-set welded to the current embodiment
// (v1.1 两个家). It names which level the intent is welded to, NOT who
// scheduled it (author is always the welded handle's identity — orthogonal,
// §9.1 Q2).
type Bind string

const (
	// BindIdentity: the intent survives a restart, cleared only by explicit
	// Cancel or by the author's deregister (cascading delete, same tx as the
	// other actor-scoped loci).
	BindIdentity Bind = "identity"
	// BindIncarnation: the intent is welded to the CURRENT live embodiment —
	// it dies the instant that embodiment dies (Despawn or process restart),
	// never persisted.
	BindIncarnation Bind = "incarnation"
)

// reservedTypePrefix is the fire-time-poison-row guard's INGRESS half (v1.2
// 8.8): a Type starting with this prefix is permanently rejected by the
// harness's reserved-type step no matter how many times it is retried, so
// Schedule refuses to accept one at all — the structural half of "handle the
// main entrance so the poison-row disposal path only has to catch what
// slips past it" (rule evolution during a durable timer's sleep, 8.8 二).
// References the protocol's one home so this ingress half can never drift
// from the harness's authoritative gate on what "reserved" means.
const reservedTypePrefix = message.ReservedTypePrefix

// ScheduleReq is what an actor asks: "at FireAt, wake ME with this message."
// No target (self-targeted, red line ❶ — a timer can only ever produce a
// message authored by the actor that scheduled it), no sender (welded by
// ScheduleHandle at fire time), no fire-time choice of kind (kind=event is
// welded, 拍点 8.3): the only degrees of freedom are WHEN, WHICH lifecycle
// level, and WHAT the self-message says.
type ScheduleReq struct {
	Bind    Bind
	FireAt  int64 // UnixMilli, absolute instant (delay→FireAt conversion lives downstream, lib)
	Type    string
	Payload []byte
	// CorrelationID is the scheduler's current causal coordinate (empty is a
	// legal root — a spontaneous intent). The engine does not validate it
	// (§1.4/拍点 8.6: substrate records the causal chain, never audits its
	// truthfulness — same posture as the pen not validating correlation on
	// an ordinary write).
	CorrelationID string
}

// ScheduleHandle is the caps-injected access surface for the time channel —
// welded to one author at Mint (non-ambient, mirrors harness.Pen /
// accessdoor.AccessHandle). The cell (in-process) implementation lands this
// period; the port (cross-wire) twin speaks the same contract over a wire
// frame (接线期, §7.5) — the interface is the transport-neutral contract both
// sides share.
type ScheduleHandle interface {
	Schedule(ctx context.Context, req ScheduleReq) (TimerID, error)
	// Cancel is a no-op (existed=false, no error) for an id that has already
	// fired, never existed, or belongs to a different author — Cancel never
	// leaks whether some OTHER author's timer exists. It is deliberately
	// ack-less (error-only): a Cancel racing an already-due, in-flight fire
	// may still see that fire land as truth (deadline race, accepted — see
	// engine.cancel doc), so "Cancel returned nil" is never a promise that
	// the timer will not ring.
	Cancel(ctx context.Context, id TimerID) error
}

// Minter is the engine's caps-injection mint surface (期3/期4 accessdoor/
// harness 同款模式): the platform assembly root draws a per-author
// ScheduleHandle from here when it wires caps. Mint is deterministic and
// cheap (no per-handle state beyond the welded author), so admission points
// may Mint per-caller freely.
type Minter interface {
	Mint(author actor.ActorID) ScheduleHandle
}

// FireSink is the injection-point contract for fire's single action: append
// one envelope AS author, through the full harness chain (pen-welded, same
// enforcement as any ordinary message — the emitSink mirror). The assembly
// root implements it by minting a pen per fire (Mint is cheap, pen.go 注释
// 明言); the engine itself can never choose an author other than the row's
// author_id (red line ❹) — author is a PARAMETER here, never read off env.
//
// TRI-STATE contract (v1.2 双线审撞车项, A 档验收断言 — a naive
// `_, err := pen.Write(...); return err` implementation would swallow the
// harness's DETERMINISTIC reject into a false "success" and let the engine
// delete the row, silently losing the fire — that failure mode is the entire
// reason this contract is spelled out. An implementation MUST translate
// harness.WriteResult into:
//   - a duplicate append (this fire's message.id already landed — crash
//     replay) → ErrDuplicateFire (treat as fired, complete the row deletion);
//   - any other non-empty RejectReason (a deterministic, retry-will-not-help
//     reject — reserved type, malformed shape, …) → FireRejected{Reason,
//     Detail} (poison row, disposed per 拍点 8.8);
//   - an empty RejectReason → nil (truth has landed);
//   - a genuine Go error (store fault, transport fault, …) → returned as-is
//     (transient — the engine leaves the row/entry in place and retries).
type FireSink interface {
	Append(ctx context.Context, author actor.ActorID, env *message.Envelope) error
}

// ErrDuplicateFire is the crash-replay idempotency signal: this timer's fire
// message is ALREADY in truth (a messages.id UNIQUE hit on the deterministic
// fire-message id, 命题8). The engine treats it exactly like a successful
// append — complete the row/entry deletion. Sentinel: test with errors.Is.
var ErrDuplicateFire = errors.New("schedule: fire already appended")

// ErrBadSchedule is returned by Schedule for a structurally invalid request:
// Bind outside the closed set, FireAt<=0, an empty or reserved-prefixed
// Type, or (bind=incarnation) an author with no live embodiment to weld to
// right now. A PAST FireAt is legal (it fires immediately — refusing it would
// make "a millisecond before vs after the deadline" two different
// behaviours, §3.2 钉5).
var ErrBadSchedule = errors.New("schedule: invalid schedule request")

// FireRejected is the deterministic-reject class surfaced by a FireSink
// implementation: the harness refused this envelope for a reason that will
// NOT change on retry (reserved type, malformed shape, …). Retrying it is a
// hot loop, not at-least-once delivery — disposal is delete-the-row + a loud
// log (拍点 8.8), never silent, never left to retry forever. Typed error:
// test with errors.As.
type FireRejected struct {
	Reason string
	Detail string
}

func (e FireRejected) Error() string {
	return "schedule: fire rejected by harness: " + e.Reason + " (" + e.Detail + ")"
}

// LivenessProbe is the engine's read-only window into actorrt's addressing
// map — it serves the incarnation-bind home TWICE, at two different times,
// for two different questions:
//
//   - CurrentIncarnation, at Schedule time: the ATTACH — weld the new
//     in-memory entry to whichever embodiment is live for author RIGHT NOW
//     (拍点 8.4, authority self-read — an incarnation is never
//     caller-reported, never serialised, §5.2/§5.3). *actorrt.Runtime
//     satisfies this directly.
//   - IsLive, at fire time: the DROP check — a since-replaced or since-dead
//     embodiment reads false by POINTER identity (ABA-safe, §5.3), and the
//     engine drops the entry instead of firing (a same-id successor being
//     live does NOT rescue a predecessor's timer).
type LivenessProbe interface {
	CurrentIncarnation(id actor.ActorID) (actorrt.Incarnation, bool)
	IsLive(inc actorrt.Incarnation) bool
}

// Reviver is the identity-fire activation seam (§7.7's lazy-activation entry
// point's first real producer, 拍点 8.2): a wake with no live actor is the
// NORMAL restart path (overdue fires run before the eager reconcile ring
// re-mints the always-on set), and append has no backfill, so firing without
// reviving first would silently lose the wake. EnsureLive MUST be idempotent
// for an already-live author (SpawnIfAbsent semantics — the platform
// implementation wraps actorrt.Runtime.SpawnIfAbsent + a builder factory,
// §7.5, not this package's concern).
//
// TWO-CLASS error contract (mirrors FireSink's tri-state, same rationale —
// "a deterministic failure retried forever is a hot loop, not
// at-least-once"). An implementation MUST distinguish:
//   - permanently unrevivable (the author's class is gone from the Builder
//     table, the id can never resolve to a build closure, …) →
//     ReviveRejected{Reason, Detail} — the engine disposes the row per 拍点
//     8.8 (delete + loud log), because a row that can never revive is a
//     poison row: left in place it retries hot forever AND, once such rows
//     fill a due page (≥ dueBatchLimit with the oldest fire_at), they starve
//     every later-due legitimate row behind them.
//   - transient (host busy, momentary spawn failure, …) → any other error —
//     the row stays, retried next tick (at-least-once, current semantics).
type Reviver interface {
	EnsureLive(ctx context.Context, id actor.ActorID) error
}

// ReviveRejected is the deterministic-failure class a Reviver surfaces from
// EnsureLive: retrying can NEVER succeed (rule evolution during a durable
// timer's sleep — the 8.8 scenario's identity-family twin). Typed error:
// test with errors.As. FireRejected 同款母板.
type ReviveRejected struct {
	Reason string
	Detail string
}

func (e ReviveRejected) Error() string {
	return "schedule: revive rejected: " + e.Reason + " (" + e.Detail + ")"
}

// Deps bundles every collaborator the engine needs. New fail-fasts on any
// missing (Store/Fire/Host/Revive/Clock ALL required — Revive is not an
// increment, it is the reason a timer exists at all, 拍点 8.2; Clock is
// required here so tests can never accidentally fall back to the wall clock
// — OpenScheduler, the runtime-root assembly seam, is the ONLY place that
// defaults a nil Clock to the real one, v1.2 codex-minor: "两层各守各的").
type Deps struct {
	Store  timerspec.TimerStore
	Fire   FireSink
	Host   LivenessProbe
	Revive Reviver
	Clock  Clock
	// Logger receives obs-plane diagnostics — most notably 拍点 8.8's loud
	// disposal log for a poison row/entry. nil → discard (harness.Deps.Logger
	// 同款母板 — the substrate does not invent its own logging vocabulary).
	Logger *slog.Logger
}

// AssemblyDeps is runtime.OpenScheduler's (the runtime-root assembly seam,
// §3.4) input — Deps minus Store: the durable TimerStore always comes from
// the channel's own ChannelStores (an unexported field there), never from
// the assembly-root caller (red line ❻ — a raw TimerStore reachable
// downstream is a delayed forged-author write path around the pen). Fire/
// Host/Revive are still required (OpenScheduler forwards them into Deps
// unchanged and lets New's existing fail-fast checks reject a nil one — no
// duplicate validation here). Clock is the one field OpenScheduler DEFAULTS
// (nil → the real wall clock, NewSystemClock()): New itself stays fail-fast
// on a nil Clock so a test that constructs the engine directly (bypassing
// OpenScheduler) can never silently fall back to real time (v1.2
// codex-minor "两层各守各的默认"). Logger nil→discard is already handled by
// New, so it is simply forwarded.
type AssemblyDeps struct {
	Fire   FireSink
	Host   LivenessProbe
	Revive Reviver
	Clock  Clock
	Logger *slog.Logger
}
