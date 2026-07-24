package schedule

import (
	"context"
	"errors"
	"log/slog"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
	"github.com/wanpengxie/atoll/runtime/timerspec"
)

// TimerID is schedule's re-export of the engine-minted timer name — canonical
// definition lives in timerspec (a kernel-only leaf), downstream code names it
// through THIS package (the public face) without ever importing timerspec
// directly (archtest confines the raw contract leaf to the runtime tree). A Go
// type alias, not a new type: the two names are structurally identical, so a
// timerspec.TimerID and a schedule.TimerID are interchangeable at every call
// site.
type TimerID = timerspec.TimerID

var (
	ErrScheduleQuota = errors.New("schedule: schedule quota exceeded")
)

// TimerHome chooses the Scheduler storage home. It is not an actor lifecycle
// coordinate: both homes are owned by ActorID and cross actor replacements.
type TimerHome string

const (
	// TimerHomeDurable stores the timer in Scheduler DB.
	TimerHomeDurable TimerHome = "durable"
	// TimerHomeMemory stores the timer in the current Channel/Scheduler
	// instance's in-memory alarm set.
	TimerHomeMemory TimerHome = "memory"
)

// reservedTypePrefix is the fire-time-poison-row guard's INGRESS half: a Type
// starting with this prefix is permanently rejected by the harness's
// reserved-type step no matter how many times it is retried, so Schedule
// refuses to accept one at all — the structural half of "handle the main
// entrance so the poison-row disposal path only has to catch what slips past
// it". References the protocol's one home so this ingress half can never
// drift from the harness's authoritative gate on what "reserved" means.
const reservedTypePrefix = message.ReservedTypePrefix

// ScheduleReq is what an actor asks: "at FireAt, wake ME with this message."
// No target (self-targeted — a timer can only ever produce a message
// authored by the actor that scheduled it), no sender (welded by
// ScheduleHandle at fire time), no fire-time choice of kind (kind=event is
// welded): the only degrees of freedom are WHEN, WHICH Scheduler storage
// home, and WHAT the self-message says. Home is unrelated to actor
// AttemptKey or Incarnation.
type ScheduleReq struct {
	Home    TimerHome
	FireAt  int64 // UnixMilli, absolute instant (delay→FireAt conversion lives downstream, lib)
	Type    string
	Payload []byte
	// CorrelationID is the scheduler's current causal coordinate (empty is a
	// legal root — a spontaneous intent). The engine does not validate it —
	// substrate records the causal chain, never audits its truthfulness (same
	// posture as the pen not validating correlation on an ordinary write).
	CorrelationID string
}

// ScheduleHandle is the caps-injected access surface for the time channel —
// welded to one author at Mint (non-ambient, mirrors harness.Pen /
// accessdoor.AccessHandle). The cell (in-process) implementation and the port
// (cross-wire) twin speak the same contract over a wire frame — the interface
// is the transport-neutral contract both sides share.
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
	Ack(ctx context.Context, id TimerID) error
}

// Minter is the engine's caps-injection mint surface (same pattern as
// accessdoor/harness): the platform assembly root draws a per-author
// ScheduleHandle from here when it wires caps. Mint is deterministic and
// cheap (no per-handle state beyond the welded author), so admission points
// may Mint per-caller freely.
type Minter interface {
	AdmittedMinter
}

// AdmittedMinter consumes one completed ActorID collaboration admission.
// It is used by remote ingress; TimerHome remains only a Scheduler storage
// choice.
type AdmittedMinter interface {
	MintAdmitted(storespec.IdentityAdmission) ScheduleHandle
}

// FireSink is the injection-point contract for fire's single action: append
// one envelope AS author, through the full harness chain (pen-welded, same
// enforcement as any ordinary message — the emitSink mirror). The assembly
// root implements it by minting a pen per fire (Mint is cheap); the engine
// itself can never choose an author other than the row's author_id — author
// is a PARAMETER here, never read off env.
//
// TRI-STATE contract: a naive `_, err := pen.Write(...); return err`
// implementation would swallow the harness's DETERMINISTIC reject into a
// false "success" and let the engine delete the row, silently losing the
// fire — that failure mode is the entire reason this contract is spelled
// out. An implementation MUST translate harness.WriteResult into:
//   - a duplicate append (this fire's message.id already landed — crash
//     replay) → ErrDuplicateFire (treat as fired, complete the row deletion);
//   - any other non-empty RejectReason (a deterministic, retry-will-not-help
//     reject — reserved type, malformed shape, …) → FireRejected{Reason,
//     Detail} (poison row, disposed of);
//   - an empty RejectReason → nil (truth has landed);
//   - a genuine Go error (store fault, transport fault, …) → returned as-is
//     (transient — the engine leaves the row/entry in place and retries).
type FireSink interface {
	Append(ctx context.Context, author actor.ActorID, env *message.Envelope) error
}

// ErrDuplicateFire is the crash-replay idempotency signal: this timer's fire
// message is ALREADY in truth (a messages.id UNIQUE hit on the deterministic
// fire-message id). The engine treats it exactly like a successful append —
// complete the row/entry deletion. Sentinel: test with errors.Is.
var ErrDuplicateFire = errors.New("schedule: fire already appended")

// ErrBadSchedule is returned by Schedule for a structurally invalid request:
// Home outside the closed set, FireAt<=0, or an empty/reserved-prefixed Type.
// A PAST FireAt is legal (it fires immediately — refusing it would make "a
// millisecond before vs after the deadline" two different behaviours).
var ErrBadSchedule = errors.New("schedule: invalid schedule request")

// FireRejected is the deterministic-reject class surfaced by a FireSink
// implementation: the harness refused this envelope for a reason that will
// NOT change on retry (reserved type, malformed shape, …). Retrying it is a
// hot loop, not at-least-once delivery — disposal is delete-the-row + a loud
// log, never silent, never left to retry forever. Typed error: test with
// errors.As.
type FireRejected struct {
	Reason string
	Detail string
}

func (e FireRejected) Error() string {
	return "schedule: fire rejected by harness: " + e.Reason + " (" + e.Detail + ")"
}

// Deps bundles every collaborator the engine needs. New fail-fasts on every
// required dependency; Clock is required here
// so tests can never accidentally fall back to the wall clock — OpenScheduler,
// the runtime-root assembly seam, is the ONLY place that defaults a nil Clock
// to the real one).
type Deps struct {
	Store timerspec.TimerStore
	Fire  FireSink
	Clock Clock
	// Logger receives obs-plane diagnostics — most notably the loud disposal
	// log for a poison row/entry. nil → discard (same shape as
	// harness.Deps.Logger — the substrate does not invent its own logging
	// vocabulary).
	Logger *slog.Logger
}

// AssemblyDeps is runtime.OpenScheduler's (the runtime-root assembly seam)
// input — Deps minus Store: the durable TimerStore always comes from the
// channel's own ChannelStores (an unexported field there), never from the
// assembly-root caller (a raw TimerStore reachable downstream is a delayed
// forged-author write path around the pen). Fire/Host/Revive are still
// required (OpenScheduler forwards them into Deps unchanged and lets New's
// existing fail-fast checks reject a nil one — no duplicate validation here).
// Clock is the one field OpenScheduler DEFAULTS (nil → the real wall clock,
// NewSystemClock()): New itself stays fail-fast on a nil Clock so a test that
// constructs the engine directly (bypassing OpenScheduler) can never silently
// fall back to real time. Logger nil→discard is already handled by New, so it
// is simply forwarded.
type AssemblyDeps struct {
	Fire   FireSink
	Clock  Clock
	Logger *slog.Logger
}
