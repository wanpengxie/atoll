package schedule

import (
	"context"
	"errors"
	"log/slog"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/capauth"
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

// TimerInfo is one pending alarm as a READ answers it: the coordinates that
// say "which alarm, when, and what will it say", and nothing else. Payload is
// deliberately absent — an alarm's contents are the author's composed bytes,
// and listing alarms must not become reading their contents (a separate
// question deserving its own word if it is ever wanted).
type TimerInfo struct {
	ID        TimerID
	Home      TimerHome
	FireAt    int64 // UnixMilli
	Type      string
	CreatedAt int64
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
	// List returns every pending alarm belonging to THIS handle's welded
	// author, both homes merged, earliest first. Author is not a parameter for
	// the same reason it is not one on Schedule: there is structurally nowhere
	// for a caller to name a different one. An author with no alarms gets an
	// empty slice, never an error.
	List(ctx context.Context) ([]TimerInfo, error)
}

// Minter is the engine's caps-injection mint surface (same pattern as
// accessdoor/harness): the platform assembly root draws a per-author
// ScheduleHandle from here when it wires caps. Mint is deterministic and cheap
// (no per-handle state beyond the welded author), so admission points may Mint
// per-caller freely.
//
// The engine mints against a LIVE identity authority and nothing else: the
// returned handle runs that authority's one complete verdict at the door on
// every call — the same shell a local body keeps for its whole term and a
// remote ingress builds for one operation. TimerHome remains a Scheduler
// storage choice and never a caller-visible distinction.
type Minter interface {
	MintAuthority(capauth.Authority) ScheduleHandle
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
// so tests can never accidentally fall back to the wall clock. The Platform
// composition root is the only place that supplies the real clock default.
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
