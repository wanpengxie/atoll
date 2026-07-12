package actorbase

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// Sys is the per-incarnation minted verb set — the "process's uid + fd table"
// (spec §1.1): the substrate welds identity into it before the Proc's first
// line ever runs, and nothing in this interface lets the holder self-report a
// different one. It is a SATISFYING MAP of the substrate's closed capability
// face (spec §1.2): every internal-primitive column of the verb table has
// exactly one Sys method — EXCEPT the identity-dimension variants
// (SubmitEnvelope/RespondEnvelope/AfterIdentity/CancelTimerIdentity/
// ResourceIdentity), which are a WithoutCancel + identity-bound reflection of
// the same columns for the off-process subject (gateway 期 S1). Sys defines
// NOTHING beyond those tables (additive only — a capability-face addition earns
// a new method; nothing here exists for a hypothetical future consumer).
//
// ctx PROVENANCE (the one rule every Proc author must internalise — owner
// 2026-07-04, "只修 Wait 修不了生态" is why this is a rule and not a magic
// parameter): 这单的活传 msg.Ctx(),命长的活传 sys.Life(),永远不传
// Background。Sys's own verbs need no ctx parameter (each already knows which
// ctx it writes under — Reply/Fail/Progress derive it from the Msg in hand,
// every other verb runs under sys.Life()); the ONE place a Proc author
// supplies a ctx explicitly is Pending.Wait, because a wait window is
// legitimately bounded by something OTHER than the process's own lifetime.
// Serve's sugar hides this entirely — its handler ctx arrives pre-bound to
// msg.Ctx(), so a Serve-shaped author needs the rule only if they drop to a
// raw Proc loop.
//
// FAN-OUT: Sys is concurrency-safe and Msg is immutable, so both may cross
// into a spawned goroutine legally — but the goroutine loses the mailbox's
// free serial-execution guarantee the moment it splits off; synchronising its
// own working state is the author's job, not Sys's.
type Sys interface {
	// --- Pen: response / terminal writes -----------------------------
	// Reply commits a final completed response for the request Msg in hand.
	Reply(msg Msg, v any) (message.ID, error)
	// Fail commits a final failed response, carrying the conventional
	// {error_code,detail} payload. This is the PRECISE error tier (spec
	// B-P2) — the counterpart to a bare `return err`, which Serve's sugar
	// maps to the generic internal_error tier instead.
	Fail(msg Msg, code, detail string) (message.ID, error)
	// Progress commits a non-final provisional response for the request Msg
	// in hand — the request stays Admitted, awaiting its eventual terminal.
	Progress(msg Msg, v any) (message.ID, error)

	// --- Pen: event write ---------------------------------------------
	// Emit writes a kind=event message. Events carry no closure obligation
	// (spec §1.5) — nothing in the in-station account ever waits on one.
	Emit(msgType string, payload any, audience ...actor.ActorID) (message.ID, error)

	// --- Pen: request write + caller closure ---------------------------
	// Call writes a kind=request message addressed to target and returns the
	// sealed ticket for its own out-station account entry.
	Call(target actor.ActorID, msgType string, payload any) (Pending, error)

	// --- State arm ------------------------------------------------------
	State() StateHandle

	// --- Access arm -------------------------------------------------
	Resource() ResourceHandle

	// --- Schedule arm ---------------------------------------------------
	// After arms a self-targeted timer that wakes this incarnation with a
	// self-authored message after d.
	After(d time.Duration, msgType string, payload any) (schedule.TimerID, error)
	CancelTimer(id schedule.TimerID) error

	// --- Spawn arm --------------------------------------------------
	// Fork mints a child owned by this incarnation, returning the child's
	// name only (never a live handle — the handle never leaves substrate).
	// config is the parent's opaque per-instance委托 for the child (the fork
	// counterpart of admission's InstanceSpec.Config — the argv/Args a parent
	// hands its child); substrate passes it through verbatim to the domain's
	// build table, never interpreting it. A daemon-hosted incarnation returns
	// ErrUnsupported (spec §3's known gap: fork is a cell-only capability in v1).
	Fork(class, nameHint string, config json.RawMessage) (actor.ActorID, error)
	DespawnChild(id actor.ActorID) error

	// --- ActorContext -----------------------------------------------
	// PublishObs pushes one opaque obs snapshot on the actor-source push
	// channel (actorrt.ObsWatcher's producer side). kind/val are opaque to
	// the substrate by design (spec: "泛型签名不收窄") — the substrate
	// forwards, it never interprets an actor's own operational vocabulary.
	PublishObs(kind actorrt.ObsKind, val actorrt.ObsValue) error
	// Self returns this incarnation's own identity.
	Self() actor.ActorID

	// --- Input stream -----------------------------------------------
	// Recv blocks for the next deliverable Msg. It returns an error when the
	// cell is dead or Stop has been requested — that error IS the loop
	// termination contract; a Proc's main loop ends by propagating it.
	Recv() (Msg, error)

	// --- Process life -------------------------------------------------
	// Life returns the process-life ctx — the one "long-lived" ctx this
	// package ever names (see the provenance rule above). It is done when
	// this incarnation's occupant forsakes the arena (return/panic), never
	// before.
	Life() context.Context

	// --- Identity-dimension variants (gateway 期 S1) ------------------
	// Four write/schedule verbs + one resource handle whose lifecycle promise
	//系于 IDENTITY (the log is truth), not this incarnation's serve projection:
	// any Proc may drive them, an off-process subject's door (gateway's human
	// driver) is the first动词级 consumer. Each mirrors its in-process sibling
	// but detaches ctx cancellation (WithoutCancel) so a write the live membrane
	// would still accept is not aborted by concurrent teardown.
	//
	// SubmitEnvelope receives a SPEC, not a finished envelope: the engine builds
	// and validates it (kind whitelist / visibility / audience). Returns
	// (message id, harness seq, err).
	SubmitEnvelope(spec behavior.SubjectWriteSpec) (message.ID, int64, error)
	// RespondEnvelope answers a request recovered from the log this incarnation
	// may never have Recv'd (cross-incarnation response — the serve account is
	// only a per-life projection: an entry closes if present, else zero action).
	RespondEnvelope(req *message.Envelope, spec behavior.ResponseSpec) (message.ID, error)
	// AfterIdentity arms an IDENTITY-bound durable timer (survives incarnations)
	// — the Bind value is the ONE difference from After. payload is carried
	// verbatim as RawMessage (never []byte→base64).
	AfterIdentity(d time.Duration, msgType string, payload json.RawMessage) (schedule.TimerID, error)
	CancelTimerIdentity(id schedule.TimerID) error
	// ResourceIdentity is Resource()'s WithoutCancel variant: the same access
	// membrane driven under a ctx detached from this incarnation's teardown.
	ResourceIdentity() ResourceHandle
}

// ErrUnsupported is returned by a Sys verb that a given host cannot honour —
// today only daemon-hosted Fork (spec §3's out-generation matrix: the daemon
// host mints via NewLiveArms, which has no local Runtime.Fork to call
// through). Not a typed-error constructor family (spec red line: zero typed
// error constructors) — one sentinel for "this host does not have this verb",
// tested with errors.Is.
var ErrUnsupported = errors.New("actorbase: verb unsupported on this host")

// ErrSelfCall is submit's fail-fast verdict for a Call/Submit addressed to the
// caller's OWN id (spec §1.3: "自 Call 自 = 在写请求/登记之前 fail-fast 返错,
// 零残留"): a single worker goroutine runs the Proc, so Call(Self()) followed
// by Wait would block that goroutine on a reply only the same, now-blocked
// goroutine could ever author — a structural deadlock. Rejected before any
// build/register/write, so it leaves zero out-station residue. Tested with
// errors.Is.
var ErrSelfCall = errors.New("actorbase: cannot Call self — single-worker deadlock")

// ErrRequestClosed is Reply/Fail/Progress's verdict for a request Msg whose
// in-station account entry already closed (deadline passed, cancel delivered,
// or a concurrent write already landed the terminal) BEFORE this write ran —
// spec §1.5's "late Reply" case. It is a local, host-side judgement; the pen's
// own terminal-uniqueness index underneath remains the final backstop
// regardless. Tested with errors.Is.
var ErrRequestClosed = errors.New("actorbase: request already closed")
