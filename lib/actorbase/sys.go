package actorbase

import (
	"context"
	"errors"
	"time"

	"github.com/wanpengxie/atoll/lib/actorcaps"
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
// exactly ONE Sys method — there is no parallel, suffixed twin of any column.
// The off-process subject's drive face used to be exactly that (a set of
// Identity-suffixed variants); it collapsed back into this one table once the
// real distinction it carried was named where it belongs — on the Msg, as
// MsgOrigin, "which ledger authorises this write". Sys defines NOTHING beyond
// those tables (additive only — a capability-face addition earns a new method;
// nothing here exists for a hypothetical future consumer).
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
	// The Msg's origin picks the gate: a mailbox Msg is checked against the
	// serve ledger, a log Msg writes straight through (the log is its
	// authority; the harness is the backstop). Either way a successful — or
	// idempotently absorbed — write closes the serve ledger entry if one
	// exists.
	Reply(msg Msg, v any) (message.ID, error)
	// Fail commits a final failed response, carrying the conventional
	// {error_code,detail} payload. This is the PRECISE error tier (spec
	// B-P2) — the counterpart to a bare `return err`, which Serve's sugar
	// maps to the generic internal_error tier instead.
	//
	// The terminal REASON is derived here, not supplied: answering someone
	// else's request is a receiver failure (receiver_internal_error), while
	// failing a request this same identity SENT is the caller closing its own
	// account (unanswered_timeout) — and the latter additionally stamps
	// cancelled:true into the payload, the structured bit consumers use to
	// tell a deliberate close from a deadline that simply passed.
	Fail(msg Msg, code, detail string) (message.ID, error)
	// Progress commits a non-final provisional response for the request Msg
	// in hand — the request stays Admitted, awaiting its eventual terminal.
	// A log-origin Msg is a TERMINAL-ONLY handle and is refused here.
	Progress(msg Msg, v any) (message.ID, error)

	// --- Pen: event write ---------------------------------------------
	// Emit writes a kind=event message. Events carry no closure obligation
	// (spec §1.5) — nothing in the in-station account ever waits on one. It
	// takes the FULL event surface (own id, parent, correlation, visibility,
	// audience); the narrow "type + value + audience" shape is sugar and
	// lives in lib/behavior, not in the verb table.
	Emit(spec behavior.EventSpec) (message.ID, error)

	// --- Pen: request write, no caller closure -------------------------
	// Post writes a kind=request message and returns its id — nothing more.
	// It is Call's sibling on the OTHER side of the closure question: the
	// author takes on NO caller obligation, so there is no out-station ledger
	// entry, no author#2 timer, and no ticket to Wait on. Consequently it
	// neither resolves a default timeout (an absent ExpiresAt rides through
	// untouched, for the substrate to stamp its global TTL on) nor refuses a
	// self-addressed request (nothing here can deadlock a worker).
	//
	// Closure of a Posted request is the substrate's: the declared deadline is
	// truth and the expiry reaper enforces it.
	Post(spec behavior.RequestSpec) (message.ID, error)

	// --- Pen: request write + caller closure ---------------------------
	// Call writes a kind=request message addressed to target and returns the
	// sealed ticket for its own out-station account entry.
	Call(target actor.ActorID, msgType string, payload any) (Pending, error)

	// --- State arm ------------------------------------------------------
	State() StateHandle

	// --- Access arm -------------------------------------------------
	Resource() ResourceHandle

	// --- Schedule arm ---------------------------------------------------
	// After arms a self-targeted timer, in the storage home the caller names.
	// home is DURABILITY, not lifetime: both homes are ActorID-owned and both
	// survive body replacement (schedule/types.go says so in as many words) —
	// Memory lives in the current Channel/Scheduler instance's alarm set,
	// Durable in the Scheduler DB and so across a Scheduler restart. There is
	// no default: picking how long a reminder must survive is the caller's
	// declaration, not something to inherit from a sugar.
	After(d time.Duration, msgType string, payload any, home schedule.TimerHome) (schedule.TimerID, error)
	CancelTimer(id schedule.TimerID) error

	// --- Spawn arm --------------------------------------------------
	// Fork mints a child owned by this incarnation, returning the child's
	// name only (never a live handle — the handle never leaves substrate).
	// config is the parent's opaque per-instance委托 for the child (the fork
	// counterpart of admission's InstanceSpec.Config — the argv/Args a parent
	// hands its child); substrate passes it through verbatim to the domain's
	// build table, never interpreting it. Server and daemon incarnations use the
	// same lifecycle contract; the daemon arm relays this full spec over its port.
	Fork(requestID message.ID, spec actorcaps.ForkSpec) (actor.ActorID, error)
	// End commits this identity's lifecycle end and fences subsequent effects.
	End() error

	// --- ActorContext -----------------------------------------------
	// PublishObs pushes one opaque obs snapshot on the actor-source push
	// channel. kind/val are opaque to
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
}

// ErrUnsupported is returned by a Sys verb whose concrete host lacks the
// required capability (for example, server-hosted file-byte redemption). It is
// one sentinel rather than a typed-error constructor family and is tested with
// errors.Is.
var (
	ErrUnsupported     = errors.New("actorbase: verb unsupported on this host")
	ErrNotTimerMessage = errors.New("actorbase: message is not a timer fire")
)

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

// ErrMsgOriginUnset is every write verb's verdict for a Msg carrying the zero
// MsgOrigin — a Msg that never went through NewMsg (a zero-value discard that
// escaped into a live path). It is deliberately an ERROR arm and not a silent
// fallback: defaulting an unknown origin to the mailbox would let such a Msg
// sail through the serve-ledger gate on an id the ledger has never heard of,
// which is exactly the failure mode making origin mandatory was meant to kill.
// Tested with errors.Is.
var ErrMsgOriginUnset = errors.New("actorbase: msg origin unset — Msg was not built by NewMsg")

// ErrLogOriginTerminalOnly is Progress's verdict for a log-origin Msg: that
// handle exists to write ONE terminal and be dropped (see MsgOrigin), and a
// provisional is not a terminal. Nothing is written and no account is closed.
//
// This is misuse, not a race: behavior.Progress's tolerance for "the terminal
// already landed" serves a live occupant whose ledger gate passed a moment
// before another author's final — a genuine window. A log holder calling
// Progress has no such window to be caught in, so answering it with a fake
// success would hide the mistake instead of reporting it. Tested with errors.Is.
var ErrLogOriginTerminalOnly = errors.New("actorbase: log-origin Msg is a terminal-only write handle")
