package actorrt

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// Actor is the minimal contract the substrate requires of an actor
// implementation. The runtime guarantees
// Receive (and the optional Start/Stop lifecycle hooks) are invoked serially by
// the cell's single goroutine, so WORK state needs no locks/atomics — the
// mailbox IS the serialization. The actor entry surface is work (Receive) +
// lifecycle (Start/Stop); there is no control lane.
//
// The one obs surface an actor may drive is the optional PUSH producer
// (ActorContext.PublishObs). The substrate forwards the published ObsValue by
// reference to watchers on other goroutines, so an actor MUST publish an immutable
// snapshot (ObsValue is immutable by contract) — NOT its raw mailbox-confined work
// state. Work state stays lock-free.
//
// There is deliberately no required call/cast/info/Tick and no self-send:
// an actor receives ONLY collaboration envelopes (through the harness→fanout
// path); any internal continuation is plain code on the cell goroutine, not a
// substrate-imposed hook or a self-delivered message.
type Actor interface {
	// Receive processes exactly one envelope addressed to this actor.
	// Returning an error does NOT itself synthesise a terminal — closure
	// is the sender's responsibility (caller-scoped timer) and the
	// substrate's only obligation is to publish the down edge
	// (death) for watchers. A returned error is recorded for observability;
	// a panic is caught by the cell and published as that death edge.
	Receive(ctx context.Context, env *message.Envelope) error
}

// Starter is the optional lifecycle hook for acquiring resources. The
// runtime invokes Start once, before the first Receive, on the cell
// goroutine.
type Starter interface {
	Start(ctx context.Context, self ActorContext) error
}

// Stopper is the optional lifecycle hook for releasing resources. The
// runtime invokes Stop once, after the mailbox is closed and the last
// in-flight Receive has returned, on the cell goroutine.
type Stopper interface {
	// Stop must be safe before Start, return promptly, and tolerate being called
	// at most once for an implementation that loses a construction race.
	Stop(ctx context.Context) error
}

// BuildFailure reports a deterministic actor-construction failure. PanicValue
// retains the recovered value and Stack captures the builder stack.
type BuildFailure struct {
	PanicValue any
	Stack      []byte
	NilActor   bool
}

func (e *BuildFailure) Error() string {
	if e.NilActor {
		return "actorrt: builder returned a nil actor"
	}
	return fmt.Sprintf("actorrt: builder panicked: %v", e.PanicValue)
}

func buildActor(build func(Incarnation) Actor, inc Incarnation) (impl Actor, err error) {
	defer func() {
		if v := recover(); v != nil {
			impl = nil
			err = &BuildFailure{PanicValue: v, Stack: debug.Stack()}
		}
	}()
	impl = build(inc)
	if impl == nil {
		return nil, &BuildFailure{NilActor: true, Stack: debug.Stack()}
	}
	return impl, nil
}

// abortBuild releases a never-started shell. Stop is deliberately outside all
// runtime locks and isolated because cleanup must not turn a CAS loss into a
// process-wide failure.
func abortBuild(c *cell) {
	c.cancel()
	if stopper, ok := c.impl.(Stopper); ok {
		func() {
			defer func() { _ = recover() }()
			_ = stopper.Stop(context.Background())
		}()
	}
}

// RequestCanceller is the optional occupant hook for the request-cancel signal
// (the "down" half of §1.4's three signal lines, cell-hosted twin of port's
// wire-crossing cancelRequest). cell.cancelRequest hands the id off to it in
// ONE HOP — dispatch is the runtime's job, disposition is the occupant's
// (mirrors port writing a KindCancel frame and leaving the remote to act on
// it). An occupant that does not implement RequestCanceller keeps the
// built-in per-request reqCtx cancellation (the fallback path test doubles
// rely on) — this interface is purely additive, it does not replace that
// path for non-implementers.
type RequestCanceller interface {
	CancelRequest(id message.ID)
}

// DownReporter is the optional occupant hook for the exit signal (the "up"
// half of §1.4's three signal lines): the cell's main loop adds a select arm
// on Dying(). A value read from it is the occupant's own exit code — nil
// (return nil) is quiet (dies without a down edge, no receiver_unavailable
// fanout), a non-nil error (return err or panic) is loud (down edge,
// author#3). An occupant that does not implement DownReporter leaves the
// arm disabled (a nil channel never fires in a select) and the cell's
// existing ctx.Done()/panic-recover death path is unchanged.
type DownReporter interface {
	Dying() <-chan error
}

// OccupantDriver is the optional occupant hook for off-process subject drive
// (第四条占用者缝; siblings: RequestCanceller / DownReporter / Stopper). It is
// the SYNCHRONOUS calling face an off-process subject's door (platform's
// HumanHandle) uses to drive the occupant's own capabilities — the metatool
// JobTable precedent: a cross-goroutine caller invokes the same engine whose
// ledgers are self-locking, never routed through the mailbox/workQ (no queue,
// no backpressure, no ack correlator). Concurrency safety is the pen/ledger
// locks' own contract.
//
// Every verb drives the occupant's OWN welded caps (pen/sched/access live on
// the cell, minted only at buildCaps — P2 能力取用不现铸), so WHEN-validity is
// the live-membrane rejection the caps already carry: cell dead/replaced →
// membrane sentinel error, no second liveness mechanism.
//
// The resource verbs are the D10 day-1 set (KV six + Share two), typed with
// runtime/accessdoor (a leaf of protocol/* + resourcespec — no cycle back
// into actorrt). They dispatch onto the REAL resource face, not a generic
// invoke (the door's Invoke structurally rejects op=create —
// ErrCreateViaInvoke); Open/CreateFile are NOT on this seam (a home-hosted
// occupant has no byte-redemption path day-1 — deferred with 债② file route).
type OccupantDriver interface {
	DriveWrite(spec DriveWrite) (message.ID, int64, error)
	DriveRespond(req *message.Envelope, spec DriveRespond) (message.ID, error)
	// DriveAfter arms an IDENTITY-bound durable timer (schedule.BindIdentity —
	// an occupant's reminder is a promise that outlives incarnations; the verb
	// semantics pick the Bind value, not the actor's category). The timer id
	// crosses this seam as a plain string: timerspec (the raw durable-store
	// contract) is archtest-confined to the runtime tree, and runtime/schedule
	// itself imports actorrt (a cycle) — implementors on either side re-wrap
	// into their own typed handle (schedule.TimerID shares the string底型).
	DriveAfter(d time.Duration, msgType string, payload []byte) (string, error)
	DriveCancelTimer(id string) error
	DriveResourceCreate(id resource.ResourceID, args []byte) (accessdoor.Outcome, error)
	DriveResourceRead(id resource.ResourceID) (accessdoor.Outcome, error)
	DriveResourceWrite(id resource.ResourceID, args []byte) (accessdoor.Outcome, error)
	DriveResourceDelete(id resource.ResourceID) (accessdoor.Outcome, error)
	DriveResourceStat(id resource.ResourceID) (accessdoor.StatResult, error)
	DriveResourceList(q accessdoor.ListQuery) (accessdoor.ListPage, error)
	DriveResourceShareActor(id resource.ResourceID, target actor.ActorID, ops []access.Operation) (accessdoor.Outcome, error)
	DriveResourceShareMembers(id resource.ResourceID, ops []access.Operation) (accessdoor.Outcome, error)
}

// DriveWrite is the occupant-drive write spec: the full envelope shape a
// subject may author (kind=request/event, visibility, parent, own ID,
// ExpiresAt) — deliberately WIDER than the engine's own submit/Emit sugar
// (which hardcode kind/visibility for in-process Proc ergonomics). Field
// validation (kind whitelist, visibility normalisation) is the implementor's
// job, not this DTO's.
type DriveWrite struct {
	ID         message.ID
	Type       string
	Kind       message.Kind
	Payload    json.RawMessage
	Audience   []actor.ActorID
	Visibility message.Visibility
	ParentID   message.ID
	ExpiresAt  *int64
}

// DriveRespond is the occupant-drive response spec for answering a request
// held in hand (the subject's Resolve/Cancel verbs behind the door's own
// from-log authorization).
type DriveRespond struct {
	Status  string // message.StatusCompleted / message.StatusFailed
	Reason  string
	Payload json.RawMessage
}

// ActorContext is the handle the substrate hands an actor at Start. It exposes
// the actor's own identity (Erlang self()) and the obs PUSH/producer end
// (PublishObs) — and nothing else: there is no self-send. A message reaches an
// actor ONLY through the harness→fanout collaboration path — the cell mailbox is
// the private egress of that path, not a channel an actor can inject into.
// Internal continuations are the actor's own concern (plain code on the cell
// goroutine), not a substrate-delivered message.
type ActorContext interface {
	// Self returns this actor's id.
	Self() actor.ActorID
	// PublishObs publishes an opaque, operational obs snapshot about this actor
	// to the substrate's population obs watchers (obs push/actor). Read-only,
	// non-truth, never a message — the substrate forwards it without
	// interpreting. No watcher → no-op. The actor pushes on a CHANGE it deems
	// worth surfacing (no timer); time-varying derivations are the consumer's.
	PublishObs(kind ObsKind, val ObsValue)
}
