package actorrt

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// Actor is the minimal contract the substrate requires of an actor
// implementation. One Unit guarantees
// Receive (and the optional Start/Stop lifecycle hooks) are invoked serially by
// the Unit's single goroutine, so WORK state needs no locks/atomics — the
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
// path); any internal continuation is plain code on the Unit goroutine, not a
// substrate-imposed hook or a self-delivered message.
type Actor interface {
	// Receive processes exactly one envelope addressed to this actor.
	// Returning an error does NOT itself synthesise a terminal — closure
	// is the sender's responsibility (caller-scoped timer) and the
	// substrate's only obligation is to publish the down edge
	// (death) for watchers. A returned error is recorded for observability;
	// a panic is caught by the Unit and published as that exact exit edge.
	Receive(ctx context.Context, env *message.Envelope) error
}

// Starter is the optional lifecycle hook for acquiring resources. The
// Unit invokes Start once, before the first Receive, on its goroutine.
type Starter interface {
	Start(ctx context.Context, self ActorContext) error
}

// Stopper is the optional lifecycle hook for releasing resources. The
// Unit invokes Stop once, after the mailbox is closed and the last
// in-flight Receive has returned, on its goroutine.
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

// RequestCanceller is the optional actor hook for one request-cancel signal.
// Unit hands the ID to it in one hop; dispatch belongs to the Unit and
// disposition belongs to the actor. A remote endpoint provides the same
// semantic operation over its physical stream. An actor that does not
// implement RequestCanceller treats cancel as a best-effort no-op and leaves
// the request to its existing closure contract.
//
// This is one of the three occupant seams (siblings: DownReporter / Stopper).
// The former off-process-subject drive seam (OccupantDriver, 缝家族第四条) is
// GONE with the gateway 期: an off-process subject now drives its OWN Unit's
// identity-dimension Sys verbs through the subjectgate frame protocol, not a
// door-side synchronous call face.
type RequestCanceller interface {
	CancelRequest(id message.ID)
}

// DownReporter is the optional occupant hook for the exit signal (the "up"
// half of §1.4's three signal lines): the Unit's main loop adds a select arm
// on Dying(). A value read from it is the occupant's own exit code — nil
// (return nil) is quiet (dies without a down edge, no receiver_unavailable
// fanout), a non-nil error (return err or panic) is loud (down edge,
// author#3). An occupant that does not implement DownReporter leaves the
// arm disabled (a nil channel never fires in a select) and the Unit's
// existing ctx.Done()/panic-recover death path is unchanged.
type DownReporter interface {
	Dying() <-chan error
}

// ActorContext is the handle the substrate hands an actor at Start. It exposes
// the actor's own identity (Erlang self()) and the obs PUSH/producer end
// (PublishObs) — and nothing else: there is no self-send. A message reaches an
// actor ONLY through the harness→fanout collaboration path — the Unit mailbox is
// the private egress of that path, not a channel an actor can inject into.
// Internal continuations are the actor's own concern (plain code on the Unit
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
