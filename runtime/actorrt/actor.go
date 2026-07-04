package actorrt

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
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
	Stop(ctx context.Context) error
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
	// to the substrate's per-actor obs watchers (obs push/actor). Read-only,
	// non-truth, never a message — the substrate forwards it without
	// interpreting. No watcher → no-op. The actor pushes on a CHANGE it deems
	// worth surfacing (no timer); time-varying derivations are the consumer's.
	PublishObs(kind ObsKind, val ObsValue)
}
