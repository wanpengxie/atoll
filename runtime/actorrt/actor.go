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
// lifecycle (Start/Stop) + obs (Observer); there is no control lane.
//
// EXCEPTION: the optional obs Observer hook (PublishObs/Observe) is the only
// surface invoked OUT-OF-BAND (concurrently with Receive, off the cell
// goroutine). An actor that exposes obs MUST keep the OBSERVED state
// concurrency-safe — a separate atomic/locked snapshot, copy-on-write, or
// immutable value it publishes — NOT its raw mailbox-confined work state. Work
// state stays lock-free; obs state is the one thing that crosses the goroutine
// boundary and must be guarded.
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
