package actorrt

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// Actor is the minimal contract the substrate requires of an actor
// implementation (actor-runtime-redesign.md §1.2). The runtime guarantees
// Receive is invoked serially by the cell's single goroutine, so an
// implementation MUST NOT add locks/atomics to its own logical state — the
// mailbox IS the serialization.
//
// There is deliberately no required call/cast/info/Tick: periodic behaviour
// is an actor scheduling a message to itself, not a substrate-imposed hook.
type Actor interface {
	// Receive processes exactly one envelope addressed to this actor.
	// Returning an error does NOT itself synthesise a terminal — closure
	// is the sender's responsibility (caller-scoped timer) and the
	// substrate's only obligation is the death signal. A returned error is
	// recorded for observability; a panic is caught by the cell and routed
	// to the Supervisor as a death signal.
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

// ActorContext is the handle the substrate hands an actor at Start. It
// exposes the actor's own identity and a self-send capability (so an actor
// can schedule work to itself, e.g. a timer fire) WITHOUT exposing any
// pointer into another actor's state.
type ActorContext interface {
	// Self returns this actor's id.
	Self() actor.ActorID
	// Deliver enqueues an envelope into this actor's own mailbox. It is the
	// only substrate-provided way for an actor to feed itself a message
	// (self-timer, relay lifecycle frame folded back in, etc.). It never
	// blocks indefinitely: a full mailbox returns ErrMailboxFull.
	Deliver(env *message.Envelope) error
}
