package actorrt

import (
	"context"
	"errors"

	"github.com/wanpengxie/ActOS/kernel/actor"
)

// ErrUnknownSignal is returned by Controller.Raise for a SignalKind outside the
// substrate's closed set — the control vocabulary is substrate-owned and
// enforced, not a free string channel.
var ErrUnknownSignal = errors.New("actorrt: unknown signal kind")

// validSignalKind enforces the closed control vocabulary at the raise seam.
func validSignalKind(k SignalKind) bool {
	switch k {
	case SignalReload, SignalQuota, SignalStop:
		return true
	default:
		return false
	}
}

// SignalKind is the closed set of control signals — substrate-OWNED vocabulary,
// non-truth, delivered out-of-band to a unit's CONTROL LANE (separate from the
// work mailbox). The separate lane buys ONE concrete thing: a control signal
// PREEMPTS a deep work backlog (a responsive cell with a full inbox drains the
// control lane first). It is COOPERATIVE — it does NOT interrupt a Receive that
// is already executing, and a cell WEDGED inside Receive drains neither lane.
// In-process there is no forcible interruption of a running goroutine (Go has
// none); even Despawn's join blocks on a Receive that ignores its ctx. Forcible
// teardown therefore exists only at the process/port boundary (close the conn).
// Death is NOT in this set: death is the DELETED edge of presence (obs push),
// not a command to the unit.
//
// Adding a member = one more Kind here + one more case in the cell's control
// handler. The transport口 stays O(1) (à la Unix signal.h numbers / ioctl
// requests): a hundred members never mean a hundred methods.
type SignalKind string

const (
	// SignalReload commands the unit to reload its configuration (SIGHUP).
	SignalReload SignalKind = "reload"
	// SignalQuota commands a resource/flow-control adjustment (SIGXCPU).
	SignalQuota SignalKind = "quota"
	// SignalStop asks the unit to stop gracefully. Default disposition (no
	// Controllable) is self-cancel. Forcible teardown of a wedged unit is the
	// runtime's hosting power (Despawn/StopAll), not this cooperative signal.
	SignalStop SignalKind = "stop"
)

// Signal is one control-lane message: a closed-set Kind plus an opaque Payload
// the substrate never interprets (the actor reads it). It is NOT a
// message.Envelope — control does not share the work envelope's truth/harness
// semantics, only the runtime and its O(1) dispatch.
type Signal struct {
	Kind    SignalKind
	Payload []byte
}

// Controllable is the OPTIONAL hook an actor implements to handle control
// signals on its control lane. The runtime invokes OnControl serially on the
// cell goroutine, prioritized ahead of work — so the mailbox stays the sole
// serialization (one processor), but control jumps the work queue. An actor
// that does not implement Controllable gets the runtime DEFAULT DISPOSITION
// (Unix default action): SignalStop → cell self-cancels; others → ignored. This
// is complete delivery even with no catcher, exactly as a kernel delivers SIGHUP
// whether or not the program installs a handler.
type Controllable interface {
	OnControl(ctx context.Context, sig Signal)
}

// Controller is the privileged capability to raise a control signal at a hosted
// unit. Like Deliverer it is the confined egress New hands out EXACTLY ONCE —
// to the substrate / composition root, deliberately NOT a method on
// the broadly-shared *Runtime. So code that merely holds a *Runtime (to Spawn /
// address / Stat) CANNOT inject control, just as it cannot inject work.
type Controller interface {
	// Raise enqueues sig into id's control lane. Non-blocking: a full lane
	// returns ErrMailboxFull, an unhosted id returns ErrNotHosted, a stopped
	// unit ErrCellStopped.
	Raise(id actor.ActorID, sig Signal) error
}
