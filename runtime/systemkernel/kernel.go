package systemkernel

import (
	"context"
	"errors"
	"sync"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

var (
	ErrClosed         = errors.New("systemkernel: closed")
	ErrAlreadyStarted = errors.New("systemkernel: already started")
	ErrInvalidUnit    = errors.New("systemkernel: invalid system unit")
	ErrExited         = errors.New("systemkernel: system unit exited")
)

// Kernel owns one exact SystemActor Unit. It has no managed AttemptKey and no
// dependency on Controller, Host, Platform callbacks, or capability minters.
type Kernel struct {
	mu      sync.Mutex
	unit    *actorrt.Unit
	closing bool
	started bool
	failed  chan error
	fatal   sync.Once
}

func New() *Kernel {
	return &Kernel{failed: make(chan error, 1)}
}

// Start adopts and starts exactly one prepared SystemActor Unit.
func (k *Kernel) Start(unit *actorrt.Unit) error {
	if k == nil || unit == nil ||
		unit.Self().ID() != actor.SystemActorID ||
		unit.Stat().Kind != actor.KindSystem ||
		unit.State() != actorrt.UnitPrepared {
		return ErrInvalidUnit
	}
	k.mu.Lock()
	if k.closing {
		k.mu.Unlock()
		return ErrClosed
	}
	if k.started {
		k.mu.Unlock()
		return ErrAlreadyStarted
	}
	if err := unit.InstallEventSink(k); err != nil {
		k.mu.Unlock()
		return err
	}
	k.unit = unit
	k.started = true
	k.mu.Unlock()

	if err := unit.Start(); err != nil {
		unit.Stop()
		<-unit.Done()
		k.release(unit)
		return err
	}
	if !unit.IsAlive() {
		unit.Stop()
		<-unit.Done()
		k.release(unit)
		return ErrInvalidUnit
	}
	return nil
}

func (k *Kernel) release(unit *actorrt.Unit) {
	k.mu.Lock()
	if k.unit == unit && !k.closing {
		k.unit = nil
		k.started = false
	}
	k.mu.Unlock()
}

// OnExited is the exit report. It is the only one: the Unit emits it from
// inside finish, before Done closes, so a second watcher blocking on Done could
// never reach fail first — and it would report a bare ErrExited, dropping the
// cause the event carries.
func (k *Kernel) OnExited(event actorrt.ExitedEvent) {
	k.mu.Lock()
	closing := k.closing
	current := event.Unit == k.unit
	k.mu.Unlock()
	if !closing && current {
		k.fail(errors.Join(ErrExited, event.Cause))
	}
}

func (*Kernel) OnObs(actorrt.UnitObsEvent) {}

func (k *Kernel) fail(cause error) {
	k.fatal.Do(func() {
		k.failed <- cause
		close(k.failed)
	})
}

// Failed reports an unexpected exact Unit exit. Platform decides the Channel
// close workflow; Kernel does not call back into it.
func (k *Kernel) Failed() <-chan error {
	if k == nil {
		return nil
	}
	return k.failed
}

func (k *Kernel) Close(ctx context.Context) error {
	if k == nil {
		return nil
	}
	k.mu.Lock()
	k.closing = true
	unit := k.unit
	k.mu.Unlock()
	if unit == nil {
		return nil
	}
	unit.Stop()
	select {
	case <-unit.Done():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (k *Kernel) Deliver(env *message.Envelope) error {
	k.mu.Lock()
	unit := k.unit
	closing := k.closing
	k.mu.Unlock()
	if closing || unit == nil {
		return actorhost.ErrNotHosted
	}
	return unit.Deliver(env)
}

func (k *Kernel) CancelRequest(id message.ID) {
	k.mu.Lock()
	unit := k.unit
	closing := k.closing
	k.mu.Unlock()
	if !closing && unit != nil {
		unit.CancelRequest(id)
	}
}

func (k *Kernel) Stat() (actorrt.UnitStat, bool) {
	k.mu.Lock()
	unit := k.unit
	closing := k.closing
	k.mu.Unlock()
	if closing || unit == nil || !unit.IsAlive() {
		return actorrt.UnitStat{}, false
	}
	return unit.Stat(), true
}

// IsRunning is the kernel's addressability answer: the routing organ asks it
// when a message is addressed to the system. It is not a membership query —
// the kernel has no record to look up.
func (k *Kernel) IsRunning() bool {
	k.mu.Lock()
	unit := k.unit
	closing := k.closing
	k.mu.Unlock()
	return !closing && unit != nil && unit.IsAlive()
}

func (k *Kernel) Incarnation() (actorrt.Incarnation, bool) {
	k.mu.Lock()
	unit := k.unit
	closing := k.closing
	k.mu.Unlock()
	if closing || unit == nil || !unit.IsAlive() {
		return actorrt.Incarnation{}, false
	}
	return unit.Self(), true
}

var _ actorrt.UnitEventSink = (*Kernel)(nil)
