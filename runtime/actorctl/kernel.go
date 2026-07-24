package actorctl

import (
	"context"
	"errors"
	"sync"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

type systemKernel struct {
	owner *ChannelActors
	unit  *actorrt.Unit

	mu      sync.Mutex
	closing bool
	fatal   sync.Once
}

func (k *systemKernel) adopt(unit *actorrt.Unit) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.closing {
		return ErrClosed
	}
	if k.unit != nil {
		return ErrAlreadyStarted
	}
	k.unit = unit
	return nil
}

func (k *systemKernel) release(unit *actorrt.Unit) {
	k.mu.Lock()
	if k.unit == unit && !k.closing {
		k.unit = nil
	}
	k.mu.Unlock()
}

func (k *systemKernel) startWatch(unit *actorrt.Unit) {
	go func() {
		<-unit.Done()
		k.mu.Lock()
		closing := k.closing
		current := k.unit == unit
		k.mu.Unlock()
		if !closing && current {
			k.fail(errors.New("actorctl: system kernel exited"))
		}
	}()
}

func (k *systemKernel) OnExited(event actorrt.ExitedEvent) {
	k.mu.Lock()
	closing := k.closing
	current := event.Unit == k.unit
	k.mu.Unlock()
	if !closing && current {
		k.fail(errors.Join(errors.New("actorctl: system kernel exited"), event.Cause))
	}
}

func (*systemKernel) OnObs(actorrt.UnitObsEvent) {}

func (k *systemKernel) fail(cause error) {
	k.fatal.Do(func() { k.owner.failStop(cause) })
}

func (k *systemKernel) close(ctx context.Context) error {
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

func (k *systemKernel) deliver(env *message.Envelope) error {
	k.mu.Lock()
	unit := k.unit
	closing := k.closing
	k.mu.Unlock()
	if closing || unit == nil {
		return actorhost.ErrNotHosted
	}
	return unit.Deliver(env)
}

func (k *systemKernel) cancelRequest(id message.ID) {
	k.mu.Lock()
	unit := k.unit
	closing := k.closing
	k.mu.Unlock()
	if !closing && unit != nil {
		unit.CancelRequest(id)
	}
}

func (k *systemKernel) stat() (actorrt.UnitStat, bool) {
	k.mu.Lock()
	unit := k.unit
	closing := k.closing
	k.mu.Unlock()
	if closing || unit == nil || !unit.IsAlive() {
		return actorrt.UnitStat{}, false
	}
	return unit.Stat(), true
}

func (k *systemKernel) incarnation() (actorrt.Incarnation, bool) {
	k.mu.Lock()
	unit := k.unit
	closing := k.closing
	k.mu.Unlock()
	if closing || unit == nil || !unit.IsAlive() {
		return actorrt.Incarnation{}, false
	}
	return unit.Self(), true
}
