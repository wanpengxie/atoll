package actorctl

import (
	"context"
	"sync"
)

type commandOwner struct {
	mu      sync.Mutex
	sealed  bool
	wg      sync.WaitGroup
	drained chan struct{}
}

func (o *commandOwner) begin() (func(), error) {
	o.mu.Lock()
	if o.sealed {
		o.mu.Unlock()
		return nil, ErrChannelClosing
	}
	o.wg.Add(1)
	o.mu.Unlock()
	var once sync.Once
	return func() { once.Do(o.wg.Done) }, nil
}

func (o *commandOwner) quiesce(ctx context.Context) error {
	o.mu.Lock()
	if !o.sealed {
		o.sealed = true
		o.drained = make(chan struct{})
		drained := o.drained
		go func() {
			o.wg.Wait()
			close(drained)
		}()
	}
	drained := o.drained
	o.mu.Unlock()

	// Once drained is true it remains true. Prefer that level over an already
	// expired caller context so repeated Close/Quiesce calls cannot turn a
	// completed owner join back into a timeout.
	select {
	case <-drained:
		return nil
	default:
	}
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		select {
		case <-drained:
			return nil
		default:
			return ctx.Err()
		}
	}
}
