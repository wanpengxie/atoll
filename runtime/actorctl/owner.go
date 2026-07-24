package actorctl

import (
	"context"
	"sync"
)

type commandOwner struct {
	mu     sync.Mutex
	sealed bool
	wg     sync.WaitGroup
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
	o.sealed = true
	o.mu.Unlock()
	done := make(chan struct{})
	go func() {
		o.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
