package scheduler_test

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/scheduler"
)

func TestDeliverer_Routes(t *testing.T) {
	d := scheduler.NewDeliverer()
	var hits sync.Map
	mkHit := func(id actor.ActorID) scheduler.HandlerFn {
		return func(_ context.Context, _ actor.ActorID, _ *message.Envelope) error {
			hits.Store(id, true)
			return nil
		}
	}
	d.Register("agent:a", mkHit("agent:a"))
	d.Register("agent:b", mkHit("agent:b"))

	env := &message.Envelope{ID: "m-1", Payload: json.RawMessage(`{}`)}
	if err := d.Deliver(context.Background(),
		[]actor.ActorID{"agent:a", "agent:b", "agent:missing"}, env); err != nil {
		t.Fatal(err)
	}
	if _, ok := hits.Load(actor.ActorID("agent:a")); !ok {
		t.Error("agent:a not hit")
	}
	if _, ok := hits.Load(actor.ActorID("agent:b")); !ok {
		t.Error("agent:b not hit")
	}
}

func TestDeliverer_ConcurrentRegisterDeliver(t *testing.T) {
	d := scheduler.NewDeliverer()
	env := &message.Envelope{ID: "m-race", Payload: json.RawMessage(`{}`)}

	audience := make([]actor.ActorID, 100)
	for i := range audience {
		audience[i] = actor.ActorID("agent:" + strconv.Itoa(i))
	}

	var hits atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup

	for _, id := range audience {
		id := id
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 50; j++ {
				d.Register(id, func(context.Context, actor.ActorID, *message.Envelope) error {
					hits.Add(1)
					return nil
				})
				if j%5 == 0 {
					d.Register(id, nil)
				}
			}
		}()
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 50; j++ {
				if err := d.Deliver(context.Background(), audience, env); err != nil {
					t.Errorf("Deliver: %v", err)
				}
			}
		}()
	}

	close(start)
	wg.Wait()
	_ = hits.Load()
}

// TestDeliverer_ReentrantDeliver exercises the Y13 deadlock: a handler that
// re-enters Deliver on the same goroutine while concurrent Register calls
// contend for the write lock. With the lock held across invocation, the inner
// RLock starves behind a queued writer that waits on the outer RLock. Snapshot-
// then-invoke must keep this deadlock-free; a watchdog fails the test instead
// of hanging the suite.
func TestDeliverer_ReentrantDeliver(t *testing.T) {
	d := scheduler.NewDeliverer()
	env := &message.Envelope{ID: "m-reentrant", Payload: json.RawMessage(`{}`)}

	var depth atomic.Int64
	d.Register("agent:outer", func(ctx context.Context, _ actor.ActorID, e *message.Envelope) error {
		// Re-enter on the same goroutine, simulating a synchronous
		// framework response routed back through the deliverer.
		if depth.Add(1) <= 3 {
			return d.Deliver(ctx, []actor.ActorID{"agent:outer"}, e)
		}
		return nil
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		// Concurrent writers queue on the write lock while reentrant
		// Deliver holds/releases read locks.
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				id := actor.ActorID("agent:w" + strconv.Itoa(n))
				for j := 0; j < 100; j++ {
					d.Register(id, func(context.Context, actor.ActorID, *message.Envelope) error { return nil })
					d.Register(id, nil)
				}
			}(i)
		}
		for k := 0; k < 200; k++ {
			depth.Store(0)
			if err := d.Deliver(context.Background(), []actor.ActorID{"agent:outer"}, env); err != nil {
				t.Errorf("Deliver: %v", err)
			}
		}
		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Deliver deadlocked (Y13)")
	}
}

func TestTimer_Tick(t *testing.T) {
	var count atomic.Int64
	tm, err := scheduler.NewTimer(scheduler.TimerConfig{
		Period: time.Millisecond,
		Scan: func(_ context.Context, _ int64) error {
			count.Add(1)
			return nil
		},
		NowFn: func() int64 { return 100 },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tm.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if count.Load() != 1 {
		t.Errorf("count = %d", count.Load())
	}
}

func TestRecoverer_Run(t *testing.T) {
	var called bool
	r, err := scheduler.NewRecoverer(func(_ context.Context, now int64) error {
		called = true
		return nil
	}, func() int64 { return 42 })
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Error("recover not called")
	}
}
