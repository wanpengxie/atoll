package scheduler_test

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coagent-ai/coagent/kernel/actor"
	"github.com/coagent-ai/coagent/kernel/message"
	"github.com/coagent-ai/coagent/runtime/scheduler"
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
