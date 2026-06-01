package scheduler_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/runtime/scheduler"
)

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
