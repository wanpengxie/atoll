package worker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/wanpengxie/ActOS/runtime/worker"
)

// TestBridgeFuncRunPropagates — BridgeFunc forwards its inputs to the
// wrapped function and propagates the returned error.
func TestBridgeFuncRunPropagates(t *testing.T) {
	want := errors.New("bridge failed")
	var called atomic.Bool

	bridge := worker.BridgeFunc(func(ctx context.Context, client *worker.IPCClient) error {
		called.Store(true)
		if ctx == nil {
			t.Error("bridge received nil ctx")
		}
		if client != nil {
			t.Error("expected nil client (test harness)")
		}
		return want
	})

	got := bridge.Run(context.Background(), nil)
	if !called.Load() {
		t.Error("BridgeFunc body not invoked")
	}
	if !errors.Is(got, want) {
		t.Errorf("Run returned %v want %v", got, want)
	}
}

// TestBridgeFuncSatisfiesBridge — BridgeFunc is the canonical Bridge
// adapter; assert the interface link statically + dynamically.
func TestBridgeFuncSatisfiesBridge(t *testing.T) {
	var b worker.Bridge = worker.BridgeFunc(func(context.Context, *worker.IPCClient) error {
		return nil
	})
	if err := b.Run(context.Background(), nil); err != nil {
		t.Errorf("nil-result bridge returned err: %v", err)
	}
}

// TestBridgeFuncRespectsCanceledContext — a bridge that consults ctx.Err
// should see the cancellation propagated through Run.
func TestBridgeFuncRespectsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	bridge := worker.BridgeFunc(func(ctx context.Context, _ *worker.IPCClient) error {
		return ctx.Err()
	})
	err := bridge.Run(ctx, nil)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run() err=%v want context.Canceled", err)
	}
}
