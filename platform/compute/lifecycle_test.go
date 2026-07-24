package compute

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
)

type emptyComputePlan struct{}

func (emptyComputePlan) ApplyPlan([]platform.PlanActor) error { return nil }
func (emptyComputePlan) LookupExact(
	actor.ActorID,
	actorhost.AttemptKey,
	actorhost.ExecutionSpec,
) (platform.ActorFactory, bool) {
	return platform.ActorFactory{}, false
}

func TestRunComputeReturnsAfterBothForwardersExit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	storage := make(chan struct{})
	err := runCompute(ctx, Config{ServerWS: "ws://invalid", PlanSource: emptyComputePlan{}}, &computeLifecycleHooks{
		forwarderTimeout: time.Second,
		storageExited:    func() { close(storage) },
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-storage:
	default:
		t.Fatal("compute.Run returned before storage forwarder exited")
	}
}

func TestRunComputeForwarderTimeoutTransfersRootOwnership(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	entered, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var leaked atomic.Int64
	err := runCompute(ctx, Config{ServerWS: "ws://invalid", PlanSource: emptyComputePlan{}}, &computeLifecycleHooks{
		forwarderTimeout: 25 * time.Millisecond,
		forwarderLeaked:  &leaked,
		storagePump:      func(context.Context, *storageHostForwarder) { close(entered); <-release },
		storageExited:    func() { close(exited) },
	})
	<-entered
	if !errors.Is(err, ErrForwardersLeaked) {
		t.Fatalf("err = %v", err)
	}
	if got := leaked.Load(); got != 1 {
		t.Fatalf("forwarder leak account = %d, want exactly one incident", got)
	}
	select {
	case <-exited:
		t.Fatal("blocked storage forwarder unexpectedly exited")
	default:
	}
	close(release)
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("released storage forwarder did not exit")
	}
}
