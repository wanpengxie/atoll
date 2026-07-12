package platform

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

type emptyComputePlan struct{}

func (emptyComputePlan) Members(context.Context) ([]actorrt.DesiredMember, error) { return nil, nil }
func (emptyComputePlan) Lookup(actor.ActorID) (ActorFactory, bool)                { return ActorFactory{}, false }

func TestRunComputeReturnsAfterBothForwardersExit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	obs, storage := make(chan struct{}), make(chan struct{})
	err := runCompute(ctx, ComputeConfig{ServerWS: "ws://invalid", Desired: emptyComputePlan{}, Builder: emptyComputePlan{}}, &computeLifecycleHooks{
		forwarderTimeout: time.Second,
		obsExited:        func() { close(obs) }, storageExited: func() { close(storage) },
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-obs:
	default:
		t.Fatal("RunCompute returned before obs forwarder exited")
	}
	select {
	case <-storage:
	default:
		t.Fatal("RunCompute returned before storage forwarder exited")
	}
}

func TestRunComputeForwarderTimeoutTransfersRootOwnership(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	entered, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	err := runCompute(ctx, ComputeConfig{ServerWS: "ws://invalid", Desired: emptyComputePlan{}, Builder: emptyComputePlan{}}, &computeLifecycleHooks{
		forwarderTimeout: 25 * time.Millisecond,
		storagePump:      func(context.Context, *storageHostForwarder) { close(entered); <-release },
		storageExited:    func() { close(exited) },
	})
	<-entered
	if !errors.Is(err, ErrComputeForwardersLeaked) {
		t.Fatalf("err = %v", err)
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
