package compute

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
)

type emptyComputeFactories struct{}

func (emptyComputeFactories) BuildClass(
	actor.ActorID,
	string,
	json.RawMessage,
) (platform.ActorFactory, bool) {
	return platform.ActorFactory{}, false
}

// Run's ordered close DAG must terminate on an already-canceled context: every
// forwarder joins and the run returns without a leak incident.
func TestRunReturnsOnCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Run(ctx, Config{ServerWS: "ws://invalid", Factories: emptyComputeFactories{}}); err != nil {
		t.Fatal(err)
	}
}

func TestAwaitForwardersJoins(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	go wg.Done()
	if err := awaitForwarders(&wg, time.Second); err != nil {
		t.Fatalf("awaitForwarders = %v, want nil", err)
	}
}

// A forwarder that outlives the join timeout must surface ErrForwardersLeaked
// while root ownership still transfers (the call returns; the blocked
// goroutine keeps running until released).
func TestAwaitForwardersTimeoutTransfersRootOwnership(t *testing.T) {
	release, exited := make(chan struct{}), make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(exited)
		<-release
	}()
	err := awaitForwarders(&wg, 25*time.Millisecond)
	if !errors.Is(err, ErrForwardersLeaked) {
		t.Fatalf("err = %v, want ErrForwardersLeaked", err)
	}
	select {
	case <-exited:
		t.Fatal("blocked forwarder unexpectedly exited")
	default:
	}
	close(release)
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("released forwarder did not exit")
	}
}
