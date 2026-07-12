package platform

import (
	"context"
	"errors"
	"sync/atomic"
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
	var leaked atomic.Int64
	err := runCompute(ctx, ComputeConfig{ServerWS: "ws://invalid", Desired: emptyComputePlan{}, Builder: emptyComputePlan{}}, &computeLifecycleHooks{
		forwarderTimeout: 25 * time.Millisecond,
		forwarderLeaked:  &leaked,
		storagePump:      func(context.Context, *storageHostForwarder) { close(entered); <-release },
		storageExited:    func() { close(exited) },
	})
	<-entered
	if !errors.Is(err, ErrComputeForwardersLeaked) {
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

// glue-v2 #9: the redial ladder must reset ONLY after a link proves itself, not on
// Dial success. Before the fix, backoff = redialInitialBackoff the instant Dial
// returned — so a "attach succeeds → link dies in ~1s" flap kept the ladder pinned
// on its first rung, hammering the home at 1/s forever. redialBackoffAfterLink is
// the extracted decision the redial loop consults; these tests pin its behaviour
// (the loop itself is network/goroutine-bound, so the ladder policy is unit-tested
// here in isolation).
func TestRedialBackoffAfterLink_SecondDropClimbsToCap(t *testing.T) {
	// 秒断循环: every session dies well under the ceiling, so the ladder must climb
	// monotonically to the cap and NEVER reset to the floor.
	const shortLived = time.Second
	backoff := redialInitialBackoff
	var seen []time.Duration
	for i := 0; i < 12; i++ {
		backoff = redialBackoffAfterLink(backoff, shortLived) // pre-sleep value the loop logs/waits on
		seen = append(seen, backoff)
		backoff = nextRedialBackoff(backoff) // ladder grows once more before the next dial
	}
	for i := 1; i < len(seen); i++ {
		if seen[i] < seen[i-1] {
			t.Fatalf("ladder not monotonic at rung %d: %s then %s", i, seen[i-1], seen[i])
		}
	}
	want := []time.Duration{1, 2, 4, 8, 16, 30, 30, 30, 30, 30, 30, 30}
	for i, w := range want {
		if seen[i] != w*time.Second {
			t.Fatalf("rung %d = %s, want %ds", i, seen[i], w)
		}
	}
	if seen[len(seen)-1] != redialMaxBackoff {
		t.Fatalf("ladder never reached the cap; last rung = %s", seen[len(seen)-1])
	}
}

func TestRedialBackoffAfterLink_ProvenLinkResets(t *testing.T) {
	// A session that lived at least the ceiling is a genuine connection that later
	// dropped — reset to the floor.
	if got := redialBackoffAfterLink(redialMaxBackoff, redialMaxBackoff); got != redialInitialBackoff {
		t.Fatalf("proven link (=ceiling) = %s, want reset to %s", got, redialInitialBackoff)
	}
	if got := redialBackoffAfterLink(redialMaxBackoff, 40*time.Second); got != redialInitialBackoff {
		t.Fatalf("proven link (>ceiling) = %s, want reset to %s", got, redialInitialBackoff)
	}
	// Just under the ceiling: a flap — hold the ladder where it is.
	if got := redialBackoffAfterLink(8*time.Second, redialMaxBackoff-time.Millisecond); got != 8*time.Second {
		t.Fatalf("sub-ceiling link = %s, want ladder held at 8s", got)
	}
}
