package actorhost

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// leakProbeStopActor never returns from Stop until released, so its Unit stays
// short of Done and Close must account for it as a physical leak rather than
// silently reporting success.
type leakProbeStopActor struct {
	entered   chan struct{}
	release   chan struct{}
	enterOnce sync.Once
}

func newLeakProbeStopActor() *leakProbeStopActor {
	return &leakProbeStopActor{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (*leakProbeStopActor) Receive(context.Context, *message.Envelope) error { return nil }

func (a *leakProbeStopActor) Stop(context.Context) error {
	a.enterOnce.Do(func() { close(a.entered) })
	<-a.release
	return nil
}

// leakProbeBinding parks the reconcile worker inside Binding.Close, the one
// piece of foreign code the worker calls on its own goroutine.
type leakProbeBinding struct {
	entered   chan struct{}
	release   chan struct{}
	done      chan struct{}
	enterOnce sync.Once
}

func newLeakProbeBinding() *leakProbeBinding {
	return &leakProbeBinding{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		done:    make(chan struct{}),
	}
}

func (*leakProbeBinding) Deliver(*message.Envelope) error { return nil }
func (*leakProbeBinding) CancelRequest(message.ID)        {}
func (b *leakProbeBinding) Done() <-chan struct{}         { return b.done }

func (b *leakProbeBinding) Close() error {
	b.enterOnce.Do(func() { close(b.entered) })
	<-b.release
	return nil
}

func waitLeakProbe(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// TestCloseReportsRetiringUnitLeak pins the accounting duty §10 F13 moved out of
// actorrt: a retiring Unit that never reaches Done inside the close budget is
// reported by its host, named by ActorID, instead of being dropped.
func TestCloseReportsRetiringUnitLeak(t *testing.T) {
	t.Parallel()

	impl := newLeakProbeStopActor()
	host, err := New(Config{
		Domain:       "server",
		PollInterval: 5 * time.Millisecond,
		BodyBuilder:  func(BodyBuildInput) actorrt.Actor { return impl },
	})
	if err != nil {
		t.Fatal(err)
	}
	id := actor.ActorID("agent:retiring-leak")
	if err := host.AcceptFullDesired([]Desired{bodyDesiredFor(t, id, testAttempt(t))}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		snapshot, ok := host.Inspect(id)
		return ok && snapshot.Actual == ActualBody && snapshot.Unit.IsAlive()
	})
	snapshot, _ := host.Inspect(id)
	unit := snapshot.Unit

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	closeErr := host.Close(ctx)
	if closeErr == nil {
		t.Fatal("Close reported success while a retiring unit was still stuck in Stop")
	}
	want := "actorhost: retiring unit leak: " + string(id)
	if !strings.Contains(closeErr.Error(), want) {
		t.Fatalf("Close error = %v, want it to contain %q", closeErr, want)
	}
	waitLeakProbe(t, impl.entered, "the actor Stop hook")

	close(impl.release)
	waitLeakProbe(t, unit.Done(), "the released unit to finish")
}

// TestCloseReportsBodyBuilderLeak pins the second fault: a build goroutine that
// has not returned is named, and the close deliberately stops before the
// retiring scan so a late builder's own retire is not raced.
func TestCloseReportsBodyBuilderLeak(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce sync.Once
	host, err := New(Config{
		Domain:       "server",
		PollInterval: 5 * time.Millisecond,
		BodyBuilder: func(BodyBuildInput) actorrt.Actor {
			enterOnce.Do(func() { close(entered) })
			<-release
			return newHostTestActor()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := actor.ActorID("agent:builder-leak")
	if err := host.AcceptFullDesired([]Desired{bodyDesiredFor(t, id, testAttempt(t))}); err != nil {
		t.Fatal(err)
	}
	waitLeakProbe(t, entered, "the body builder")

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	closeErr := host.Close(ctx)
	if closeErr == nil {
		t.Fatal("Close reported success while a body builder was still running")
	}
	if !strings.Contains(closeErr.Error(), "actorhost: body builder leak") {
		t.Fatalf("Close error = %v, want a body builder leak", closeErr)
	}
	if strings.Contains(closeErr.Error(), "retiring unit leak") {
		t.Fatalf("Close scanned retiring units while a builder could still add one: %v", closeErr)
	}

	close(release)
	// The late builder is still allowed to retire its prepared loser, after
	// which the row disappears on its own.
	eventually(t, func() bool {
		_, ok := host.Inspect(id)
		return !ok
	})
}

// TestCloseReportsReconcileWorkerLeak pins the third fault: the reconcile
// worker parked inside foreign teardown code is reported, not waited on
// forever.
func TestCloseReportsReconcileWorkerLeak(t *testing.T) {
	t.Parallel()

	host, err := New(Config{
		Domain:       "server",
		PollInterval: 5 * time.Millisecond,
		BodyBuilder:  func(BodyBuildInput) actorrt.Actor { return newHostTestActor() },
	})
	if err != nil {
		t.Fatal(err)
	}
	id := actor.ActorID("agent:worker-leak")
	key := testAttempt(t)
	if err := host.AcceptFullDesired([]Desired{CarrierDesired{
		ActorID: id, AttemptKey: key, PeerDomain: "daemon",
	}}); err != nil {
		t.Fatal(err)
	}
	resource := newLeakProbeBinding()
	if err := host.Attach(id, key, exactTestBinding(t, resource)); err != nil {
		t.Fatal(err)
	}
	// Dropping the desired makes the reconcile worker close this route on its
	// own goroutine, where the probe parks it.
	if err := host.AcceptFullDesired(nil); err != nil {
		t.Fatal(err)
	}
	waitLeakProbe(t, resource.entered, "the reconcile worker to enter Binding.Close")

	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	closeErr := host.Close(ctx)
	if closeErr == nil {
		t.Fatal("Close reported success while the reconcile worker was parked")
	}
	if !strings.Contains(closeErr.Error(), "actorhost: reconcile worker leak") {
		t.Fatalf("Close error = %v, want a reconcile worker leak", closeErr)
	}

	close(resource.release)
}

// TestCloseReportsNoFaultWhenEverythingJoins is the negative control for the
// three faults above: an ordinary close of a live body reports nothing.
func TestCloseReportsNoFaultWhenEverythingJoins(t *testing.T) {
	t.Parallel()

	host, err := New(Config{
		Domain:       "server",
		PollInterval: 5 * time.Millisecond,
		BodyBuilder:  func(BodyBuildInput) actorrt.Actor { return newHostTestActor() },
	})
	if err != nil {
		t.Fatal(err)
	}
	id := actor.ActorID("agent:clean-close")
	if err := host.AcceptFullDesired([]Desired{bodyDesiredFor(t, id, testAttempt(t))}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		snapshot, ok := host.Inspect(id)
		return ok && snapshot.Actual == ActualBody && snapshot.Unit.IsAlive()
	})
	closeHost(t, host)
}
