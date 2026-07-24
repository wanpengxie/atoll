package actorctl

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// newActorsWithGrace mirrors newActorsWithBuilder but pins WakeGrace so the
// one-shot delivery contract (spec: delivery-pump one-shot v1.3) is testable
// without gambling on the wall clock.
func newActorsWithGrace(
	t *testing.T,
	store *fakeStore,
	grace time.Duration,
	build ManagedBodyBuilder,
) *ChannelActors {
	t.Helper()
	if build == nil {
		build = func(ManagedBodyInput, actorcaps.Caps) actorrt.Actor {
			return inertActor{}
		}
	}
	actors, err := NewChannelActors(Config{
		Store:        store,
		ServerDomain: "server",
		ServerHost: actorhost.Config{
			PollInterval: time.Millisecond,
		},
		BuildManagedBody: build,
		WakeGrace:        grace,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := actors.Start(context.Background(), prepareSystem(t)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = actors.Close(ctx)
	})
	return actors
}

func testEnvelope(id message.ID) *message.Envelope {
	return &message.Envelope{ID: id, Kind: message.KindEvent}
}

func parkDormant(t *testing.T, actors *ChannelActors, id actor.ActorID) {
	t.Helper()
	row, ok, err := actors.controller.lookup(id)
	if err != nil || !ok {
		t.Fatalf("lookup %s: %#v %v %v", id, row, ok, err)
	}
	if err := actors.requestIdle(context.Background(), id, row.Desired.AttemptKey); err != nil {
		t.Fatal(err)
	}
}

// Dormant→Run wake applies the blind grace exactly once and then delivers
// exactly once; the already-Run follow-up skips the grace entirely.
func TestDeliverCommittedWakeGraceThenOneShot(t *testing.T) {
	const grace = 200 * time.Millisecond
	actors := newActorsWithGrace(t, newFakeStore("agent"), grace, nil)
	waitUntil(t, "initial body never hosted", func() bool {
		return actors.host.Deliver("agent", testEnvelope("warm")) == nil
	})
	parkDormant(t, actors, "agent")

	start := time.Now()
	if err := actors.DeliverCommitted(
		context.Background(), "agent", testEnvelope("m1"),
	); err != nil {
		t.Fatalf("wake delivery failed: %v", err)
	}
	if elapsed := time.Since(start); elapsed < grace {
		t.Fatalf("wake path skipped the grace: %v < %v", elapsed, grace)
	}

	// Already Run and hosted: no grace, immediate single delivery.
	start = time.Now()
	if err := actors.DeliverCommitted(
		context.Background(), "agent", testEnvelope("m2"),
	); err != nil {
		t.Fatalf("hosted delivery failed: %v", err)
	}
	if elapsed := time.Since(start); elapsed >= grace {
		t.Fatalf("already-Run path slept the grace: %v", elapsed)
	}
}

// changed=false with no endpoint (the detached-daemon shape): zero grace,
// exactly one attempt, honest NotHosted, cursor free to advance.
func TestDeliverCommittedNoGraceWhenAlreadyRunUnhosted(t *testing.T) {
	const grace = 3 * time.Second
	broken := func(ManagedBodyInput, actorcaps.Caps) actorrt.Actor { return nil }
	actors := newActorsWithGrace(t, newFakeStore("agent"), grace, broken)

	// Seeded durable actors boot as Run: EnsureRun reports changed=false.
	start := time.Now()
	err := actors.DeliverCommitted(context.Background(), "agent", testEnvelope("m1"))
	if !errors.Is(err, actorhost.ErrNotHosted) {
		t.Fatalf("expected ErrNotHosted, got %v", err)
	}
	if elapsed := time.Since(start); elapsed >= grace/2 {
		t.Fatalf("already-Run unhosted path slept: %v", elapsed)
	}
}

// A wake whose build never lands still makes exactly one attempt after the
// grace — no retry loop, no waiting for NotHosted to clear.
func TestDeliverCommittedWakeStillUnhostedIsSingleAttempt(t *testing.T) {
	const grace = 50 * time.Millisecond
	broken := func(ManagedBodyInput, actorcaps.Caps) actorrt.Actor { return nil }
	actors := newActorsWithGrace(t, newFakeStore("agent"), grace, broken)
	parkDormant(t, actors, "agent")

	start := time.Now()
	err := actors.DeliverCommitted(context.Background(), "agent", testEnvelope("m1"))
	elapsed := time.Since(start)
	if !errors.Is(err, actorhost.ErrNotHosted) {
		t.Fatalf("expected ErrNotHosted, got %v", err)
	}
	if elapsed < grace {
		t.Fatalf("wake path skipped the grace: %v", elapsed)
	}
	if elapsed >= time.Second {
		t.Fatalf("single attempt took %v — smells like a retry loop", elapsed)
	}
}

// The grace is interruptible: pump close must not be held hostage.
func TestDeliverCommittedGraceInterruptedByCtx(t *testing.T) {
	actors := newActorsWithGrace(t, newFakeStore("agent"), 10*time.Second, nil)
	parkDormant(t, actors, "agent")

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err := actors.DeliverCommitted(ctx, "agent", testEnvelope("m1"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if time.Since(start) >= time.Second {
		t.Fatal("ctx cancel did not interrupt the grace")
	}
}

// Kernel routing survives the rewrite: SystemActorID bypasses EnsureRun and
// the grace entirely.
func TestDeliverCommittedRoutesSystemToKernel(t *testing.T) {
	actors := newActorsWithGrace(t, newFakeStore(), 5*time.Second, nil)
	start := time.Now()
	if err := actors.DeliverCommitted(
		context.Background(), actor.SystemActorID, testEnvelope("sys"),
	); err != nil {
		t.Fatalf("kernel delivery failed: %v", err)
	}
	if time.Since(start) >= time.Second {
		t.Fatal("kernel path slept the wake grace")
	}
}
