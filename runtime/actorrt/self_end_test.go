package actorrt

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// selfEndingActor calls DespawnID/DespawnIDReason on its OWN id from WITHIN its
// own Receive call — the actorrt-level shape of lib/actorbase's self-End tail
// (Sys.End → lifecycle.EndSelf → Home.EndIdentity → runtime.DespawnID(self)).
// It reports how long the call took (via a buffered channel — Receive must not
// block on the send, so the test's own read races nothing).
type selfEndingActor struct {
	rt      *Runtime
	id      actor.ActorID
	elapsed chan time.Duration
}

func (a *selfEndingActor) Start(context.Context, ActorContext) error { return nil }
func (a *selfEndingActor) Stop(context.Context) error                { return nil }

func (a *selfEndingActor) Receive(ctx context.Context, env *message.Envelope) error {
	start := time.Now()
	a.rt.DespawnIDReason(a.id, "self_end")
	a.elapsed <- time.Since(start)
	return nil
}

// TestSelfDespawnFromWithinOwnReceiveDoesNotSelfJoin locks in the §S9 "self-End
// 收尾 join" verification: when an actor's own worker goroutine calls
// DespawnID/DespawnIDReason on ITS OWN id (the actorrt primitive
// Home.prepareEndIdentity's tail drives for self-End), the call must return
// promptly — it must NOT join the caller's own goroutine (that goroutine is,
// by construction, still inside Receive and cannot also be waiting on its own
// exit). The zombie-ledger design (runtime.go DespawnIDReason →
// retireCurrentLocked → runRetirement → escort in a freshly spawned goroutine,
// zombie.go) guarantees this structurally: the ONLY blocking join in the
// runtime (escort's <-doneCh()) runs on a goroutine the terminating actor
// never touches. This test exercises that guarantee end-to-end rather than
// trusting the doc comments.
func TestSelfDespawnFromWithinOwnReceiveDoesNotSelfJoin(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background(), ZombieGrace: time.Second})
	a := &selfEndingActor{rt: rt, id: "self-ender", elapsed: make(chan time.Duration, 1)}
	if _, _, err := rt.SpawnIfAbsent(a.id, actor.KindAgent, static(a)); err != nil {
		t.Fatalf("SpawnIfAbsent: %v", err)
	}

	mustDeliver(t, rt, a.id, env("trigger-self-end"))

	select {
	case d := <-a.elapsed:
		// Same DoD① bound as mustReturnFast (zombie_test.go): a public
		// termination entry is O(judge-dead), not O(worker exit) — and here
		// the caller IS the worker, so O(worker exit) would be a self-join
		// deadlock, not merely slow.
		if d > 250*time.Millisecond {
			t.Fatalf("DespawnIDReason(self) took %v from within own Receive — looks like a self-join, want O(judge-dead)", d)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Receive calling DespawnIDReason on its own id never returned — self-join deadlock")
	}

	// The retirement itself completes normally (bounded escort join, off the
	// actor's own goroutine) once Receive has returned and the body can exit.
	waitZombiesZero(t, rt, 2*time.Second)
}
