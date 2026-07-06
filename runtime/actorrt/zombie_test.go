package actorrt

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// blockActor's Receive blocks until released. With honorCtx it also unblocks on
// ctx cancel (so it exits within grace on teardown); without it, it IGNORES ctx
// — a stuck worker no cancel can reach (the 卡死 case the zombie ledger exists
// for). entered signals the moment Receive is in-flight.
type blockActor struct {
	release  chan struct{}
	honorCtx bool
	entered  chan struct{}
}

func newBlockActor(honorCtx bool) *blockActor {
	return &blockActor{release: make(chan struct{}), honorCtx: honorCtx, entered: make(chan struct{}, 1)}
}

func (a *blockActor) Receive(ctx context.Context, _ *message.Envelope) error {
	select {
	case a.entered <- struct{}{}:
	default:
	}
	if a.honorCtx {
		select {
		case <-a.release:
		case <-ctx.Done():
		}
	} else {
		<-a.release // ignores ctx: a stuck worker
	}
	return nil
}

// wedge spawns id with a ctx-ignoring blockActor, delivers one envelope, and
// waits until the worker is in-flight (stuck in Receive) — so a termination entry
// called next must judge-dead-and-return without joining the stuck goroutine.
func wedge(t *testing.T, rt *Runtime, id actor.ActorID) (Incarnation, *blockActor) {
	t.Helper()
	a := newBlockActor(false)
	inc := rt.Spawn(id, actor.KindAgent, static(a))
	mustDeliver(t, rt, id, env("x"))
	select {
	case <-a.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("worker never entered Receive")
	}
	return inc, a
}

// mustReturnFast asserts fn returns within O(judge-dead)+ε — the DoD① bound
// (a public termination entry never joins a stuck goroutine).
func mustReturnFast(t *testing.T, what string, fn func()) {
	t.Helper()
	start := time.Now()
	fn()
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("%s blocked %v on a stuck worker — must be O(judge-dead)", what, elapsed)
	}
}

// TestG0_TerminationEntriesNonBlocking (DoD①): every public by-name termination
// entry returns promptly even when the target's worker is wedged (卡死不陪葬).
func TestG0_TerminationEntriesNonBlocking(t *testing.T) {
	t.Parallel()

	t.Run("Despawn", func(t *testing.T) {
		t.Parallel()
		rt, _ := New(Config{Parent: context.Background(), ZombieGrace: time.Second})
		inc, _ := wedge(t, rt, "a")
		mustReturnFast(t, "Despawn", func() { rt.Despawn(inc) })
	})
	t.Run("DespawnQuiet", func(t *testing.T) {
		t.Parallel()
		rt, _ := New(Config{Parent: context.Background(), ZombieGrace: time.Second})
		inc, _ := wedge(t, rt, "a")
		mustReturnFast(t, "DespawnQuiet", func() { rt.DespawnQuiet(inc) })
	})
	t.Run("DespawnID", func(t *testing.T) {
		t.Parallel()
		rt, _ := New(Config{Parent: context.Background(), ZombieGrace: time.Second})
		wedge(t, rt, "a")
		mustReturnFast(t, "DespawnID", func() { rt.DespawnID("a") })
	})
	t.Run("StopAll", func(t *testing.T) {
		t.Parallel()
		rt, _ := New(Config{Parent: context.Background(), ZombieGrace: time.Second})
		wedge(t, rt, "a")
		mustReturnFast(t, "StopAll", func() { rt.StopAll() })
	})
	t.Run("replace", func(t *testing.T) {
		t.Parallel()
		rt, _ := New(Config{Parent: context.Background(), ZombieGrace: time.Second})
		wedge(t, rt, "a")
		// Re-Spawn the same id: the wedged predecessor is judged dead + enrolled;
		// the successor must go live without waiting for it.
		mustReturnFast(t, "Spawn-replace", func() {
			rt.Spawn("a", actor.KindAgent, static(newRecordActor()))
		})
	})
	t.Run("DespawnChild", func(t *testing.T) {
		t.Parallel()
		rt, _ := New(Config{Parent: context.Background(), ZombieGrace: time.Second})
		parent := rt.Spawn("p", actor.KindAgent, static(newRecordActor()))
		child := newBlockActor(false)
		childID := actor.ActorID("p/c")
		if _, err := rt.Fork(parent, childID, actor.KindAgent, static(child)); err != nil {
			t.Fatalf("Fork: %v", err)
		}
		mustDeliver(t, rt, childID, env("x"))
		select {
		case <-child.entered:
		case <-time.After(2 * time.Second):
			t.Fatal("child worker never entered Receive")
		}
		mustReturnFast(t, "DespawnChild", func() {
			if err := rt.DespawnChild(parent, childID); err != nil {
				t.Fatalf("DespawnChild: %v", err)
			}
		})
	})
}

// TestG0_AccountEqualsResidue (DoD③): a judged-dead body appears on the ledger;
// a body that exits within grace reaps to zero (no leak); a stuck body is marked
// leaked (+logged+counted) at grace and, when it finally wakes, late-reaps.
func TestG0_AccountEqualsResidue(t *testing.T) {
	t.Parallel()

	t.Run("reap_within_grace", func(t *testing.T) {
		t.Parallel()
		rt, _ := New(Config{Parent: context.Background(), ZombieGrace: 2 * time.Second})
		a := newBlockActor(true) // honours ctx → exits when cancelled
		inc := rt.Spawn("a", actor.KindAgent, static(a))
		mustDeliver(t, rt, "a", env("x"))
		<-a.entered
		rt.Despawn(inc)
		// The corpse is on the ledger while its goroutine finishes.
		if got := len(rt.Zombies()); got != 1 {
			t.Fatalf("Zombies = %d just after Despawn, want 1", got)
		}
		waitZombiesZero(t, rt, 2*time.Second)
		if rt.LeakedTotal() != 0 {
			t.Fatalf("LeakedTotal = %d, want 0 (exited within grace)", rt.LeakedTotal())
		}
	})

	t.Run("leaked_then_late_reap", func(t *testing.T) {
		t.Parallel()
		rt, _ := New(Config{Parent: context.Background(), ZombieGrace: 60 * time.Millisecond})
		a := newBlockActor(false) // ignores ctx → stuck past grace
		inc := rt.Spawn("a", actor.KindAgent, static(a))
		mustDeliver(t, rt, "a", env("x"))
		<-a.entered
		rt.Despawn(inc)
		// Grace elapses while the worker is wedged → leaked (kept on account).
		deadline := time.Now().Add(2 * time.Second)
		for rt.LeakedTotal() == 0 && time.Now().Before(deadline) {
			time.Sleep(2 * time.Millisecond)
		}
		if rt.LeakedTotal() != 1 {
			t.Fatalf("LeakedTotal = %d, want 1 (worker wedged past grace)", rt.LeakedTotal())
		}
		zs := rt.Zombies()
		if len(zs) != 1 || !zs[0].Leaked {
			t.Fatalf("Zombies = %+v, want one entry marked leaked", zs)
		}
		// The corpse wakes late → self-strikes (late-reap): account clears.
		close(a.release)
		waitZombiesZero(t, rt, 2*time.Second)
	})
}

// TestG0_DrainZombiesLeakedList (DoD④): one wedged cell + N healthy ones — after
// StopAll, DrainZombies returns within the aggregate deadline with a leaked list
// of exactly the wedged one.
func TestG0_DrainZombiesLeakedList(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background(), ZombieGrace: 80 * time.Millisecond})
	// N healthy cells (exit promptly on teardown).
	for _, id := range []actor.ActorID{"h1", "h2", "h3"} {
		rt.Spawn(id, actor.KindAgent, static(newRecordActor()))
	}
	stuck, _ := wedge(t, rt, "stuck")
	_ = stuck

	rt.StopAll()
	start := time.Now()
	leaked := rt.DrainZombies(200 * time.Millisecond)
	if elapsed := time.Since(start); elapsed > 400*time.Millisecond {
		t.Fatalf("DrainZombies took %v, want ≤ aggregate deadline", elapsed)
	}
	if len(leaked) != 1 || leaked[0] != "stuck" {
		t.Fatalf("leaked list = %v, want [stuck]", leaked)
	}
}

// TestG0_ReplaceImmediately (DoD②): a wedged occupant is replaced by a responsive
// successor, and a request to the id is answered by the successor within one
// delivery cycle — the挤位 does not wait for the predecessor to exit.
func TestG0_ReplaceImmediately(t *testing.T) {
	t.Parallel()
	rt, del := New(Config{Parent: context.Background(), ZombieGrace: 2 * time.Second})
	wedge(t, rt, "a")

	got := make(chan struct{}, 1)
	succ := newRecordActor()
	succ.receive = func() {
		select {
		case got <- struct{}{}:
		default:
		}
	}
	rt.Spawn("a", actor.KindAgent, static(succ)) // replace
	if _, err := del.Deliver([]actor.ActorID{"a"}, env("y")); err != nil {
		t.Fatalf("deliver to successor: %v", err)
	}
	select {
	case <-got:
	case <-time.After(1 * time.Second):
		t.Fatal("successor did not answer within one delivery cycle — replace waited on predecessor")
	}
}

// TestG0_PortDespawnUnreadConn (DoD① port form, P1-5): despawning a port whose
// remote never drains the wire (so the KindDespawn frame write would block on
// wmu) still returns immediately, and the escort closeConn's the port within
// grace so it reaps (no leak of the frame-write goroutine).
func TestG0_PortDespawnUnreadConn(t *testing.T) {
	t.Parallel()
	rt, del := New(Config{Parent: context.Background(), ZombieGrace: 80 * time.Millisecond})
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"), nil)
	defer remote.conn.Close()
	// The remote never reads: an inline KindDespawn frame write would block on the
	// synchronous pipe forever. Saturate the send queue first so writeLoop is also
	// parked on the wire — proving neither the entry nor the escort blocks.
	for i := 0; i < portSendQueue+8; i++ {
		_, _ = del.Deliver([]actor.ActorID{id}, env("z"))
	}
	mustReturnFast(t, "DespawnID(port,unread)", func() { rt.DespawnID(id) })
	// The escort's closeConn (at grace) unblocks the parked frame write and both
	// loops, so the port reaps — the ledger drains to zero.
	waitZombiesZero(t, rt, 2*time.Second)
}

func waitZombiesZero(t *testing.T, rt *Runtime, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for len(rt.Zombies()) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("zombie ledger not empty after %v: %+v", timeout, rt.Zombies())
		}
		time.Sleep(2 * time.Millisecond)
	}
}
