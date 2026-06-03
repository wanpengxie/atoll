package actorrt

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// TestDeliverNilEnvelopeIsError: a nil envelope is a true exception (not a
// delivery condition), so Deliver returns an error and no per-audience map —
// the substrate never fabricates an Outcome for a non-message.
func TestDeliverNilEnvelopeIsError(t *testing.T) {
	t.Parallel()
	rt := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.Spawn("a", newRecordActor())

	res, err := rt.Deliver([]actor.ActorID{"a"}, nil)
	if err == nil {
		t.Fatal("Deliver(nil) returned nil error")
	}
	if res.Per != nil {
		t.Fatalf("Deliver(nil) returned a per-audience map %+v, want none", res.Per)
	}
}

// TestDeliverPerAudienceTruth is the core A3 contract: a multi-member audience
// gets a per-member Outcome that truthfully reflects what the runtime did to
// EACH — a hosted live actor is Delivered, an unhosted id is NotHosted (never
// silently skipped), and a hosted-but-stopped presence is Stopped. The map has
// exactly one entry per audience id.
func TestDeliverPerAudienceTruth(t *testing.T) {
	t.Parallel()
	rt := New(Config{Parent: context.Background(), Mailbox: 16})

	live := newRecordActor()
	rt.Spawn("live", live)

	// "gone" is spawned then despawned: hosted history but no live presence now,
	// so it must read NotHosted, NOT some stale Delivered.
	rt.Spawn("gone", newRecordActor())
	rt.Despawn("gone")

	audience := []actor.ActorID{"live", "ghost", "gone"}
	res, err := rt.Deliver(audience, env("m"))
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	defer rt.StopAll()

	if len(res.Per) != len(audience) {
		t.Fatalf("per-audience map has %d entries, want %d", len(res.Per), len(audience))
	}
	want := map[actor.ActorID]Outcome{
		"live":  Delivered,
		"ghost": NotHosted,
		"gone":  NotHosted,
	}
	for id, w := range want {
		if got := res.Per[id]; got != w {
			t.Fatalf("audience %q outcome = %v, want %v", id, got, w)
		}
	}
}

// TestDeliverDuplicateAudienceMember: the per-audience result is keyed by
// ActorID, so a duplicated audience member collapses to a single entry (the
// last write wins) — the runtime addresses by identity, not by list position.
func TestDeliverDuplicateAudienceMember(t *testing.T) {
	t.Parallel()
	rt := New(Config{Parent: context.Background(), Mailbox: 16})
	defer rt.StopAll()
	rt.Spawn("a", newRecordActor())

	res, err := rt.Deliver([]actor.ActorID{"a", "a"}, env("m"))
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if len(res.Per) != 1 {
		t.Fatalf("per map size = %d, want 1 (keyed by id)", len(res.Per))
	}
	if got := res.Per["a"]; got != Delivered {
		t.Fatalf("outcome = %v, want Delivered", got)
	}
}

// TestSpawnReplaceStopsOld: spawning the same ActorID twice is a replace (one
// actor, one owner) — the prior instance is stopped (its Stop hook runs) and the
// id stays addressable as the new instance.
func TestSpawnReplaceStopsOld(t *testing.T) {
	t.Parallel()
	rt := New(Config{Parent: context.Background()})
	defer rt.StopAll()

	first := newRecordActor()
	rt.Spawn("a", first)
	select {
	case <-first.startedCh:
	case <-time.After(time.Second):
		t.Fatal("first instance never started")
	}

	second := newRecordActor()
	rt.Spawn("a", second)

	select {
	case <-first.stoppedCh:
	case <-time.After(time.Second):
		t.Fatal("first instance not stopped on replace")
	}
	select {
	case <-second.startedCh:
	case <-time.After(time.Second):
		t.Fatal("second instance never started")
	}
	if !rt.Has("a") {
		t.Fatal("id not addressable after replace")
	}
	// The replacement actually receives — proving the live presence is the new one.
	mustDeliver(t, rt, "a", env("x"))
}

// selfSendActor schedules one follow-up message to itself on first receipt,
// exercising ActorContext.Deliver (the only substrate path for an actor to feed
// its own mailbox — a self-timer fold-back).
type selfSendActor struct {
	self    ActorContext
	seen    chan string
	relayed bool
}

func (a *selfSendActor) Start(_ context.Context, self ActorContext) error {
	a.self = self
	return nil
}

func (a *selfSendActor) Receive(_ context.Context, env *message.Envelope) error {
	a.seen <- string(env.ID)
	if !a.relayed {
		a.relayed = true
		return a.self.Deliver(&message.Envelope{ID: "self-followup"})
	}
	return nil
}

// TestActorContextSelfDeliver: an actor can enqueue into its OWN mailbox via the
// ActorContext handed at Start, and Self() reports its bound id. This is the
// only isolation-preserving self-signal path (no closure runs on the cell
// goroutine; it's a message the actor sends itself).
func TestActorContextSelfDeliver(t *testing.T) {
	t.Parallel()
	a := &selfSendActor{seen: make(chan string, 4)}
	rt := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.Spawn("a", a)

	mustDeliver(t, rt, "a", env("external"))

	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case id := <-a.seen:
			got[id] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("only saw %v, expected external + self-followup", got)
		}
	}
	if !got["external"] || !got["self-followup"] {
		t.Fatalf("seen = %v, want both external and self-followup", got)
	}
	if a.self.Self() != actor.ActorID("a") {
		t.Fatalf("Self() = %q, want a", a.self.Self())
	}
}

// TestStopAllClearsPresences: channel teardown stops every presence and empties
// the addressing map (no id remains addressable, Stop hooks run).
func TestStopAllClearsPresences(t *testing.T) {
	t.Parallel()
	rt := New(Config{Parent: context.Background()})

	actors := map[actor.ActorID]*recordActor{}
	for _, id := range []actor.ActorID{"a", "b", "c"} {
		ra := newRecordActor()
		actors[id] = ra
		rt.Spawn(id, ra)
		select {
		case <-ra.startedCh:
		case <-time.After(time.Second):
			t.Fatalf("%s never started", id)
		}
	}

	rt.StopAll()

	for id, ra := range actors {
		if rt.Has(id) {
			t.Fatalf("%s still addressable after StopAll", id)
		}
		select {
		case <-ra.stoppedCh:
		case <-time.After(time.Second):
			t.Fatalf("%s Stop hook never ran", id)
		}
	}
}

// TestDespawnAbsentIsNoop: despawning an id the runtime never hosted is a no-op
// (no panic, stays unaddressable).
func TestDespawnAbsentIsNoop(t *testing.T) {
	t.Parallel()
	rt := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.Despawn("never-existed")
	if rt.Has("never-existed") {
		t.Fatal("absent id became addressable after Despawn")
	}
}
