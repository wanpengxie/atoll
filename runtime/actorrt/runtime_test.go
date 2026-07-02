package actorrt

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// TestDeliverNilEnvelopeIsError: a nil envelope is a true exception (not a
// delivery condition), so Deliver returns an error and no per-audience map —
// the substrate never fabricates an Outcome for a non-message.
func TestDeliverNilEnvelopeIsError(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.Spawn("a", static(newRecordActor()))

	res, err := rt.deliver([]actor.ActorID{"a"}, nil)
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
// silently skipped), and a hosted-but-stopped embodiment is Stopped. The map has
// exactly one entry per audience id.
func TestDeliverPerAudienceTruth(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background(), Mailbox: 16})

	live := newRecordActor()
	rt.Spawn("live", static(live))

	// "gone" is spawned then despawned: hosted history but no live embodiment now,
	// so it must read NotHosted, NOT some stale Delivered.
	gone := rt.Spawn("gone", static(newRecordActor()))
	rt.Despawn(gone)

	audience := []actor.ActorID{"live", "ghost", "gone"}
	res, err := rt.deliver(audience, env("m"))
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
	rt, _ := New(Config{Parent: context.Background(), Mailbox: 16})
	defer rt.StopAll()
	rt.Spawn("a", static(newRecordActor()))

	res, err := rt.deliver([]actor.ActorID{"a", "a"}, env("m"))
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
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()

	first := newRecordActor()
	rt.Spawn("a", static(first))
	select {
	case <-first.startedCh:
	case <-time.After(time.Second):
		t.Fatal("first instance never started")
	}

	second := newRecordActor()
	rt.Spawn("a", static(second))

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
	if _, ok := rt.Stat("a"); !ok {
		t.Fatal("id not addressable after replace")
	}
	// The replacement actually receives — proving the live embodiment is the new one.
	mustDeliver(t, rt, "a", env("x"))
}

// selfIDActor captures the ActorContext.Self() handed at Start.
type selfIDActor struct{ id chan actor.ActorID }

func (a *selfIDActor) Start(_ context.Context, self ActorContext) error {
	a.id <- self.Self()
	return nil
}
func (a *selfIDActor) Receive(context.Context, *message.Envelope) error { return nil }

// TestActorContextSelf: the substrate hands an actor its own id at Start (Erlang
// self()). ActorContext exposes identity ONLY — there is no self-send; a message
// reaches an actor only through the harness→fanout collaboration path.
func TestActorContextSelf(t *testing.T) {
	t.Parallel()
	a := &selfIDActor{id: make(chan actor.ActorID, 1)}
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.Spawn("a", static(a))
	select {
	case got := <-a.id:
		if got != actor.ActorID("a") {
			t.Fatalf("Self() = %q, want a", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start never ran / Self() not observed")
	}
}

// TestStopAllClearsEmbodiments: channel teardown stops every embodiment and empties
// the addressing map (no id remains addressable, Stop hooks run).
func TestStopAllClearsEmbodiments(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})

	actors := map[actor.ActorID]*recordActor{}
	for _, id := range []actor.ActorID{"a", "b", "c"} {
		ra := newRecordActor()
		actors[id] = ra
		rt.Spawn(id, static(ra))
		select {
		case <-ra.startedCh:
		case <-time.After(time.Second):
			t.Fatalf("%s never started", id)
		}
	}

	rt.StopAll()

	for id, ra := range actors {
		if _, ok := rt.Stat(id); ok {
			t.Fatalf("%s still addressable after StopAll", id)
		}
		select {
		case <-ra.stoppedCh:
		case <-time.After(time.Second):
			t.Fatalf("%s Stop hook never ran", id)
		}
	}
}

// TestDespawnAbsentIsNoop: despawning an incarnation handle the runtime never
// hosted is a no-op (no panic, stays unaddressable). The guarded Despawn's
// pointer check never matches, so it never touches the (nil) embodiment.
func TestDespawnAbsentIsNoop(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.Despawn(Incarnation{id: "never-existed"})
	if _, ok := rt.Stat("never-existed"); ok {
		t.Fatal("absent id became addressable after Despawn")
	}
}

// TestStatReportsClockStampedStartedAt: Stat is the substrate-owned obs pull —
// present = the second return, and StartedAt is the bind instant the runtime
// stamps from its injected Clock (uptime = now - StartedAt, derived by the
// consumer). Only the substrate produces it; the actor never self-reports it.
func TestStatReportsClockStampedStartedAt(t *testing.T) {
	t.Parallel()
	pinned := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	rt, _ := New(Config{Parent: context.Background(), Clock: func() time.Time { return pinned }})
	defer rt.StopAll()

	if _, ok := rt.Stat("a"); ok {
		t.Fatal("Stat reported present for an unhosted id")
	}
	rt.Spawn("a", static(newRecordActor()))

	st, ok := rt.Stat("a")
	if !ok {
		t.Fatal("Stat reported absent for a hosted cell")
	}
	if !st.StartedAt.Equal(pinned) {
		t.Fatalf("StartedAt = %v, want clock-stamped %v", st.StartedAt, pinned)
	}
}

// TestCurrentIncarnationLiveHandle: the schedule engine's attach seam —
// a live embodiment's CurrentIncarnation returns a handle whose IsLive reads
// true, the same addressing authority Deliver/Stat consult.
func TestCurrentIncarnationLiveHandle(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.Spawn("a", static(newRecordActor()))

	inc, ok := rt.CurrentIncarnation("a")
	if !ok {
		t.Fatal("CurrentIncarnation reported absent for a hosted cell")
	}
	if inc.ID() != actor.ActorID("a") {
		t.Fatalf("ID() = %q, want a", inc.ID())
	}
	if !rt.IsLive(inc) {
		t.Fatal("IsLive(handle) = false for a freshly spawned live embodiment")
	}
}

// TestCurrentIncarnationAbsent: no embodiment hosted for id → ok=false, no
// handle fabricated — mirrors Stat's present=false discipline (the schedule
// engine's ErrBadSchedule attach-failure path).
func TestCurrentIncarnationAbsent(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()

	if _, ok := rt.CurrentIncarnation("never-existed"); ok {
		t.Fatal("CurrentIncarnation reported present for an unhosted id")
	}
}

// TestCurrentIncarnationReplaceIsPointerLevel: after a same-id replace,
// CurrentIncarnation returns the NEW embodiment's handle, and a handle
// captured BEFORE the replace reads IsLive=false afterward — the ABA guard
// (pointer-identity discipline) extended to the schedule engine's attach
// seam: a same-id successor taking over never revives a predecessor's welded
// incarnation-bind timer — a same-id successor being present never rescues
// its predecessor.
func TestCurrentIncarnationReplaceIsPointerLevel(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.Spawn("a", static(newRecordActor()))

	old, ok := rt.CurrentIncarnation("a")
	if !ok {
		t.Fatal("CurrentIncarnation reported absent before replace")
	}

	rt.Spawn("a", static(newRecordActor())) // same-id replace (successor go-live)

	next, ok := rt.CurrentIncarnation("a")
	if !ok {
		t.Fatal("CurrentIncarnation reported absent after replace")
	}
	if next.p == old.p {
		t.Fatal("CurrentIncarnation returned the predecessor's embodiment pointer after a replace")
	}
	if rt.IsLive(old) {
		t.Fatal("stale predecessor handle still reads IsLive=true after replace")
	}
	if !rt.IsLive(next) {
		t.Fatal("successor handle reads IsLive=false right after go-live")
	}
}
