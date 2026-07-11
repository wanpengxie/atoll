package presence

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type inertActor struct{}

func (inertActor) Start(context.Context, actorrt.ActorContext) error { return nil }
func (inertActor) Receive(context.Context, *message.Envelope) error  { return nil }

type fakeRegistry struct {
	rows map[actor.ActorID]storespec.Record
	err  error
}

func (r *fakeRegistry) Lookup(_ context.Context, id actor.ActorID) (storespec.Record, bool, error) {
	if r.err != nil {
		return storespec.Record{}, false, r.err
	}
	row, ok := r.rows[id]
	return row, ok, nil
}
func (r *fakeRegistry) Exists(ctx context.Context, id actor.ActorID) (bool, error) {
	_, ok, err := r.Lookup(ctx, id)
	return ok, err
}
func (r *fakeRegistry) ListActive(context.Context) ([]storespec.Record, error) {
	if r.err != nil {
		return nil, r.err
	}
	rows := make([]storespec.Record, 0, len(r.rows))
	for _, row := range r.rows {
		if row.IsActive() {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func spawn(t *testing.T, rt *actorrt.Runtime, id actor.ActorID) actorrt.Incarnation {
	t.Helper()
	inc, _, err := rt.SpawnIfAbsent(id, actor.KindAgent, func(actorrt.Incarnation) actorrt.Actor { return inertActor{} })
	if err != nil {
		t.Fatal(err)
	}
	return inc
}

func TestSnapshotFourCellStateSpace(t *testing.T) {
	now := time.Unix(100, 0)
	fold := New(nil, func() time.Time { return now }, []actorrt.ObsKind{"level"}, time.Second)
	rt, _ := actorrt.New(actorrt.Config{Clock: func() time.Time { return now }})
	t.Cleanup(rt.StopAll)
	reg := &fakeRegistry{rows: map[actor.ActorID]storespec.Record{
		"member-live":   {ID: "member-live"},
		"member-absent": {ID: "member-absent"},
	}}
	liveGen := spawn(t, rt, "member-live")
	ephGen := spawn(t, rt, "ephemeral")
	fold.OnObs(context.Background(), "member-live", liveGen, "level", []byte("a"))
	fold.OnObs(context.Background(), "member-absent", liveGen, "level", []byte("b"))
	fold.OnObs(context.Background(), "ephemeral", ephGen, "level", []byte("c"))
	fold.PutDoor("neither", "level", []byte("must-not-leak"))

	view := NewView(fold, rt, reg)
	tests := []struct {
		id              actor.ActorID
		member, present bool
		l3              int
	}{
		{"member-live", true, true, 1},
		{"member-absent", true, false, 1},
		{"ephemeral", false, true, 1},
		{"neither", false, false, 0},
	}
	for _, test := range tests {
		snap, err := view.Snapshot(context.Background(), test.id)
		if err != nil {
			t.Fatal(err)
		}
		if snap.Member != test.member || snap.L1Present != test.present || len(snap.L3) != test.l3 {
			t.Errorf("%s = member:%v present:%v l3:%d", test.id, snap.Member, snap.L1Present, len(snap.L3))
		}
	}
}

func TestGenerationAndSourceRules(t *testing.T) {
	now := time.Unix(100, 0)
	fold := New(nil, func() time.Time { return now }, []actorrt.ObsKind{"broker", "door"}, time.Second)
	rt, _ := actorrt.New(actorrt.Config{})
	t.Cleanup(rt.StopAll)
	old := spawn(t, rt, "a")
	fold.OnObs(context.Background(), "a", old, "broker", []byte("old"))
	fold.PutDoor("a", "door", []byte("online"))
	rt.Despawn(old)
	current := spawn(t, rt, "a")
	view := NewView(fold, rt, &fakeRegistry{rows: map[actor.ActorID]storespec.Record{"a": {ID: "a"}}})
	snap, err := view.Snapshot(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	if !snap.L3["broker"].StaleFromPriorLife || snap.L3["door"].StaleFromPriorLife {
		t.Fatalf("stale flags = broker:%v door:%v", snap.L3["broker"].StaleFromPriorLife, snap.L3["door"].StaleFromPriorLife)
	}
	fold.OnObs(context.Background(), "a", current, "broker", []byte("new"))
	fold.OnDown(context.Background(), "a", old, nil)
	if got, ok := fold.Device("a", "broker"); !ok || string(got) != "new" {
		t.Fatalf("old down removed new testimony: %q %v", got, ok)
	}
	fold.OnDown(context.Background(), "a", current, nil)
	if _, ok := fold.Device("a", "broker"); ok {
		t.Fatal("matching down did not remove broker testimony")
	}
	if _, ok := fold.Device("a", "door"); !ok {
		t.Fatal("down removed door-owned testimony")
	}
}

func TestFilteringIsBoundedAndSweepHonorsGrace(t *testing.T) {
	now := time.Unix(100, 0)
	fold := New(nil, func() time.Time { return now }, []actorrt.ObsKind{"level"}, time.Second)
	fold.OnObs(context.Background(), "orphan", actorrt.Incarnation{}, "level", []byte("v"))
	for i := 0; i < 100; i++ {
		fold.OnObs(context.Background(), "a", actorrt.Incarnation{}, actorrt.ObsKind("unknown-"+time.Duration(i).String()), nil)
	}
	fold.OnObs(context.Background(), "a", actorrt.Incarnation{}, "queue_overflow", nil)
	counts := fold.DroppedCounts()
	if len(counts) != len(eventKinds)+2 || counts[unknownDropBucket] != 100 || counts["queue_overflow"] != 1 {
		t.Fatalf("dropped counts = %#v", counts)
	}
	if removed := fold.Sweep(func(actor.ActorID) bool { return false }); removed != 0 {
		t.Fatalf("fresh row swept: %d", removed)
	}
	now = now.Add(2 * time.Second)
	if removed := fold.Sweep(func(actor.ActorID) bool { return false }); removed != 1 {
		t.Fatalf("old orphan sweep = %d, want 1", removed)
	}
}
