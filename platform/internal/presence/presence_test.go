package presence

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
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
	fold := New(nil, func() time.Time { return now }, []actorrt.ObsKind{"level"}, nil, time.Second)
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
	fold.OnObs(context.Background(), "neither", actorrt.Incarnation{}, "level", []byte("must-not-leak"))

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

// TestBrokerGenerationRules covers the ONLY remaining source semantics after the
// 来源轴归一 (design v0.6.1): every testimony is broker-sourced, marked stale when
// its incarnation is not the current one, and a down edge deletes only the rows
// whose gen matches (never a fresh successor's).
func TestBrokerGenerationRules(t *testing.T) {
	now := time.Unix(100, 0)
	fold := New(nil, func() time.Time { return now }, []actorrt.ObsKind{"broker"}, nil, time.Second)
	rt, _ := actorrt.New(actorrt.Config{})
	t.Cleanup(rt.StopAll)
	old := spawn(t, rt, "a")
	fold.OnObs(context.Background(), "a", old, "broker", []byte("old"))
	rt.Despawn(old)
	current := spawn(t, rt, "a")
	view := NewView(fold, rt, &fakeRegistry{rows: map[actor.ActorID]storespec.Record{"a": {ID: "a"}}})
	snap, err := view.Snapshot(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	if !snap.L3["broker"].StaleFromPriorLife {
		t.Fatalf("prior-life testimony not marked stale: %v", snap.L3["broker"].StaleFromPriorLife)
	}
	fold.OnObs(context.Background(), "a", current, "broker", []byte("new"))
	fold.OnDown(context.Background(), "a", old, nil)
	snap, _ = view.Snapshot(context.Background(), "a")
	if got, ok := snap.L3["broker"]; !ok || string(got.Val) != "new" {
		t.Fatalf("old down removed new testimony: %q %v", got.Val, ok)
	}
	fold.OnDown(context.Background(), "a", current, nil)
	snap, _ = view.Snapshot(context.Background(), "a")
	if _, ok := snap.L3["broker"]; ok {
		t.Fatal("matching down did not remove broker testimony")
	}
}

func TestFilteringIsBoundedAndSweepHonorsGrace(t *testing.T) {
	now := time.Unix(100, 0)
	eventKinds := []actorrt.ObsKind{"queue_overflow", "closure_fault"}
	fold := New(nil, func() time.Time { return now }, []actorrt.ObsKind{"level"}, eventKinds, time.Second)
	fold.OnObs(context.Background(), "orphan", actorrt.Incarnation{}, "level", []byte("v"))
	for i := 0; i < 100; i++ {
		fold.OnObs(context.Background(), "a", actorrt.Incarnation{}, actorrt.ObsKind("unknown-"+time.Duration(i).String()), nil)
	}
	fold.OnObs(context.Background(), "a", actorrt.Incarnation{}, "queue_overflow", nil)
	counts := fold.DroppedCounts()
	// buckets = one level kind + injected event kinds + the unknown bucket.
	if len(counts) != 1+len(eventKinds)+1 || counts[unknownDropBucket] != 100 || counts["queue_overflow"] != 1 {
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

// TestRealProducerKindsLandInNamedBuckets is the #18 regression: fed the EXACT
// namespaced kinds real producers publish (actorbase/agentbase's own exported
// constants — the same union app hands the fold), every event drops into its
// OWN named bucket, and the unknown bucket stays 0. Before the injection fix the
// fold's private shadow table held bare strings, so every producer kind fell
// through to unknown and all named buckets were永远 0 — this asserts that flip.
func TestRealProducerKindsLandInNamedBuckets(t *testing.T) {
	// agentbase's kind is a LITERAL COPY here (agentbase.ObsCheckpointDrop):
	// platform may not import drivers/* (围栏 Fence B covers _test), and the
	// vocabulary value is a wire-stable constant — drift would be caught by the
	// producer's own tests, this one only needs a real namespaced kind.
	eventKinds := append(actorbase.ObsDropKinds(), actorrt.ObsKind("agentbase.checkpoint_drop"))
	fold := New(nil, time.Now, []actorrt.ObsKind{"level"}, eventKinds, time.Second)
	for _, kind := range eventKinds {
		fold.OnObs(context.Background(), "producer", actorrt.Incarnation{}, kind, nil)
	}
	counts := fold.DroppedCounts()
	if counts[unknownDropBucket] != 0 {
		t.Fatalf("real producer kinds leaked into unknown bucket: %#v", counts)
	}
	for _, kind := range eventKinds {
		if counts[kind] != 1 {
			t.Fatalf("producer kind %q did not land in its named bucket (got %d): %#v", kind, counts[kind], counts)
		}
	}
}
