package presence

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type inertActor struct{}

func (inertActor) Start(context.Context, actorrt.ActorContext) error { return nil }
func (inertActor) Receive(context.Context, *message.Envelope) error  { return nil }

type fakeExecution struct {
	units map[actor.ActorID]*actorrt.Unit
	clock func() time.Time
}

func (f *fakeExecution) Stat(id actor.ActorID) (actorrt.UnitStat, bool) {
	unit := f.units[id]
	if unit == nil || !unit.IsAlive() {
		return actorrt.UnitStat{}, false
	}
	return unit.Stat(), true
}

func (f *fakeExecution) Incarnation(id actor.ActorID) (actorrt.Incarnation, bool) {
	unit := f.units[id]
	if unit == nil || !unit.IsAlive() {
		return actorrt.Incarnation{}, false
	}
	return unit.Self(), true
}

func (*fakeExecution) Attempt(actor.ActorID) (actorhost.AttemptKey, bool) {
	return "", false
}

type fakeRegistry struct {
	rows map[actor.ActorID]storespec.Record
	err  error
}

func (r *fakeRegistry) LookupActive(_ context.Context, id actor.ActorID) (storespec.ActorControlRow, bool, error) {
	if r.err != nil {
		return storespec.ActorControlRow{}, false, r.err
	}
	row, ok := r.rows[id]
	return storespec.ActorControlRow{ID: row.ID, Kind: row.Kind, Principal: row.Principal, Binding: row.Binding, CreatedAt: row.CreatedAt, CurrentDeclVersion: 1}, ok && row.IsActive(), nil
}
func (r *fakeRegistry) ListActive(context.Context) ([]storespec.ActorControlRow, error) {
	if r.err != nil {
		return nil, r.err
	}
	rows := make([]storespec.ActorControlRow, 0, len(r.rows))
	for _, row := range r.rows {
		if row.IsActive() {
			rows = append(rows, storespec.ActorControlRow{ID: row.ID, Kind: row.Kind, CurrentDeclVersion: 1})
		}
	}
	return rows, nil
}
func (r *fakeRegistry) WorldOf(context.Context, actor.ActorID) (storespec.ActorWorld, bool, error) {
	return storespec.WorldDurable, true, r.err
}
func (r *fakeRegistry) CheckAuthor(ctx context.Context, stamp storespec.AuthorStamp) (storespec.AuthorVerdict, error) {
	_, ok, err := r.LookupActive(ctx, stamp.ID)
	if !ok {
		return storespec.AuthorNotMember, err
	}
	return storespec.AuthorOK, err
}

func spawn(t *testing.T, execution *fakeExecution, id actor.ActorID) actorrt.Incarnation {
	t.Helper()
	unit, err := actorrt.Prepare(actorrt.UnitConfig{
		ActorID: id, Kind: actor.KindAgent, Clock: execution.clock,
	}, func(actorrt.Incarnation) actorrt.Actor { return inertActor{} }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := unit.Start(); err != nil {
		t.Fatal(err)
	}
	execution.units[id] = unit
	t.Cleanup(func() { unit.Stop(); <-unit.Done() })
	return unit.Self()
}

func TestSnapshotFourCellStateSpace(t *testing.T) {
	now := time.Unix(100, 0)
	fold := New(nil, func() time.Time { return now }, []actorrt.ObsKind{"level"}, time.Second)
	execution := &fakeExecution{units: map[actor.ActorID]*actorrt.Unit{}, clock: func() time.Time { return now }}
	reg := &fakeRegistry{rows: map[actor.ActorID]storespec.Record{
		"member-live":   {ID: "member-live"},
		"member-absent": {ID: "member-absent"},
	}}
	liveGen := spawn(t, execution, "member-live")
	ephGen := spawn(t, execution, "ephemeral")
	fold.OnObs(context.Background(), "member-live", liveGen, "level", []byte("a"))
	fold.OnObs(context.Background(), "member-absent", liveGen, "level", []byte("b"))
	fold.OnObs(context.Background(), "ephemeral", ephGen, "level", []byte("c"))
	fold.OnObs(context.Background(), "neither", actorrt.Incarnation{}, "level", []byte("must-not-leak"))

	view := NewView(fold, execution, reg)
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
	fold := New(nil, func() time.Time { return now }, []actorrt.ObsKind{"broker"}, time.Second)
	execution := &fakeExecution{units: map[actor.ActorID]*actorrt.Unit{}}
	old := spawn(t, execution, "a")
	fold.OnObs(context.Background(), "a", old, "broker", []byte("old"))
	execution.units["a"].Stop()
	<-execution.units["a"].Done()
	current := spawn(t, execution, "a")
	view := NewView(fold, execution, &fakeRegistry{rows: map[actor.ActorID]storespec.Record{"a": {ID: "a"}}})
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

func TestNonLevelIgnoredAndSweepHonorsGrace(t *testing.T) {
	now := time.Unix(100, 0)
	fold := New(nil, func() time.Time { return now }, []actorrt.ObsKind{"level"}, time.Second)
	fold.OnObs(context.Background(), "orphan", actorrt.Incarnation{}, "level", []byte("v"))
	for i := 0; i < 100; i++ {
		fold.OnObs(context.Background(), "a", actorrt.Incarnation{}, actorrt.ObsKind("unknown-"+time.Duration(i).String()), nil)
	}
	fold.OnObs(context.Background(), "a", actorrt.Incarnation{}, "queue_overflow", nil)
	if len(fold.latest) != 1 {
		t.Fatalf("non-level observations entered latest-value state: %#v", fold.latest)
	}
	if removed := fold.Sweep(func(actor.ActorID) bool { return false }); removed != 0 {
		t.Fatalf("fresh row swept: %d", removed)
	}
	now = now.Add(2 * time.Second)
	if removed := fold.Sweep(func(actor.ActorID) bool { return false }); removed != 1 {
		t.Fatalf("old orphan sweep = %d, want 1", removed)
	}
}
