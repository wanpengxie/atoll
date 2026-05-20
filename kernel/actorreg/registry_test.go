package actorreg_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
)

// memRegistry is a minimal in-package implementation of actorreg.Registry
// (kept here so the kernel test doesn't pull adapters/framework). It
// mirrors the runtime/store contract closely enough to exercise the
// register → deregister → still-resolvable lifecycle from L1 §12.4.
type memRegistry struct {
	mu   sync.Mutex
	rows map[actor.ActorID]actorreg.Record
}

func newMemRegistry() *memRegistry {
	return &memRegistry{rows: map[actor.ActorID]actorreg.Record{}}
}

func (r *memRegistry) Lookup(_ context.Context, id actor.ActorID) (actorreg.Record, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.rows[id]
	return rec, ok, nil
}

func (r *memRegistry) Exists(_ context.Context, id actor.ActorID) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.rows[id]
	return ok, nil
}

func (r *memRegistry) ListActive(_ context.Context) ([]actorreg.Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]actorreg.Record, 0)
	for _, rec := range r.rows {
		if rec.IsActive() {
			out = append(out, rec)
		}
	}
	return out, nil
}

func (r *memRegistry) Insert(_ context.Context, rec actorreg.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.rows[rec.ID]; dup {
		return errors.New("duplicate")
	}
	r.rows[rec.ID] = rec
	return nil
}

func (r *memRegistry) Deregister(_ context.Context, id actor.ActorID, at int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.rows[id]
	if !ok {
		return errors.New("not found")
	}
	rec.DeregisteredAt = at
	r.rows[id] = rec
	return nil
}

// TestRecordIsActive covers the L1 §12.2 active-bit semantic.
func TestRecordIsActive(t *testing.T) {
	r := actorreg.Record{ID: "agent:a"}
	if !r.IsActive() {
		t.Error("DeregisteredAt=0 should be active")
	}
	r.DeregisteredAt = 42
	if r.IsActive() {
		t.Error("DeregisteredAt!=0 should be inactive")
	}
}

// TestRegisterDeregisterStillResolvable covers the L1 §12.4 lifecycle:
//   - Lookup of a deregistered actor still returns the record (so
//     historical message sender.id references resolve)
//   - Exists returns true regardless of deregistration
//   - ListActive excludes deregistered actors
func TestRegisterDeregisterStillResolvable(t *testing.T) {
	ctx := context.Background()
	reg := newMemRegistry()

	rec := actorreg.Record{
		ID:        "agent:alpha",
		Kind:      actor.KindAgent,
		Binding:   actor.BindingEmbedded,
		CreatedAt: 1000,
	}
	if err := reg.Insert(ctx, rec); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	got, ok, err := reg.Lookup(ctx, "agent:alpha")
	if err != nil || !ok || !got.IsActive() {
		t.Fatalf("post-insert Lookup: ok=%v active=%v err=%v", ok, got.IsActive(), err)
	}

	active, err := reg.ListActive(ctx)
	if err != nil || len(active) != 1 {
		t.Fatalf("ListActive pre-deregister: len=%d err=%v", len(active), err)
	}

	if err := reg.Deregister(ctx, "agent:alpha", 2000); err != nil {
		t.Fatalf("Deregister: %v", err)
	}

	got, ok, _ = reg.Lookup(ctx, "agent:alpha")
	if !ok {
		t.Fatal("Lookup of deregistered actor must still return the row (historical resolvability)")
	}
	if got.IsActive() {
		t.Errorf("deregistered actor must report inactive; got DeregisteredAt=%d", got.DeregisteredAt)
	}
	if got.DeregisteredAt != 2000 {
		t.Errorf("DeregisteredAt=%d want 2000", got.DeregisteredAt)
	}

	ex, _ := reg.Exists(ctx, "agent:alpha")
	if !ex {
		t.Error("Exists must return true for deregistered actor")
	}

	active, _ = reg.ListActive(ctx)
	if len(active) != 0 {
		t.Errorf("ListActive should exclude deregistered actor, got %d", len(active))
	}
}

// TestSystemActorIDIsConstant guards the well-known L1 §3.2 fixed id.
func TestSystemActorIDIsConstant(t *testing.T) {
	if actor.SystemActorID.String() != "system" {
		t.Errorf("SystemActorID=%s want=system", actor.SystemActorID)
	}
}

// TestConcurrentInsertSameID — N goroutines inserting the same actor id
// must end up with exactly one row; backends MUST enforce uniqueness
// (mirror SQL UNIQUE).
func TestConcurrentInsertSameID(t *testing.T) {
	ctx := context.Background()
	reg := newMemRegistry()

	const n = 32
	var (
		ok       atomic.Int32
		conflict atomic.Int32
		wg       sync.WaitGroup
	)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			err := reg.Insert(ctx, actorreg.Record{
				ID:        "agent:concurrent",
				Kind:      actor.KindAgent,
				CreatedAt: int64(i),
			})
			if err == nil {
				ok.Add(1)
			} else {
				conflict.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if ok.Load() != 1 {
		t.Errorf("concurrent Insert succeeded %d times; want exactly 1", ok.Load())
	}
	if conflict.Load() != n-1 {
		t.Errorf("conflicting Inserts = %d; want %d", conflict.Load(), n-1)
	}
}

// TestDeregisterMissingActor — Deregister of an unknown id surfaces an
// error (caller bug, not silent no-op).
func TestDeregisterMissingActor(t *testing.T) {
	ctx := context.Background()
	reg := newMemRegistry()
	if err := reg.Deregister(ctx, "ghost", 1); err == nil {
		t.Error("Deregister on missing id should surface error")
	}
}

// TestExistsAfterFullLifecycle — Exists must return true even after
// soft-delete so historical message sender.id references resolve.
func TestExistsAfterFullLifecycle(t *testing.T) {
	ctx := context.Background()
	reg := newMemRegistry()
	_ = reg.Insert(ctx, actorreg.Record{ID: "tool:x", Kind: actor.KindTool})
	_ = reg.Deregister(ctx, "tool:x", 1)
	ex, err := reg.Exists(ctx, "tool:x")
	if err != nil {
		t.Fatal(err)
	}
	if !ex {
		t.Error("Exists must return true post-deregister")
	}
}

// TestRecordBindingZeroValueForHuman — Binding is empty string for
// human / system actors per L1 §12.2.
func TestRecordBindingZeroValueForHuman(t *testing.T) {
	r := actorreg.Record{ID: "user:alice", Kind: actor.KindHuman}
	if r.Binding != "" {
		t.Errorf("human Record.Binding=%q want empty", r.Binding)
	}
}
