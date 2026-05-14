package adapter_test

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/adapter"
)

// memCorrelation is a minimal in-process implementation of the F2
// CorrelationTracker interface (L2 §8.2). It mirrors the runtime/store
// row semantics closely enough to exercise the reserve → mark_done /
// mark_expired / mark_rejected lifecycle from the kernel-side without
// pulling sqlite.
type memCorrelation struct {
	mu   sync.Mutex
	rows map[adapter.CorrelationKey]adapter.CorrelationEntry
}

func newMemCorrelation() *memCorrelation {
	return &memCorrelation{rows: map[adapter.CorrelationKey]adapter.CorrelationEntry{}}
}

func (m *memCorrelation) Reserve(_ context.Context, e adapter.CorrelationEntry) (adapter.CorrelationEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.rows[e.RequestID]; ok {
		// Reserve is idempotent — return the existing row.
		return existing, nil
	}
	if e.State == "" {
		e.State = adapter.CorrelationPending
	}
	m.rows[e.RequestID] = e
	return e, nil
}

func (m *memCorrelation) Get(_ context.Context, id adapter.CorrelationKey) (adapter.CorrelationEntry, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.rows[id]
	return e, ok, nil
}

func (m *memCorrelation) MarkDone(_ context.Context, id adapter.CorrelationKey) error {
	return m.transition(id, adapter.CorrelationDone)
}

func (m *memCorrelation) MarkExpired(_ context.Context, id adapter.CorrelationKey) error {
	return m.transition(id, adapter.CorrelationExpired)
}

func (m *memCorrelation) MarkRejected(_ context.Context, id adapter.CorrelationKey, _ string) error {
	return m.transition(id, adapter.CorrelationRejected)
}

func (m *memCorrelation) transition(id adapter.CorrelationKey, to adapter.CorrelationState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.rows[id]
	if !ok {
		return errors.New("not found")
	}
	// Idempotent — same-state writes are a no-op.
	if e.State == to {
		return nil
	}
	// Terminal states are final; reject pending → terminal → other terminal.
	if e.State != adapter.CorrelationPending {
		return errors.New("not pending")
	}
	e.State = to
	m.rows[id] = e
	return nil
}

func (m *memCorrelation) ListPending(_ context.Context) ([]adapter.CorrelationEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]adapter.CorrelationEntry, 0, len(m.rows))
	for _, e := range m.rows {
		if e.State == adapter.CorrelationPending {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RequestID < out[j].RequestID })
	return out, nil
}

// TestCorrelationStateClosedSet pins the 4-value closed set per L2 §8.2.
func TestCorrelationStateClosedSet(t *testing.T) {
	want := map[adapter.CorrelationState]string{
		adapter.CorrelationPending:  "pending",
		adapter.CorrelationDone:     "done",
		adapter.CorrelationExpired:  "expired",
		adapter.CorrelationRejected: "rejected",
	}
	for state, wire := range want {
		if string(state) != wire {
			t.Errorf("state %v wire form = %q want %q", state, string(state), wire)
		}
	}
}

// TestReserveIdempotent — same RequestID twice returns the original row.
func TestReserveIdempotent(t *testing.T) {
	ctx := context.Background()
	c := newMemCorrelation()

	e := adapter.CorrelationEntry{
		RequestID:     "req-1",
		CorrelationID: "corr-1",
		ChannelID:     "ch-1",
		AudienceActor: "tool:xhs",
		ParentID:      "req-1",
		EnqueuedAt:    100,
		ExpiresAt:     200,
	}
	first, err := c.Reserve(ctx, e)
	if err != nil {
		t.Fatalf("Reserve1: %v", err)
	}
	if first.State != adapter.CorrelationPending {
		t.Errorf("state=%v want pending", first.State)
	}

	// Second reserve with a different EnqueuedAt MUST NOT overwrite — we
	// get the original row back (idempotent contract).
	second, err := c.Reserve(ctx, adapter.CorrelationEntry{
		RequestID:  "req-1",
		EnqueuedAt: 999,
	})
	if err != nil {
		t.Fatalf("Reserve2: %v", err)
	}
	if second.EnqueuedAt != 100 {
		t.Errorf("second reserve overwrote enqueued_at: got %d want 100", second.EnqueuedAt)
	}
}

// TestPendingToDoneTransition — happy path: reserve → mark_done → state=done.
func TestPendingToDoneTransition(t *testing.T) {
	ctx := context.Background()
	c := newMemCorrelation()
	if _, err := c.Reserve(ctx, adapter.CorrelationEntry{RequestID: "r"}); err != nil {
		t.Fatal(err)
	}
	if err := c.MarkDone(ctx, "r"); err != nil {
		t.Fatalf("MarkDone: %v", err)
	}
	got, ok, _ := c.Get(ctx, "r")
	if !ok {
		t.Fatal("Get: not found after MarkDone")
	}
	if got.State != adapter.CorrelationDone {
		t.Errorf("state=%v want done", got.State)
	}
	// MarkDone is idempotent.
	if err := c.MarkDone(ctx, "r"); err != nil {
		t.Errorf("idempotent MarkDone returned %v", err)
	}
}

// TestMarkExpiredAndRejectedTransitions covers the other two terminal
// states per L2 §8.2.
func TestMarkExpiredAndRejectedTransitions(t *testing.T) {
	ctx := context.Background()
	c := newMemCorrelation()
	_, _ = c.Reserve(ctx, adapter.CorrelationEntry{RequestID: "x"})
	_, _ = c.Reserve(ctx, adapter.CorrelationEntry{RequestID: "y"})

	if err := c.MarkExpired(ctx, "x"); err != nil {
		t.Fatalf("MarkExpired: %v", err)
	}
	if err := c.MarkRejected(ctx, "y", "schema_violation"); err != nil {
		t.Fatalf("MarkRejected: %v", err)
	}

	got, _, _ := c.Get(ctx, "x")
	if got.State != adapter.CorrelationExpired {
		t.Errorf("x state=%v want expired", got.State)
	}
	got, _, _ = c.Get(ctx, "y")
	if got.State != adapter.CorrelationRejected {
		t.Errorf("y state=%v want rejected", got.State)
	}
}

// TestTerminalIsFinal — once done/expired/rejected, no further mark calls
// succeed (other than idempotent same-state).
func TestTerminalIsFinal(t *testing.T) {
	ctx := context.Background()
	c := newMemCorrelation()
	_, _ = c.Reserve(ctx, adapter.CorrelationEntry{RequestID: "r"})
	if err := c.MarkDone(ctx, "r"); err != nil {
		t.Fatal(err)
	}
	if err := c.MarkExpired(ctx, "r"); err == nil {
		t.Error("MarkExpired after done should fail (terminal final)")
	}
	if err := c.MarkRejected(ctx, "r", ""); err == nil {
		t.Error("MarkRejected after done should fail (terminal final)")
	}
}

// TestListPendingFiltersOutTerminals — L2 §8.6 boot recovery uses
// ListPending so terminals must be excluded.
func TestListPendingFiltersOutTerminals(t *testing.T) {
	ctx := context.Background()
	c := newMemCorrelation()
	_, _ = c.Reserve(ctx, adapter.CorrelationEntry{RequestID: "a"})
	_, _ = c.Reserve(ctx, adapter.CorrelationEntry{RequestID: "b"})
	_, _ = c.Reserve(ctx, adapter.CorrelationEntry{RequestID: "c"})
	_ = c.MarkDone(ctx, "b")

	pending, err := c.ListPending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 {
		t.Fatalf("ListPending len=%d want 2 (a + c)", len(pending))
	}
	ids := []string{string(pending[0].RequestID), string(pending[1].RequestID)}
	if ids[0] != "a" || ids[1] != "c" {
		t.Errorf("ListPending ids=%v want [a c]", ids)
	}
}

// TestConcurrentReserve — N goroutines reserving the same RequestID see
// exactly one row in the end. Validates the idempotency contract under
// contention (kernel-level test gives runtime backends a target to hit).
func TestConcurrentReserve(t *testing.T) {
	ctx := context.Background()
	c := newMemCorrelation()

	const n = 32
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_, _ = c.Reserve(ctx, adapter.CorrelationEntry{
				RequestID:  "r",
				EnqueuedAt: int64(i),
			})
		}(i)
	}
	wg.Wait()
	pending, _ := c.ListPending(ctx)
	if len(pending) != 1 {
		t.Errorf("concurrent reserve produced %d rows; want 1", len(pending))
	}
}

// TestUnknownTransitionRejected — MarkDone on a never-reserved key fails.
func TestUnknownTransitionRejected(t *testing.T) {
	ctx := context.Background()
	c := newMemCorrelation()
	if err := c.MarkDone(ctx, "ghost"); err == nil {
		t.Error("MarkDone on missing key should fail")
	}
}
