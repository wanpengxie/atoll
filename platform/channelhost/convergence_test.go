package channelhost

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type desiredRegistry struct {
	mu   sync.RWMutex
	rows map[channel.ID]lagoon.ChannelRow
}

func newDesiredRegistry() *desiredRegistry {
	return &desiredRegistry{rows: make(map[channel.ID]lagoon.ChannelRow)}
}

func (r *desiredRegistry) put(row lagoon.ChannelRow) {
	r.mu.Lock()
	r.rows[row.ID] = row
	r.mu.Unlock()
}

func (r *desiredRegistry) retire(id channel.ID) {
	r.mu.Lock()
	row := r.rows[id]
	row.Status = lagoon.ChannelRetired
	r.rows[id] = row
	r.mu.Unlock()
}

func (r *desiredRegistry) ListChannels(context.Context) ([]lagoon.ChannelRow, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]lagoon.ChannelRow, 0, len(r.rows))
	for _, row := range r.rows {
		out = append(out, row)
	}
	return out, nil
}

func (r *desiredRegistry) GetChannelDesired(_ context.Context, id channel.ID) (lagoon.ChannelRow, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	row, ok := r.rows[id]
	return row, ok, nil
}

func desiredRow(t *testing.T, id channel.ID) lagoon.ChannelRow {
	t.Helper()
	now := time.Now().UnixMilli()
	spec := lagoon.GenesisSpec{ChannelID: id, Type: "group", OwnerPrincipal: "owner", CreatedAt: now, ParentID: protocol.C0ChannelID, InitiatorPrincipal: "owner"}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	return lagoon.ChannelRow{ID: id, ParentID: protocol.C0ChannelID, Name: string(id), Type: "group", Status: lagoon.ChannelPresent, OwnerPrincipal: "owner", Spec: raw, CreatedAt: now}
}

func newConvergingHost(t *testing.T, registry RegistryReader) *ChannelHost {
	t.Helper()
	h, err := New(t.TempDir(), registry, HomeDeps{CompositionResolver: testResolver{}, IntroductionResolver: testResolver{}, RegistryBindings: testBindings{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close(context.Background()) })
	return h
}

func TestRegistryChangeReconcilesBeforeReturning(t *testing.T) {
	registry := newDesiredRegistry()
	h := newConvergingHost(t, registry)
	row := desiredRow(t, "synchronous-create")
	registry.put(row)

	h.RegistryChanged(lagoon.Change{ChannelID: row.ID})
	if _, ok := h.Acquire(row.ID); !ok {
		t.Fatal("post-commit edge returned before the created channel was open")
	}
}

func TestSlowScanRepairsDroppedEdge(t *testing.T) {
	registry := newDesiredRegistry()
	h := newConvergingHost(t, registry)
	h.convergence.fullScan = 20 * time.Millisecond
	if err := h.StartConvergence(); err != nil {
		t.Fatal(err)
	}
	row := desiredRow(t, "lost-edge")
	registry.put(row) // deliberately no RegistryChanged call

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, ok := h.Acquire(row.ID); ok {
			registry.retire(row.ID) // also deliberately drops the retire edge
			for time.Now().Before(deadline) {
				if _, ok := h.Acquire(row.ID); !ok {
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
			t.Fatal("authoritative scan did not destroy a retired channel after a lost edge")
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("authoritative scan did not repair a completely lost edge")
}

func TestOrphanSweepProtectsOnlyC0(t *testing.T) {
	registry := newDesiredRegistry()
	h := newConvergingHost(t, registry)
	if _, err := h.Provision(context.Background(), provisionSpec("orphan")); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Provision(context.Background(), provisionSpec(protocol.C0ChannelID)); err != nil {
		t.Fatal(err)
	}
	if err := h.StartConvergence(); err != nil {
		t.Fatal(err)
	}
	if entries, err := h.Census(context.Background()); err != nil {
		t.Fatal(err)
	} else if len(entries) != 1 || entries[0].ChannelID != protocol.C0ChannelID {
		t.Fatalf("orphan sweep census=%+v", entries)
	}
}

func TestTransientConvergenceUsesExponentialBackoff(t *testing.T) {
	registry := newDesiredRegistry()
	h := newConvergingHost(t, registry)
	h.convergence.edgeDelay = 10 * time.Millisecond
	now := time.Now().UnixMilli()
	spec := lagoon.GenesisSpec{ChannelID: protocol.C0ChannelID, Type: "group", OwnerPrincipal: "owner", CreatedAt: now}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	// A missing c0 file is transient: installation/startup may still be making
	// the well-known physical channel available.
	row := lagoon.ChannelRow{ID: protocol.C0ChannelID, Name: string(protocol.C0ChannelID), Type: "group", Status: lagoon.ChannelPresent, OwnerPrincipal: "owner", Spec: raw, CreatedAt: now}

	if err := h.reconcileTracked(context.Background(), row); err == nil {
		t.Fatal("missing c0 unexpectedly converged")
	}
	first := h.convergence.retries[row.ID]
	if first.permanent || first.failures != 1 {
		t.Fatalf("first retry state = %+v", first)
	}
	if err := h.reconcileTracked(context.Background(), row); err != nil {
		t.Fatalf("backoff window should suppress work, got %v", err)
	}
	if got := h.convergence.retries[row.ID].failures; got != 1 {
		t.Fatalf("backoff did not suppress retry: failures=%d", got)
	}
	h.convergence.retryMu.Lock()
	state := h.convergence.retries[row.ID]
	state.next = time.Now().Add(-time.Millisecond)
	h.convergence.retries[row.ID] = state
	h.convergence.retryMu.Unlock()
	if err := h.reconcileTracked(context.Background(), row); err == nil {
		t.Fatal("due missing c0 unexpectedly converged")
	}
	second := h.convergence.retries[row.ID]
	if second.failures != 2 || second.next.Sub(time.Now()) < 15*time.Millisecond {
		t.Fatalf("second retry state = %+v", second)
	}
}

func TestPermanentFailureClearsOnlyWhenDesiredValueChanges(t *testing.T) {
	registry := newDesiredRegistry()
	h := newConvergingHost(t, registry)
	id := channel.ID(strings.Repeat("x", maxEncodedIDBytes+1))
	row := desiredRow(t, id)
	if err := h.reconcileTracked(context.Background(), row); err == nil {
		t.Fatal("invalid channel id unexpectedly converged")
	}
	first := h.convergence.retries[id]
	if !first.permanent || first.failures != 1 {
		t.Fatalf("permanent state = %+v", first)
	}
	if err := h.reconcileTracked(context.Background(), row); err != nil {
		t.Fatalf("unchanged permanent row should be suppressed, got %v", err)
	}
	if got := h.convergence.retries[id].failures; got != 1 {
		t.Fatalf("unchanged desired value retried: failures=%d", got)
	}

	row.Type = "different"
	if err := h.reconcileTracked(context.Background(), row); err == nil {
		t.Fatal("changed but still-invalid row unexpectedly converged")
	}
	second := h.convergence.retries[id]
	if !second.permanent || second.failures != 1 || second.fingerprint == first.fingerprint {
		t.Fatalf("changed desired value did not replace permanent mark: before=%+v after=%+v", first, second)
	}
}
