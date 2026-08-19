package channelhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type desiredRegistry struct {
	mu   sync.RWMutex
	rows map[channel.ID]regspec.ChannelRow
	gets atomic.Int64
}

func newDesiredRegistry() *desiredRegistry {
	return &desiredRegistry{rows: make(map[channel.ID]regspec.ChannelRow)}
}

func (r *desiredRegistry) put(row regspec.ChannelRow) {
	r.mu.Lock()
	r.rows[row.ID] = row
	r.mu.Unlock()
}

func (r *desiredRegistry) retire(id channel.ID) {
	r.mu.Lock()
	row := r.rows[id]
	row.Status = regspec.ChannelRetired
	r.rows[id] = row
	r.mu.Unlock()
}

func (r *desiredRegistry) ListChannels(context.Context) ([]regspec.ChannelRow, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]regspec.ChannelRow, 0, len(r.rows))
	for _, row := range r.rows {
		out = append(out, row)
	}
	return out, nil
}

func (r *desiredRegistry) GetChannelDesired(_ context.Context, id channel.ID) (regspec.ChannelRow, bool, error) {
	r.gets.Add(1)
	r.mu.RLock()
	defer r.mu.RUnlock()
	row, ok := r.rows[id]
	return row, ok, nil
}

func TestRebuildKeepsGenesisLineageAfterCurrentParentChanges(t *testing.T) {
	root := t.TempDir()
	registry := newDesiredRegistry()
	row := desiredRow(t, "reattached-child")
	row.ParentID = "retired-parent"
	var genesis lagoon.GenesisSpec
	if err := json.Unmarshal(row.Spec, &genesis); err != nil {
		t.Fatal(err)
	}
	genesis.ParentID = row.ParentID
	row.Spec, _ = json.Marshal(genesis)

	first, err := New(root, registry, HomeDeps{CompositionResolver: testResolver{}, IntroductionResolver: testResolver{}, RegistryBindings: testBindings{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.provisionGenesis(context.Background(), genesis, row.QualifiedName); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	path, err := DBPath(root, row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}

	// Retiring the old parent changes only the directory's current parent. The
	// immutable genesis JSON deliberately keeps the child's original lineage.
	row.ParentID = channelspec.C0ChannelID
	registry.put(row)
	second, err := New(root, registry, HomeDeps{CompositionResolver: testResolver{}, IntroductionResolver: testResolver{}, RegistryBindings: testBindings{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close(context.Background()) })
	if err := second.StartConvergence(); err != nil {
		t.Fatal(err)
	}
	if _, ok := second.Acquire(row.ID); !ok {
		t.Fatal("reattached child was not rebuilt from its genesis lineage")
	}
	if state, tracked := second.convergence.retries[row.ID]; tracked && state.permanent {
		t.Fatalf("reattached child entered permanent stop set: %+v", state)
	}
}

func desiredRow(t *testing.T, id channel.ID) regspec.ChannelRow {
	t.Helper()
	now := time.Now().UnixMilli()
	spec := lagoon.GenesisSpec{ChannelID: id, Type: "group", OwnerPrincipal: "owner", CreatedAt: now, ParentID: channelspec.C0ChannelID, InitiatorPrincipal: "owner"}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	return regspec.ChannelRow{ID: id, ParentID: channelspec.C0ChannelID, Name: string(id), QualifiedName: "c0.test", Type: "group", Status: regspec.ChannelPresent, OwnerPrincipal: "owner", Spec: raw, CreatedAt: now}
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

func TestRegistryChangeDoesNotReconcileInline(t *testing.T) {
	registry := newDesiredRegistry()
	h := newConvergingHost(t, registry)
	row := desiredRow(t, "queued-only")
	registry.put(row)

	h.RegistryChanged(lagoon.Change{ChannelID: row.ID})
	if _, ok := h.Acquire(row.ID); ok {
		t.Fatal("post-commit edge reconciled inline")
	}
}

func TestCrashBeforePublishConvergesFromCommittedRegistryOnRestart(t *testing.T) {
	root := t.TempDir()
	registry := newDesiredRegistry()
	row := desiredRow(t, "crash-before-publish")
	registry.put(row)
	first, err := New(root, registry, HomeDeps{CompositionResolver: testResolver{}, IntroductionResolver: testResolver{}, RegistryBindings: testBindings{}})
	if err != nil {
		t.Fatal(err)
	}
	crash := errors.New("crash cut before publish")
	first.beforePublish = func(id channel.ID) error {
		if id != row.ID {
			t.Fatalf("hook id=%q", id)
		}
		return crash
	}
	if err := first.StartConvergence(); err != nil {
		t.Fatalf("first convergence err=%v", err)
	}
	if _, serving := first.Acquire(row.ID); serving {
		t.Fatal("crash-cut channel was published")
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	path, _ := DBPath(root, row.ID)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("physical mirror missing after crash cut: %v", err)
	}

	second, err := New(root, registry, HomeDeps{CompositionResolver: testResolver{}, IntroductionResolver: testResolver{}, RegistryBindings: testBindings{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close(context.Background()) })
	if err := second.StartConvergence(); err != nil {
		t.Fatal(err)
	}
	if _, serving := second.Acquire(row.ID); !serving {
		t.Fatal("restart did not publish the committed channel")
	}
}

func TestRegistryChangeQueuesFastReconcile(t *testing.T) {
	registry := newDesiredRegistry()
	h := newConvergingHost(t, registry)
	h.convergence.edgeDelay = 10 * time.Millisecond
	if err := h.StartConvergence(); err != nil {
		t.Fatal(err)
	}
	row := desiredRow(t, "edge-create")
	registry.put(row)

	h.RegistryChanged(lagoon.Change{ChannelID: row.ID})
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, ok := h.Acquire(row.ID); ok {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("post-commit edge did not open the channel through fast reconciliation")
}

func TestRegistryChangeBurstPaysOneEdgeDelay(t *testing.T) {
	registry := newDesiredRegistry()
	h := newConvergingHost(t, registry)
	h.convergence.edgeDelay = 50 * time.Millisecond
	if err := h.StartConvergence(); err != nil {
		t.Fatal(err)
	}
	const count = 8
	started := time.Now()
	for i := 0; i < count; i++ {
		h.RegistryChanged(lagoon.Change{ChannelID: channel.ID(fmt.Sprintf("burst-%d", i))})
	}
	deadline := started.Add(2 * time.Second)
	for registry.gets.Load() < count && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := registry.gets.Load(); got < count {
		t.Fatalf("processed %d/%d burst IDs", got, count)
	}
	if elapsed := time.Since(started); elapsed >= 4*h.convergence.edgeDelay {
		t.Fatalf("burst paid serial edge delays: elapsed=%v delay=%v", elapsed, h.convergence.edgeDelay)
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
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("authoritative scan did not repair a completely lost edge")
}

// Retirement ends the run. A retired channel that kept its Home open would hold
// its database open, keep firing its own timers, and never give the seat back —
// so convergence has to close it. What retirement must NOT do is touch bytes:
// the channel's directory on every daemon is the user's own disk, and no
// cross-machine deletion is ever coordinated (pinned end-to-end by
// TestQualifiedChannelAddressMatchesDiskAndRetirementLeavesBytes).
func TestRetiredDesiredRowReleasesItsRunningInstance(t *testing.T) {
	registry := newDesiredRegistry()
	var membraneCloses atomic.Int64
	h, err := New(t.TempDir(), registry, HomeDeps{
		CompositionResolver: testResolver{}, IntroductionResolver: testResolver{}, RegistryBindings: testBindings{},
		OnMembraneClose: func(channel.ID, uint64) { membraneCloses.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close(context.Background()) })
	row := desiredRow(t, "retire-releases-run")
	registry.put(row)
	if err := h.StartConvergence(); err != nil {
		t.Fatal(err)
	}
	if _, ok := h.Acquire(row.ID); !ok {
		t.Fatal("present channel did not open")
	}
	registry.retire(row.ID)
	if err := h.reconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := h.Acquire(row.ID); ok {
		t.Fatal("retired channel kept serving")
	}
	if membraneCloses.Load() != 1 {
		t.Fatalf("membrane closes=%d, want exactly one", membraneCloses.Load())
	}
	// Convergence keeps seeing the retired row; closing an already-closed
	// channel must stay a no-op rather than re-announcing the edge.
	if err := h.reconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if membraneCloses.Load() != 1 {
		t.Fatalf("repeated reconcile re-closed the membrane: %d", membraneCloses.Load())
	}
}

func TestOrphanSweepProtectsOnlyC0(t *testing.T) {
	registry := newDesiredRegistry()
	h := newConvergingHost(t, registry)
	if err := h.provisionGenesis(context.Background(), genesisSpec("orphan"), "c0.orphan"); err != nil {
		t.Fatal(err)
	}
	if err := h.provisionGenesis(context.Background(), genesisSpec(channelspec.C0ChannelID), "c0"); err != nil {
		t.Fatal(err)
	}
	if err := h.StartConvergence(); err != nil {
		t.Fatal(err)
	}
	if entries, err := h.Census(context.Background()); err != nil {
		t.Fatal(err)
	} else if len(entries) != 1 || entries[0].ChannelID != channelspec.C0ChannelID {
		t.Fatalf("orphan sweep census=%+v", entries)
	}
}

func TestTransientConvergenceUsesExponentialBackoff(t *testing.T) {
	registry := newDesiredRegistry()
	h := newConvergingHost(t, registry)
	h.convergence.edgeDelay = 10 * time.Millisecond
	now := time.Now().UnixMilli()
	spec := lagoon.GenesisSpec{ChannelID: channelspec.C0ChannelID, Type: "group", OwnerPrincipal: "owner", CreatedAt: now}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	// A missing c0 file is transient: installation/startup may still be making
	// the well-known physical channel available.
	row := regspec.ChannelRow{ID: channelspec.C0ChannelID, Name: string(channelspec.C0ChannelID), Type: "group", Status: regspec.ChannelPresent, OwnerPrincipal: "owner", Spec: raw, CreatedAt: now}

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
