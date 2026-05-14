package main

// T112 R2-FIX-6: unit tests for multiAdapterManager fanout filtering.
//
// multiAdapterManager fans an xhs callback across every channel's
// adapter.Manager. Channels that do not host xhs (their actor_registry
// has no row for xhs.AdapterActorID) — or whose Manager has not yet
// committed Install — return adapter.ErrAdapterNotInstalled. Those
// returns must be treated as "not me" and skipped, NOT propagated as
// a 500 to the external caller (which would otherwise interpret a
// retry as L2 §8.2 duplicate callback).

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/coagent-ai/daemon-go/pkg/adapter"
)

// stubAdapterTarget records each call and returns the configured err.
type stubAdapterTarget struct {
	name  string
	err   error
	calls int
}

func (s *stubAdapterTarget) OnExternalCallback(_ context.Context, _ string, _ []byte) error {
	s.calls++
	return s.err
}

// TestMultiAdapterManager_NotInstalledIgnored — every Manager returns
// ErrAdapterNotInstalled (e.g. no channel hosts the adapter). Fanout
// must surface nil, NOT a 500-equivalent error.
func TestMultiAdapterManager_NotInstalledIgnored(t *testing.T) {
	t1 := &stubAdapterTarget{name: "a", err: fmt.Errorf("%w: Install has not run", adapter.ErrAdapterNotInstalled)}
	t2 := &stubAdapterTarget{name: "b", err: fmt.Errorf("%w: unknown adapter %q", adapter.ErrAdapterNotInstalled, "xhs")}
	m := &multiAdapterManager{managers: []adapterCallbackTarget{t1, t2}}

	if err := m.OnExternalCallback(context.Background(), "xhs", []byte("{}")); err != nil {
		t.Fatalf("OnExternalCallback err = %v; want nil", err)
	}
	if t1.calls != 1 || t2.calls != 1 {
		t.Fatalf("call counts = (%d, %d); want (1, 1)", t1.calls, t2.calls)
	}
}

// TestMultiAdapterManager_OwnerErrorPropagates — one Manager is
// "not me" (ErrAdapterNotInstalled), the other owns the callback and
// returns a real error. The owning Manager's error must propagate.
func TestMultiAdapterManager_OwnerErrorPropagates(t *testing.T) {
	notMe := &stubAdapterTarget{err: fmt.Errorf("%w: unknown adapter %q", adapter.ErrAdapterNotInstalled, "xhs")}
	ownerErr := errors.New("xhs: malformed payload")
	owner := &stubAdapterTarget{err: ownerErr}
	m := &multiAdapterManager{managers: []adapterCallbackTarget{notMe, owner}}

	err := m.OnExternalCallback(context.Background(), "xhs", []byte("{}"))
	if err == nil {
		t.Fatalf("expected owner error to propagate, got nil")
	}
	if !errors.Is(err, ownerErr) {
		// errors.Is on a sentinel via direct comparison works because
		// we passed the raw ownerErr without wrapping.
		t.Fatalf("err = %v; want errors.Is(ownerErr)", err)
	}
}

// TestMultiAdapterManager_FirstNonSentinelErrorWins — two managers
// return non-sentinel errors; the first one in fanout order wins.
// Documents the existing "firstErr" contract so a future change
// doesn't silently flip ordering semantics.
func TestMultiAdapterManager_FirstNonSentinelErrorWins(t *testing.T) {
	first := errors.New("first error")
	second := errors.New("second error")
	t1 := &stubAdapterTarget{err: first}
	t2 := &stubAdapterTarget{err: second}
	m := &multiAdapterManager{managers: []adapterCallbackTarget{t1, t2}}

	err := m.OnExternalCallback(context.Background(), "xhs", []byte("{}"))
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, first) {
		t.Fatalf("err = %v; want errors.Is(first)", err)
	}
	if t1.calls != 1 || t2.calls != 1 {
		t.Fatalf("call counts = (%d, %d); want (1, 1) (fanout must touch every peer even after first error)", t1.calls, t2.calls)
	}
}

// TestMultiAdapterManager_EmptyManagersIsNil — no managers wired in
// returns nil cleanly. Guard against an off-by-one panic on the
// degenerate single-channel/no-adapter daemon boot.
func TestMultiAdapterManager_EmptyManagersIsNil(t *testing.T) {
	m := &multiAdapterManager{}
	if err := m.OnExternalCallback(context.Background(), "xhs", []byte("{}")); err != nil {
		t.Fatalf("OnExternalCallback err = %v; want nil", err)
	}
}
