package main

import (
	"testing"
	"time"
)

// TestResolveDaemonEpoch_ExplicitHonored: a non-zero --daemon-epoch is
// returned verbatim and the clock is not consulted.
func TestResolveDaemonEpoch_ExplicitHonored(t *testing.T) {
	got := resolveDaemonEpoch(42, func() time.Time {
		t.Fatal("clock must not be consulted when an explicit epoch is supplied")
		return time.Time{}
	})
	if got != 42 {
		t.Fatalf("explicit epoch not honored: got %d want 42", got)
	}
}

// TestResolveDaemonEpoch_SameSecondRestartDistinct guards INVARIANT-4: two
// daemon process starts within the SAME wall-clock second MUST resolve to
// distinct epochs, otherwise a previous process's stale worker IPC would
// pass fence_check after a fast restart. Unix-second resolution failed this;
// nanosecond resolution passes.
func TestResolveDaemonEpoch_SameSecondRestartDistinct(t *testing.T) {
	base := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	first := resolveDaemonEpoch(0, func() time.Time { return base.Add(10 * time.Millisecond) })
	second := resolveDaemonEpoch(0, func() time.Time { return base.Add(200 * time.Millisecond) })

	if first == second {
		t.Fatalf("same-second restarts produced identical epoch %d — fencing uniqueness broken", first)
	}
	// Monotonic: the later restart yields a strictly larger epoch.
	if second <= first {
		t.Fatalf("epoch not monotonic across restart: first=%d second=%d", first, second)
	}
}
