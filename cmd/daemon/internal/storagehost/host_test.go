package storagehost

import (
	"testing"
)

// TestHost_OpenWriteSurvivesReconcilePassUntilCommit (期11 S1 #6, end-to-end
// through the real Host — not just Scrubber.Pass in isolation): a plain
// OpenWrite via Host registers coord as active for the ENTIRE handle
// lifetime; a Reconcile pass mid-write (simulating the daemon's own
// periodic ticker firing while a caller is still streaming bytes) must not
// sweep its staging entry, and the write must remain fully usable
// afterward.
func TestHost_OpenWriteSurvivesReconcilePassUntilCommit(t *testing.T) {
	h, err := Open(t.TempDir(), "ch1", nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	wh, err := h.OpenWrite("plain-coord")
	if err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}
	if _, err := wh.Write([]byte("in flight")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// A scrubber pass mid-write, no reservation anywhere (a plain OpWrite —
	// §3.5's own "无 outbox involvement") — Host's own activeWrites registry
	// is the ONLY thing protecting this staging entry.
	h.Reconcile(t.Context(), nil, nil, nil, nil, nil)

	if _, err := wh.Write([]byte(" more bytes")); err != nil {
		t.Fatalf("write must still succeed after a scrubber pass swept nothing: %v", err)
	}

	if err := wh.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	rh, err := h.OpenRead("plain-coord")
	if err != nil {
		t.Fatalf("committed bytes must be readable: %v", err)
	}
	_ = rh.Close()
}

// TestHost_ActiveWriteDeregistersOnAbort confirms the registration is a
// bracket around the handle's OPEN window, not a one-shot flag — a second
// OpenWrite for the same coord after an Abort is a fresh, independent
// registration (refcount back to zero in between), and an orphaned entry
// left behind by a write that never called Commit/Abort at all is still
// swept once the (never-registered-in-the-first-place) case is exercised
// directly against Streamer (see scrubber_test.go's crash-abandoned case).
func TestHost_ActiveWriteDeregistersOnAbort(t *testing.T) {
	h, err := Open(t.TempDir(), "ch1", nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	wh, err := h.OpenWrite("abort-coord")
	if err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}
	if err := wh.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}

	if got := len(h.ActiveWriteCoords()); got != 0 {
		t.Fatalf("ActiveWriteCoords after Abort = %d entries, want 0", got)
	}

	// Now nothing protects abort-coord's (already-removed) staging entry —
	// a fresh Reconcile pass must not panic or misbehave over an empty set.
	h.Reconcile(t.Context(), nil, nil, nil, nil, nil)
}

// TestHost_LandedCoords_ListsLiveDirectory is 期11 review §2.5 #A's daemon
// half: LandedCoords reports exactly the coords present in live/ (an
// Alloc'd content-less create lands there directly), disk-derived so it is
// authoritative across a restart. A staging-only write (never committed) is
// NOT landed and must not appear.
func TestHost_LandedCoords_ListsLiveDirectory(t *testing.T) {
	h, err := Open(t.TempDir(), "ch1", nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	// Two content-less allocations land directly in live/.
	if err := h.Alloc("coord-dir", true); err != nil {
		t.Fatalf("Alloc dir: %v", err)
	}
	if err := h.Alloc("coord-file", false); err != nil {
		t.Fatalf("Alloc file: %v", err)
	}
	// An in-flight plain write stays in staging/, never live/ until Commit.
	wh, err := h.OpenWrite("coord-staging")
	if err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}
	t.Cleanup(func() { _ = wh.Abort() })

	coords, err := h.LandedCoords()
	if err != nil {
		t.Fatalf("LandedCoords: %v", err)
	}
	got := map[string]bool{}
	for _, c := range coords {
		got[c] = true
	}
	if !got["coord-dir"] || !got["coord-file"] {
		t.Fatalf("LandedCoords = %v, want both coord-dir and coord-file", got)
	}
	if got["coord-staging"] {
		t.Fatal("LandedCoords included an un-committed staging coord — only live/ entries are landed")
	}
}

// TestHost_LandedCoords_ReadErrorIsReturned is 期11 review残余#1's DoD: a
// read failure must surface as an error, NEVER a silently-fabricated empty
// slice — an empty answer here would tell the home NOTHING landed, and its
// SAME-TICK SweepExpiredReservations would then sweep an already-landed
// reservation as abandoned before any "retry next tick" ever happens (the
// pre-fix doc's promise was false: sweep runs in the SAME ReconcilePull, not
// a later one). Closing h before calling LandedCoords breaks the live/ read
// (the *os.Root handle is closed), exercising the real error path with no
// fakes/mocks needed.
func TestHost_LandedCoords_ReadErrorIsReturned(t *testing.T) {
	h, err := Open(t.TempDir(), "ch1", nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	coords, err := h.LandedCoords()
	if err == nil {
		t.Fatal("LandedCoords on a closed root must return an error, not a fabricated empty slice")
	}
	if coords != nil {
		t.Fatalf("LandedCoords on error must return a nil slice, got %v", coords)
	}
}
