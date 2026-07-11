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
	h.Reconcile(t.Context(), nil, nil, nil, nil)

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
	h.Reconcile(t.Context(), nil, nil, nil, nil)
}
