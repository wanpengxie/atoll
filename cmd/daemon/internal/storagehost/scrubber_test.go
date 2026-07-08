package storagehost

import (
	"context"
	"strings"
	"testing"
)

func TestScrubber_ReclaimsPendingTombstonesAndAcks(t *testing.T) {
	cr := newTestChannelRoot(t)
	var a Allocator
	if err := a.Alloc(cr, "coord1", false); err != nil {
		t.Fatalf("Alloc: %v", err)
	}

	s := &Scrubber{}
	var acked []string
	ack := func(ctx context.Context, tombstoneID string) (bool, error) {
		acked = append(acked, tombstoneID)
		return true, nil
	}
	s.Pass(t.Context(), cr, nil, nil, []TombstoneToReclaim{
		{TombstoneID: "ts1", Coord: "coord1", Provenance: provenanceAxisAllocated},
	}, ack, nil)

	if _, err := (Streamer{}).OpenRead(cr, "coord1"); err == nil {
		t.Fatal("coord1 bytes must be gone after a reclaim pass")
	}
	if len(acked) != 1 || acked[0] != "ts1" {
		t.Fatalf("acked = %v, want [ts1]", acked)
	}
}

func TestScrubber_ReclaimFailureSkipsAck(t *testing.T) {
	cr := newTestChannelRoot(t)
	s := &Scrubber{}
	var ackCalled bool
	ack := func(ctx context.Context, tombstoneID string) (bool, error) {
		ackCalled = true
		return true, nil
	}
	// A bad coord makes Reclaimer.Reclaim fail (assertPathSegment) — ack
	// must never fire for a collection that did not actually happen.
	s.Pass(t.Context(), cr, nil, nil, []TombstoneToReclaim{
		{TombstoneID: "ts1", Coord: "../escape", Provenance: provenanceAxisAllocated},
	}, ack, nil)
	if ackCalled {
		t.Fatal("ack must not fire when Reclaim failed")
	}
}

func TestScrubber_SweepsOrphanStagingButKeepsPending(t *testing.T) {
	cr := newTestChannelRoot(t)
	var st Streamer

	orphan, err := st.OpenWrite(cr, "orphan-coord")
	if err != nil {
		t.Fatalf("OpenWrite orphan: %v", err)
	}
	_, _ = orphan.Write([]byte("abandoned"))
	// Deliberately never Commit/Abort — simulates a crash mid-write.

	pending, err := st.OpenWrite(cr, "pending-coord")
	if err != nil {
		t.Fatalf("OpenWrite pending: %v", err)
	}
	_, _ = pending.Write([]byte("still uploading"))

	s := &Scrubber{}
	s.Pass(t.Context(), cr, nil, []ReservationPending{
		{ReservationID: "res1", Coord: "pending-coord"},
	}, nil, nil, nil)

	orphanName := strings.TrimPrefix(orphan.stagingRelPath, stagingDir+"/")
	pendingName := strings.TrimPrefix(pending.stagingRelPath, stagingDir+"/")

	entries := listStagingEntries(t, cr)
	foundPending := false
	for _, e := range entries {
		if e == orphanName {
			t.Errorf("orphan staging entry %q must have been swept", e)
		}
		if e == pendingName {
			foundPending = true
		}
	}
	if !foundPending {
		t.Error("pending staging entry must survive the sweep")
	}
}

// TestScrubber_ResendsCommittedForLandedPendingReservation (期11 S6, the
// daemon-crash recovery path §1.7/§6.3 names: "daemon rename后Committed未
//达即daemon crash"): a pending reservation whose coord is ALREADY present
// in live/ (the crash happened after fsync+rename, before/during the
// Committed RPC) must resend Committed — exactly once, and it must NOT
// touch a pending reservation whose coord never landed (still legitimately
// staging).
func TestScrubber_ResendsCommittedForLandedPendingReservation(t *testing.T) {
	cr := newTestChannelRoot(t)
	var a Allocator
	if err := a.Alloc(cr, "landed-coord", false); err != nil {
		t.Fatalf("Alloc landed-coord: %v", err)
	}
	// still-staging-coord has NO live/ entry at all (never touched Alloc or
	// a completed write) — resend must skip it.

	s := &Scrubber{}
	var resent []string
	resend := func(ctx context.Context, reservationID string) (bool, bool, error) {
		resent = append(resent, reservationID)
		return true, false, nil
	}
	s.Pass(t.Context(), cr, nil, []ReservationPending{
		{ReservationID: "res-landed", Coord: "landed-coord"},
		{ReservationID: "res-staging", Coord: "still-staging-coord"},
	}, nil, nil, resend)

	if len(resent) != 1 || resent[0] != "res-landed" {
		t.Fatalf("resent = %v, want exactly [res-landed]", resent)
	}
}

// TestScrubber_NilResendIsNoop confirms a nil resend callback (a daemon
// build that never wires one, or StorageHost.Reconcile's own nil-safe
// default) does not panic.
func TestScrubber_NilResendIsNoop(t *testing.T) {
	cr := newTestChannelRoot(t)
	var a Allocator
	if err := a.Alloc(cr, "landed-coord", false); err != nil {
		t.Fatalf("Alloc landed-coord: %v", err)
	}
	s := &Scrubber{}
	s.Pass(t.Context(), cr, nil, []ReservationPending{
		{ReservationID: "res-landed", Coord: "landed-coord"},
	}, nil, nil, nil)
}

func TestScrubber_LogsMissingLiveEntriesWithoutPanicking(t *testing.T) {
	cr := newTestChannelRoot(t)
	s := &Scrubber{}
	// coord "never-landed" has no matching live/ entry — must log, not panic
	// or error (Pass has no return value to surface it through).
	s.Pass(t.Context(), cr, []ResourceLanded{{Coord: "never-landed"}}, nil, nil, nil, nil)
}
