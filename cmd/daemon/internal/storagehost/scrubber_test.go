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
	}, nil, ack, nil)

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
	}, nil, ack, nil)
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
	}, nil, nil, nil, nil)

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

// TestScrubber_ActiveWriteSurvivesSweepWithNoReservationAtAll (期11 S1 #6):
// a PLAIN OpWrite carries no reservation row whatsoever (§3.5: "无 outbox
// involvement") — pendingReservations is empty, yet the in-flight write must
// still survive a scrubber pass purely on Host's own activeWrites signal. A
// genuinely orphaned entry (never registered as active, never Commit/Abort'd
// — a true crash) is still swept, proving the new signal narrows the sweep
// rather than disabling it.
func TestScrubber_ActiveWriteSurvivesSweepWithNoReservationAtAll(t *testing.T) {
	cr := newTestChannelRoot(t)
	var st Streamer

	active, err := st.OpenWrite(cr, "active-plain-coord")
	if err != nil {
		t.Fatalf("OpenWrite active: %v", err)
	}
	_, _ = active.Write([]byte("in flight, no reservation"))

	orphan, err := st.OpenWrite(cr, "orphan-plain-coord")
	if err != nil {
		t.Fatalf("OpenWrite orphan: %v", err)
	}
	_, _ = orphan.Write([]byte("abandoned, no reservation, not active either"))

	s := &Scrubber{}
	// pendingReservations is nil throughout — the ONLY thing distinguishing
	// active-plain-coord from orphan-plain-coord is the activeWrites entry.
	s.Pass(t.Context(), cr, nil, nil, nil, func() []ActiveStaging { return []ActiveStaging{{Coord: "active-plain-coord"}} }, nil, nil)

	activeName := strings.TrimPrefix(active.stagingRelPath, stagingDir+"/")
	orphanName := strings.TrimPrefix(orphan.stagingRelPath, stagingDir+"/")

	entries := listStagingEntries(t, cr)
	foundActive := false
	for _, e := range entries {
		if e == orphanName {
			t.Errorf("orphan-plain-coord's staging entry %q must have been swept", e)
		}
		if e == activeName {
			foundActive = true
		}
	}
	if !foundActive {
		t.Error("active-plain-coord's staging entry must survive the sweep")
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
	}, nil, nil, nil, resend)

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
	}, nil, nil, nil, nil)
}

// TestScrubber_ResendLostReclaimsCoord is 期11 S2's crash-recovery-path DoD
// proof (transfer-lifecycle-spec.md §3's #2): a resend that comes back
// lost=true (this reservation landed locally BEFORE the daemon crashed, but
// lost the same-resource_id race at the home, discovered only on this later
// resumed Committed) must reclaim the coord's already-landed live bytes —
// never leave them as a permanent orphan just because the discovery happened
// on a resend rather than the original synchronous Commit.
func TestScrubber_ResendLostReclaimsCoord(t *testing.T) {
	cr := newTestChannelRoot(t)
	var a Allocator
	if err := a.Alloc(cr, "loser-coord", false); err != nil {
		t.Fatalf("Alloc loser-coord: %v", err)
	}

	s := &Scrubber{}
	resend := func(ctx context.Context, reservationID string) (bool, bool, error) {
		return true, true, nil // found, LOST
	}
	s.Pass(t.Context(), cr, nil, []ReservationPending{
		{ReservationID: "res-loser", Coord: "loser-coord"},
	}, nil, nil, nil, resend)

	if _, err := (Streamer{}).OpenRead(cr, "loser-coord"); err == nil {
		t.Fatal("loser-coord's live bytes must be reclaimed after a lost resend, no orphan left behind")
	}
}

func TestScrubber_LogsMissingLiveEntriesWithoutPanicking(t *testing.T) {
	cr := newTestChannelRoot(t)
	s := &Scrubber{}
	// coord "never-landed" has no matching live/ entry — must log, not panic
	// or error (Pass has no return value to surface it through).
	s.Pass(t.Context(), cr, []ResourceLanded{{Coord: "never-landed"}}, nil, nil, nil, nil, nil)
}

// TestScrubber_ActiveWritesSnapshotAfterNetworkPhase pins 期11 review P1-1: the
// active-writes snapshot must be taken AFTER Pass's (multi-second, network-RPC)
// reclaim/resend phase, immediately before the staging sweep — not at Pass
// entry. A snapshot taken too early would miss a write that begins during the
// network phase and delete its staging out from under a live writer. The
// activeWrites arg is a snapshotTER for exactly this reason; here it asserts the
// network phase already ran when it is called, and protects a live plain-write
// staging with no reservation.
func TestScrubber_ActiveWritesSnapshotAfterNetworkPhase(t *testing.T) {
	cr := newTestChannelRoot(t)

	live, err := (Streamer{}).OpenWrite(cr, "late-coord")
	if err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}
	_, _ = live.Write([]byte("in flight"))
	liveName := strings.TrimPrefix(live.stagingRelPath, stagingDir+"/")

	var ackRan bool
	ack := func(ctx context.Context, tombstoneID string) (bool, error) {
		ackRan = true // stands in for the network phase completing
		return true, nil
	}
	activeWrites := func() []ActiveStaging {
		if !ackRan {
			t.Error("P1-1 regression: active-writes snapshot taken BEFORE the network phase")
		}
		return []ActiveStaging{{Coord: "late-coord"}}
	}

	s := &Scrubber{}
	s.Pass(t.Context(), cr, nil, nil, []TombstoneToReclaim{
		{TombstoneID: "t1", Coord: "gone-coord", Provenance: provenanceAxisAllocated},
	}, activeWrites, ack, nil)

	for _, e := range listStagingEntries(t, cr) {
		if e == liveName {
			return // survived — correct
		}
	}
	t.Fatal("active write's staging was swept — the snapshot missed a write started during the pass (P1-1 regression)")
}
