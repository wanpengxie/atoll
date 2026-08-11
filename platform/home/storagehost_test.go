package home

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

// fakeOutbox is a configurable resourcespec.ResourceOutbox stub —
// homeStorageHostControl's sender-auth + outbox-completion tests drive it
// one branch at a time.
type fakeOutbox struct {
	reservationDaemon    string
	reservationFound     bool
	reservationDaemonErr error
	commitFound          bool
	commitErr            error
	commitCalls          []string

	tombstoneDaemon    string
	tombstoneFound     bool
	tombstoneDaemonErr error
	clearFound         bool
	clearErr           error
	clearCalls         []string

	byPlacementRows []resourcespec.ResourceRow
	byPlacementErr  error
	reservationRows []resourcespec.ReservationRow
	reservationsErr error
	tombstoneRows   []resourcespec.TombstoneRow
	tombstonesErr   error

	sweepCalls []sweepCall
	sweptRows  []resourcespec.ReservationRow
	sweepErr   error

	touchCalls []touchCall
	touchErr   error
}

type sweepCall struct {
	daemonID string
	cutoffMs int64
}

type touchCall struct {
	daemonID string
	coords   []string
	atMs     int64
}

func (f *fakeOutbox) CommitReservation(ctx context.Context, reservationID string) (resourcespec.LandedResource, bool, error) {
	f.commitCalls = append(f.commitCalls, reservationID)
	return resourcespec.LandedResource{}, f.commitFound, f.commitErr
}
func (f *fakeOutbox) ClearTombstone(ctx context.Context, tombstoneID string) (bool, error) {
	f.clearCalls = append(f.clearCalls, tombstoneID)
	return f.clearFound, f.clearErr
}
func (f *fakeOutbox) ReservationDaemon(ctx context.Context, reservationID string) (string, bool, error) {
	return f.reservationDaemon, f.reservationFound, f.reservationDaemonErr
}
func (f *fakeOutbox) TombstoneDaemon(ctx context.Context, tombstoneID string) (string, bool, error) {
	return f.tombstoneDaemon, f.tombstoneFound, f.tombstoneDaemonErr
}
func (f *fakeOutbox) ListReservationsByDaemon(ctx context.Context, daemonID string) ([]resourcespec.ReservationRow, error) {
	return f.reservationRows, f.reservationsErr
}
func (f *fakeOutbox) ListTombstonesByDaemon(ctx context.Context, daemonID string) ([]resourcespec.TombstoneRow, error) {
	return f.tombstoneRows, f.tombstonesErr
}
func (f *fakeOutbox) ListByPlacementDaemon(ctx context.Context, daemonID string) ([]resourcespec.ResourceRow, error) {
	return f.byPlacementRows, f.byPlacementErr
}
func (f *fakeOutbox) SweepExpiredReservations(ctx context.Context, daemonID string, cutoffMs int64) ([]resourcespec.ReservationRow, error) {
	f.sweepCalls = append(f.sweepCalls, sweepCall{daemonID: daemonID, cutoffMs: cutoffMs})
	return f.sweptRows, f.sweepErr
}
func (f *fakeOutbox) TouchReservationsByCoords(ctx context.Context, daemonID string, coords []string, atMs int64) error {
	f.touchCalls = append(f.touchCalls, touchCall{daemonID: daemonID, coords: coords, atMs: atMs})
	return f.touchErr
}

var _ resourcespec.ResourceOutbox = (*fakeOutbox)(nil)

// fixedRoutes answers every storage route with one canned error — these tests
// are about what this seam does to that error, not about transport.
type fixedRoutes struct{ err error }

func (fixedRoutes) PokePlan(string, string) {}
func (r fixedRoutes) SendAlloc(context.Context, string, string, string, bool) error {
	return r.err
}
func (r fixedRoutes) SendReclaim(context.Context, string, string, string) error { return r.err }
func (fixedRoutes) OpenTransfer(
	context.Context, string, string, string, access.Operation, string,
) (string, error) {
	return "", nil
}
func (fixedRoutes) AttachedDaemons(string) []string  { return nil }
func (fixedRoutes) LaneAttached(string, string) bool { return false }

var _ platform.DaemonRoutes = fixedRoutes{}

// TestStorageControlKeepsNotAttemptedDistinctFromRefused pins the one point in
// the process where the transport's "the daemon attempted nothing" answer and
// the door's own name for it can meet: runtime never imports platform, so if
// this seam drops the translation, the door's caller sees an opaque error and
// treats a still-building daemon as a hard create failure. Nothing downstream
// can recover the distinction once it is lost here.
func TestStorageControlKeepsNotAttemptedDistinctFromRefused(t *testing.T) {
	control := daemonStorageControl{routes: fixedRoutes{err: platform.ErrDaemonNotReady}, chID: "channel-a"}
	if err := control.AllocRequest(context.Background(), "daemon-1", accessdoor.StorageAllocSpec{
		Coord: "coord-a",
	}); !errors.Is(err, accessdoor.ErrStorageNotReady) {
		t.Fatalf("AllocRequest error=%v, want it to carry accessdoor.ErrStorageNotReady", err)
	}
	if err := control.ReclaimRequest(context.Background(), "daemon-1", "coord-a"); !errors.Is(err, accessdoor.ErrStorageNotReady) {
		t.Fatalf("ReclaimRequest error=%v, want it to carry accessdoor.ErrStorageNotReady", err)
	}

	// The other half: a refusal must not acquire the sentinel on the way
	// through, or the guard above would hold for a seam that relabelled
	// everything.
	refused := errors.New("disk full")
	control = daemonStorageControl{routes: fixedRoutes{err: refused}, chID: "channel-a"}
	err := control.AllocRequest(context.Background(), "daemon-1", accessdoor.StorageAllocSpec{Coord: "coord-a"})
	if !errors.Is(err, refused) || errors.Is(err, accessdoor.ErrStorageNotReady) {
		t.Fatalf("a refusal must stay a refusal, got %v", err)
	}
}

func TestHomeStorageHostControl_Committed_SenderAuth(t *testing.T) {
	t.Run("matching sender lands the reservation", func(t *testing.T) {
		ob := &fakeOutbox{reservationDaemon: "daemon-1", reservationFound: true, commitFound: true}
		h := homeStorageHostControl{outbox: ob}
		found, lost, err := h.Committed(t.Context(), "daemon-1", "res-1")
		if err != nil || !found || lost {
			t.Fatalf("Committed = (%v,%v,%v), want (true,false,nil)", found, lost, err)
		}
		if len(ob.commitCalls) != 1 || ob.commitCalls[0] != "res-1" {
			t.Fatalf("CommitReservation calls = %v", ob.commitCalls)
		}
	})

	t.Run("mismatched sender is rejected before touching the outbox", func(t *testing.T) {
		ob := &fakeOutbox{reservationDaemon: "daemon-A", reservationFound: true}
		h := homeStorageHostControl{outbox: ob}
		_, _, err := h.Committed(t.Context(), "daemon-B", "res-1")
		if !errors.Is(err, errSenderDaemonMismatch) {
			t.Fatalf("err = %v, want errSenderDaemonMismatch", err)
		}
		if len(ob.commitCalls) != 0 {
			t.Fatal("CommitReservation must not run on a sender mismatch")
		}
	})

	t.Run("unknown reservation is a clean no-op, not an error", func(t *testing.T) {
		ob := &fakeOutbox{reservationFound: false}
		h := homeStorageHostControl{outbox: ob}
		found, lost, err := h.Committed(t.Context(), "daemon-1", "res-gone")
		if err != nil || found || lost {
			t.Fatalf("Committed = (%v,%v,%v), want (false,false,nil)", found, lost, err)
		}
	})

	t.Run("ErrReservationLost surfaces as lost=true, not a Go error", func(t *testing.T) {
		ob := &fakeOutbox{reservationDaemon: "daemon-1", reservationFound: true, commitFound: true, commitErr: resourcespec.ErrReservationLost}
		h := homeStorageHostControl{outbox: ob}
		found, lost, err := h.Committed(t.Context(), "daemon-1", "res-1")
		if err != nil || !found || !lost {
			t.Fatalf("Committed = (%v,%v,%v), want (true,true,nil)", found, lost, err)
		}
	})
}

func TestHomeStorageHostControl_ReclaimAck_SenderAuth(t *testing.T) {
	t.Run("matching sender clears the tombstone", func(t *testing.T) {
		ob := &fakeOutbox{tombstoneDaemon: "daemon-1", tombstoneFound: true, clearFound: true}
		h := homeStorageHostControl{outbox: ob}
		found, err := h.ReclaimAck(t.Context(), "daemon-1", "ts-1")
		if err != nil || !found {
			t.Fatalf("ReclaimAck = (%v,%v), want (true,nil)", found, err)
		}
	})

	t.Run("mismatched sender is rejected before touching the outbox", func(t *testing.T) {
		ob := &fakeOutbox{tombstoneDaemon: "daemon-A", tombstoneFound: true}
		h := homeStorageHostControl{outbox: ob}
		_, err := h.ReclaimAck(t.Context(), "daemon-B", "ts-1")
		if !errors.Is(err, errSenderDaemonMismatch) {
			t.Fatalf("err = %v, want errSenderDaemonMismatch", err)
		}
		if len(ob.clearCalls) != 0 {
			t.Fatal("ClearTombstone must not run on a sender mismatch")
		}
	})
}

func TestHomeStorageHostControl_ReconcilePull_ProjectsPerDaemonRows(t *testing.T) {
	ob := &fakeOutbox{
		byPlacementRows: []resourcespec.ResourceRow{{Meta: resourcespec.ResourceMeta{PlacementCoord: "c1"}}},
		reservationRows: []resourcespec.ReservationRow{{ReservationID: "r1", PlacementCoord: "c2"}},
		tombstoneRows:   []resourcespec.TombstoneRow{{TombstoneID: "t1", PlacementCoord: "c3"}},
	}
	h := homeStorageHostControl{outbox: ob}
	resources, reservations, tombstones, err := h.ReconcilePull(t.Context(), "daemon-1", []string{"c2"})
	if err != nil {
		t.Fatalf("ReconcilePull: %v", err)
	}
	if len(resources) != 1 || resources[0].Coord != "c1" {
		t.Errorf("resources = %+v", resources)
	}
	if len(reservations) != 1 || reservations[0].ReservationID != "r1" || reservations[0].Coord != "c2" {
		t.Errorf("reservations = %+v", reservations)
	}
	if len(tombstones) != 1 || tombstones[0].TombstoneID != "t1" || tombstones[0].Coord != "c3" {
		t.Errorf("tombstones = %+v", tombstones)
	}
}

// TestHomeStorageHostControl_ReconcilePull_SweepsExpiredReservationsFirst
// (期11 spec §1.7's third reservation-deletion trigger, S6's account): the
// server ages out a reservation older than the injected timeout BEFORE
// answering ListReservationsByDaemon — a daemon that never received the
// AllocRequest for it (so never stages anything, never has cause to resend
// Committed) does not see the same abandoned row forever.
func TestHomeStorageHostControl_ReconcilePull_SweepsExpiredReservationsFirst(t *testing.T) {
	ob := &fakeOutbox{}
	fixedNow := func() time.Time { return time.UnixMilli(1_000_000) }
	h := homeStorageHostControl{outbox: ob, timeout: 30 * time.Second, now: fixedNow}

	if _, _, _, err := h.ReconcilePull(t.Context(), "daemon-1", nil); err != nil {
		t.Fatalf("ReconcilePull: %v", err)
	}
	if len(ob.sweepCalls) != 1 {
		t.Fatalf("sweep calls = %d, want 1", len(ob.sweepCalls))
	}
	call := ob.sweepCalls[0]
	if call.daemonID != "daemon-1" {
		t.Fatalf("sweep daemonID = %q, want daemon-1", call.daemonID)
	}
	wantCutoff := fixedNow().Add(-30 * time.Second).UnixMilli()
	if call.cutoffMs != wantCutoff {
		t.Fatalf("sweep cutoffMs = %d, want %d", call.cutoffMs, wantCutoff)
	}
}

// ReconcilePull has no landed-phase mutation and still runs the age sweep.
func TestHomeStorageHostControl_ReconcilePull_HasNoLandedPhase(t *testing.T) {
	ob := &fakeOutbox{}
	h := homeStorageHostControl{outbox: ob}
	if _, _, _, err := h.ReconcilePull(t.Context(), "daemon-1", nil); err != nil {
		t.Fatalf("ReconcilePull: %v", err)
	}
	if len(ob.sweepCalls) != 1 {
		t.Fatalf("sweep calls = %d, want 1", len(ob.sweepCalls))
	}
}

// TestHomeStorageHostControl_ReconcilePull_TouchesOnlyActiveCoordsAfterSweep
// (期11 review's own narrowing fix of S1's "在途登记" liveness bump,
// transfer-lifecycle-spec.md §2 item 1): the sender's OWN ActiveCoords list
// — not "this daemon called ReconcilePull at all" — is what
// TouchReservationsByCoords must run exactly once for, stamped with the SAME
// clock ReconcilePull used for its cutoff, and it must run AFTER Sweep (so
// sweep judges the PRE-touch value, never masking a genuinely stale daemon).
func TestHomeStorageHostControl_ReconcilePull_TouchesOnlyActiveCoordsAfterSweep(t *testing.T) {
	ob := &fakeOutbox{}
	fixedNow := func() time.Time { return time.UnixMilli(2_000_000) }
	h := homeStorageHostControl{outbox: ob, timeout: 30 * time.Second, now: fixedNow}

	activeCoords := []string{"coord-active-1", "coord-active-2"}
	if _, _, _, err := h.ReconcilePull(t.Context(), "daemon-1", activeCoords); err != nil {
		t.Fatalf("ReconcilePull: %v", err)
	}
	if len(ob.touchCalls) != 1 {
		t.Fatalf("touch calls = %d, want 1", len(ob.touchCalls))
	}
	call := ob.touchCalls[0]
	if call.daemonID != "daemon-1" {
		t.Fatalf("touch daemonID = %q, want daemon-1", call.daemonID)
	}
	if !reflect.DeepEqual(call.coords, activeCoords) {
		t.Fatalf("touch coords = %v, want %v — the sender's ActiveCoords must pass through unfiltered, never widened to \"every reservation this daemon owns\"", call.coords, activeCoords)
	}
	if call.atMs != fixedNow().UnixMilli() {
		t.Fatalf("touch atMs = %d, want %d", call.atMs, fixedNow().UnixMilli())
	}
	if len(ob.sweepCalls) != 1 {
		t.Fatalf("sweep calls = %d, want 1", len(ob.sweepCalls))
	}
}

// TestHomeStorageHostControl_ReconcilePull_EmptyActiveCoordsTouchesNothing is
// the abandoned-reservation regression this whole fix exists for: a daemon
// that is online and calling ReconcilePull, but has NOTHING currently open
// (every AllocRequest either never landed a write or already closed), must
// touch ZERO coords — not fall back to touching every reservation it owns,
// which is exactly the bug (an always-online daemon suppressing age-sweep
// forever for an abandoned reservation).
func TestHomeStorageHostControl_ReconcilePull_EmptyActiveCoordsTouchesNothing(t *testing.T) {
	ob := &fakeOutbox{}
	h := homeStorageHostControl{outbox: ob}

	if _, _, _, err := h.ReconcilePull(t.Context(), "daemon-1", nil); err != nil {
		t.Fatalf("ReconcilePull: %v", err)
	}
	if len(ob.touchCalls) != 1 {
		t.Fatalf("touch calls = %d, want 1", len(ob.touchCalls))
	}
	if got := ob.touchCalls[0].coords; len(got) != 0 {
		t.Fatalf("touch coords = %v, want empty", got)
	}
}

// TestHomeStorageHostControl_ReconcilePull_DefaultTimeoutAndClock: the zero
// value (no injected timeout/clock, production's own construction shape at
// home.go) must not panic and must use a positive, real-clock-derived cutoff
// — nil-safe defaulting, not a required field.
func TestHomeStorageHostControl_ReconcilePull_DefaultTimeoutAndClock(t *testing.T) {
	ob := &fakeOutbox{}
	h := homeStorageHostControl{outbox: ob}
	before := time.Now()
	if _, _, _, err := h.ReconcilePull(t.Context(), "daemon-1", nil); err != nil {
		t.Fatalf("ReconcilePull: %v", err)
	}
	if len(ob.sweepCalls) != 1 {
		t.Fatalf("sweep calls = %d, want 1", len(ob.sweepCalls))
	}
	wantCutoff := before.Add(-defaultReservationTimeout).UnixMilli()
	if ob.sweepCalls[0].cutoffMs < wantCutoff {
		t.Fatalf("sweep cutoffMs = %d, want >= %d (default timeout applied)", ob.sweepCalls[0].cutoffMs, wantCutoff)
	}
}
