package platform

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
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
}

type sweepCall struct {
	daemonID string
	cutoffMs int64
}

func (f *fakeOutbox) CommitReservation(ctx context.Context, reservationID string) (bool, error) {
	f.commitCalls = append(f.commitCalls, reservationID)
	return f.commitFound, f.commitErr
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

var _ resourcespec.ResourceOutbox = (*fakeOutbox)(nil)

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
		tombstoneRows:   []resourcespec.TombstoneRow{{TombstoneID: "t1", PlacementCoord: "c3", Provenance: resourcespec.ProvenanceAxisAllocated}},
	}
	h := homeStorageHostControl{outbox: ob}
	resources, reservations, tombstones, err := h.ReconcilePull(t.Context(), "daemon-1")
	if err != nil {
		t.Fatalf("ReconcilePull: %v", err)
	}
	if len(resources) != 1 || resources[0].Coord != "c1" {
		t.Errorf("resources = %+v", resources)
	}
	if len(reservations) != 1 || reservations[0].ReservationID != "r1" || reservations[0].Coord != "c2" {
		t.Errorf("reservations = %+v", reservations)
	}
	if len(tombstones) != 1 || tombstones[0].TombstoneID != "t1" || tombstones[0].Coord != "c3" || tombstones[0].Provenance != "axis-allocated" {
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

	if _, _, _, err := h.ReconcilePull(t.Context(), "daemon-1"); err != nil {
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

// TestHomeStorageHostControl_ReconcilePull_DefaultTimeoutAndClock: the zero
// value (no injected timeout/clock, production's own construction shape at
// home.go) must not panic and must use a positive, real-clock-derived cutoff
// — nil-safe defaulting, not a required field.
func TestHomeStorageHostControl_ReconcilePull_DefaultTimeoutAndClock(t *testing.T) {
	ob := &fakeOutbox{}
	h := homeStorageHostControl{outbox: ob}
	before := time.Now()
	if _, _, _, err := h.ReconcilePull(t.Context(), "daemon-1"); err != nil {
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

func TestLateStorageMounts_BeforeAndAfterBind(t *testing.T) {
	acc := &lateAcceptor{}
	m := lateStorageMounts{acc: acc}

	mounts, err := m.ListStorageDaemons(t.Context(), "ch1")
	if err != nil || len(mounts) != 0 {
		t.Fatalf("before bind: (%v,%v), want (empty,nil)", mounts, err)
	}

	a := link.NewAcceptor(link.Config{})
	acc.bind(a)
	// No attach has happened, so the mount list is still empty — this just
	// proves the late-bind seam itself works (a real attach is exercised by
	// the link package's own tests), not that Acceptor state is non-empty.
	mounts, err = m.ListStorageDaemons(t.Context(), "ch1")
	if err != nil || len(mounts) != 0 {
		t.Fatalf("after bind, no attach: (%v,%v), want (empty,nil)", mounts, err)
	}
}

func TestLateStorageControl_BeforeBindIsHonestError(t *testing.T) {
	acc := &lateAcceptor{}
	c := lateStorageControl{acc: acc}
	err := c.AllocRequest(t.Context(), "daemon-1", accessdoor.StorageAllocSpec{ChannelID: "ch1", Coord: "coord-1"})
	if err == nil {
		t.Fatal("expected an error before the Acceptor is bound")
	}
}
