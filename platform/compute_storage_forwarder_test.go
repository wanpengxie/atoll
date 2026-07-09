package platform

// compute_storage_forwarder_test.go covers 期11 review残余#1/#2b's own home
// half of storageHostForwarder.pass — the daemon-side orchestration layer
// SITTING ABOVE cmd/daemon/internal/storagehost.Host.LandedCoords (whose own
// read-error fail-closed contract host_test.go covers) and above the wire
// (whose reply.Reason plumbing platform/internal/link/storagecontrol_test.go
// covers): pass() must (1) never send a ReconcilePull at all when
// LandedCoords itself failed, and (2) never call StorageHost.Reconcile when
// the home's reply carries a non-empty Reason (a NAK, not an invitation to
// run the scrubber against fabricated empty truth). A REAL WS link is used
// (not a fake Dialer — pass() takes a concrete *link.Dialer, so there is no
// interface seam to fake through), mirroring platform/internal/link's own
// storagecontrol_test.go rig.

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// forwarderTestStorageHostControl is a minimal link.StorageHostControl stub:
// ReconcilePull answers either a canned reject (err != nil, surfaced by the
// Acceptor as reply.Reason) or a canned accept, and counts calls.
type forwarderTestStorageHostControl struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (f *forwarderTestStorageHostControl) Committed(context.Context, string, string) (bool, bool, error) {
	return false, false, nil
}
func (f *forwarderTestStorageHostControl) ReclaimAck(context.Context, string, string) (bool, error) {
	return false, nil
}
func (f *forwarderTestStorageHostControl) ReconcilePull(ctx context.Context, senderDaemonID string, activeCoords, landedCoords []string) ([]link.ReconcileResource, []link.ReconcileReservation, []link.ReconcileTombstone, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, nil, nil, f.err
	}
	return nil, nil, nil, nil
}

func (f *forwarderTestStorageHostControl) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

var _ link.StorageHostControl = (*forwarderTestStorageHostControl)(nil)

// forwarderTestStorageHost is a configurable platform.StorageHost double:
// LandedCoords answers either a canned error or a canned slice; Reconcile
// records whether it ran at all.
type forwarderTestStorageHost struct {
	mu             sync.Mutex
	landedErr      error
	landedCoords   []string
	reconcileCalls int
}

func (h *forwarderTestStorageHost) Alloc(string, bool) error { return nil }
func (h *forwarderTestStorageHost) Reconcile(ctx context.Context, resources []StorageResourceCoord, pendingReservations []StorageReservationCoord, pendingTombstones []StorageTombstoneCoord, ack StorageReclaimAckFunc, resend StorageCommittedResendFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reconcileCalls++
}
func (h *forwarderTestStorageHost) ActiveWriteCoords() []string { return nil }
func (h *forwarderTestStorageHost) LandedCoords() ([]string, error) {
	if h.landedErr != nil {
		return nil, h.landedErr
	}
	return h.landedCoords, nil
}
func (h *forwarderTestStorageHost) reconciled() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.reconcileCalls
}

var _ StorageHost = (*forwarderTestStorageHost)(nil)

// dialForwarderRig wires a real Acceptor+Dialer pair (over httptest) with the
// given StorageHostControl, and returns the Dialer already Rebind'd onto the
// forwarder under test.
func dialForwarderRig(t *testing.T, shc link.StorageHostControl) *link.Dialer {
	t.Helper()
	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	acc := link.NewAcceptor(link.Config{
		Runtime:            rt,
		ChannelID:          channel.ID("test-channel"),
		StorageHostControl: shc,
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		acc.Serve(w, req, "daemon-1")
	}))
	t.Cleanup(func() { _ = acc.Close(); srv.Close() })

	d, err := link.Dial(context.Background(), "ws"+srv.URL[4:], "daemon-1", nil, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// TestStorageHostForwarder_LandedCoordsErrorSkipsPullEntirely is 期11 review
// 残余#1's compute.go-level DoD: a LandedCoords read failure must skip the
// ReconcilePull round trip ENTIRELY — never send one with a fabricated empty
// landedCoords (which would tell the home nothing landed, letting its
// same-round-trip SweepExpiredReservations sweep an already-landed
// reservation as abandoned).
func TestStorageHostForwarder_LandedCoordsErrorSkipsPullEntirely(t *testing.T) {
	shc := &forwarderTestStorageHostControl{}
	d := dialForwarderRig(t, shc)

	host := &forwarderTestStorageHost{landedErr: errors.New("read live/ failed")}
	f := newStorageHostForwarder(host, slog.New(slog.DiscardHandler), time.Second)
	f.Rebind(d)

	f.pass(context.Background())

	if got := shc.callCount(); got != 0 {
		t.Fatalf("ReconcilePull calls = %d, want 0 (LandedCoords error must skip the pull entirely)", got)
	}
	if got := host.reconciled(); got != 0 {
		t.Fatalf("Reconcile calls = %d, want 0", got)
	}
}

// TestStorageHostForwarder_ReplyReasonSkipsReconcile is 期11 review残余#2b's
// DoD: a ReconcilePullReply carrying a non-empty Reason (the home's explicit
// NAK) must never reach StorageHost.Reconcile — its Resources/
// PendingReservations/PendingTombstones are all zero-value on that branch,
// so running the scrubber against them would be reconciling against
// fabricated empty truth.
func TestStorageHostForwarder_ReplyReasonSkipsReconcile(t *testing.T) {
	shc := &forwarderTestStorageHostControl{err: errors.New("no storage host control wired")}
	d := dialForwarderRig(t, shc)

	host := &forwarderTestStorageHost{}
	f := newStorageHostForwarder(host, slog.New(slog.DiscardHandler), time.Second)
	f.Rebind(d)

	f.pass(context.Background())

	if got := shc.callCount(); got != 1 {
		t.Fatalf("ReconcilePull calls = %d, want 1 (the pull itself must still be sent)", got)
	}
	if got := host.reconciled(); got != 0 {
		t.Fatalf("Reconcile calls = %d, want 0 (a Reason'd reply must never drive Reconcile)", got)
	}
}

// TestStorageHostForwarder_HappyPathStillReconciles is the regression guard:
// a clean reply (no Reason, LandedCoords succeeds) must still drive
// Reconcile exactly once — the two skip branches above must not have turned
// into an unconditional skip.
func TestStorageHostForwarder_HappyPathStillReconciles(t *testing.T) {
	shc := &forwarderTestStorageHostControl{}
	d := dialForwarderRig(t, shc)

	host := &forwarderTestStorageHost{landedCoords: []string{"c1"}}
	f := newStorageHostForwarder(host, slog.New(slog.DiscardHandler), time.Second)
	f.Rebind(d)

	f.pass(context.Background())

	if got := shc.callCount(); got != 1 {
		t.Fatalf("ReconcilePull calls = %d, want 1", got)
	}
	if got := host.reconciled(); got != 1 {
		t.Fatalf("Reconcile calls = %d, want 1 (a clean reply must still drive Reconcile)", got)
	}
}
