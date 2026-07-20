package compute

// compute_storage_forwarder_test.go covers storageHostForwarder.pass above the
// wire: a reply Reason is a NAK and must not drive local reconciliation. A REAL WS link is used
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
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type forwarderAuthorities struct{}

func (forwarderAuthorities) ValidateAttachment(context.Context, link.PortOwner, string, []storespec.ComputeDeclaration) ([]storespec.ComputeDeclaration, error) {
	return nil, nil
}
func (forwarderAuthorities) PrepareAttachmentFence(context.Context, actor.ActorID, string, int64) (link.AttachmentFence, error) {
	return forwarderAttachmentFence{}, nil
}

type forwarderAttachmentFence struct{}

func (forwarderAttachmentFence) Valid() bool { return true }
func (forwarderAuthorities) LookupActive(context.Context, actor.ActorID) (storespec.ActorControlRow, bool, error) {
	return storespec.ActorControlRow{}, false, nil
}
func (forwarderAuthorities) ListActive(context.Context) ([]storespec.ActorControlRow, error) {
	return nil, nil
}
func (forwarderAuthorities) WorldOf(context.Context, actor.ActorID) (storespec.ActorWorld, bool, error) {
	return 0, false, nil
}
func (forwarderAuthorities) CheckAuthor(context.Context, storespec.AuthorStamp) (storespec.AuthorVerdict, error) {
	return storespec.AuthorNotMember, nil
}
func (forwarderAuthorities) LockAndValidate(context.Context, string, channel.ID) (func(), error) {
	return func() {}, nil
}
func (forwarderAuthorities) Register(link.PortOwner, actorrt.Incarnation, string, int64) bool {
	return true
}
func (forwarderAuthorities) Remove(link.PortOwner, actorrt.Incarnation) {}
func (forwarderAuthorities) Take(link.PortOwner, actor.ActorID) (actorrt.Incarnation, bool) {
	return actorrt.Incarnation{}, false
}
func (forwarderAuthorities) TakeOwner(link.PortOwner) []actorrt.Incarnation { return nil }
func (forwarderAuthorities) ExpireOwner(link.PortOwner)                     {}

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
func (f *forwarderTestStorageHostControl) ReconcilePull(ctx context.Context, senderDaemonID string, activeCoords []string) ([]link.ReconcileResource, []link.ReconcileReservation, []link.ReconcileTombstone, error) {
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

// forwarderTestStorageHost records whether reconciliation ran.
type forwarderTestStorageHost struct {
	mu             sync.Mutex
	reconcileCalls int
}

func (h *forwarderTestStorageHost) Alloc(string, bool) error { return nil }
func (h *forwarderTestStorageHost) Reconcile(ctx context.Context, resources []StorageResourceCoord, pendingReservations []StorageReservationCoord, pendingTombstones []StorageTombstoneCoord, ack StorageReclaimAckFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reconcileCalls++
}
func (h *forwarderTestStorageHost) ActiveWriteCoords() []string { return nil }
func (h *forwarderTestStorageHost) reconciled() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.reconcileCalls
}

var _ StorageHost = (*forwarderTestStorageHost)(nil)

type unusedStateHandles struct{}

func (unusedStateHandles) AdmitRun(actor.ActorID) error { return nil }
func (unusedStateHandles) EndBatch([]actor.ActorID)     {}
func (unusedStateHandles) Resolve(context.Context, storespec.AuthorStamp) (accessdoor.AccessHandle, error) {
	return nil, accessdoor.ErrStateHandleUnavailable
}

// dialForwarderRig wires a real Acceptor+Dialer pair (over httptest) with the
// given StorageHostControl, and returns the Dialer already Rebind'd onto the
// forwarder under test.
func dialForwarderRig(t *testing.T, shc link.StorageHostControl) *link.Dialer {
	t.Helper()
	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	auth := forwarderAuthorities{}
	acc, err := link.NewAcceptor(link.Config{
		Runtime:            rt,
		ChannelID:          channel.ID("test-channel"),
		StorageHostControl: shc,
		Declarations:       auth,
		Authority:          auth,
		StateHandles:       unusedStateHandles{},
		CanAttach:          func(context.Context, string) error { return nil },
		ActorLock:          func(actor.ActorID) func() { return func() {} },
		PortIndex:          auth,
	})
	if err != nil {
		t.Fatalf("NewAcceptor: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		acc.Serve(w, req, "daemon-1")
	}))
	t.Cleanup(func() { _ = acc.Close(); srv.Close() })

	d, err := link.Dial(context.Background(), "ws"+srv.URL[4:], nil, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func TestStorageHostForwarder_ReconcilePullsWithoutLandedPhase(t *testing.T) {
	shc := &forwarderTestStorageHostControl{}
	d := dialForwarderRig(t, shc)

	host := &forwarderTestStorageHost{}
	f := newStorageHostForwarder(host, slog.New(slog.DiscardHandler), time.Second)
	f.Rebind(d)

	f.pass(context.Background())

	if got := shc.callCount(); got != 1 {
		t.Fatalf("ReconcilePull calls = %d, want 1", got)
	}
	if got := host.reconciled(); got != 1 {
		t.Fatalf("Reconcile calls = %d, want 1", got)
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
// a clean reply (no Reason) must still drive
// Reconcile exactly once — the two skip branches above must not have turned
// into an unconditional skip.
func TestStorageHostForwarder_HappyPathStillReconciles(t *testing.T) {
	shc := &forwarderTestStorageHostControl{}
	d := dialForwarderRig(t, shc)

	host := &forwarderTestStorageHost{}
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
