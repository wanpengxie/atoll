package link_test

// storagecontrol_test.go — end-to-end coverage for 期11 §4.7's daemon
// control-RPC plane over a REAL WS link (httptest.Server + link.Dial), not
// just the frame codec in isolation: AllocRequest home→daemon (Acceptor.
// SendAllocRequest / Dialer.SetAllocHandler) and the three daemon-initiated
// frames (Committed/ReclaimAck/ReconcilePull, via link.StorageHostControl).

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// fakeStorageHostControl is a configurable link.StorageHostControl stub —
// records every call for assertion.
type fakeStorageHostControl struct {
	mu sync.Mutex

	committedFound, committedLost bool
	committedErr                  error
	committedCalls                []struct{ sender, reservationID string }

	reclaimFound bool
	reclaimErr   error
	reclaimCalls []struct{ sender, tombstoneID string }

	reconcileResources    []link.ReconcileResource
	reconcileReservations []link.ReconcileReservation
	reconcileTombstones   []link.ReconcileTombstone
	reconcileErr          error
	reconcileCalls        []string
}

func (f *fakeStorageHostControl) Committed(ctx context.Context, senderDaemonID, reservationID string) (bool, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.committedCalls = append(f.committedCalls, struct{ sender, reservationID string }{senderDaemonID, reservationID})
	return f.committedFound, f.committedLost, f.committedErr
}

func (f *fakeStorageHostControl) ReclaimAck(ctx context.Context, senderDaemonID, tombstoneID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reclaimCalls = append(f.reclaimCalls, struct{ sender, tombstoneID string }{senderDaemonID, tombstoneID})
	return f.reclaimFound, f.reclaimErr
}

func (f *fakeStorageHostControl) ReconcilePull(ctx context.Context, senderDaemonID string) ([]link.ReconcileResource, []link.ReconcileReservation, []link.ReconcileTombstone, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reconcileCalls = append(f.reconcileCalls, senderDaemonID)
	return f.reconcileResources, f.reconcileReservations, f.reconcileTombstones, f.reconcileErr
}

// storageRig is a minimal home+daemon pair wired for the storage control-RPC
// plane (a leaner sibling of newHomeRig, which does not expose
// StorageHostControl).
type storageRig struct {
	acc *link.Acceptor
	shc *fakeStorageHostControl
	srv *httptest.Server
}

func newStorageRig(t *testing.T) *storageRig {
	t.Helper()
	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	shc := &fakeStorageHostControl{}
	acc := link.NewAcceptor(link.Config{
		Minter:             &stubMinter{},
		Runtime:            rt,
		Membership:         &stubMembership{},
		ChannelID:          testChannelID,
		StorageHostControl: shc,
	})
	r := &storageRig{acc: acc, shc: shc}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.acc.Serve(w, req, "daemon-1")
	}))
	t.Cleanup(func() { _ = acc.Close(); r.srv.Close() })
	return r
}

func (r *storageRig) wsURL() string { return "ws" + r.srv.URL[4:] }

func dialStorageDaemon(t *testing.T, r *storageRig) *link.Dialer {
	t.Helper()
	d, err := link.Dial(context.Background(), r.wsURL(), "daemon-1", nil, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// TestAllocRequest_HomeToDaemonRoundTrip drives §4.7's ONE home-initiated
// frame end to end: the door's (simulated) AllocRequest arrives at the
// daemon's installed handler and the daemon's AllocReply arrives back at
// the home's waiting caller.
func TestAllocRequest_HomeToDaemonRoundTrip(t *testing.T) {
	r := newStorageRig(t)
	d := dialStorageDaemon(t, r)

	var gotReq link.AllocRequest
	d.SetAllocHandler(func(req link.AllocRequest) link.AllocReply {
		gotReq = req
		return link.AllocReply{OK: true}
	})

	err := r.acc.SendAllocRequest(context.Background(), "daemon-1", link.AllocRequest{
		ChannelID: "ch1", Coord: "coord-1", Dir: true,
	})
	if err != nil {
		t.Fatalf("SendAllocRequest: %v", err)
	}
	if gotReq.ChannelID != "ch1" || gotReq.Coord != "coord-1" || !gotReq.Dir {
		t.Errorf("daemon received AllocRequest = %+v", gotReq)
	}
}

// TestAllocRequest_NakSurfacesAsError proves a daemon-side OK:false reply
// surfaces as a Go error at the home's SendAllocRequest caller.
func TestAllocRequest_NakSurfacesAsError(t *testing.T) {
	r := newStorageRig(t)
	d := dialStorageDaemon(t, r)
	d.SetAllocHandler(func(link.AllocRequest) link.AllocReply {
		return link.AllocReply{OK: false, Reason: "disk full"}
	})

	err := r.acc.SendAllocRequest(context.Background(), "daemon-1", link.AllocRequest{ChannelID: "ch1", Coord: "coord-1"})
	if err == nil {
		t.Fatal("expected an error on OK:false")
	}
}

// TestAllocRequest_NoLiveConnectionIsHonestError proves the home never hangs
// forever trying to reach a daemon id with no live link.
func TestAllocRequest_NoLiveConnectionIsHonestError(t *testing.T) {
	r := newStorageRig(t)
	err := r.acc.SendAllocRequest(context.Background(), "no-such-daemon", link.AllocRequest{Coord: "c"})
	if err == nil {
		t.Fatal("expected an error for an unattached daemon id")
	}
}

// TestAllocRequest_NoHandlerAnswersHonestNak proves a daemon with no
// AllocHandler installed answers OK:false rather than hanging the home.
func TestAllocRequest_NoHandlerAnswersHonestNak(t *testing.T) {
	r := newStorageRig(t)
	dialStorageDaemon(t, r) // no SetAllocHandler call

	err := r.acc.SendAllocRequest(context.Background(), "daemon-1", link.AllocRequest{Coord: "c"})
	if err == nil {
		t.Fatal("expected an error when no handler is installed")
	}
}

// TestCommitted_DaemonToHomeRoundTrip drives §4.7's Committed frame: the
// daemon's SendCommitted reaches the home's StorageHostControl.Committed
// with the connection's attach-authenticated sender id, and the reply
// (found/lost) comes back to the daemon caller.
func TestCommitted_DaemonToHomeRoundTrip(t *testing.T) {
	r := newStorageRig(t)
	d := dialStorageDaemon(t, r)
	r.shc.committedFound = true
	r.shc.committedLost = false

	reply, err := d.SendCommitted(context.Background(), "res-1")
	if err != nil {
		t.Fatalf("SendCommitted: %v", err)
	}
	if !reply.Found || reply.Lost {
		t.Errorf("CommittedReply = %+v", reply)
	}
	if len(r.shc.committedCalls) != 1 || r.shc.committedCalls[0].sender != "daemon-1" || r.shc.committedCalls[0].reservationID != "res-1" {
		t.Errorf("Committed calls = %+v, want sender=daemon-1 reservationID=res-1", r.shc.committedCalls)
	}
}

// TestReclaimAck_DaemonToHomeRoundTrip drives §4.7's ReclaimAck frame.
func TestReclaimAck_DaemonToHomeRoundTrip(t *testing.T) {
	r := newStorageRig(t)
	d := dialStorageDaemon(t, r)
	r.shc.reclaimFound = true

	reply, err := d.SendReclaimAck(context.Background(), "ts-1")
	if err != nil {
		t.Fatalf("SendReclaimAck: %v", err)
	}
	if !reply.Found {
		t.Errorf("ReclaimAckReply = %+v", reply)
	}
	if len(r.shc.reclaimCalls) != 1 || r.shc.reclaimCalls[0].sender != "daemon-1" || r.shc.reclaimCalls[0].tombstoneID != "ts-1" {
		t.Errorf("ReclaimAck calls = %+v", r.shc.reclaimCalls)
	}
}

// TestReconcilePull_DaemonToHomeRoundTrip drives §4.7's ReconcilePull frame
// — the daemon-scoped recovery picture round-trips intact.
func TestReconcilePull_DaemonToHomeRoundTrip(t *testing.T) {
	r := newStorageRig(t)
	d := dialStorageDaemon(t, r)
	r.shc.reconcileResources = []link.ReconcileResource{{Coord: "c1"}}
	r.shc.reconcileReservations = []link.ReconcileReservation{{ReservationID: "r1", Coord: "c2"}}
	r.shc.reconcileTombstones = []link.ReconcileTombstone{{TombstoneID: "t1", Coord: "c3", Provenance: "axis-allocated"}}

	reply, err := d.SendReconcilePull(context.Background())
	if err != nil {
		t.Fatalf("SendReconcilePull: %v", err)
	}
	if len(reply.Resources) != 1 || reply.Resources[0].Coord != "c1" {
		t.Errorf("Resources = %+v", reply.Resources)
	}
	if len(reply.PendingReservations) != 1 || reply.PendingReservations[0].ReservationID != "r1" {
		t.Errorf("PendingReservations = %+v", reply.PendingReservations)
	}
	if len(reply.PendingTombstones) != 1 || reply.PendingTombstones[0].TombstoneID != "t1" || reply.PendingTombstones[0].Provenance != "axis-allocated" {
		t.Errorf("PendingTombstones = %+v", reply.PendingTombstones)
	}
	if len(r.shc.reconcileCalls) != 1 || r.shc.reconcileCalls[0] != "daemon-1" {
		t.Errorf("ReconcilePull calls = %+v", r.shc.reconcileCalls)
	}
}

// TestStorageControl_NoHandlerWiredAnswersHonestReject proves every one of
// the three daemon-initiated frames gets an honest Reason (never a silent
// drop / hang) when no StorageHostControl is wired on the home.
func TestStorageControl_NoHandlerWiredAnswersHonestReject(t *testing.T) {
	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	acc := link.NewAcceptor(link.Config{
		Minter: &stubMinter{}, Runtime: rt, Membership: &stubMembership{}, ChannelID: testChannelID,
		// StorageHostControl deliberately nil.
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		acc.Serve(w, req, "daemon-1")
	}))
	defer func() { _ = acc.Close(); srv.Close() }()

	d, err := link.Dial(context.Background(), "ws"+srv.URL[4:], "daemon-1", nil, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	if reply, err := d.SendCommitted(context.Background(), "res-1"); err != nil || reply.Reason == "" {
		t.Errorf("SendCommitted with no host wired: reply=%+v err=%v, want a non-empty Reason and no transport error", reply, err)
	}
	if reply, err := d.SendReclaimAck(context.Background(), "ts-1"); err != nil || reply.Reason == "" {
		t.Errorf("SendReclaimAck with no host wired: reply=%+v err=%v", reply, err)
	}
	if reply, err := d.SendReconcilePull(context.Background()); err != nil || reply.Reason == "" {
		t.Errorf("SendReconcilePull with no host wired: reply=%+v err=%v", reply, err)
	}
}

// blockingStorageHostControl.Committed never returns until unblock is
// closed — the wedged-home rig TestSendCommitted_CtxCancelUnblocksWaiter
// drives.
type blockingStorageHostControl struct{ unblock chan struct{} }

func (b *blockingStorageHostControl) Committed(ctx context.Context, senderDaemonID, reservationID string) (bool, bool, error) {
	<-b.unblock
	return true, false, nil
}
func (b *blockingStorageHostControl) ReclaimAck(context.Context, string, string) (bool, error) {
	return false, nil
}
func (b *blockingStorageHostControl) ReconcilePull(context.Context, string) ([]link.ReconcileResource, []link.ReconcileReservation, []link.ReconcileTombstone, error) {
	return nil, nil, nil, nil
}

// TestSendCommitted_CtxCancelUnblocksWaiter proves the daemon caller does
// NOT hang past its own ctx even when the home's handler never replies —
// the bounded-wait contract every storage control-RPC send shares. NOTE
// (known limitation, matching handleAttach's existing precedent): the
// home-side handler runs SYNCHRONOUSLY on that link's own read loop
// (onControl), so a genuinely wedged StorageHostControl call also blocks
// that ONE link's graceful Close/wg.Wait until it returns — this test
// unblocks the handler itself (rather than via defer ordering) precisely
// to avoid THAT hang inside the test's own cleanup, which would otherwise
// mask the real assertion (the CALLER-side ctx bound) behind an unrelated
// teardown deadlock.
func TestSendCommitted_CtxCancelUnblocksWaiter(t *testing.T) {
	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	shc := &blockingStorageHostControl{unblock: make(chan struct{})}
	acc := link.NewAcceptor(link.Config{
		Minter: &stubMinter{}, Runtime: rt, Membership: &stubMembership{}, ChannelID: testChannelID,
		StorageHostControl: shc,
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		acc.Serve(w, req, "daemon-1")
	}))
	// Unblock the wedged handler BEFORE tearing anything down (explicit call
	// below, not a defer — defer order alone is easy to get backwards here):
	// acc.Close() joins every Serve goroutine (wg.Wait()), and that
	// goroutine's read loop is parked inside the still-blocked handler until
	// this runs.
	defer func() { _ = acc.Close(); srv.Close() }()

	d, err := link.Dial(context.Background(), "ws"+srv.URL[4:], "daemon-1", nil, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = d.SendCommitted(ctx, "res-1")
	elapsed := time.Since(start)
	close(shc.unblock) // let the still-blocked home-side handler goroutine finish
	if err == nil {
		t.Fatal("expected a ctx-deadline error from a wedged home")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("SendCommitted took %v, want bounded near the 100ms ctx deadline", elapsed)
	}
}
