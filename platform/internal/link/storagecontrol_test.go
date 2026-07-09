package link_test

// storagecontrol_test.go — end-to-end coverage for 期11 §4.7's daemon
// control-RPC plane over a REAL WS link (httptest.Server + link.Dial), not
// just the frame codec in isolation: AllocRequest home→daemon (Acceptor.
// SendAllocRequest / Dialer.SetAllocHandler) and the three daemon-initiated
// frames (Committed/ReclaimAck/ReconcilePull, via link.StorageHostControl).

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
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
	reconcileActiveCoords [][]string
	reconcileLandedCoords [][]string
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

func (f *fakeStorageHostControl) ReconcilePull(ctx context.Context, senderDaemonID string, activeCoords, landedCoords []string) ([]link.ReconcileResource, []link.ReconcileReservation, []link.ReconcileTombstone, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reconcileCalls = append(f.reconcileCalls, senderDaemonID)
	f.reconcileActiveCoords = append(f.reconcileActiveCoords, activeCoords)
	f.reconcileLandedCoords = append(f.reconcileLandedCoords, landedCoords)
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

	reply, err := d.SendReconcilePull(context.Background(), []string{"coord-active"}, []string{"coord-landed"})
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
	// 期11 review's own narrowing addition: ActiveCoords must round-trip
	// intact over the real WS link, not just in the local struct literal.
	if len(r.shc.reconcileActiveCoords) != 1 || len(r.shc.reconcileActiveCoords[0]) != 1 || r.shc.reconcileActiveCoords[0][0] != "coord-active" {
		t.Errorf("ReconcilePull activeCoords = %+v, want [[coord-active]]", r.shc.reconcileActiveCoords)
	}
	// 期11 review §2.5 #A: LandedCoords must likewise round-trip intact.
	if len(r.shc.reconcileLandedCoords) != 1 || len(r.shc.reconcileLandedCoords[0]) != 1 || r.shc.reconcileLandedCoords[0][0] != "coord-landed" {
		t.Errorf("ReconcilePull landedCoords = %+v, want [[coord-landed]]", r.shc.reconcileLandedCoords)
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
	if reply, err := d.SendReconcilePull(context.Background(), nil, nil); err != nil || reply.Reason == "" {
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
func (b *blockingStorageHostControl) ReconcilePull(context.Context, string, []string, []string) ([]link.ReconcileResource, []link.ReconcileReservation, []link.ReconcileTombstone, error) {
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

// TestReclaimRequest_HomeToDaemonRoundTrip drives 期11 review §2.5 #B's new
// home-initiated frame end to end: the door's (simulated) ReclaimRequest
// reaches the daemon's wired LocalFileOpener.ReclaimCoord with the coord, and
// the daemon's OK reply returns to the home caller.
func TestReclaimRequest_HomeToDaemonRoundTrip(t *testing.T) {
	r := newStorageRig(t)
	d := dialStorageDaemon(t, r)
	opener := &extReclaimOpener{}
	d.SetLocalFileOpener(opener)

	if err := r.acc.SendReclaimRequest(context.Background(), "daemon-1", "coord-orphan"); err != nil {
		t.Fatalf("SendReclaimRequest: %v", err)
	}
	if len(opener.reclaimed) != 1 || opener.reclaimed[0] != "coord-orphan" {
		t.Fatalf("daemon reclaimed = %v, want [coord-orphan]", opener.reclaimed)
	}
}

// TestReclaimRequest_NoOpenerAnswersHonestNak proves a daemon with no storage
// host wired answers OK:false (an error at the home caller), never a hang.
func TestReclaimRequest_NoOpenerAnswersHonestNak(t *testing.T) {
	r := newStorageRig(t)
	dialStorageDaemon(t, r) // no SetLocalFileOpener
	if err := r.acc.SendReclaimRequest(context.Background(), "daemon-1", "c"); err == nil {
		t.Fatal("expected an error when no storage host is wired on the daemon")
	}
}

// --- 期11 review #D: committingWriteHandle.Commit must never fabricate success ---

// fakeCommitWH is a minimal accessdoor.LocalWriteHandle whose own Commit
// succeeds — committingWriteHandle wraps it, so the test isolates the
// Committed-relay behavior layered ON TOP of a locally-successful write.
type fakeCommitWH struct{ committed, aborted bool }

func (f *fakeCommitWH) Write(p []byte) (int, error) { return len(p), nil }
func (f *fakeCommitWH) Commit() error               { f.committed = true; return nil }
func (f *fakeCommitWH) Abort() error                { f.aborted = true; return nil }

// extReclaimOpener is the external-package (link_test) LocalFileOpener double
// for #D's Lost branch — records the reclaimed coord.
type extReclaimOpener struct{ reclaimed []string }

func (o *extReclaimOpener) OpenRead(string) (io.ReadSeekCloser, error) {
	return nil, errors.New("extReclaimOpener: OpenRead unexercised")
}
func (o *extReclaimOpener) OpenWrite(string) (accessdoor.LocalWriteHandle, error) {
	return nil, errors.New("extReclaimOpener: OpenWrite unexercised")
}
func (o *extReclaimOpener) OpenDir(string) (accessdoor.LocalDirHandle, error) {
	return nil, errors.New("extReclaimOpener: OpenDir unexercised")
}
func (o *extReclaimOpener) ReclaimCoord(coord string) error {
	o.reclaimed = append(o.reclaimed, coord)
	return nil
}

// TestCommittingWriteHandle_Commit_DoesNotFabricateSuccess is #D's守测: the
// three ways the Committed relay does NOT cleanly land a row must all surface
// (never the pre-review silent `nil`):
//   - a send failure (home torn down) → error, resend backstop owns it;
//   - reply.Lost → reclaim the loser's coord + error;
//   - reply.Found && !Lost → success (nil), the one真landing case.
func TestCommittingWriteHandle_Commit_DoesNotFabricateSuccess(t *testing.T) {
	t.Run("Committed relay failure is surfaced, never a false success", func(t *testing.T) {
		r := newStorageRig(t)
		d := dialStorageDaemon(t, r)
		// Tear the home down so SendCommitted cannot complete.
		_ = r.acc.Close()
		r.srv.Close()

		wh := &fakeCommitWH{}
		h := link.NewCommittingWriteHandleForTest(d, wh, "res-fail", "coord-fail")
		if err := h.Commit(); err == nil {
			t.Fatal("Commit returned nil on a failed Committed relay — the pre-#D false-success bug")
		}
		if !wh.committed {
			t.Fatal("the underlying local Commit must still have run (bytes landed locally)")
		}
	})

	t.Run("reply.Lost reclaims the coord and fails loud", func(t *testing.T) {
		r := newStorageRig(t)
		d := dialStorageDaemon(t, r)
		opener := &extReclaimOpener{}
		d.SetLocalFileOpener(opener)
		r.shc.committedFound = true
		r.shc.committedLost = true

		wh := &fakeCommitWH{}
		h := link.NewCommittingWriteHandleForTest(d, wh, "res-lost", "coord-lost")
		if err := h.Commit(); err == nil {
			t.Fatal("Commit returned nil on a Lost reservation — a permanent reject must be loud")
		}
		if len(opener.reclaimed) != 1 || opener.reclaimed[0] != "coord-lost" {
			t.Fatalf("reclaimed = %v, want [coord-lost]", opener.reclaimed)
		}
	})

	t.Run("Found && !Lost is the one clean success", func(t *testing.T) {
		r := newStorageRig(t)
		d := dialStorageDaemon(t, r)
		r.shc.committedFound = true
		r.shc.committedLost = false

		wh := &fakeCommitWH{}
		h := link.NewCommittingWriteHandleForTest(d, wh, "res-ok", "coord-ok")
		if err := h.Commit(); err != nil {
			t.Fatalf("Commit errored on a clean landing: %v", err)
		}
	})

	// 期11 review残余#2a: a non-empty reply.Reason with Lost==false is the
	// home explicitly NAK'ing the commit (sender/placement mismatch, a store
	// error, or no storage control wired — handleCommitted's own
	// Reason-setting branches, none of which set Found/Lost). The pre-fix
	// code only checked transport err + reply.Lost, so this branch fell
	// through to the bottom `return nil` and told the caller the create
	// succeeded even though the home never landed the row.
	t.Run("reply.Reason without Lost is a home NAK, fails loud", func(t *testing.T) {
		r := newStorageRig(t)
		d := dialStorageDaemon(t, r)
		r.shc.committedErr = errors.New("store unavailable")

		wh := &fakeCommitWH{}
		h := link.NewCommittingWriteHandleForTest(d, wh, "res-nak", "coord-nak")
		if err := h.Commit(); err == nil {
			t.Fatal("Commit returned nil on a home-rejected commit (reply.Reason set, Lost=false) — the pre-fix false-success bug")
		}
		if !wh.committed {
			t.Fatal("the underlying local Commit must still have run (bytes landed locally)")
		}
	})
}
