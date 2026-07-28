package link

// The WRITE direction of both byte routes, end to end over a real websocket
// link. lanecontrol_test.go walks the read direction of each
// (TestLaneTicketsLocalRouteResolvesDirectly,
// TestLaneTicketsCrossHostFullRoundTrip); the write direction is the harder
// half and was uncovered:
//
//   - Local route (§5 item 0's zerocopy): the coord still resolves over the
//     real control plane, but the bytes never touch the lane — and when the
//     route carries a reservation, the SAME link then carries the Committed
//     landing signal. Three mechanisms on one connection, none of which an
//     in-process accessdoor test can reach.
//   - Stream route: requester→home→target, with the home relaying raw bytes
//     between two independently authenticated sessions. The landing signal
//     comes from the TARGET's session (it is the one holding the bytes),
//     never the requester's.
//
// Both write paths also have a failure half the read path does not: the target
// may refuse to open at all, after the requester has already been handed a
// live-looking stream. That refusal has to arrive as a rejection, never as a
// silently short transfer.

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// laneWriteOpener is a daemon-side storage host that lands committed bytes in
// memory, keyed by coord. openErr, when set, makes every OpenWrite fail — the
// "target cannot accept these bytes" half.
type laneWriteOpener struct {
	mu       sync.Mutex
	landed   map[string][]byte
	aborted  []string
	openErr  error
	reclaims []string
}

func (o *laneWriteOpener) OpenRead(string) (io.ReadSeekCloser, error) {
	return nil, errors.New("laneWriteOpener: OpenRead unexercised")
}

func (o *laneWriteOpener) OpenWrite(coord string) (accessdoor.LocalWriteHandle, error) {
	o.mu.Lock()
	err := o.openErr
	o.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &laneWriteHandle{owner: o, coord: coord}, nil
}

func (o *laneWriteOpener) OpenDir(string) (accessdoor.LocalDirHandle, error) {
	return nil, errors.New("laneWriteOpener: OpenDir unexercised")
}

func (o *laneWriteOpener) ReclaimCoord(coord string) error {
	o.mu.Lock()
	o.reclaims = append(o.reclaims, coord)
	o.mu.Unlock()
	return nil
}

func (o *laneWriteOpener) bytesAt(coord string) ([]byte, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	value, ok := o.landed[coord]
	return value, ok
}

func (o *laneWriteOpener) abortedCoords() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.aborted...)
}

var _ LocalFileOpener = (*laneWriteOpener)(nil)

type laneWriteHandle struct {
	owner *laneWriteOpener
	coord string
	buf   bytes.Buffer
}

func (h *laneWriteHandle) Write(p []byte) (int, error) { return h.buf.Write(p) }

func (h *laneWriteHandle) Commit() error {
	h.owner.mu.Lock()
	if h.owner.landed == nil {
		h.owner.landed = map[string][]byte{}
	}
	h.owner.landed[h.coord] = append([]byte(nil), h.buf.Bytes()...)
	h.owner.mu.Unlock()
	return nil
}

func (h *laneWriteHandle) Abort() error {
	h.owner.mu.Lock()
	h.owner.aborted = append(h.owner.aborted, h.coord)
	h.owner.mu.Unlock()
	return nil
}

func laneByDaemonRig(t *testing.T, storage StorageHostControl) *acceptorRig {
	t.Helper()
	return newAcceptorRig(t, acceptorRigConfig{
		storage:  storage,
		daemonID: func(req *http.Request) string { return req.URL.Query().Get("daemon") },
	})
}

// Same-daemon write: the route resolves over the real control plane, the bytes
// land through the daemon's own storage host without ever entering the lane,
// and the reservation's Committed signal then travels back on that same link —
// carrying the reservation id under the authenticated daemon identity.
func TestLaneLocalWriteRouteLandsBytesAndReportsCommitted(t *testing.T) {
	storage := &fakeStorageControl{}
	rig := laneByDaemonRig(t, storage)
	opener := &laneWriteOpener{}
	daemon := dialLaneTestDaemon(t, rig, "local-daemon", opener)

	tickets, err := rig.acc.OpenLaneTransfer(
		t.Context(), "local-daemon", "local-daemon",
		"coord-local-write", access.OpWrite, "res-local-write",
	)
	if err != nil {
		t.Fatal(err)
	}
	file, err := daemon.redeemFileRoute(t.Context(), accessdoor.FileRoute{
		Local: true, Token: tickets.Resolve, Mode: access.OpWrite,
		ReservationID: "res-local-write",
	})
	if err != nil {
		t.Fatalf("local write redemption: %v", err)
	}
	if file.Local == nil || file.Local.Write == nil || file.Stream != nil {
		t.Fatalf("local write redemption returned the wrong access shape: %+v", file)
	}

	payload := []byte("local-write-bytes")
	if _, err := file.Local.Write.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := file.Local.Write.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	got, ok := opener.bytesAt("coord-local-write")
	if !ok || !bytes.Equal(got, payload) {
		t.Fatalf("landed bytes = %q (present=%v), want %q", got, ok, payload)
	}
	storage.mu.Lock()
	calls := append([][2]string(nil), storage.committedCalls...)
	storage.mu.Unlock()
	if len(calls) != 1 || calls[0] != [2]string{"local-daemon", "res-local-write"} {
		t.Fatalf("home saw %v, want one landing {local-daemon res-local-write}", calls)
	}
}

// A local write route with NO reservation is a plain OpWrite (§3.5: OpWrite
// never fires Committed). The bytes still land; the home hears nothing.
func TestLaneLocalWriteWithoutReservationSendsNoCommitted(t *testing.T) {
	storage := &fakeStorageControl{}
	rig := laneByDaemonRig(t, storage)
	opener := &laneWriteOpener{}
	daemon := dialLaneTestDaemon(t, rig, "local-daemon", opener)

	tickets, err := rig.acc.OpenLaneTransfer(
		t.Context(), "local-daemon", "local-daemon", "coord-plain", access.OpWrite, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	file, err := daemon.redeemFileRoute(t.Context(), accessdoor.FileRoute{
		Local: true, Token: tickets.Resolve, Mode: access.OpWrite,
	})
	if err != nil {
		t.Fatalf("local write redemption: %v", err)
	}
	if _, err := file.Local.Write.Write([]byte("plain")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := file.Local.Write.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got, ok := opener.bytesAt("coord-plain"); !ok || string(got) != "plain" {
		t.Fatalf("landed bytes = %q (present=%v)", got, ok)
	}
	storage.mu.Lock()
	sent := len(storage.committedCalls)
	storage.mu.Unlock()
	if sent != 0 {
		t.Fatalf("a reservation-less write fired %d Committed frame(s), want 0", sent)
	}
}

// Cross-daemon write: the requester holds the redeem ticket on ITS session, the
// bytes live on the target's, and the home relays between the two. The landing
// signal must come from the TARGET's authenticated session — it is the daemon
// that actually holds the bytes; a home that credited the requester would file
// the row against a daemon that never stored anything.
func TestLaneCrossHostWriteRelaysBytesAndCommitsFromTheTarget(t *testing.T) {
	storage := &fakeStorageControl{}
	rig := laneByDaemonRig(t, storage)
	target := &laneWriteOpener{}
	dialLaneTestDaemon(t, rig, "target-daemon", target)
	requester := dialLaneTestDaemon(t, rig, "requester-daemon", nil)

	tickets, err := rig.acc.OpenLaneTransfer(
		t.Context(), "target-daemon", "requester-daemon",
		"coord-cross-write", access.OpWrite, "res-cross-write",
	)
	if err != nil {
		t.Fatal(err)
	}
	file, err := requester.redeemFileRoute(t.Context(), accessdoor.FileRoute{
		Local: false, Token: tickets.Redeem, Mode: access.OpWrite,
		ReservationID: "res-cross-write",
	})
	if err != nil {
		t.Fatalf("cross-host write redemption: %v", err)
	}
	if file.Stream == nil || file.Local != nil {
		t.Fatalf("cross-host write returned the wrong access shape: %+v", file)
	}

	// A payload large enough that the relay must actually pump more than one
	// chunk in each direction rather than hand over a single small frame.
	payload := bytes.Repeat([]byte("cross-host-write-"), 4096)
	if _, err := file.Stream.Write(payload); err != nil {
		t.Fatalf("stream write: %v", err)
	}
	// Closing the requester end is the transfer's EOF: it unwinds the home's
	// pump, ends the target's copy, and triggers its Commit.
	if err := file.Stream.Close(); err != nil {
		t.Fatalf("stream close: %v", err)
	}

	if !sessRaceEventually(30*time.Second, func() bool {
		got, ok := target.bytesAt("coord-cross-write")
		return ok && bytes.Equal(got, payload)
	}) {
		got, ok := target.bytesAt("coord-cross-write")
		t.Fatalf("target landed %d bytes (present=%v), want %d", len(got), ok, len(payload))
	}
	if aborted := target.abortedCoords(); len(aborted) != 0 {
		t.Fatalf("the target aborted %v on a complete transfer", aborted)
	}
	if !sessRaceEventually(30*time.Second, func() bool {
		storage.mu.Lock()
		defer storage.mu.Unlock()
		return len(storage.committedCalls) == 1
	}) {
		t.Fatal("the target never reported the landing home")
	}
	storage.mu.Lock()
	calls := append([][2]string(nil), storage.committedCalls...)
	storage.mu.Unlock()
	if calls[0] != [2]string{"target-daemon", "res-cross-write"} {
		t.Fatalf("home credited %v, want the TARGET daemon's own session", calls[0])
	}
}

// The target refusing to open is the write path's own failure half: the
// requester must be told, before it streams anything, rather than being handed
// a stream that quietly swallows every byte.
func TestLaneCrossHostWriteSurfacesTheTargetsRefusal(t *testing.T) {
	rig := laneByDaemonRig(t, &fakeStorageControl{})
	target := &laneWriteOpener{openErr: errors.New("live root is full")}
	dialLaneTestDaemon(t, rig, "target-daemon", target)
	requester := dialLaneTestDaemon(t, rig, "requester-daemon", nil)

	tickets, err := rig.acc.OpenLaneTransfer(
		t.Context(), "target-daemon", "requester-daemon",
		"coord-refused", access.OpWrite, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	file, err := requester.redeemFileRoute(t.Context(), accessdoor.FileRoute{
		Local: false, Token: tickets.Redeem, Mode: access.OpWrite,
	})
	if err == nil {
		if file.Stream != nil {
			_ = file.Stream.Close()
		}
		t.Fatal("a target that could not open the coord still produced a live byte stream")
	}
	if !strings.Contains(err.Error(), "live root is full") {
		t.Fatalf("err = %v, want the target's own reason relayed to the requester", err)
	}
}
