package link

// The WRITE direction of the file byte route, end to end over a real websocket
// link. transfer_ticket_test.go walks the read direction; the write direction
// is the harder half: the coord resolves over the real control plane, the bytes
// land through the daemon's own storage host, and when the route carries a
// reservation the SAME link then carries the Committed landing signal. Three
// mechanisms on one connection, none of which an in-process accessdoor test
// can reach.
//
// Byte access is same-daemon only, so there is no relay half here and no
// "target refused after the requester was already handed a stream" failure
// mode: the daemon that opens the handle is the daemon that asked.

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// fileWriteOpener is a daemon-side storage host that lands committed bytes in
// memory, keyed by coord. openErr, when set, makes every OpenWrite fail.
type fileWriteOpener struct {
	mu       sync.Mutex
	landed   map[string][]byte
	aborted  []string
	openErr  error
	reclaims []string
}

func (o *fileWriteOpener) OpenRead(string) (io.ReadSeekCloser, error) {
	return nil, errors.New("fileWriteOpener: OpenRead unexercised")
}

func (o *fileWriteOpener) OpenWrite(coord string) (accessdoor.LocalWriteHandle, error) {
	o.mu.Lock()
	err := o.openErr
	o.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &fileWriteHandle{owner: o, coord: coord}, nil
}

func (o *fileWriteOpener) OpenDir(string) (accessdoor.LocalDirHandle, error) {
	return nil, errors.New("fileWriteOpener: OpenDir unexercised")
}

func (o *fileWriteOpener) ReclaimCoord(coord string) error {
	o.mu.Lock()
	o.reclaims = append(o.reclaims, coord)
	o.mu.Unlock()
	return nil
}

func (o *fileWriteOpener) bytesAt(coord string) ([]byte, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	value, ok := o.landed[coord]
	return value, ok
}

func (o *fileWriteOpener) abortedCoords() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.aborted...)
}

var _ LocalFileOpener = (*fileWriteOpener)(nil)

type fileWriteHandle struct {
	owner *fileWriteOpener
	coord string
	buf   bytes.Buffer
}

func (h *fileWriteHandle) Write(p []byte) (int, error) { return h.buf.Write(p) }

func (h *fileWriteHandle) Commit() error {
	h.owner.mu.Lock()
	if h.owner.landed == nil {
		h.owner.landed = map[string][]byte{}
	}
	h.owner.landed[h.coord] = append([]byte(nil), h.buf.Bytes()...)
	h.owner.mu.Unlock()
	return nil
}

func (h *fileWriteHandle) Abort() error {
	h.owner.mu.Lock()
	h.owner.aborted = append(h.owner.aborted, h.coord)
	h.owner.mu.Unlock()
	return nil
}

func fileRouteRig(t *testing.T, storage StorageHostControl) *acceptorRig {
	t.Helper()
	return newAcceptorRig(t, acceptorRigConfig{
		storage:  storage,
		daemonID: func(req *http.Request) string { return req.URL.Query().Get("daemon") },
	})
}

// Same-daemon write: the route resolves over the real control plane, the bytes
// land through the daemon's own storage host, and the reservation's Committed
// signal then travels back on that same link — carrying the reservation id
// under the authenticated daemon identity.
func TestFileWriteRouteLandsBytesAndReportsCommitted(t *testing.T) {
	storage := &fakeStorageControl{}
	rig := fileRouteRig(t, storage)
	opener := &fileWriteOpener{}
	daemon := dialLaneTestDaemon(t, rig, "local-daemon", opener)

	ticket, err := rig.acc.OpenTransfer(
		t.Context(), "local-daemon",
		"coord-local-write", access.OpWrite, "res-local-write",
	)
	if err != nil {
		t.Fatal(err)
	}
	file, err := daemon.redeemFileRoute(t.Context(), accessdoor.FileRoute{
		Token: ticket, Mode: access.OpWrite,
		ReservationID: "res-local-write",
	})
	if err != nil {
		t.Fatalf("write redemption: %v", err)
	}
	if file.Local == nil || file.Local.Write == nil {
		t.Fatalf("write redemption returned the wrong access shape: %+v", file)
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

// A write route with NO reservation is a plain OpWrite (§3.5: OpWrite never
// fires Committed). The bytes still land; the home hears nothing.
func TestFileWriteWithoutReservationSendsNoCommitted(t *testing.T) {
	storage := &fakeStorageControl{}
	rig := fileRouteRig(t, storage)
	opener := &fileWriteOpener{}
	daemon := dialLaneTestDaemon(t, rig, "local-daemon", opener)

	ticket, err := rig.acc.OpenTransfer(
		t.Context(), "local-daemon", "coord-plain", access.OpWrite, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	file, err := daemon.redeemFileRoute(t.Context(), accessdoor.FileRoute{
		Token: ticket, Mode: access.OpWrite,
	})
	if err != nil {
		t.Fatalf("write redemption: %v", err)
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
