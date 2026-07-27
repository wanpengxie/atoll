package link_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// lane_commit_test.go covers glue #19: handleLaneInbound's OpWrite arm now
// reuses committingWriteHandle (redeemFileRoute's SAME ReservationID!=""
// wrapper) instead of a hand-written SendCommitted段, so a home Lost verdict
// reclaims the orphaned coord and every commit failure is a Warn (the lane
// protocol has no completion-reply frame — the "发送方知情" half stays A4). The
// full committingWriteHandle.Commit verdict matrix is locked at the wrapper
// level in storagecontrol_test.go (期11 review #D); these tests prove the LANE
// path actually routes through it.

// --- test doubles ------------------------------------------------------------

// logCapture is a slog.Handler that records every emitted message so a test can
// assert a Warn was surfaced (never a silent drop).
type logCapture struct {
	mu   sync.Mutex
	msgs []string
}

func (h *logCapture) Enabled(context.Context, slog.Level) bool { return true }
func (h *logCapture) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.msgs = append(h.msgs, r.Message)
	h.mu.Unlock()
	return nil
}
func (h *logCapture) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *logCapture) WithGroup(string) slog.Handler      { return h }
func (h *logCapture) has(msg string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, m := range h.msgs {
		if m == msg {
			return true
		}
	}
	return false
}

// recordingLaneOpener is a LocalFileOpener whose OpenWrite lands bytes in memory
// and whose ReclaimCoord records the reclaimed coord — so a test can assert
// whether the Lost branch actually reclaimed.
type recordingLaneOpener struct {
	mu        sync.Mutex
	written   map[string][]byte
	reclaimed []string
}

func (o *recordingLaneOpener) OpenRead(string) (io.ReadSeekCloser, error) {
	return nil, errors.New("recordingLaneOpener: OpenRead unexercised")
}
func (o *recordingLaneOpener) OpenWrite(coord string) (accessdoor.LocalWriteHandle, error) {
	return &recordingWH{o: o, coord: coord}, nil
}
func (o *recordingLaneOpener) OpenDir(string) (accessdoor.LocalDirHandle, error) {
	return nil, errors.New("recordingLaneOpener: OpenDir unexercised")
}
func (o *recordingLaneOpener) ReclaimCoord(coord string) error {
	o.mu.Lock()
	o.reclaimed = append(o.reclaimed, coord)
	o.mu.Unlock()
	return nil
}
func (o *recordingLaneOpener) reclaimedCoords() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.reclaimed...)
}
func (o *recordingLaneOpener) hasBytes(coord string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	_, ok := o.written[coord]
	return ok
}

var _ link.LocalFileOpener = (*recordingLaneOpener)(nil)

type recordingWH struct {
	o     *recordingLaneOpener
	coord string
	buf   bytes.Buffer
}

func (h *recordingWH) Write(p []byte) (int, error) { return h.buf.Write(p) }
func (h *recordingWH) Commit() error {
	h.o.mu.Lock()
	if h.o.written == nil {
		h.o.written = map[string][]byte{}
	}
	h.o.written[h.coord] = append([]byte(nil), h.buf.Bytes()...)
	h.o.mu.Unlock()
	return nil
}
func (h *recordingWH) Abort() error { return nil }

// --- lane wire helpers (newline-delimited JSON, lane.go's tiny framing) -------

type laneHeaderT struct {
	Token string `json:"token"`
}
type laneAckT struct {
	OK     bool   `json:"ok"`
	Reason string `json:"reason,omitempty"`
}

func writeJSONLine(t *testing.T, w io.Writer, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal lane line: %v", err)
	}
	if _, err := w.Write(append(b, '\n')); err != nil {
		t.Fatalf("write lane line: %v", err)
	}
}

func readAckLine(t *testing.T, r io.Reader) laneAckT {
	t.Helper()
	var buf []byte
	one := make([]byte, 1)
	for {
		n, err := r.Read(one)
		if n == 1 {
			if one[0] == '\n' {
				break
			}
			buf = append(buf, one[0])
		}
		if err != nil {
			t.Fatalf("read ack line: %v", err)
		}
	}
	var ack laneAckT
	if err := json.Unmarshal(buf, &ack); err != nil {
		t.Fatalf("unmarshal ack: %v", err)
	}
	return ack
}

// dialWithLogger dials the storage rig's home with a capturing logger so a test
// can assert the Warn the lane commit arm emits.
func dialWithLogger(t *testing.T, r *storageRig, opener link.LocalFileOpener) (*link.Dialer, *logCapture) {
	t.Helper()
	cap := &logCapture{}
	d, err := link.Dial(context.Background(), r.wsURL(), link.DialConfig{
		LocalFileOpener: opener,
		SessionLedger:   link.NewRemoteSessionLedger(nil),
	}, slog.New(cap))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d, cap
}

// driveLaneWrite runs one inbound write-redeem against handleLaneInbound: the
// home has already registered a transfer for tok. It writes the redeem header,
// reads the ack, streams payload, then EOFs — returning once the daemon handler
// has fully finished (so reclaim/commit side effects are observable).
func driveLaneWrite(t *testing.T, d *link.Dialer, tok string, payload []byte) laneAckT {
	t.Helper()
	srvConn, cliConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		d.HandleLaneInboundForTest(srvConn)
		close(done)
	}()

	writeJSONLine(t, cliConn, laneHeaderT{Token: tok})
	ack := readAckLine(t, cliConn)
	if ack.OK {
		if _, err := cliConn.Write(payload); err != nil {
			t.Fatalf("write payload: %v", err)
		}
	}
	_ = cliConn.Close() // EOF: io.Copy on the daemon side completes → Commit

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleLaneInbound never returned")
	}
	return ack
}

// --- #19 tests ---------------------------------------------------------------

// TestLaneCommit_LostReclaimsAndWarns: a home Lost verdict on the lane write
// route reclaims THIS write's orphaned coord (it lost the same-resource_id race)
// and Warns — never the pre-#19 silent SendCommitted with no reclaim.
func TestLaneCommit_LostReclaimsAndWarns(t *testing.T) {
	r := newStorageRig(t)
	r.shc.committedFound = true
	r.shc.committedLost = true

	opener := &recordingLaneOpener{}
	d, logs := dialWithLogger(t, r, opener)

	tickets, err := r.acc.OpenLaneTransfer(context.Background(), "daemon-1", "daemon-1", "coord-lost", access.OpWrite, "res-lost")
	if err != nil {
		t.Fatalf("OpenLaneTransfer: %v", err)
	}
	ack := driveLaneWrite(t, d, tickets.Resolve, []byte("bytes"))
	if !ack.OK {
		t.Fatalf("lane write ack = %+v, want OK", ack)
	}
	if got := opener.reclaimedCoords(); len(got) != 1 || got[0] != "coord-lost" {
		t.Fatalf("Lost verdict reclaimed = %v, want [coord-lost]", got)
	}
	if !logs.has("link.lane_commit_failed") {
		t.Fatal("Lost commit did not Warn (silent drop is the pre-#19 洞)")
	}
}

// TestLaneCommit_NakWarnsNoReclaim: an explicit home NAK (reply.Reason set,
// Lost=false — e.g. a store error) Warns but does NOT reclaim (only a definitive
// Lost race collects the orphan; a NAK may be transient).
func TestLaneCommit_NakWarnsNoReclaim(t *testing.T) {
	r := newStorageRig(t)
	r.shc.committedErr = errors.New("store unavailable")

	opener := &recordingLaneOpener{}
	d, logs := dialWithLogger(t, r, opener)

	tickets, err := r.acc.OpenLaneTransfer(context.Background(), "daemon-1", "daemon-1", "coord-nak", access.OpWrite, "res-nak")
	if err != nil {
		t.Fatalf("OpenLaneTransfer: %v", err)
	}
	driveLaneWrite(t, d, tickets.Resolve, []byte("bytes"))

	if got := opener.reclaimedCoords(); len(got) != 0 {
		t.Fatalf("NAK falsely reclaimed %v, want none", got)
	}
	if !logs.has("link.lane_commit_failed") {
		t.Fatal("NAK commit did not Warn")
	}
}

// TestLaneCommit_TransportErrorNoReclaim: when the Committed relay itself fails
// on the wire (home torn down), the wrapper reports the failure but must NOT
// reclaim — a transport error is not a definitive Lost verdict, so deleting the
// landed bytes would be data loss.
func TestLaneCommit_TransportErrorNoReclaim(t *testing.T) {
	r := newStorageRig(t)
	opener := &recordingLaneOpener{}
	d := dialStorageDaemon(t, r, link.DialConfig{LocalFileOpener: opener})
	// Tear the home down so SendCommitted transport-fails.
	_ = r.acc.Close()
	r.srv.Close()

	wh := &fakeCommitWH{}
	h := link.NewCommittingWriteHandleForTest(d, wh, "res-x", "coord-x")
	if err := h.Commit(); err == nil {
		t.Fatal("Commit returned nil on a torn-down home (false success)")
	}
	if got := opener.reclaimedCoords(); len(got) != 0 {
		t.Fatalf("transport error falsely reclaimed %v, want none", got)
	}
}

// TestLaneCommit_NoReservationPlainCommit: a reservation-less write transfer
// takes the plain Commit path — no committingWriteHandle wrap, so no Committed
// frame is ever sent (home records zero Committed calls) and the bytes land.
func TestLaneCommit_NoReservationPlainCommit(t *testing.T) {
	r := newStorageRig(t)
	opener := &recordingLaneOpener{}
	d, _ := dialWithLogger(t, r, opener)

	tickets, err := r.acc.OpenLaneTransfer(context.Background(), "daemon-1", "daemon-1", "coord-plain", access.OpWrite, "")
	if err != nil {
		t.Fatalf("OpenLaneTransfer: %v", err)
	}
	ack := driveLaneWrite(t, d, tickets.Resolve, []byte("plain-bytes"))
	if !ack.OK {
		t.Fatalf("lane write ack = %+v, want OK", ack)
	}
	if !opener.hasBytes("coord-plain") {
		t.Fatal("plain write did not land bytes")
	}
	r.shc.mu.Lock()
	n := len(r.shc.committedCalls)
	r.shc.mu.Unlock()
	if n != 0 {
		t.Fatalf("reservation-less write sent %d Committed frame(s), want 0 (plain Commit path)", n)
	}
	if got := opener.reclaimedCoords(); len(got) != 0 {
		t.Fatalf("plain write reclaimed %v, want none", got)
	}
}
