package link_test

// committingWriteHandle's FULL Commit verdict matrix at the wrapper level —
// the "never fake success" contract (期11 review #D).
//
// The wrapper sits between a landed local write and the home's own landing of
// the resource row, so it is the single place where a rejected commit could be
// laundered into a nil error. Every branch below therefore asserts the SAME
// two things: what the caller is told, and whether the already-renamed bytes
// were reclaimed. Reclaim is as dangerous as false success in the other
// direction — collecting bytes on anything short of a DEFINITIVE Lost verdict
// is data loss.
//
// file_write_route_test.go proves the write route actually routes through this
// wrapper; the matrix itself lives here, driven directly through
// link.NewCommittingWriteHandleForTest so each verdict is reached without
// staging a whole transfer for it.

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// recordingFileOpener is a LocalFileOpener whose OpenWrite lands bytes in memory
// and whose ReclaimCoord records the reclaimed coord — so a test can assert
// whether the Lost branch actually reclaimed.
type recordingFileOpener struct {
	mu        sync.Mutex
	written   map[string][]byte
	reclaimed []string
}

func (o *recordingFileOpener) OpenRead(string) (io.ReadSeekCloser, error) {
	return nil, errors.New("recordingFileOpener: OpenRead unexercised")
}
func (o *recordingFileOpener) OpenWrite(coord string) (accessdoor.LocalWriteHandle, error) {
	return &recordingWH{o: o, coord: coord}, nil
}
func (o *recordingFileOpener) OpenDir(string) (accessdoor.LocalDirHandle, error) {
	return nil, errors.New("recordingFileOpener: OpenDir unexercised")
}
func (o *recordingFileOpener) ReclaimCoord(coord string) error {
	o.mu.Lock()
	o.reclaimed = append(o.reclaimed, coord)
	o.mu.Unlock()
	return nil
}
func (o *recordingFileOpener) reclaimedCoords() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.reclaimed...)
}

var _ link.LocalFileOpener = (*recordingFileOpener)(nil)

type recordingWH struct {
	o     *recordingFileOpener
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

// failingCommitWH is a LocalWriteHandle whose own Commit fails — the bytes
// never landed, so nothing downstream of the local commit may run.
type failingCommitWH struct {
	err     error
	aborted bool
}

func (h *failingCommitWH) Write(p []byte) (int, error) { return len(p), nil }
func (h *failingCommitWH) Commit() error               { return h.err }
func (h *failingCommitWH) Abort() error                { h.aborted = true; return nil }

// commitWrapperRig wires one live daemon↔home pair plus the recording opener
// the Lost branch reclaims through.
func commitWrapperRig(t *testing.T) (*storageRig, *link.Dialer, *recordingFileOpener) {
	t.Helper()
	r := newStorageRig(t)
	opener := &recordingFileOpener{}
	d := dialStorageDaemon(t, r, link.DialConfig{LocalFileOpener: opener})
	return r, d, opener
}

// A local commit failure stops the chain dead: the bytes never landed, so no
// Committed frame may be sent (a home told a reservation landed when it did
// not would file a row for bytes that do not exist) and nothing is reclaimed.
func TestCommittingWriteHandle_LocalCommitFailureSendsNoCommitted(t *testing.T) {
	r, d, opener := commitWrapperRig(t)
	wh := &failingCommitWH{err: errors.New("fsync failed")}

	err := link.NewCommittingWriteHandleForTest(d, wh, "res-local-fail", "coord-local-fail").Commit()
	if err == nil {
		t.Fatal("a failed local commit reported success")
	}
	if !strings.Contains(err.Error(), "fsync failed") {
		t.Fatalf("err = %v, want the local failure surfaced", err)
	}
	r.shc.mu.Lock()
	sent := len(r.shc.committedCalls)
	r.shc.mu.Unlock()
	if sent != 0 {
		t.Fatalf("home received %d Committed frame(s) for bytes that never landed", sent)
	}
	if got := opener.reclaimedCoords(); len(got) != 0 {
		t.Fatalf("a local commit failure reclaimed %v; the wrapper must not touch bytes it never landed", got)
	}
}

// The landed path: the home files the row, the caller is told success, and the
// reservation id the home saw is the one this handle carries — under the
// endpoint's authenticated daemon identity, which the frame never states.
func TestCommittingWriteHandle_FoundLandsAndReportsSuccess(t *testing.T) {
	r, d, opener := commitWrapperRig(t)
	r.shc.committedFound = true

	if err := link.NewCommittingWriteHandleForTest(d, &fakeCommitWH{}, "res-landed", "coord-landed").Commit(); err != nil {
		t.Fatalf("a landed commit was reported as a failure: %v", err)
	}
	r.shc.mu.Lock()
	calls := append([]struct{ sender, reservationID string }(nil), r.shc.committedCalls...)
	r.shc.mu.Unlock()
	if len(calls) != 1 || calls[0].reservationID != "res-landed" || calls[0].sender != "daemon-1" {
		t.Fatalf("home saw %+v, want one landing from the authenticated daemon-1", calls)
	}
	if got := opener.reclaimedCoords(); len(got) != 0 {
		t.Fatalf("a successful commit reclaimed %v", got)
	}
}

// Found=false with no reason is the documented benign no-op (an earlier replay
// or a superseding delete already settled this reservation). It is a success
// for the caller AND explicitly not a reclaim: "no row for this reservation"
// cannot be distinguished from "someone else already landed these bytes", and
// deleting live bytes is strictly worse than leaving a stray empty coord.
func TestCommittingWriteHandle_NotFoundIsABenignNoOpWithoutReclaim(t *testing.T) {
	_, d, opener := commitWrapperRig(t)

	if err := link.NewCommittingWriteHandleForTest(d, &fakeCommitWH{}, "res-gone", "coord-gone").Commit(); err != nil {
		t.Fatalf("a replay-safe no-op was reported as a failure: %v", err)
	}
	if got := opener.reclaimedCoords(); len(got) != 0 {
		t.Fatalf("a not-found reply reclaimed %v; only a definitive Lost may collect bytes", got)
	}
}

// An explicit home NAK (Reason set, Lost=false — a store error, a placement
// mismatch, no storage control on the channel) is a failure the caller must
// see: the bytes are on disk but no row is visible, so a nil here would strand
// them forever. It is NOT a reclaim: a NAK may be transient.
func TestCommittingWriteHandle_HomeNakIsAFailureWithoutReclaim(t *testing.T) {
	r, d, opener := commitWrapperRig(t)
	r.shc.committedFound = true
	r.shc.committedErr = errors.New("resource store unavailable")

	err := link.NewCommittingWriteHandleForTest(d, &fakeCommitWH{}, "res-nak", "coord-nak").Commit()
	if err == nil {
		t.Fatal("an explicit home NAK was laundered into success")
	}
	if !strings.Contains(err.Error(), "resource store unavailable") ||
		!strings.Contains(err.Error(), "res-nak") {
		t.Fatalf("err = %v, want the home's reason and the reservation named", err)
	}
	if got := opener.reclaimedCoords(); len(got) != 0 {
		t.Fatalf("a NAK reclaimed %v; a possibly-transient reject must not delete landed bytes", got)
	}
}

// Lost is the one DEFINITIVE non-landing verdict: another reservation owns the
// resource id, so these bytes can never be claimed. The caller is told loudly
// (never retried into the same id) and exactly this handle's own coord — the
// coord never rides the wire, the wrapper carries it — is collected.
func TestCommittingWriteHandle_LostFailsLoudAndReclaimsItsOwnCoord(t *testing.T) {
	r, d, opener := commitWrapperRig(t)
	r.shc.committedFound = true
	r.shc.committedLost = true

	err := link.NewCommittingWriteHandleForTest(d, &fakeCommitWH{}, "res-lost", "coord-lost").Commit()
	if err == nil {
		t.Fatal("a Lost race was reported as a successful commit")
	}
	if !strings.Contains(err.Error(), "res-lost") {
		t.Fatalf("err = %v, want the losing reservation named", err)
	}
	if got := opener.reclaimedCoords(); len(got) != 1 || got[0] != "coord-lost" {
		t.Fatalf("Lost reclaimed %v, want exactly [coord-lost]", got)
	}
}

// A reservation-less handle is never wrapped in production; the wrapper's own
// arms are what the two write routes share, so an unwired opener on the Lost
// branch must degrade to a logged best-effort miss rather than a panic or a
// second failure mode — the next Scrubber pass is the backstop.
func TestCommittingWriteHandle_LostWithoutOpenerStillFailsTheCaller(t *testing.T) {
	r := newStorageRig(t)
	r.shc.committedFound = true
	r.shc.committedLost = true
	d := dialStorageDaemon(t, r, link.DialConfig{})

	err := link.NewCommittingWriteHandleForTest(d, &fakeCommitWH{}, "res-lost", "coord-lost").Commit()
	if err == nil {
		t.Fatal("a Lost race on a compute with no opener was reported as success")
	}
}
