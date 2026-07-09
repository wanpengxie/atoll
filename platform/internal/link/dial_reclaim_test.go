package link

import (
	"errors"
	"io"
	"testing"

	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// dial_reclaim_test.go is 期11 S2's own DoD proof (transfer-lifecycle-spec.md
// §3's #2, "非-land 终态回收"): committingWriteHandle.Commit's Lost branch
// must actually reach the daemon's LocalFileOpener.ReclaimCoord with the
// write's OWN coord — the fix for CommittedReply.Lost previously reaching
// dial.go and going nowhere (a silent drop → the concurrent-loser-orphan
// bug). Unit-level (reclaimLostCoord itself, not the full Commit→
// SendCommitted round trip): a live wire is lane_test.go's job
// (TestLaneSameDaemonLocalRoute et al.), this file isolates the ONE branch
// that was missing.

// fakeReclaimOpener is a minimal LocalFileOpener double: OpenRead/OpenWrite/
// OpenDir are never exercised here (unit-level — only ReclaimCoord's
// plumbing is under test).
type fakeReclaimOpener struct {
	reclaimed []string
	err       error
}

func (f *fakeReclaimOpener) OpenRead(coord string) (io.ReadSeekCloser, error) {
	return nil, errors.New("fakeReclaimOpener: OpenRead unexercised")
}

func (f *fakeReclaimOpener) OpenWrite(coord string) (accessdoor.LocalWriteHandle, error) {
	return nil, errors.New("fakeReclaimOpener: OpenWrite unexercised")
}

func (f *fakeReclaimOpener) OpenDir(coord string) (accessdoor.LocalDirHandle, error) {
	return nil, errors.New("fakeReclaimOpener: OpenDir unexercised")
}

func (f *fakeReclaimOpener) ReclaimCoord(coord string) error {
	f.reclaimed = append(f.reclaimed, coord)
	return f.err
}

var _ LocalFileOpener = (*fakeReclaimOpener)(nil)

// TestDialer_ReclaimLostCoord_CallsOpener proves a Lost reservation's coord
// reaches the wired LocalFileOpener.
func TestDialer_ReclaimLostCoord_CallsOpener(t *testing.T) {
	d := testDialer()
	opener := &fakeReclaimOpener{}
	d.localFileOpener = opener

	d.reclaimLostCoord("coord-lost-1")

	if len(opener.reclaimed) != 1 || opener.reclaimed[0] != "coord-lost-1" {
		t.Fatalf("reclaimed = %v, want [coord-lost-1]", opener.reclaimed)
	}
}

// TestDialer_ReclaimLostCoord_NilOpenerNoPanic confirms a Dialer with no
// LocalFileOpener wired (a compute that never redeems file bytes at all)
// just logs, never panics.
func TestDialer_ReclaimLostCoord_NilOpenerNoPanic(t *testing.T) {
	d := testDialer()
	d.reclaimLostCoord("coord-lost-2") // must not panic
}

// TestDialer_ReclaimLostCoord_ReclaimErrorNoPanic confirms a failing Reclaim
// (e.g. the local fs op itself errored) is logged, not propagated/panicked —
// the NEXT Scrubber pass's own orphan sweep is the backstop.
func TestDialer_ReclaimLostCoord_ReclaimErrorNoPanic(t *testing.T) {
	d := testDialer()
	opener := &fakeReclaimOpener{err: errors.New("boom")}
	d.localFileOpener = opener

	d.reclaimLostCoord("coord-lost-3") // must not panic

	if len(opener.reclaimed) != 1 || opener.reclaimed[0] != "coord-lost-3" {
		t.Fatalf("reclaimed = %v, want [coord-lost-3] even though Reclaim errored", opener.reclaimed)
	}
}
