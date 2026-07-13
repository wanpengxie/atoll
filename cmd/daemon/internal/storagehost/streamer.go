package storagehost

import (
	"fmt"
	"io"
	"os"

	"github.com/google/uuid"
)

// Streamer is §4.1's symmetric-data-path component, daemon half: LOCAL
// os.Root-scoped handles for a same-machine consumer (§3.4's "daemon 本地
// 颁 os.Root 子句柄给 caller"). The lane/forwarding half (cross-daemon bytes)
// lives in platform/internal/link's lane.go, not here — this type stays the
// pure local-handle primitive, self-contained and independently testable
// (mkdir/touch, staging→commit/abort) regardless of who calls it. Its
// consumer is cmd/daemon's storageadapter.go, which wraps *Host (this type's
// owner) as compute.LocalFileOpener for lane.go to consult.
type Streamer struct{}

// ReadHandle is the read-side local handle: a裸 read-only *os.File opened
// against the FINAL coord — no commit boundary (§3.9': "读无 commit 边界").
type ReadHandle struct{ f *os.File }

func (h *ReadHandle) Read(p []byte) (int, error)                   { return h.f.Read(p) }
func (h *ReadHandle) ReadAt(p []byte, off int64) (int, error)      { return h.f.ReadAt(p, off) }
func (h *ReadHandle) Seek(offset int64, whence int) (int64, error) { return h.f.Seek(offset, whence) }
func (h *ReadHandle) Close() error                                 { return h.f.Close() }

var _ io.ReadSeekCloser = (*ReadHandle)(nil)

// WriteHandle is the write-side local handle: a staging-file writer with an
// explicit Commit/Abort boundary (§3.9' P1 precision, v1.2's own late
// addition) — NOT a裸 handle to the final coord (the caller never sees the
// live path; only Commit's fsync+rename places bytes there, so
// coord/path visibility stays daemon-internal per §3.4's coord-confinement
// red line even for a same-machine caller). Commit is idempotent-safe to
// call once; a second call after a successful Commit fails (the staging
// file is gone) — callers own calling it exactly once.
type WriteHandle struct {
	cr             *channelRoot
	stagingRelPath string
	liveRelPath    string
	f              *os.File
	done           bool

	// onDone is Host.OpenWrite's in-flight-registration deregistration hook
	// (期11 S1 #6 — see Host.activeWrites' doc): fires exactly once, at
	// Commit or Abort, regardless of outcome (a failed Commit/rename still
	// ends this handle's "active write" window — a later Scrubber pass
	// falling back to reserved_at/last_progress_at staleness, or plain
	// crash-orphan sweep, is what decides whether a leftover staging file is
	// now safe to sweep, not this hook). nil for a handle minted directly
	// off Streamer (tests, or any caller that bypasses Host) — never invoked
	// in that case, matching Streamer's own stateless, self-contained doc.
	onDone func()
}

func (h *WriteHandle) Write(p []byte) (int, error) { return h.f.Write(p) }

// Commit fsyncs the staged bytes then atomically renames staging→live
// (§3.5's "daemon 侧 fsync+rename 到 live coord") — the SOLE point at which
// this write becomes visible to a reader of the same coord.
func (h *WriteHandle) Commit() error {
	if h.done {
		return fmt.Errorf("storagehost: write handle already closed")
	}
	h.done = true
	if h.onDone != nil {
		defer h.onDone()
	}
	if err := h.f.Sync(); err != nil {
		_ = h.f.Close()
		return fmt.Errorf("storagehost: commit fsync: %w", err)
	}
	if err := h.f.Close(); err != nil {
		return fmt.Errorf("storagehost: commit close: %w", err)
	}
	if err := h.cr.root.Rename(h.stagingRelPath, h.liveRelPath); err != nil {
		return fmt.Errorf("storagehost: commit rename: %w", err)
	}
	// 期11 S3 (transfer-lifecycle-spec.md §3's #7): the rename's directory
	// ENTRY into live/ needs its own fsync — see fsyncDir's doc.
	if err := fsyncDir(h.cr, liveDir); err != nil {
		return err
	}
	return nil
}

// Abort discards the staged bytes (caller error / cancellation) — the
// staging file is removed, the live coord is never touched (it may not
// even exist yet, for a first-time content-bearing create).
func (h *WriteHandle) Abort() error {
	if h.done {
		return nil
	}
	h.done = true
	if h.onDone != nil {
		defer h.onDone()
	}
	_ = h.f.Close()
	if err := h.cr.root.Remove(h.stagingRelPath); err != nil {
		return fmt.Errorf("storagehost: abort remove staging: %w", err)
	}
	return nil
}

// OpenRead returns a read-only handle onto coord's LIVE bytes.
func (Streamer) OpenRead(cr *channelRoot, coord string) (*ReadHandle, error) {
	p, err := livePath(coord)
	if err != nil {
		return nil, err
	}
	f, err := cr.root.Open(p)
	if err != nil {
		return nil, fmt.Errorf("storagehost: open read %q: %w", coord, err)
	}
	return &ReadHandle{f: f}, nil
}

// OpenDir opens coord as a directory-shaped resource's SUBTREE handle (期11
// 丁12): an os.Root re-confined to live/<coord> (nested inside the channel
// root's own os.Root, so the chroot-style confinement composes — a workspace
// lease can never escape its own subtree, let alone the channel root). Unlike
// OpenWrite there is NO staging/Commit boundary: the caller's os.Create/Mkdir/
// Open/Remove land IMMEDIATELY in the live tree (a directory is not a single
// atomic blob to stage-then-rename). The coord dir must already exist (the
// Allocator mkdir'd it at create time, §4.7) — a missing coord surfaces as an
// honest open error, never a silent mkdir here.
func (Streamer) OpenDir(cr *channelRoot, coord string) (*os.Root, error) {
	p, err := livePath(coord)
	if err != nil {
		return nil, err
	}
	root, err := cr.root.OpenRoot(p)
	if err != nil {
		return nil, fmt.Errorf("storagehost: open dir %q: %w", coord, err)
	}
	return root, nil
}

// OpenWrite mints a fresh, mktemp-unique staging file for coord (§3.5's
// "staging 临时名每写唯一" — concurrent writes to the same coord each get
// their own staging file, so last-rename-wins can only ever replace a
// FULL, never-interleaved write) and returns the WriteHandle bound to it.
func (Streamer) OpenWrite(cr *channelRoot, coord string) (*WriteHandle, error) {
	suffix := uuid.NewString()
	sp, err := stagingPath(coord, suffix)
	if err != nil {
		return nil, err
	}
	lp, err := livePath(coord)
	if err != nil {
		return nil, err
	}
	f, err := cr.root.OpenFile(sp, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("storagehost: open write staging %q: %w", coord, err)
	}
	return &WriteHandle{cr: cr, stagingRelPath: sp, liveRelPath: lp, f: f}, nil
}
