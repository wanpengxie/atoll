package storagehost

import (
	"errors"
	"fmt"
	"os"
)

// Reclaimer is §4.1's delete-side component: it collects a tombstoned
// resource's bytes.
type Reclaimer struct{}

// Reclaim removes coord's live bytes (idempotent: an already-gone entry —
// e.g. a repeat ReclaimAck's collection request, or an Alloc that never
// actually landed anything — is a clean no-op, never an error).
func (Reclaimer) Reclaim(cr *channelRoot, coord string) error {
	p, err := livePath(coord)
	if err != nil {
		return err
	}
	if err := cr.root.RemoveAll(p); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("storagehost: reclaim %q: %w", coord, err)
	}
	// 期11 review残余#5: the directory ENTRY removal RemoveAll just performed
	// is itself a separate piece of metadata the containing live/ directory
	// owns — the same POSIX "fsync the parent" rule fsyncDir's own doc names
	// (already applied on the CREATE side by Allocator/Streamer). Without
	// this, a crash right after RemoveAll can resurrect the reclaimed coord
	// on reboot even though the registry/tombstone bookkeeping already
	// considers it gone. Runs even on the idempotent no-op case (coord never
	// existed) — a fsync of an already-consistent directory is a harmless no-op.
	if err := fsyncDir(cr, liveDir); err != nil {
		return fmt.Errorf("storagehost: reclaim %q: fsync live dir: %w", coord, err)
	}
	return nil
}
