package main

// storageadapter.go is cmd/daemon's own boundary-crossing adapter (期11
// §4): compute.StorageHost's method shapes and
// cmd/daemon/internal/storagehost.Host's method shapes are structurally
// identical (same field names/types throughout) but nominally distinct Go
// types — cmd/daemon can import BOTH platform/compute and its own internal
// storagehost package (the only place that is simultaneously true), so this
// is the one seam that translates between them, the exact mirror of
// platform's OWN storageHostForwarder translating platform/internal/link's
// wire types into compute.StorageHost's plain shapes.

import (
	"context"
	"io"

	"github.com/wanpengxie/atoll/cmd/daemon/internal/storagehost"
	"github.com/wanpengxie/atoll/platform/compute"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// storageHostAdapter wraps a *storagehost.Host to satisfy compute.
// StorageHost.
type storageHostAdapter struct{ host *storagehost.Host }

func (a storageHostAdapter) Alloc(coord string, dir bool) error {
	return a.host.Alloc(coord, dir)
}

func (a storageHostAdapter) Reconcile(ctx context.Context, resources []compute.StorageResourceCoord, pendingReservations []compute.StorageReservationCoord, pendingTombstones []compute.StorageTombstoneCoord, ack compute.StorageReclaimAckFunc) {
	rs := make([]storagehost.ResourceLanded, 0, len(resources))
	for _, r := range resources {
		rs = append(rs, storagehost.ResourceLanded{Coord: r.Coord})
	}
	prs := make([]storagehost.ReservationPending, 0, len(pendingReservations))
	for _, r := range pendingReservations {
		prs = append(prs, storagehost.ReservationPending{ReservationID: r.ReservationID, Coord: r.Coord})
	}
	pts := make([]storagehost.TombstoneToReclaim, 0, len(pendingTombstones))
	for _, t := range pendingTombstones {
		pts = append(pts, storagehost.TombstoneToReclaim{TombstoneID: t.TombstoneID, Coord: t.Coord, Provenance: t.Provenance})
	}
	var hostAck storagehost.ReclaimAckFunc
	if ack != nil {
		hostAck = storagehost.ReclaimAckFunc(ack)
	}
	a.host.Reconcile(ctx, rs, prs, pts, hostAck)
}

// ActiveWriteCoords satisfies compute.StorageHost's own narrowing addition
// (期11 review): a plain []string mirror of *storagehost.Host.
// ActiveWriteCoords's []storagehost.ActiveStaging snapshot — this seam
// exists for the exact same reason Reconcile's own translation does (plain
// wrapping type this package alone can convert between).
func (a storageHostAdapter) ActiveWriteCoords() []string {
	staging := a.host.ActiveWriteCoords()
	out := make([]string, 0, len(staging))
	for _, s := range staging {
		out = append(out, s.Coord)
	}
	return out
}

// OpenRead / OpenWrite satisfy compute.LocalFileOpener (期11 §5) — the SAME
// *storagehost.Host, its Streamer facet rather than its Allocator/Scrubber
// facet. *storagehost.ReadHandle already has Read/Seek/Close
// (io.ReadSeekCloser); *storagehost.WriteHandle already has Write/Commit/
// Abort (accessdoor.LocalWriteHandle) — both satisfy the target interfaces
// structurally, no further wrapping needed.
func (a storageHostAdapter) OpenRead(coord string) (io.ReadSeekCloser, error) {
	return a.host.OpenRead(coord)
}

func (a storageHostAdapter) OpenWrite(coord string) (accessdoor.LocalWriteHandle, error) {
	return a.host.OpenWrite(coord)
}

// OpenDir satisfies compute.LocalFileOpener's directory arm (期11 丁12): the
// *storagehost.Host's Streamer facet hands out an *os.Root confined to
// live/<coord>, which structurally satisfies accessdoor.LocalDirHandle (its
// method set is a subset of *os.Root's) with no further wrapping — the os.Root
// TYPE stays inside cmd/daemon, only its method surface crosses to platform.
func (a storageHostAdapter) OpenDir(coord string) (accessdoor.LocalDirHandle, error) {
	return a.host.OpenDir(coord)
}

// ReclaimCoord satisfies compute.LocalFileOpener's ReclaimCoord (期11 S2) —
// same Host, its Reclaimer facet (reused from the tombstone delete path,
// storagehost.Host.ReclaimCoord's own doc).
func (a storageHostAdapter) ReclaimCoord(coord string) error {
	return a.host.ReclaimCoord(coord)
}

var (
	_ compute.StorageHost     = storageHostAdapter{}
	_ compute.LocalFileOpener = storageHostAdapter{}
)
