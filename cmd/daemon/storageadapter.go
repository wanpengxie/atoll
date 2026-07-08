package main

// storageadapter.go is cmd/daemon's own boundary-crossing adapter (期11
// §4): platform.StorageHost's method shapes and
// cmd/daemon/internal/storagehost.Host's method shapes are structurally
// identical (same field names/types throughout) but nominally distinct Go
// types — cmd/daemon can import BOTH platform and its own internal
// storagehost package (the only place that is simultaneously true), so this
// is the one seam that translates between them, the exact mirror of
// platform's OWN storageHostForwarder translating platform/internal/link's
// wire types into platform.StorageHost's plain shapes.

import (
	"context"
	"io"

	"github.com/wanpengxie/atoll/cmd/daemon/internal/storagehost"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// storageHostAdapter wraps a *storagehost.Host to satisfy platform.
// StorageHost.
type storageHostAdapter struct{ host *storagehost.Host }

func (a storageHostAdapter) Alloc(coord string, dir bool) error {
	return a.host.Alloc(coord, dir)
}

func (a storageHostAdapter) Reconcile(ctx context.Context, resources []platform.StorageResourceCoord, pendingReservations []platform.StorageReservationCoord, pendingTombstones []platform.StorageTombstoneCoord, ack platform.StorageReclaimAckFunc, resend platform.StorageCommittedResendFunc) {
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
	var hostResend storagehost.CommittedResendFunc
	if resend != nil {
		hostResend = storagehost.CommittedResendFunc(resend)
	}
	a.host.Reconcile(ctx, rs, prs, pts, hostAck, hostResend)
}

// OpenRead / OpenWrite satisfy platform.LocalFileOpener (期11 §5) — the SAME
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

// OpenDir satisfies platform.LocalFileOpener's directory arm (期11 丁12): the
// *storagehost.Host's Streamer facet hands out an *os.Root confined to
// live/<coord>, which structurally satisfies accessdoor.LocalDirHandle (its
// method set is a subset of *os.Root's) with no further wrapping — the os.Root
// TYPE stays inside cmd/daemon, only its method surface crosses to platform.
func (a storageHostAdapter) OpenDir(coord string) (accessdoor.LocalDirHandle, error) {
	return a.host.OpenDir(coord)
}

var (
	_ platform.StorageHost     = storageHostAdapter{}
	_ platform.LocalFileOpener = storageHostAdapter{}
)
