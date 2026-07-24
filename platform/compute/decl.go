package compute

import (
	"context"
	"io"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
)

// ActorFactorySource resolves only the exact immutable plan generation that
// created a Host build claim. ActorID-only lookup is intentionally absent: a
// newer plan must never supply its factory to an older in-flight build.
type ActorFactorySource interface {
	LookupExact(
		id actor.ActorID,
		attempt actorhost.AttemptKey,
		spec actorhost.ExecutionSpec,
	) (def platform.ActorFactory, ok bool)
}

// LocalFileOpener mirrors platform/internal/link.LocalFileOpener's exact
// method set (期11 spec §5/§3.4's "daemon 本地颁 os.Root 子句柄") — a
// SEPARATE named interface (not an alias) purely so cmd/daemon/main.go's
// wiring code reads against platform's own public vocabulary rather than
// reaching into platform/internal/link (which it cannot import); Go's
// structural interface typing makes the two directly interchangeable at
// Run's Dialer.SetLocalFileOpener call site with no adapter needed.
type LocalFileOpener interface {
	OpenRead(coord string) (io.ReadSeekCloser, error)
	OpenWrite(coord string) (accessdoor.LocalWriteHandle, error)
	// OpenDir opens coord as a directory-shaped resource's SUBTREE lease (期11
	// 丁12) — an os.Root confined to live/<coord>, surfaced behind
	// accessdoor.LocalDirHandle (the os.Root TYPE stays inside cmd/daemon per
	// the server-zero-storage archtest; this interface names only its method
	// set). Redeemed for Open(dir资源) on the same-daemon Local route only.
	OpenDir(coord string) (accessdoor.LocalDirHandle, error)
	// ReclaimCoord mirrors platform/internal/link.LocalFileOpener's own
	// ReclaimCoord (期11 S2's "非-land 终态回收") — see its doc.
	ReclaimCoord(coord string) error
}

// StorageResourceCoord / StorageReservationCoord / StorageTombstoneCoord are
// StorageHost.Reconcile's injection-point shapes — plain data, deliberately
// NOT aliases of platform/internal/link's own wire types: the implementor
// (cmd/daemon/internal/storagehost.Host) lives OUTSIDE platform/internal's
// Go-enforced visibility boundary and cannot reference those types even by
// alias-name. This mirrors the CONCEPTUAL layering resourcespec/store and
// accessdoor/resourcespec already draw — a fresh mirror type at a boundary a
// downstream package cannot import across, translated by the ONE adapter
// that can see both sides (storageHostForwarder below, for this boundary;
// StorageHost.Alloc's own two arguments are plain string/bool, needing no
// mirror struct at all).
type (
	StorageResourceCoord    struct{ Coord string }
	StorageReservationCoord struct{ ReservationID, Coord string }
	StorageTombstoneCoord   struct{ TombstoneID, Coord string }
)

// StorageReclaimAckFunc is Reconcile's network callback — Run's
// bridge (storageHostForwarder) supplies a closure bound to whichever
// *link.Dialer is CURRENTLY connected; the StorageHost implementor never
// holds a live connection reference itself.
type StorageReclaimAckFunc func(ctx context.Context, tombstoneID string) (found bool, err error)

// StorageHost is the daemon storage host's injection-point contract (期11
// §4): implemented by cmd/daemon/internal/storagehost.Host (via a thin
// cmd/daemon-side adapter — Host's own method shapes already match this
// exactly, see its doc). Every method uses only the plain types above, never
// platform/internal/link's wire types, because the implementor cannot
// import that package (outside its Go-enforced internal/ visibility).
type StorageHost interface {
	// Alloc performs the mkdir/touch for one AllocRequest.
	Alloc(coord string, dir bool) error
	// Reconcile runs one Scrubber pass against the home's ReconcilePullReply
	// (already translated to plain types by storageHostForwarder).
	Reconcile(ctx context.Context, resources []StorageResourceCoord, pendingReservations []StorageReservationCoord, pendingTombstones []StorageTombstoneCoord, ack StorageReclaimAckFunc)
	// ActiveWriteCoords snapshots every coord this daemon currently has an
	// OPEN local WriteHandle for (期11 review's own narrowing addition,
	// cmd/daemon/internal/storagehost.Host.ActiveWriteCoords's plain-typed
	// mirror) — storageHostForwarder.pass reads this BEFORE every
	// ReconcilePull round trip and forwards it as link.ReconcilePull.
	// ActiveCoords, so the home's liveness touch bumps ONLY reservations
	// this daemon can actually prove are still being written, never every
	// reservation it happens to still own.
	ActiveWriteCoords() []string
}
