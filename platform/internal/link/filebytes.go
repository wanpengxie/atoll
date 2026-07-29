package link

import (
	"fmt"
	"io"

	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// filebytes.go is the daemon-side half of §5's file byte access. There is no
// byte transport here and no byte transport anywhere in this package: byte
// access is same-daemon only (the door refuses any other caller before a route
// is minted — accessdoor.door.resolveFileRoute), so a redemption is one small
// ResolveCoord control round trip followed by a LOCAL open. The bytes never
// leave the machine they live on.

// LocalFileOpener is the daemon-side same-machine byte-access capability
// (期11 spec §3.4's "daemon 本地颁 os.Root 子句柄") — the injection-point
// contract implemented (via a platform-layer bridge mirroring StorageHost)
// by cmd/daemon/internal/storagehost.Host, consulted from the one call site
// that redeems a file route (remoteResourceHandle.Redeem → redeemFileRoute,
// dial.go). It resolves coord via the ResolveCoord control-RPC round trip
// first — this interface itself never sees a coord it did not already receive
// from home over that channel (§1.3's "daemon 无 truth": nothing here is
// derived locally).
type LocalFileOpener interface {
	OpenRead(coord string) (io.ReadSeekCloser, error)
	// OpenWrite's return type reuses accessdoor.LocalWriteHandle directly —
	// this package already imports accessdoor (relaywire.go's pre-existing
	// edge), so unlike platform/compute.go's StorageHost (whose implementor,
	// cmd/daemon/internal/storagehost, sits OUTSIDE what platform can
	// import, forcing a mirror type) there is no visibility boundary here
	// to mirror across.
	OpenWrite(coord string) (accessdoor.LocalWriteHandle, error)
	// OpenDir is the directory-shaped resource's subtree-lease redemption (期11
	// 丁12): an os.Root confined to live/<coord> behind accessdoor.
	// LocalDirHandle.
	OpenDir(coord string) (accessdoor.LocalDirHandle, error)
	// ReclaimCoord removes coord's already-landed local bytes (期11 S2,
	// transfer-lifecycle-spec.md §2/§3's #2's "非-land 终态回收"):
	// committingWriteHandle's Commit calls this when the home's
	// CommittedReply comes back Lost=true — this daemon's own fsync+rename
	// won LOCALLY (§3.5) but lost the same-resource_id race at the home, so
	// its bytes at coord are now orphaned and must be collected, never
	// retried. Idempotent — a coord with nothing there is a clean no-op
	// (cmd/daemon/internal/storagehost.Host.ReclaimCoord's own doc, which
	// reuses the SAME Reclaimer a tombstone's delete already collects
	// through).
	ReclaimCoord(coord string) error
}

func fileRouteErr(reason string, args ...any) error {
	return fmt.Errorf("link: file route: "+reason, args...)
}
