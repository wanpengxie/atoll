package link

import (
	"fmt"
	"io"

	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// filebytes.go defines the daemon-local storage capability used by both local
// redemption and a cross-device exchange's host leg.

// LocalFileOpener is the daemon-side same-machine byte-access capability
// (期11 spec §3.4's "daemon 本地颁 os.Root 子句柄") — the injection-point
// contract implemented (via a platform-layer bridge mirroring StorageHost)
// by drivers/devicehost/internal/storagehost.Host, consulted from the one call site
// that redeems a file route (remoteResourceHandle.Redeem → redeemFileRoute,
// dial.go). Paths are logical names relative to the channel directory.
type LocalFileOpener interface {
	OpenRead(path string) (io.ReadSeekCloser, error)
	// OpenWrite's return type reuses accessdoor.WriteHandle directly —
	// this package already imports accessdoor (relaywire.go's pre-existing
	// edge), so unlike platform/compute.go's StorageHost (whose implementor,
	// drivers/devicehost/internal/storagehost, sits OUTSIDE what platform can
	// import, forcing a mirror type) there is no visibility boundary here
	// to mirror across.
	OpenWrite(path string) (accessdoor.WriteHandle, error)
}

func fileRouteErr(reason string, args ...any) error {
	return fmt.Errorf("link: file route: "+reason, args...)
}
