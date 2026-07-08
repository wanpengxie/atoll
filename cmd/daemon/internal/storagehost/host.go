package storagehost

import (
	"context"
	"log/slog"
	"os"
)

// Host ties the four §4.1 components into the one object cmd/daemon/main.go
// constructs and injects into platform.ComputeConfig.StorageHost (via a
// small implements-the-interface adapter cmd/daemon/main.go itself writes —
// Host's own method shapes are already exactly what that interface needs,
// see its doc for why the interface lives in plain-typed form). One Host per
// daemon process, scoped to the ONE channel RunCompute connects to (a daemon
// hosts exactly one channel's assignment, cmd/daemon/main.go's own doc).
type Host struct {
	cr        *channelRoot
	allocator Allocator
	streamer  Streamer
	scrubber  *Scrubber
}

// Open opens (creating if necessary) this channel's resource root under
// workspaceRoot and returns a ready Host. Call once per daemon process
// (mirrors RunCompute's own actorrt.Runtime — built once, outlives any
// single link connection/reconnect).
func Open(workspaceRoot, channelID string, logger *slog.Logger) (*Host, error) {
	cr, err := openChannelRoot(workspaceRoot, channelID)
	if err != nil {
		return nil, err
	}
	return &Host{
		cr:       cr,
		scrubber: &Scrubber{Logger: logger},
	}, nil
}

// Alloc performs the mkdir/touch for one AllocRequest (§4.7's first frame,
// answered synchronously — a real Allocator op against the local
// filesystem, expected fast).
func (h *Host) Alloc(coord string, dir bool) error {
	return h.allocator.Alloc(h.cr, coord, dir)
}

// OpenRead / OpenWrite hand out this channel's local Streamer handles —
// §3.4's "daemon 本地颁 os.Root 子句柄给 caller" for a same-machine consumer.
// §5's lane has since landed: cmd/daemon's storageadapter.go wraps this Host
// as platform.LocalFileOpener, which platform/internal/link's lane.go
// consults for both the same-daemon Local route and this daemon acting as a
// lane transfer's target.
func (h *Host) OpenRead(coord string) (*ReadHandle, error) { return h.streamer.OpenRead(h.cr, coord) }
func (h *Host) OpenWrite(coord string) (*WriteHandle, error) {
	return h.streamer.OpenWrite(h.cr, coord)
}

// OpenDir hands out a directory-shaped resource's os.Root subtree lease (期11
// 丁12) — the workspace lease's daemon half.
func (h *Host) OpenDir(coord string) (*os.Root, error) { return h.streamer.OpenDir(h.cr, coord) }

// Reconcile runs one Scrubber pass against the home's recovery picture. ack
// is the network callback for confirming a collected tombstone; resend is
// the network callback for resuming a landed-but-uncommitted reservation
// (§1.7's daemon-crash recovery path). Both are RunCompute-bridge-supplied,
// bound to whichever connection is currently live.
func (h *Host) Reconcile(ctx context.Context, resources []ResourceLanded, pendingReservations []ReservationPending, pendingTombstones []TombstoneToReclaim, ack ReclaimAckFunc, resend CommittedResendFunc) {
	h.scrubber.Pass(ctx, h.cr, resources, pendingReservations, pendingTombstones, ack, resend)
}

// Close releases the resource root's os.Root handle.
func (h *Host) Close() error { return h.cr.Close() }
