package storagehost

import (
	"context"
	"log/slog"
	"os"
	"sync"
)

// Host ties the four storage components into one channel-scoped resource set.
// cmd/daemon constructs one Host per compute compartment, rooted beneath the
// authenticated daemon root.
type Host struct {
	cr        *channelRoot
	allocator Allocator
	streamer  Streamer
	scrubber  *Scrubber

	// activeWritesMu / activeWrites back the in-flight "在途 staging 登记"
	// (期11 S1 #6): a coord's refcount of currently-open local WriteHandles.
	// In-memory only — the plain-OpWrite twin of the server-side reservation
	// row's durability, deliberately weak/host-scoped (transfer-lifecycle-
	// spec.md §2: "plain write(改既有文件)：轻量 staging 登记(可内存/轻量行，
	// 同形状)... 弱(随宿主)"). A refcount (not a bool/set) because the
	// streamer already hands out a fresh, independently-committable staging
	// file per concurrent OpenWrite on the SAME coord (streamer.go's own
	// "concurrent writes to the same coord each get their own staging
	// file") — the coord must stay "active" until every one of them closes.
	// Known debt: a WriteHandle never Closed pins its refcount — and thus
	// its staging entry's sweep protection — until process exit. Bounded
	// by actor count; the proper fix is tying handle lifetime to the
	// owning actor's incarnation death, not a patch here.
	activeWritesMu sync.Mutex
	activeWrites   map[string]int
}

// Open opens (creating if necessary) this channel's resource root under
// workspaceRoot and returns a ready Host. Call once per daemon process
// (mirrors compute.Run's HostSupervisor — built once, outlives any
// single link connection/reconnect).
func Open(workspaceRoot, channelID string, logger *slog.Logger) (*Host, error) {
	cr, err := openChannelRoot(workspaceRoot, channelID)
	if err != nil {
		return nil, err
	}
	return &Host{
		cr:           cr,
		scrubber:     &Scrubber{Logger: logger},
		activeWrites: make(map[string]int),
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
// as compute.LocalFileOpener, which platform/internal/link's lane.go
// consults for both the same-daemon Local route and this daemon acting as a
// lane transfer's target.
func (h *Host) OpenRead(coord string) (*ReadHandle, error) { return h.streamer.OpenRead(h.cr, coord) }

// OpenWrite mints the local staging handle AND registers coord as an active
// in-flight write (期11 S1 #6) for the duration of the handle: every
// OpenWrite call — a plain OpWrite equally with a reservation-carrying
// with-content create's local write, this method draws no distinction, §3.5
// confirms a plain write never touches the create-outbox either way — is
// this Scrubber's ONLY evidence that a given staging entry is still being
// written, so the registration must bracket the handle's ENTIRE open window,
// not just this call.
func (h *Host) OpenWrite(coord string) (*WriteHandle, error) {
	// 期11 review P1-1: register the coord active BEFORE the staging file
	// exists. The Scrubber decides orphan-vs-in-flight by whether a staging
	// entry's coord is registered active; if the file could exist before its
	// registration, a concurrent sweep would delete a live writer's staging out
	// from under it. Registering first guarantees any staging file a sweep can
	// observe already has its coord marked active. Roll back on staging failure.
	h.markWriteActive(coord)
	wh, err := h.streamer.OpenWrite(h.cr, coord)
	if err != nil {
		h.markWriteDone(coord)
		return nil, err
	}
	wh.onDone = func() { h.markWriteDone(coord) }
	return wh, nil
}

func (h *Host) markWriteActive(coord string) {
	h.activeWritesMu.Lock()
	defer h.activeWritesMu.Unlock()
	if h.activeWrites == nil {
		h.activeWrites = make(map[string]int)
	}
	h.activeWrites[coord]++
}

func (h *Host) markWriteDone(coord string) {
	h.activeWritesMu.Lock()
	defer h.activeWritesMu.Unlock()
	if n := h.activeWrites[coord]; n > 1 {
		h.activeWrites[coord] = n - 1
	} else {
		delete(h.activeWrites, coord)
	}
}

// ActiveWriteCoords snapshots every coord with at least one currently-open
// local WriteHandle — Reconcile's own feed into the Scrubber's
// sweepOrphanStaging (期11 S1 #6), AND (期11 review's own narrowing
// addition) the daemon-side source compute.storageHostForwarder.pass reads
// before every ReconcilePull round trip, to tell the home which reservations
// are actually alive (see platform/storagehost.go's ReconcilePull doc — a
// daemon merely staying online/polling is NOT liveness for a coord it has
// abandoned). Exported for that cross-package (cmd/daemon's
// storageHostAdapter) read; a snapshot, not a live view — the Scrubber pass
// this backs, like the ReconcilePull round trip, is a single point-in-time
// cycle, matching every other Reconcile input (resources/
// pendingReservations/pendingTombstones are equally point-in-time server
// snapshots).
func (h *Host) ActiveWriteCoords() []ActiveStaging {
	h.activeWritesMu.Lock()
	defer h.activeWritesMu.Unlock()
	out := make([]ActiveStaging, 0, len(h.activeWrites))
	for coord := range h.activeWrites {
		out = append(out, ActiveStaging{Coord: coord})
	}
	return out
}

// OpenDir hands out a directory-shaped resource's os.Root subtree lease (期11
// 丁12) — the workspace lease's daemon half.
func (h *Host) OpenDir(coord string) (*os.Root, error) { return h.streamer.OpenDir(h.cr, coord) }

// ReclaimCoord removes coord's LIVE bytes — 期11 S2's daemon-side half of
// "非-land 终态回收" (transfer-lifecycle-spec.md §2/§3's #2): a reservation
// that lost the create race (resourcespec.ErrReservationLost) has already
// rename→landed its bytes at live/<coord> before the home ever gets to say
// so (this daemon fsync+renamed at Commit time, §3.5; the home only decides
// the WINNER after that). Reusing the SAME Reclaimer a tombstone's delete
// already collects through — no new collection mechanism, just a new caller
// of the existing one. Idempotent (Reclaimer.Reclaim's own doc):
// a coord with nothing there is a clean no-op, never an error.
func (h *Host) ReclaimCoord(coord string) error {
	return h.scrubber.Reclaimer.Reclaim(h.cr, coord)
}

// Reconcile runs one Scrubber pass against the home's recovery picture. ack
// confirms a collected tombstone over the currently bound connection.
func (h *Host) Reconcile(ctx context.Context, resources []ResourceLanded, pendingReservations []ReservationPending, pendingTombstones []TombstoneToReclaim, ack ReclaimAckFunc) {
	// Pass the snapshotter, NOT a pre-taken snapshot (期11 review P1-1): Pass runs
	// multi-second network RPCs before its staging sweep, so the sweep must take
	// the active-writes snapshot itself, immediately before it reads staging/.
	h.scrubber.Pass(ctx, h.cr, resources, pendingReservations, pendingTombstones, h.ActiveWriteCoords, ack)
}

// Close releases the resource root's os.Root handle.
func (h *Host) Close() error { return h.cr.Close() }
