package storagehost

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"sync"
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
	activeWritesMu sync.Mutex
	activeWrites   map[string]int
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
// as platform.LocalFileOpener, which platform/internal/link's lane.go
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
// addition) the daemon-side source platform.storageHostForwarder.pass reads
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

// LandedCoords lists every coord currently present in this channel's live/
// directory (期11 review §2.5 #A) — the daemon-side source
// platform.storageHostForwarder.pass forwards as link.ReconcilePull.
// LandedCoords, from which the home flips matching pending reservations to
// phase='landed' before its age-sweep. Read straight from disk (not any
// in-memory registry), so it is authoritative across a crash/restart with no
// daemon-side truth — the very first ReconcilePull after a long gap already
// reports every already-landed coord, before the home's sweep can fire.
//
// A read error is returned, NEVER papered over as an empty snapshot (期11
// review残余#1): an empty []string here is indistinguishable on the wire from
// "genuinely nothing landed" — the home's ReconcilePull would mark NO
// reservation landed, its very next SweepExpiredReservations (same tick, same
// pull) would then sweep an already-landed-but-uncommitted reservation as
// abandoned, and the "retry next tick" this method's caller advertises never
// actually happens because the caller does not skip the pull on a fabricated
// empty answer. Returning the error lets the caller (storageHostForwarder.
// pass) skip sending this tick's ReconcilePull altogether, so the NEXT tick's
// retry is real. Every live/ entry is a coord (flat directory, root.go's own
// layout invariant), so the entry name IS the coord verbatim.
func (h *Host) LandedCoords() ([]string, error) {
	entries, err := fs.ReadDir(h.cr.root.FS(), liveDir)
	if err != nil {
		return nil, fmt.Errorf("storagehost: list live/ for landed coords: %w", err)
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out, nil
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
// already collects through (content-bearing create's placement is always
// axis-allocated, never registered) — no new collection mechanism, just a
// new caller of the existing one. Idempotent (Reclaimer.Reclaim's own doc):
// a coord with nothing there is a clean no-op, never an error.
func (h *Host) ReclaimCoord(coord string) error {
	return h.scrubber.Reclaimer.Reclaim(h.cr, coord, provenanceAxisAllocated)
}

// Reconcile runs one Scrubber pass against the home's recovery picture. ack
// is the network callback for confirming a collected tombstone; resend is
// the network callback for resuming a landed-but-uncommitted reservation
// (§1.7's daemon-crash recovery path). Both are RunCompute-bridge-supplied,
// bound to whichever connection is currently live.
func (h *Host) Reconcile(ctx context.Context, resources []ResourceLanded, pendingReservations []ReservationPending, pendingTombstones []TombstoneToReclaim, ack ReclaimAckFunc, resend CommittedResendFunc) {
	// Pass the snapshotter, NOT a pre-taken snapshot (期11 review P1-1): Pass runs
	// multi-second network RPCs before its staging sweep, so the sweep must take
	// the active-writes snapshot itself, immediately before it reads staging/.
	h.scrubber.Pass(ctx, h.cr, resources, pendingReservations, pendingTombstones, h.ActiveWriteCoords, ack, resend)
}

// Close releases the resource root's os.Root handle.
func (h *Host) Close() error { return h.cr.Close() }
