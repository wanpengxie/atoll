package storagehost

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"strings"
)

// ResourceLanded / ReservationPending / TombstoneToReclaim are Pass's inputs
// — plain mirrors of platform/internal/link's ReconcileResource/
// ReconcileReservation/ReconcileTombstone wire shapes. This package cannot
// import platform/internal/link (cmd/daemon sits outside Go's internal/
// visibility boundary for it) — RunCompute's own bridge code
// (cmd/daemon-side platform.RunCompute, which CAN see both type universes)
// does the field-by-field translation at the one seam that needs it, the
// SAME boundary-crossing shape resourcespec/store and accessdoor/resourcespec
// already draw elsewhere in this build.
type ResourceLanded struct{ Coord string }
type ReservationPending struct{ ReservationID, Coord string }
type TombstoneToReclaim struct{ TombstoneID, Coord, Provenance string }

// ActiveStaging is one coord with a currently-open local WriteHandle (期11 S1
// #6, transfer-lifecycle-spec.md §2's "plain write: 轻量 staging 登记"): the
// plain-OpWrite twin of ReservationPending. A plain write carries NO
// server-side reservation row (door.go's OpRead/OpWrite(file) never touches
// the create-outbox — §3.5's own "无 outbox involvement"), so this is the
// ONLY "still being written" evidence sweepOrphanStaging has for it. Sourced
// from Host's own in-memory registry (Host.ActiveWriteCoords) — host-scoped,
// weak/ephemeral by design (never durable, lost on process death; a
// crash-abandoned plain write's staging file falls back to being swept as an
// ordinary orphan next pass, exactly the pre-S1 behavior — S1 only protects
// a write that is ACTUALLY in flight on a live Host).
type ActiveStaging struct{ Coord string }

// ReclaimAckFunc is Pass's network callback — RunCompute's bridge supplies a
// closure bound to whichever *link.Dialer is CURRENTLY connected (this
// package never holds a live connection reference itself: unlike
// cellObsForwarder/cellCancelForwarder's Rebind pattern, there is nothing
// here that needs to survive a reconnect mid-call — each Pass is a single,
// short-lived reconcile cycle the platform-side ticker re-issues wholesale
// next tick if this one's connection died).
type ReclaimAckFunc func(ctx context.Context, tombstoneID string) (found bool, err error)

// Scrubber is §4.1's fourth component: ONE reconcile pass — reclaim pending
// tombstones (Reclaimer + ack), sweep orphan staging entries a crash left
// behind, log (never auto-repair) a landed resource whose coord is missing
// on disk. WHEN to run a pass (startup + periodic ticker) and HOW to reach
// the home (SendReconcilePull/SendReclaimAck) are platform-side concerns
// (RunCompute's bridge, which alone can hold a *link.Dialer) — this type is
// pure local-filesystem policy, independently testable with no network.
type Scrubber struct {
	Reclaimer Reclaimer
	Logger    *slog.Logger
}

func (s *Scrubber) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

// Pass runs ONE reconcile cycle against the home's recovery picture
// (resources/pendingReservations/pendingTombstones, already filtered to
// THIS daemon server-side, §4.7).
// activeWrites is a snapshotTER (期11 review P1-1), not a pre-taken snapshot:
// this Pass runs multi-second network RPCs (reclaim/resend) BEFORE its staging
// sweep, so the sweep takes the active-writes snapshot itself, immediately
// before reading staging/ — a snapshot taken at Pass entry would be stale by
// the time the sweep runs and could delete a write that began in between. nil
// is allowed (treated as "no active writes").
func (s *Scrubber) Pass(ctx context.Context, cr *channelRoot, resources []ResourceLanded, pendingReservations []ReservationPending, pendingTombstones []TombstoneToReclaim, activeWrites func() []ActiveStaging, ack ReclaimAckFunc) {
	s.reclaimPendingTombstones(ctx, cr, pendingTombstones, ack)
	s.sweepOrphanStaging(cr, pendingReservations, activeWrites)
	s.logMissingLiveEntries(cr, resources)
	s.logOrphanLiveCount(cr, resources, pendingReservations, pendingTombstones)
}

func (s *Scrubber) logOrphanLiveCount(cr *channelRoot, resources []ResourceLanded, reservations []ReservationPending, tombstones []TombstoneToReclaim) {
	accounted := make(map[string]bool, len(resources)+len(reservations)+len(tombstones))
	for _, r := range resources {
		accounted[r.Coord] = true
	}
	for _, r := range reservations {
		accounted[r.Coord] = true
	}
	for _, r := range tombstones {
		accounted[r.Coord] = true
	}
	entries, err := fs.ReadDir(cr.root.FS(), liveDir)
	if err != nil {
		s.logger().Warn("storagehost.scrubber.orphan_live_count_failed", "err", err)
		return
	}
	count := 0
	for _, e := range entries {
		if !accounted[e.Name()] {
			count++
		}
	}
	s.logger().Info("storagehost.scrubber.orphan_live_count", "count", count)
}

// reclaimPendingTombstones collects each tombstone's bytes (Reclaimer) then
// confirms via ack (§4.7's third frame, closing delete's outbox). A Reclaim
// failure is logged and left for the NEXT pass to retry (the tombstone
// stays in the home's pending set until a ReclaimAck actually lands) —
// never a partial ack.
func (s *Scrubber) reclaimPendingTombstones(ctx context.Context, cr *channelRoot, pending []TombstoneToReclaim, ack ReclaimAckFunc) {
	for _, ts := range pending {
		if err := s.Reclaimer.Reclaim(cr, ts.Coord, ts.Provenance); err != nil {
			s.logger().Warn("storagehost.scrubber.reclaim_failed", "tombstone", ts.TombstoneID, "coord", ts.Coord, "err", err)
			continue
		}
		if ack == nil {
			continue
		}
		if _, err := ack(ctx, ts.TombstoneID); err != nil {
			s.logger().Warn("storagehost.scrubber.reclaim_ack_failed", "tombstone", ts.TombstoneID, "err", err)
		}
	}
}

// sweepOrphanStaging removes staging/ entries with NO matching pending
// reservation AND no matching active local write — a crash-abandoned write, a
// losing race's leftover (§1.7's "败者reservation+字节归Scrubber清"), or a
// completed-and-committed write's staging file the daemon somehow failed to
// clean at Commit time. An entry whose coord IS still pending, or whose coord
// has a currently-open Host WriteHandle (期11 S1 #6 — a plain OpWrite carries
// no reservation at all, so activeWrites is the ONLY signal protecting it),
// is left untouched (conservative: it may be a legitimately in-flight write
// this very moment — §4.1's own "该续传还是当孤儿清" judgment call, resolved
// toward safety day-1).
func (s *Scrubber) sweepOrphanStaging(cr *channelRoot, pending []ReservationPending, activeWrites func() []ActiveStaging) {
	entries, err := fs.ReadDir(cr.root.FS(), stagingDir)
	if err != nil {
		s.logger().Warn("storagehost.scrubber.read_staging_failed", "err", err)
		return
	}
	// 期11 review P1-1: snapshot active writes AFTER ReadDir, not at Pass entry
	// (before the multi-second network RPCs). OpenWrite marks a coord active
	// BEFORE creating its staging file, so any entry ReadDir just observed
	// already has its coord registered — a snapshot taken now cannot miss a
	// concurrently-started live write and sweep its staging out from under it.
	var active []ActiveStaging
	if activeWrites != nil {
		active = activeWrites()
	}
	for _, entry := range entries {
		name := entry.Name()
		if stagingEntryIsPending(name, pending) || stagingEntryIsActive(name, active) {
			continue
		}
		rel := stagingDir + "/" + name
		if err := cr.root.RemoveAll(rel); err != nil && !errors.Is(err, fs.ErrNotExist) {
			s.logger().Warn("storagehost.scrubber.sweep_orphan_failed", "entry", name, "err", err)
		}
	}
}

// stagingEntryIsPending reports whether name (a staging/ directory entry,
// "<coord>-<suffix>") belongs to one of pending's coords — a prefix match
// against "<coord>-" rather than parsing name apart, since coord's own
// charset (assertPathSegment) forbids '-' ambiguity at the boundary this
// check needs.
func stagingEntryIsPending(name string, pending []ReservationPending) bool {
	for _, p := range pending {
		if strings.HasPrefix(name, p.Coord+"-") {
			return true
		}
	}
	return false
}

// stagingEntryIsActive is stagingEntryIsPending's plain-OpWrite twin (期11 S1
// #6): the SAME "<coord>-" prefix match, against Host's in-memory active-
// write snapshot instead of the server's reservation rows.
func stagingEntryIsActive(name string, activeWrites []ActiveStaging) bool {
	for _, a := range activeWrites {
		if strings.HasPrefix(name, a.Coord+"-") {
			return true
		}
	}
	return false
}

// logMissingLiveEntries is a read-only anomaly report: a resource the home
// believes is landed and placed on THIS daemon, but whose coord is absent
// from live/ — a lost-byte condition this section does not auto-repair
// (§6.5's own account: day-1 file has no HA, the honest failure mode is a
// loud log, never a silent gap or a fabricated recovery).
func (s *Scrubber) logMissingLiveEntries(cr *channelRoot, resources []ResourceLanded) {
	for _, r := range resources {
		p, err := livePath(r.Coord)
		if err != nil {
			continue // a malformed coord would already have failed elsewhere; defensive skip
		}
		if _, err := cr.root.Stat(p); err != nil {
			s.logger().Error("storagehost.scrubber.lost_bytes", "coord", r.Coord, "err", fmt.Sprintf("%v", err))
		}
	}
}
