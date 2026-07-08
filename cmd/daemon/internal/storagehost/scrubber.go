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

// ReclaimAckFunc is Pass's network callback — RunCompute's bridge supplies a
// closure bound to whichever *link.Dialer is CURRENTLY connected (this
// package never holds a live connection reference itself: unlike
// cellObsForwarder/cellCancelForwarder's Rebind pattern, there is nothing
// here that needs to survive a reconnect mid-call — each Pass is a single,
// short-lived reconcile cycle the platform-side ticker re-issues wholesale
// next tick if this one's connection died).
type ReclaimAckFunc func(ctx context.Context, tombstoneID string) (found bool, err error)

// CommittedResendFunc is resumeLandedReservations' network callback — the
// SAME RunCompute-bridge-supplied-closure shape as ReclaimAckFunc, bound to
// Dialer.SendCommitted (§4.7's Committed RPC, already built/tested — this is
// its first daemon-INITIATED-on-recovery caller, found+built during 期11 S6
// walk verification; see resumeLandedReservations' own doc for why it did
// not already exist).
type CommittedResendFunc func(ctx context.Context, reservationID string) (found, lost bool, err error)

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
func (s *Scrubber) Pass(ctx context.Context, cr *channelRoot, resources []ResourceLanded, pendingReservations []ReservationPending, pendingTombstones []TombstoneToReclaim, ack ReclaimAckFunc, resend CommittedResendFunc) {
	s.reclaimPendingTombstones(ctx, cr, pendingTombstones, ack)
	s.resumeLandedReservations(ctx, cr, pendingReservations, resend)
	s.sweepOrphanStaging(cr, pendingReservations)
	s.logMissingLiveEntries(cr, resources)
}

// resumeLandedReservations resends Committed(reservationID) for any pending
// reservation whose bytes are ALREADY confirmed present at their live coord
// — §1.7/§6.3's "另一路": daemon rename后Committed未达即daemon crash→重起
//对账(ReconcilePull)→server见reservation挂起→daemon续发Committed. Found+
// built during 期11 S6's platform-level crash-recovery walk verification:
// §4.1's Scrubber doc named "startup scrub + periodic reconcile" as the
// daemon's ENTIRE no-truth recovery mechanism, but S4's original Pass only
// ever swept orphan STAGING entries and reclaimed tombstones — nothing
// resent Committed for a reservation whose write had already fully landed
// locally (fsync+rename done) before the daemon died. Without this, such a
// reservation would sit in resource_reservations forever (server-side, past
// §1.7's own timeout sweep — a genuine "abandoned, but the bytes are
// actually fine" case the timeout sweep is not meant to reclaim as an
// orphan). A pending reservation whose live coord does NOT yet exist is
// deliberately left untouched — sweepOrphanStaging's own conservative
// judgment call ("still legitimately in-flight or genuinely orphaned")
// covers that case; this only ever resumes the ALREADY-COMPLETE case.
func (s *Scrubber) resumeLandedReservations(ctx context.Context, cr *channelRoot, pending []ReservationPending, resend CommittedResendFunc) {
	if resend == nil {
		return
	}
	for _, p := range pending {
		lp, err := livePath(p.Coord)
		if err != nil {
			continue // a malformed coord would already have failed elsewhere; defensive skip
		}
		if _, err := cr.root.Stat(lp); err != nil {
			continue // not yet landed locally — nothing to resend for
		}
		if _, _, err := resend(ctx, p.ReservationID); err != nil {
			s.logger().Warn("storagehost.scrubber.resend_committed_failed", "reservation", p.ReservationID, "coord", p.Coord, "err", err)
		}
	}
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
// reservation — a crash-abandoned write, a losing race's leftover (§1.7's
// "败者reservation+字节归Scrubber清"), or a completed-and-committed write's
// staging file the daemon somehow failed to clean at Commit time. An entry
// whose coord IS still pending is left untouched (conservative: it may be a
// legitimately in-flight write this very moment — §4.1's own "该续传还是当
// 孤儿清" judgment call, resolved toward safety day-1).
func (s *Scrubber) sweepOrphanStaging(cr *channelRoot, pending []ReservationPending) {
	entries, err := fs.ReadDir(cr.root.FS(), stagingDir)
	if err != nil {
		s.logger().Warn("storagehost.scrubber.read_staging_failed", "err", err)
		return
	}
	for _, entry := range entries {
		name := entry.Name()
		if stagingEntryIsPending(name, pending) {
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
