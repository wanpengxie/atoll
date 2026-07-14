package home

// storagehost.go is the home-side half of 期11 §4's daemon storage host: it
// fills the two injection-point contracts runtime/accessdoor and
// platform/internal/link DEFINE but never answer themselves —
// accessdoor.StorageMounts / accessdoor.StorageControl (§4.3, the door's
// placement-choice + AllocRequest-issue seam) and link.StorageHostControl
// (§4.7, the daemon-initiated Committed/ReclaimAck/ReconcilePull handler).
// No filesystem code lives here — Allocator/Streamer/Reclaimer/Scrubber stay
// daemon-runtime-only (§8.2's server-zero-storage red line); this file only
// ROUTES already-authorized control-plane decisions to the right live
// connection (via link.Acceptor) and completes already-authorized outbox
// transactions (via runtime.ChannelStores.Outbox).

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/access"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// lateAcceptor is the late-binding seam §4.3's own text names ("若装配时机
// 上门先于acceptor建，注入一个late-bound的StorageMounts"): Home.Open's step 2
// (runtime.OpenChannel, which needs StorageMounts/StorageControl already in
// hand) runs BEFORE step 11 (link.NewAcceptor, the only thing that can
// actually answer them — attach state lives on the Acceptor). A single
// atomic pointer, set once by bindLateAcceptor after the Acceptor exists,
// backs both lateStorageMounts and lateStorageControl below — every call
// before that point sees a nil Acceptor and answers honestly (empty mount
// list / "not wired yet" error) rather than blocking or panicking.
type lateAcceptor struct {
	p atomic.Pointer[link.Acceptor]
}

func (l *lateAcceptor) bind(a *link.Acceptor) {
	if !l.p.CompareAndSwap(nil, a) {
		panic("platform: late acceptor bound twice")
	}
}
func (l *lateAcceptor) get() *link.Acceptor { return l.p.Load() }

// lateStorageMounts implements accessdoor.StorageMounts over a lateAcceptor
// — the ONLY data source (期11 spec §4.3): every daemon with a live attach
// on this channel's Acceptor is a storage-mount candidate, Online=true by
// construction (an entry only exists while attached). This intentionally
// never imports app: attach state is a
// platform/link-native fact, not an app daemon_channels projection — day-1's
// policy chain (①③④) needs nothing else (§4.3's ② — the ONLY chain step that
// would need daemon ownership — is deferred whole).
type lateStorageMounts struct{ acc *lateAcceptor }

func (m lateStorageMounts) ListStorageDaemons(ctx context.Context, _ channelpkg.ID) ([]accessdoor.StorageMount, error) {
	a := m.acc.get()
	if a == nil {
		return nil, nil // no candidates before the Acceptor exists — an honest empty list, not an error
	}
	ids := a.AttachedDaemonIDs()
	out := make([]accessdoor.StorageMount, 0, len(ids))
	for _, id := range ids {
		out = append(out, accessdoor.StorageMount{DaemonID: id, Online: true})
	}
	return out, nil
}

// lateStorageControl implements accessdoor.StorageControl over a
// lateAcceptor: AllocRequest routes to the Acceptor's per-daemon live
// connection table and blocks for the correlated AllocReply (§4.7's first
// frame, link.Acceptor.SendAllocRequest).
type lateStorageControl struct{ acc *lateAcceptor }

func (c lateStorageControl) AllocRequest(ctx context.Context, daemonID string, spec accessdoor.StorageAllocSpec) error {
	a := c.acc.get()
	if a == nil {
		return errors.New("platform: storage control not wired yet (Acceptor not built)")
	}
	return a.SendAllocRequest(ctx, daemonID, link.AllocRequest{
		ChannelID: string(spec.ChannelID),
		Coord:     spec.Coord,
		Dir:       spec.Dir,
	})
}

// ReclaimRequest implements accessdoor.StorageControl's 期11 review §2.5 #B
// arm: routes the content-less create loser's coord reclaim to the Acceptor's
// per-daemon live connection (link.Acceptor.SendReclaimRequest), the delete
// mirror of AllocRequest above.
func (c lateStorageControl) ReclaimRequest(ctx context.Context, daemonID string, coord string) error {
	a := c.acc.get()
	if a == nil {
		return errors.New("platform: storage control not wired yet (Acceptor not built)")
	}
	return a.SendReclaimRequest(ctx, daemonID, coord)
}

// lateLaneControl implements accessdoor.LaneControl over a lateAcceptor —
// §5's Token-mint injection point, same late-bound discipline as
// lateStorageMounts/lateStorageControl above (the Acceptor owns the lane
// session/transfer tables, which do not exist until step 11).
type lateLaneControl struct{ acc *lateAcceptor }

func (c lateLaneControl) OpenTransfer(ctx context.Context, targetDaemonID, requesterDaemonID, coord string, mode access.Operation, reservationID string) (string, error) {
	a := c.acc.get()
	if a == nil {
		return "", errors.New("platform: lane control not wired yet (Acceptor not built)")
	}
	return a.OpenLaneTransfer(ctx, targetDaemonID, requesterDaemonID, coord, mode, reservationID)
}

// homeStorageHostControl implements link.StorageHostControl over
// runtime.ChannelStores.Outbox (resourcespec.ResourceOutbox) — the home-side
// answer to the daemon-INITIATED half of §4.7's control-RPC plane
// (Committed/ReclaimAck/ReconcilePull). Every method runs the §4.7
// mechanical sender-auth assertion FIRST (senderDaemonID must equal the
// reservation/tombstone's OWN placement_daemon_id) before touching the
// outbox — "otherwise daemon B could land daemon A's reservation" (§4.7's
// own words).
// defaultReservationTimeout is how long a create-outbox reservation may sit
// unCommitted before the server ages it out as abandoned (期11 spec §1.7's
// third reservation-deletion trigger — "超时未Committed"). This covers the
// case an AllocRequest never reached the daemon at all (lost frame, daemon
// down at issue time): the daemon then has NOTHING to resend Committed for,
// so without this sweep the reservation would sit in resource_reservations
// forever, reappearing in every future ReconcilePull. Generous relative to
// any single AllocRequest/Committed round trip — this is an abandonment
// backstop, not a liveness timeout.
const defaultReservationTimeout = 5 * time.Minute

type homeStorageHostControl struct {
	outbox accessdoor.ResourceOutbox
	// timeout/now are nil-safe-defaulted (zero value → defaultReservationTimeout /
	// time.Now) — tests inject both to make the sweep's boundary deterministic
	// without a real 5-minute wait.
	timeout time.Duration
	now     func() time.Time
	// logger is nil-safe (§0 A3/C12): existing test literals construct this
	// struct without it, and the zero value must never panic — logging is
	// best-effort self-description, not load-bearing behaviour.
	logger *slog.Logger
}

// log returns a non-nil logger (§0 nil-safe rule): callers unconditionally
// call methods on it, so a missing h.logger degrades to a discard sink
// instead of a nil-pointer panic.
func (h homeStorageHostControl) log() *slog.Logger {
	if h.logger != nil {
		return h.logger
	}
	return slog.New(slog.DiscardHandler)
}

func (h homeStorageHostControl) reservationTimeout() time.Duration {
	if h.timeout > 0 {
		return h.timeout
	}
	return defaultReservationTimeout
}

func (h homeStorageHostControl) nowFn() func() time.Time {
	if h.now != nil {
		return h.now
	}
	return time.Now
}

// errSenderDaemonMismatch is returned (never silently authorized) when an
// attach-authenticated sender's daemon id does not match the placement
// daemon id its named reservation/tombstone actually belongs to.
var errSenderDaemonMismatch = errors.New("platform: control RPC sender does not match the placement daemon")

func (h homeStorageHostControl) Committed(ctx context.Context, senderDaemonID, reservationID string) (found, lost bool, err error) {
	placementDaemonID, exists, err := h.outbox.ReservationDaemon(ctx, reservationID)
	if err != nil {
		return false, false, err
	}
	if !exists {
		// Already committed by an earlier replay, lost a race and already
		// cleaned up, or swept by the server's own §1.7 timeout sweep — a
		// clean no-op, level-triggered replay-safety (§4.7's own words).
		return false, false, nil
	}
	if placementDaemonID != senderDaemonID {
		return false, false, fmt.Errorf("%w: reservation %q belongs to %q, sender is %q", errSenderDaemonMismatch, reservationID, placementDaemonID, senderDaemonID)
	}
	landed, cerr := h.outbox.CommitReservation(ctx, reservationID)
	if cerr != nil {
		if errors.Is(cerr, accessdoor.ErrReservationLost) {
			return landed, true, nil
		}
		return false, false, cerr
	}
	return landed, false, nil
}

func (h homeStorageHostControl) ReclaimAck(ctx context.Context, senderDaemonID, tombstoneID string) (bool, error) {
	placementDaemonID, exists, err := h.outbox.TombstoneDaemon(ctx, tombstoneID)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil // already cleared — replay-safe no-op
	}
	if placementDaemonID != senderDaemonID {
		return false, fmt.Errorf("%w: tombstone %q belongs to %q, sender is %q", errSenderDaemonMismatch, tombstoneID, placementDaemonID, senderDaemonID)
	}
	return h.outbox.ClearTombstone(ctx, tombstoneID)
}

func (h homeStorageHostControl) ReconcilePull(ctx context.Context, senderDaemonID string, activeCoords []string) ([]link.ReconcileResource, []link.ReconcileReservation, []link.ReconcileTombstone, error) {
	// No separate sender-auth check here: unlike Committed/ReclaimAck (which
	// name a specific reservation/tombstone id that could belong to ANOTHER
	// daemon), ReconcilePull carries no target id of its own — senderDaemonID
	// IS the query, and every List*ByDaemon call below is already filtered to
	// exactly that id server-side (§4.7: "只返回该 sender 名下的 rows").
	//
	// Sweep BEFORE listing (§1.7's third trigger, level-triggered exactly
	// here — "ReconcilePull 响应时删"): an inactive reservation this daemon
	// owns that has aged past reservationTimeout() never appears in
	// the pendingReservations this call returns, so the daemon never
	// resumes/resends for one the server has already abandoned. Sweep reads the
	// PRE-touch last_progress_at (期11 S1) — a daemon that has gone silent long
	// enough still ages out on schedule, even though every call that DOES
	// arrive touches its own rows immediately below.
	cutoff := h.nowFn()().Add(-h.reservationTimeout()).UnixMilli()
	swept, err := h.outbox.SweepExpiredReservations(ctx, senderDaemonID, cutoff)
	if err != nil {
		return nil, nil, nil, err
	}
	// A3/C12: reservation age-out is the same "unrecoverable judged-dead +
	// reclaim" shape as the schedule engine's timer_dead_evicted, which IS
	// loud — this sibling was silent. edge-only: only fires when the sweep
	// actually evicted something, never on the common empty-sweep tick.
	if len(swept) > 0 {
		ids := make([]string, len(swept))
		for i, row := range swept {
			ids[i] = row.ReservationID
		}
		h.log().Warn("platform.storage.reservation_expired",
			"daemon", senderDaemonID, "count", len(swept), "reservation_ids", ids)
	}

	// Touch AFTER sweep, BEFORE listing (期11 S1's "在途登记" liveness bump —
	// resourcespec.Registry.TouchReservationsByCoords's own doc), NARROWED
	// by 期11 review to the caller-supplied activeCoords: this daemon just
	// proved ITS OWN currently-open WriteHandles (cmd/daemon/internal/
	// storagehost.Host.ActiveWriteCoords, forwarded through the
	// ReconcilePull frame) reachable, so ONLY the reservations whose coord
	// is in that set are stamped alive as of now — not every reservation
	// this daemon happens to still own. A reservation whose coord is
	// missing from activeCoords (AllocRequest never reached the daemon, or
	// the write was abandoned/never opened) gets NOTHING bumped for it,
	// so it ages out on schedule even while the daemon keeps polling on its
	// normal cadence — the bug this narrowing fixes: "daemon online" is not
	// "this reservation is alive".
	if err := h.outbox.TouchReservationsByCoords(ctx, senderDaemonID, activeCoords, h.nowFn()().UnixMilli()); err != nil {
		return nil, nil, nil, err
	}

	rows, err := h.outbox.ListByPlacementDaemon(ctx, senderDaemonID)
	if err != nil {
		return nil, nil, nil, err
	}
	reservations, err := h.outbox.ListReservationsByDaemon(ctx, senderDaemonID)
	if err != nil {
		return nil, nil, nil, err
	}
	tombstones, err := h.outbox.ListTombstonesByDaemon(ctx, senderDaemonID)
	if err != nil {
		return nil, nil, nil, err
	}

	resources := make([]link.ReconcileResource, 0, len(rows))
	for _, row := range rows {
		resources = append(resources, link.ReconcileResource{Coord: row.Meta.PlacementCoord})
	}
	pendingReservations := make([]link.ReconcileReservation, 0, len(reservations))
	for _, r := range reservations {
		pendingReservations = append(pendingReservations, link.ReconcileReservation{ReservationID: r.ReservationID, Coord: r.PlacementCoord})
	}
	pendingTombstones := make([]link.ReconcileTombstone, 0, len(tombstones))
	for _, t := range tombstones {
		pendingTombstones = append(pendingTombstones, link.ReconcileTombstone{TombstoneID: t.TombstoneID, Coord: t.PlacementCoord, Provenance: string(t.Provenance)})
	}
	return resources, pendingReservations, pendingTombstones, nil
}

var (
	_ accessdoor.StorageMounts  = lateStorageMounts{}
	_ accessdoor.StorageControl = lateStorageControl{}
	_ accessdoor.LaneControl    = lateLaneControl{}
	_ link.StorageHostControl   = homeStorageHostControl{}
)
