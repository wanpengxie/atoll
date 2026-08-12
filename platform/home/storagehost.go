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
// per-channel daemon lane and completes already-authorized outbox
// transactions (via the Platform-owned resource outbox).

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/dataplane"
	"github.com/wanpengxie/atoll/protocol/access"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

type resourceOutbox struct {
	resourcespec.ResourceOutbox
	completion accessdoor.ResourceCompletion
}

func (o resourceOutbox) CommitReservation(
	ctx context.Context,
	id string,
) (resourcespec.LandedResource, bool, error) {
	return o.completion.CommitReservation(ctx, id)
}

type daemonStorageMounts struct {
	routes    platform.DaemonRoutes
	bindings  BindingReader
	directory DeviceDirectory
	chID      channelpkg.ID
}

func (m daemonStorageMounts) ResolveStorageDaemon(ctx context.Context, ch channelpkg.ID, name string) (accessdoor.StorageMount, bool, error) {
	if m.routes == nil || m.bindings == nil || m.directory == nil {
		return accessdoor.StorageMount{}, false, nil
	}
	id, present, found, err := m.directory.ResolveDeviceName(ctx, name)
	if err != nil || !found || !present {
		return accessdoor.StorageMount{}, false, err
	}
	bound, err := m.bindings.IsBound(ctx, ch, id)
	if err != nil || !bound {
		return accessdoor.StorageMount{}, false, err
	}
	return accessdoor.StorageMount{DaemonID: id, Name: name, Online: m.routes.LaneAttached(id, string(ch))}, true, nil
}

// daemonStorageControl routes AllocRequest through the space-owned daemon
// carrier and this Home's lane, then waits for its exact-lane reply.
type daemonStorageControl struct {
	routes platform.DaemonRoutes
	chID   channelpkg.ID
}

func (c daemonStorageControl) AllocRequest(ctx context.Context, daemonID string, spec accessdoor.StorageAllocSpec) error {
	if c.routes == nil {
		return errors.New("platform: daemon routes unavailable")
	}
	return asDoorStorageError(c.routes.SendAlloc(ctx, daemonID, string(c.chID), spec.Coord, spec.Dir))
}

// asDoorStorageError translates the transport's not-attempted answer into the
// door's own vocabulary. The door cannot recognise platform.ErrDaemonNotReady
// itself — runtime does not import platform — so this assembly seam is the
// only place the two names can meet.
func asDoorStorageError(err error) error {
	if errors.Is(err, platform.ErrDaemonNotReady) {
		return fmt.Errorf("%w: %v", accessdoor.ErrStorageNotReady, err)
	}
	return err
}

// ReclaimRequest implements accessdoor.StorageControl's 期11 review §2.5 #B
// arm: routes the content-less create loser's coord reclaim through the same
// exact per-channel lane, the delete mirror of AllocRequest above.
func (c daemonStorageControl) ReclaimRequest(ctx context.Context, daemonID string, coord string) error {
	if c.routes == nil {
		return errors.New("platform: daemon routes unavailable")
	}
	return asDoorStorageError(c.routes.SendReclaim(ctx, daemonID, string(c.chID), coord))
}

// daemonTransferControl mints a transfer ticket in the space daemon host.
type daemonTransferControl struct {
	issuer dataplane.Issuer
	chID   channelpkg.ID
}

func (c daemonTransferControl) IssueTransfer(ctx context.Context, resourceID resource.ResourceID, targetDaemonID, targetDaemonName, callerDaemonID, coord string, mode access.Operation, reservationID string, dir bool) (string, accessdoor.FileRedeem, error) {
	if c.issuer == nil {
		return "", "", errors.New("platform: dataplane issuer unavailable")
	}
	grant, err := c.issuer.Issue(ctx, dataplane.IssueSpec{
		ResourceID: resourceID, ChannelID: c.chID, Mode: mode,
		HostID: targetDaemonID, HostName: targetDaemonName, CallerHostID: callerDaemonID,
		Coord: coord, Dir: dir, ReservationID: reservationID,
	})
	if err != nil {
		var offline *dataplane.HostOfflineError
		if errors.As(err, &offline) {
			return "", "", accessdoor.NewHostOfflineError(offline.Host)
		}
		return "", "", err
	}
	redeem := accessdoor.FileRedeemRemote
	if grant.Route == dataplane.RouteLocal {
		redeem = accessdoor.FileRedeemLocal
	}
	return grant.Ticket, redeem, nil
}

// homeStorageHostControl implements link.StorageHostControl over
// the Platform-owned resourcespec.ResourceOutbox — the home-side
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

func (h homeStorageHostControl) Committed(ctx context.Context, senderDaemonID string, expected platform.StorageCommitExpectation) (found, lost bool, err error) {
	if expected.ReservationID == "" || expected.ResourceID == "" || expected.DaemonID == "" || expected.Coord == "" {
		return false, false, errors.New("platform: incomplete commit expectation")
	}
	if expected.DaemonID != senderDaemonID {
		return false, false, fmt.Errorf("%w: ticket belongs to %q, sender is %q", errSenderDaemonMismatch, expected.DaemonID, senderDaemonID)
	}
	placementDaemonID, exists, err := h.outbox.ReservationDaemon(ctx, expected.ReservationID)
	if err != nil {
		return false, false, err
	}
	if !exists {
		meta, landed, rerr := h.outbox.Resolve(ctx, resource.ResourceID(expected.ResourceID))
		if rerr != nil {
			return false, false, rerr
		}
		if landed && meta.PlacementDaemonID == expected.DaemonID && meta.PlacementCoord == expected.Coord {
			return true, false, nil
		}
		return false, false, errors.New("platform: create reservation missing and landed resource identity does not match ticket")
	}
	if placementDaemonID != senderDaemonID {
		return false, false, fmt.Errorf("%w: reservation %q belongs to %q, sender is %q", errSenderDaemonMismatch, expected.ReservationID, placementDaemonID, senderDaemonID)
	}
	_, found, cerr := h.outbox.CommitReservation(ctx, expected.ReservationID)
	if cerr != nil {
		if errors.Is(cerr, accessdoor.ErrReservationLost) {
			return found, true, nil
		}
		return false, false, cerr
	}
	if found {
		return true, false, nil
	}
	// The reservation can disappear after ReservationDaemon and before the
	// transactional commit lookup. Recover the expected identity from the
	// original ticket (the caller supplied expected) and accept only an exact
	// replay of the resource that this reservation was meant to land.
	meta, landed, rerr := h.outbox.Resolve(ctx, resource.ResourceID(expected.ResourceID))
	if rerr != nil {
		return false, false, rerr
	}
	if landed && meta.PlacementDaemonID == expected.DaemonID && meta.PlacementCoord == expected.Coord {
		return true, false, nil
	}
	return false, false, errors.New("platform: create reservation disappeared before commit and landed resource identity does not match ticket")
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

func (h homeStorageHostControl) ReconcilePull(ctx context.Context, senderDaemonID string, activeCoords []string) ([]platform.StorageResourceCoord, []platform.StorageReservationCoord, []platform.StorageTombstoneCoord, error) {
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
	// proved ITS OWN currently-open WriteHandles (drivers/devicehost/internal/
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

	resources := make([]platform.StorageResourceCoord, 0, len(rows))
	for _, row := range rows {
		resources = append(resources, platform.StorageResourceCoord{Coord: row.Meta.PlacementCoord})
	}
	pendingReservations := make([]platform.StorageReservationCoord, 0, len(reservations))
	for _, r := range reservations {
		pendingReservations = append(pendingReservations, platform.StorageReservationCoord{ReservationID: r.ReservationID, Coord: r.PlacementCoord})
	}
	pendingTombstones := make([]platform.StorageTombstoneCoord, 0, len(tombstones))
	for _, t := range tombstones {
		pendingTombstones = append(pendingTombstones, platform.StorageTombstoneCoord{TombstoneID: t.TombstoneID, Coord: t.PlacementCoord})
	}
	return resources, pendingReservations, pendingTombstones, nil
}

var (
	_ accessdoor.StorageMounts        = daemonStorageMounts{}
	_ accessdoor.StorageControl       = daemonStorageControl{}
	_ accessdoor.TransferControl      = daemonTransferControl{}
	_ platform.DaemonStorageAuthority = homeStorageHostControl{}
)
