package link

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/protocol/access"
)

// laneTransferTTL bounds how long a minted ticket lingers. The ticket is
// read-only until it expires, so a target whose ResolveCoord reply was lost can
// retry; the TTL is what finally retires it. Opportunistic mint-time sweeping
// is backed by enforcement at use.
const laneTransferTTL = 10 * time.Minute

// lanecontrol.go is the HOME half of §5's file byte route: the ticket-keyed
// transfer registry (accessdoor.TransferControl's platform-side implementor
// reaches this via Acceptor.OpenTransfer) and the ResolveCoord control-RPC
// frame pair, daemon-initiated, riding the SAME control substream
// storagecontrol.go already extends — §4.7's own "fallback 触发时才落 control"
// wording, chosen here for the SAME reason: a genuinely daemon-local rid→coord
// cache would contradict "daemon 无 truth", so the caller resolves its coord
// through one small metadata round trip on this existing, tested channel.
//
// There is no byte relay here. Byte access is same-daemon only (the door
// refuses any other caller outright, accessdoor.door.resolveFileRoute), so the
// home never stands between two daemons' bytes: it authorizes, and the daemon
// that owns the file opens the handle locally.

// --- ResolveCoord: daemon-initiated control-RPC (mirrors Committed/
//     ReclaimAck/ReconcilePull's own request/response shape) ----------------

// ResolveCoordRequest is daemon→home: "what does Token mean" — sent by
// whichever daemon IS the transfer's target (§5 item 0's single mechanical
// check: only the target daemon may ever resolve a Token into a coord,
// covering both the Local route's direct caller and the Stream route's
// target-side inbound handler with the SAME assertion).
type ResolveCoordRequest struct {
	RequestID string `json:"request_id"`
	Token     string `json:"token"`
}

// ResolveCoordReply is home→daemon: the resolved coord/mode/reservation, or
// an honest reject (unknown/already-redeemed Token, or a sender/target
// mismatch — never silently treated as "no route").
type ResolveCoordReply struct {
	RequestID     string           `json:"request_id"`
	OK            bool             `json:"ok"`
	Coord         string           `json:"coord,omitempty"`
	Mode          access.Operation `json:"mode,omitempty"`
	ReservationID string           `json:"reservation_id,omitempty"`
	Reason        string           `json:"reason,omitempty"`
}

func (m ResolveCoordRequest) validate() error {
	if err := requiredControlField("resolve_coord.request_id", m.RequestID); err != nil {
		return err
	}
	return requiredControlField("resolve_coord.token", m.Token)
}

func (m ResolveCoordReply) validate() error {
	if err := requiredControlField("resolve_coord_reply.request_id", m.RequestID); err != nil {
		return err
	}
	if !m.OK {
		return requiredControlField("resolve_coord_reply.reason", m.Reason)
	}
	if err := requiredControlField("resolve_coord_reply.coord", m.Coord); err != nil {
		return err
	}
	if m.Mode != access.OpRead && m.Mode != access.OpWrite {
		return fmt.Errorf("link: resolve_coord_reply.mode must be read or write")
	}
	return nil
}

const (
	ctrlResolveCoord      controlKind = "resolve_coord"
	ctrlResolveCoordReply controlKind = "resolve_coord_reply"
)

type laneControlFrame struct {
	Kind              controlKind          `json:"kind"`
	ResolveCoord      *ResolveCoordRequest `json:"resolve_coord,omitempty"`
	ResolveCoordReply *ResolveCoordReply   `json:"resolve_coord_reply,omitempty"`
}

func encodeLaneControl(f laneControlFrame) ([]byte, error) { return json.Marshal(f) }

func decodeLaneControl(b []byte) (laneControlFrame, error) {
	var f laneControlFrame
	if err := json.Unmarshal(b, &f); err != nil {
		return laneControlFrame{}, fmt.Errorf("link: decode lane control: %w", err)
	}
	if f.ResolveCoord != nil {
		if err := f.ResolveCoord.validate(); err != nil {
			return laneControlFrame{}, err
		}
	}
	if f.ResolveCoordReply != nil {
		if err := f.ResolveCoordReply.validate(); err != nil {
			return laneControlFrame{}, err
		}
	}
	return f, nil
}

// --- transfer registry -----------------------------------------------------

// laneTransfer is one pending byte-access authorization. Its ticket is
// read-only and retryable by the target until the TTL expires — nothing
// consumes it, because only the target daemon can resolve it and resolving is
// idempotent.
type laneTransfer struct {
	ticket         string
	targetDaemonID string
	coord          string
	mode           access.Operation
	reservationID  string
	// mintedAt stamps OpenTransfer time, read only by
	// sweepExpiredTransfersLocked for the laneTransferTTL GC (期11 review #G).
	mintedAt time.Time
}

// laneState contains transfer capabilities only.
type laneState struct {
	mu          sync.Mutex
	resolutions map[string]laneTransfer
}

func newLaneState() *laneState {
	return &laneState{resolutions: map[string]laneTransfer{}}
}

// OpenTransfer implements accessdoor.TransferControl (via a thin platform-
// layer wrapper, mirroring lateStorageControl's own indirection — see
// platform/home/storagehost.go): mints one read-only-until-expiry ticket that
// only targetDaemonID can resolve.
func (a *Acceptor) OpenTransfer(ctx context.Context, targetDaemonID, coord string, mode access.Operation, reservationID string) (string, error) {
	now := time.Now()
	tr := laneTransfer{
		ticket:         uuid.NewString(),
		targetDaemonID: targetDaemonID,
		coord:          coord, mode: mode, reservationID: reservationID, mintedAt: now,
	}
	a.lane.mu.Lock()
	a.sweepExpiredTransfersLocked(now)
	a.lane.resolutions[tr.ticket] = tr
	a.lane.mu.Unlock()
	return tr.ticket, nil
}

// sweepExpiredTransfersLocked drops every transfer older than laneTransferTTL
// (期11 review #G). Caller holds a.lane.mu. O(n) over a map that is bounded by
// exactly this sweep to "transfers minted within one TTL window" — no ticker,
// no goroutine, the simplest GC that keeps the map from growing without bound
// under open-no-redeem.
func (a *Acceptor) sweepExpiredTransfersLocked(now time.Time) {
	for _, tr := range a.lane.resolutions {
		if laneTransferExpired(tr, now) {
			a.lane.deleteTransferLocked(tr)
		}
	}
}

func (s *laneState) deleteTransferLocked(tr laneTransfer) {
	delete(s.resolutions, tr.ticket)
}

// laneTransferExpired reports whether tr has aged past laneTransferTTL as of
// now — the single predicate both the opportunistic mint-time GC
// (sweepExpiredTransfersLocked) above AND the at-USE enforcement
// (handleResolveCoord, 期11 review残余#3) share, so the two can never disagree
// about what "expired" means. Before this fix, TTL
// was ONLY checked opportunistically at the next OpenTransfer call — a
// transfer nobody happened to mint again after was resolvable/redeemable
// indefinitely, silently outliving the "10 minutes to abandon" contract
// laneTransferTTL's own doc promises.
func laneTransferExpired(tr laneTransfer, now time.Time) bool {
	return now.Sub(tr.mintedAt) > laneTransferTTL
}

// handleResolveCoord answers ResolveCoord (§5 item 0's single mechanical
// check): only the daemon the transfer's target IS may resolve it. Since byte
// access is same-daemon only, that daemon is also the caller — one assertion,
// one branch.
//
// 期11 review #H reshapes two things the pre-review "delete-then-check" form
// got wrong:
//  1. AUTHORIZE BEFORE MUTATE — the sender (target) check runs BEFORE any
//     deletion, so a frame from the WRONG sender can never burn a legitimate
//     target's token (the old form deleted first, then rejected, destroying a
//     valid transfer on an unauthorized probe).
//  2. IDEMPOTENT / REPLAY-SAFE — the ticket is read-only until expiry, so a
//     dropped reply can be retried.
func (a *Acceptor) handleResolveCoord(senderDaemonID string, msg *ResolveCoordRequest) ResolveCoordReply {
	reply := ResolveCoordReply{RequestID: msg.RequestID}
	if senderDaemonID == "" {
		reply.Reason = "link: resolve_coord frame from an unattached sender"
		return reply
	}
	a.lane.mu.Lock()
	tr, ok := a.lane.resolutions[msg.Token]
	if ok && laneTransferExpired(tr, time.Now()) {
		// 期11 review残余#3: enforce TTL AT USE (see laneTransferExpired's
		// own doc) — a stale-but-still-present transfer must not resolve
		// just because the opportunistic mint-time GC has not happened to
		// run since it aged out.
		a.lane.deleteTransferLocked(tr)
		ok = false
	}
	a.lane.mu.Unlock()
	if !ok {
		reply.Reason = "unknown or expired transfer token"
		return reply
	}
	if tr.targetDaemonID != senderDaemonID {
		// Authorization failure — NON-destructive: the transfer stays, so the
		// real target can still resolve it (the old form burned it here).
		reply.Reason = fmt.Sprintf("token belongs to target %q, sender is %q", tr.targetDaemonID, senderDaemonID)
		return reply
	}
	reply.OK = true
	reply.Coord = tr.coord
	reply.Mode = tr.mode
	reply.ReservationID = tr.reservationID
	return reply
}
