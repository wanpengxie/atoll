package link

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// laneTransferTTL bounds how long a minted ticket pair lingers. It expires an
// unused pair and also retires the retryable resolve ticket after a successful
// redeem; opportunistic mint-time sweeping is backed by enforcement at use.
const laneTransferTTL = 10 * time.Minute

// lanecontrol.go is the HOME half of §5's resource lane: target selection reads
// the session ledger's current index, while the ticket-keyed transfer registry
// (accessdoor.LaneControl's platform-side implementor reaches this via
// Acceptor.OpenLaneTransfer), the
// ResolveCoord control-RPC frame pair (daemon-initiated, riding the SAME control
// substream storagecontrol.go already extends — §4.7's own "fallback 触发时才落
// control" wording, chosen here for the SAME reason: a genuinely daemon-local
// rid→coord cache would contradict "daemon 无 truth"，so even the Local/zerocopy
// route resolves its coord through one small metadata round trip on this
// existing, tested channel — never through the lane itself, which stays
// byte-only), and handleLaneRedeem, which relays a requester's redeem substream
// to the transfer's target daemon (§5). 片③ flattened the lane: a redeem is a
// plain top-level tag=lane substream dispatched by the link's own accept loop
// (onLane), not a stream on a nested per-daemon yamux session.

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

// laneTransfer is one pending byte-access authorization. Its two tickets have
// deliberately different lifecycles: redeemTicket is consumed by the first
// valid requester redemption, while resolveTicket remains read-only and
// retryable by the target until the shared TTL expires.
type laneTransfer struct {
	redeemTicket      string
	resolveTicket     string
	targetDaemonID    string
	requesterDaemonID string
	coord             string
	mode              access.Operation
	reservationID     string
	// mintedAt stamps OpenLaneTransfer time, read only by
	// sweepExpiredTransfersLocked for the laneTransferTTL GC (期11 review #G).
	mintedAt time.Time
}

// laneState contains transfer capabilities only. Target-session routing reads
// the session ledger's current index directly; there is deliberately no lane
// link table.
type laneState struct {
	mu          sync.Mutex
	redeems     map[string]laneTransfer
	resolutions map[string]laneTransfer
}

func newLaneState() *laneState {
	return &laneState{
		redeems:     map[string]laneTransfer{},
		resolutions: map[string]laneTransfer{},
	}
}

// handleLaneRedeem answers one redeem stream — dispatched by the accept loop
// (onLane) for every tag=lane substream a daemon opens toward the home, each one
// a redeem attempt. daemonID is the requester's confirmed identity. A valid
// requester consumes the redeem ticket under the lookup lock; the paired,
// retryable resolve ticket is what the home forwards to the target before
// relaying bytes.
func (a *Acceptor) handleLaneRedeem(daemonID string, conn net.Conn) {
	defer conn.Close()
	var hdr laneRedeemHeader
	if err := readLaneJSON(conn, &hdr); err != nil {
		return
	}
	a.lane.mu.Lock()
	tr, ok := a.lane.redeems[hdr.Token]
	if ok && laneTransferExpired(tr, time.Now()) {
		// 期11 review残余#3: TTL is enforced AT USE, not only opportunistically
		// at the NEXT mint (sweepExpiredTransfersLocked) — a token minted long
		// ago but never redeemed until now must not still work just because no
		// OTHER OpenLaneTransfer happened to run its GC in between.
		a.lane.deleteTransferLocked(tr)
		ok = false
	}
	if ok && tr.requesterDaemonID == daemonID {
		// Single-use is enforced AT REDEMPTION: the full evidence (exists,
		// unexpired, right requester) passed under this one lock hold, so the
		// ticket is consumed here — a replay within the TTL finds nothing.
		// A mismatched requester does NOT burn someone else's ticket.
		delete(a.lane.redeems, hdr.Token)
	} else {
		ok = false
	}
	a.lane.mu.Unlock()
	if !ok {
		_ = writeLaneJSON(conn, laneAck{OK: false, Reason: "unknown or mismatched transfer token"})
		return
	}
	targetRecord := a.sessions.currentRecord(tr.targetDaemonID)
	if targetRecord == nil {
		_ = writeLaneJSON(conn, laneAck{OK: false, Reason: fmt.Sprintf("target daemon %q has no live link", tr.targetDaemonID)})
		return
	}
	handle := targetRecord.linkHandle()
	if handle == nil || handle.openLane == nil {
		_ = writeLaneJSON(conn, laneAck{OK: false, Reason: fmt.Sprintf("target daemon %q has no live link", tr.targetDaemonID)})
		return
	}
	// openLane writes the streamHeader{lane} on the target's link; the
	// laneRedeemHeader below rides right after it, exactly as the requester's own
	// redeem substream carried its header after its streamHeader.
	openCtx, cancel := context.WithTimeout(a.ctx, streamWriteBudget)
	targetConn, err := handle.openLane(openCtx)
	cancel()
	if err != nil {
		_ = writeLaneJSON(conn, laneAck{OK: false, Reason: "open target lane stream: " + err.Error()})
		return
	}
	if err := writeLaneJSON(targetConn, laneRedeemHeader{Token: tr.resolveTicket}); err != nil {
		_ = targetConn.Close()
		_ = writeLaneJSON(conn, laneAck{OK: false, Reason: "forward token to target: " + err.Error()})
		return
	}
	// Read the TARGET's own ack FIRST, before relaying anything to the
	// requester — this ack is a protocol handshake byte, not payload, and
	// must never be forwarded into the raw byte pump below (pumpBidirectional
	// is a dumb byte relay with no framing awareness once it starts; the
	// three-step handshake — requester→home→target→home→requester — has to
	// fully resolve before ANY io.Copy begins, or the target's own ack line
	// would be indistinguishable from the first bytes of real data).
	var targetAck laneAck
	if err := readLaneJSON(targetConn, &targetAck); err != nil {
		_ = targetConn.Close()
		_ = writeLaneJSON(conn, laneAck{OK: false, Reason: "target ack: " + err.Error()})
		return
	}
	if !targetAck.OK {
		_ = targetConn.Close()
		_ = writeLaneJSON(conn, laneAck{OK: false, Reason: targetAck.Reason})
		return
	}
	if err := writeLaneJSON(conn, laneAck{OK: true}); err != nil {
		_ = targetConn.Close()
		return
	}
	pumpBidirectional(io.ReadWriteCloser(conn), io.ReadWriteCloser(targetConn))
}

// OpenLaneTransfer implements accessdoor.LaneControl (via a thin platform-
// layer wrapper, mirroring lateStorageControl's own indirection — see
// platform/home/storagehost.go): mints one consume-on-valid-use redeem ticket
// and one read-only-until-expiry resolve ticket.
func (a *Acceptor) OpenLaneTransfer(ctx context.Context, targetDaemonID, requesterDaemonID, coord string, mode access.Operation, reservationID string) (accessdoor.LaneTickets, error) {
	tickets := accessdoor.LaneTickets{Redeem: uuid.NewString(), Resolve: uuid.NewString()}
	now := time.Now()
	tr := laneTransfer{
		redeemTicket: tickets.Redeem, resolveTicket: tickets.Resolve,
		targetDaemonID: targetDaemonID, requesterDaemonID: requesterDaemonID,
		coord: coord, mode: mode, reservationID: reservationID, mintedAt: now,
	}
	a.lane.mu.Lock()
	a.sweepExpiredTransfersLocked(now)
	a.lane.redeems[tickets.Redeem] = tr
	a.lane.resolutions[tickets.Resolve] = tr
	a.lane.mu.Unlock()
	return tickets, nil
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
	delete(s.redeems, tr.redeemTicket)
	delete(s.resolutions, tr.resolveTicket)
}

// laneTransferExpired reports whether tr has aged past laneTransferTTL as of
// now — the single predicate both the opportunistic mint-time GC
// (sweepExpiredTransfersLocked) above AND the at-USE enforcement
// (handleResolveCoord / handleLaneRedeem, 期11 review残余#3) share, so the
// two can never disagree about what "expired" means. Before this fix, TTL
// was ONLY checked opportunistically at the next OpenLaneTransfer call — a
// transfer nobody happened to mint again after was resolvable/redeemable
// indefinitely, silently outliving the "10 minutes to abandon" contract
// laneTransferTTL's own doc promises.
func laneTransferExpired(tr laneTransfer, now time.Time) bool {
	return now.Sub(tr.mintedAt) > laneTransferTTL
}

// handleResolveCoord answers ResolveCoord (§5 item 0's single mechanical
// check): only the daemon the transfer's target IS may resolve it — this ONE
// assertion covers both the Local route (the caller IS the target, resolving
// directly) and the Stream route's target-side inbound handler (redeemed-to by
// home, then resolving for itself) uniformly.
//
// 期11 review #H reshapes two things the pre-review "delete-then-check" form
// got wrong:
//  1. AUTHORIZE BEFORE MUTATE — the sender (target) check runs BEFORE any
//     deletion, so a frame from the WRONG sender can never burn a legitimate
//     target's token (the old form deleted first, then rejected, destroying a
//     valid transfer on an unauthorized probe).
//  2. IDEMPOTENT / REPLAY-SAFE — the resolve ticket is read-only until expiry.
//     Local routes carry it directly; cross-host routes receive it from the
//     home after the separate redeem ticket is consumed. A dropped reply can
//     therefore be retried without making redemption replayable.
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
