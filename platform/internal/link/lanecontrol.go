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
)

// laneTransferTTL bounds how long a minted-but-unconsumed lane transfer lingers
// in the registry (期11 review #G): a Token that is opened but never
// redeemed/resolved (a requester that gives up, a target that never attaches)
// would otherwise sit in the transfers map until the Acceptor dies. A transfer
// is meant to be redeemed promptly right after the door hands its route back to
// the caller, so this is generous — it only reclaims genuinely-abandoned
// tokens. GC is opportunistic (sweepExpiredTransfersLocked runs on each mint),
// so no ticker/goroutine is added.
const laneTransferTTL = 10 * time.Minute

// lanecontrol.go is the HOME half of §5's resource lane: the per-daemon
// live-link table (boundID → linkSession, for opening a lane substream toward a
// relay target), the Token-keyed transfer registry (accessdoor.LaneControl's
// platform-side implementor reaches this via Acceptor.OpenLaneTransfer), the
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
	return f, nil
}

// --- transfer registry -----------------------------------------------------

// laneTransfer is one pending §5 byte-access authorization the door minted
// (accessdoor.LaneControl.OpenTransfer). 期11 review #H/#G: a transfer is a
// TIME-BOUNDED capability, not a consume-on-first-use ticket — both
// handleResolveCoord and handleLaneRedeem READ it (never delete), so an
// authorized target's retried resolve is idempotent, and abandoned tokens are
// reclaimed by laneTransferTTL (sweepExpiredTransfersLocked) rather than by a
// successful resolve. Re-resolution grants no authority the door had not
// already minted for this exact caller/coord/mode.
type laneTransfer struct {
	targetDaemonID    string
	requesterDaemonID string
	coord             string
	mode              access.Operation
	reservationID     string
	// mintedAt stamps OpenLaneTransfer time, read only by
	// sweepExpiredTransfersLocked for the laneTransferTTL GC (期11 review #G).
	mintedAt time.Time
}

// laneState is the Acceptor's lane bookkeeping, split into its own struct
// (embedded, not inlined into Acceptor's already-large field list) purely
// for readability — same lifetime and locking granularity as the rest of
// Acceptor's per-link tables.
//
// links maps each attached daemon's confirmed id (boundID) to its live link
// session, so a redeem arriving on the REQUESTER's link (handleLaneRedeem) can
// open a fresh lane substream toward the TARGET daemon's link (the relay). It
// is keyed by boundID (not the Serve-level pre-auth daemonID that a.links uses
// for Kick) because a transfer's target/requester ids ARE boundIDs — and in
// dev self-declared mode (empty auth id) boundID is the only id there is. One
// entry per daemon (most-recent link wins an overlapping reconnect); registered
// at attach success, deregistered pointer-guarded on link teardown.
type laneState struct {
	mu        sync.Mutex
	links     map[string]*linkSession // boundID -> live link, for opening lane substreams toward a target
	transfers map[string]laneTransfer // token -> pending transfer
}

func newLaneState() *laneState {
	return &laneState{links: map[string]*linkSession{}, transfers: map[string]laneTransfer{}}
}

// registerLaneLink records daemonID's live link for lane relay (called at
// attach success, once boundID is known). A reconnect overwrites the entry with
// the newer link — the most recent connection is the right relay target.
func (a *Acceptor) registerLaneLink(daemonID string, lc *linkSession) {
	if daemonID == "" {
		return
	}
	a.lane.mu.Lock()
	a.lane.links[daemonID] = lc
	a.lane.mu.Unlock()
}

// deregisterLaneLink drops daemonID's link IF it is still this exact one
// (pointer-guarded: an overlapping reconnect already replaced it with a newer
// link, whose teardown alone should evict it — a stale link's exit must not rip
// out its successor's registration).
func (a *Acceptor) deregisterLaneLink(daemonID string, lc *linkSession) {
	if daemonID == "" {
		return
	}
	a.lane.mu.Lock()
	if a.lane.links[daemonID] == lc {
		delete(a.lane.links, daemonID)
	}
	a.lane.mu.Unlock()
}

// laneLink returns daemonID's most-recent live link, or nil if that daemon is
// not currently attached (a redeem toward it then fails honestly, never a
// fabricated stream).
func (a *Acceptor) laneLink(daemonID string) *linkSession {
	a.lane.mu.Lock()
	defer a.lane.mu.Unlock()
	return a.lane.links[daemonID]
}

// handleLaneRedeem answers one redeem stream — dispatched by the accept loop
// (onLane) for every tag=lane substream a daemon opens toward the home, each one
// a redeem attempt (§5 item 0: the requester redeems a Token by opening a fresh
// substream on its own already-authenticated link, never by handing the Token to
// a third party). daemonID is the requester's confirmed boundID. Read the Token,
// look up its transfer (READ-ONLY — deletion is the target's ResolveCoord call,
// not this step), verify the redeeming daemon is the one the Token was minted
// for, then open a fresh tag=lane substream toward the TARGET daemon's own link,
// forward the header, and relay bytes bidirectionally until either side closes
// (§5.1④).
func (a *Acceptor) handleLaneRedeem(daemonID string, conn net.Conn) {
	defer conn.Close()
	var hdr laneRedeemHeader
	if err := readLaneJSON(conn, &hdr); err != nil {
		return
	}
	a.lane.mu.Lock()
	tr, ok := a.lane.transfers[hdr.Token]
	if ok && laneTransferExpired(tr, time.Now()) {
		// 期11 review残余#3: TTL is enforced AT USE, not only opportunistically
		// at the NEXT mint (sweepExpiredTransfersLocked) — a token minted long
		// ago but never redeemed until now must not still work just because no
		// OTHER OpenLaneTransfer happened to run its GC in between.
		delete(a.lane.transfers, hdr.Token)
		ok = false
	}
	a.lane.mu.Unlock()
	if !ok || tr.requesterDaemonID != daemonID {
		_ = writeLaneJSON(conn, laneAck{OK: false, Reason: "unknown or mismatched transfer token"})
		return
	}
	targetLC := a.laneLink(tr.targetDaemonID)
	if targetLC == nil {
		_ = writeLaneJSON(conn, laneAck{OK: false, Reason: fmt.Sprintf("target daemon %q has no live link", tr.targetDaemonID)})
		return
	}
	// openLane writes the streamHeader{lane} on the target's link; the
	// laneRedeemHeader below rides right after it, exactly as the requester's own
	// redeem substream carried its header after its streamHeader.
	targetConn, err := targetLC.openLane()
	if err != nil {
		_ = writeLaneJSON(conn, laneAck{OK: false, Reason: "open target lane stream: " + err.Error()})
		return
	}
	if err := writeLaneJSON(targetConn, laneRedeemHeader{Token: hdr.Token}); err != nil {
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
// platform/storagehost.go): mints a fresh single-use Token and registers the
// transfer, requiring no live session yet (a target daemon that never
// attaches / a requester that never redeems both fail HONESTLY at
// resolve/redeem time, never here — matching AllocRequest's own
// "resolve at use, not at mint" discipline).
func (a *Acceptor) OpenLaneTransfer(ctx context.Context, targetDaemonID, requesterDaemonID, coord string, mode access.Operation, reservationID string) (string, error) {
	token := uuid.NewString()
	a.lane.mu.Lock()
	a.sweepExpiredTransfersLocked(time.Now()) // 期11 review #G: opportunistic GC of abandoned tokens
	a.lane.transfers[token] = laneTransfer{
		targetDaemonID: targetDaemonID, requesterDaemonID: requesterDaemonID,
		coord: coord, mode: mode, reservationID: reservationID,
		mintedAt: time.Now(),
	}
	a.lane.mu.Unlock()
	return token, nil
}

// sweepExpiredTransfersLocked drops every transfer older than laneTransferTTL
// (期11 review #G). Caller holds a.lane.mu. O(n) over a map that is bounded by
// exactly this sweep to "transfers minted within one TTL window" — no ticker,
// no goroutine, the simplest GC that keeps the map from growing without bound
// under open-no-redeem.
func (a *Acceptor) sweepExpiredTransfersLocked(now time.Time) {
	for token, tr := range a.lane.transfers {
		if laneTransferExpired(tr, now) {
			delete(a.lane.transfers, token)
		}
	}
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
//  2. IDEMPOTENT / REPLAY-SAFE — resolution NO LONGER consumes the transfer.
//     The Local route re-resolves the same route.Token on a retried open (a
//     dropped reply, a re-dialed handle), and the old consume-on-resolve made
//     that second attempt fail with "already-resolved". A resolve now just
//     READS the transfer (like handleLaneRedeem already does), so a retry by
//     the authorized target returns the same coord — the "幂等 no-op" the
//     Committed/ReclaimAck handlers already have. The single-use property this
//     relaxes granted no authority the door had not ALREADY minted for exactly
//     this caller/coord/mode; the transfer is instead bounded in time by
//     laneTransferTTL (#G's GC), which reclaims it whether or not it was ever
//     resolved.
func (a *Acceptor) handleResolveCoord(senderDaemonID string, msg *ResolveCoordRequest) ResolveCoordReply {
	reply := ResolveCoordReply{RequestID: msg.RequestID}
	if senderDaemonID == "" {
		reply.Reason = "link: resolve_coord frame from an unattached sender"
		return reply
	}
	a.lane.mu.Lock()
	tr, ok := a.lane.transfers[msg.Token]
	if ok && laneTransferExpired(tr, time.Now()) {
		// 期11 review残余#3: enforce TTL AT USE (see laneTransferExpired's
		// own doc) — a stale-but-still-present transfer must not resolve
		// just because the opportunistic mint-time GC has not happened to
		// run since it aged out.
		delete(a.lane.transfers, msg.Token)
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
