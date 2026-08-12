package link

import (
	"errors"
	"fmt"
)

// The structs in this file are the lane_control storage vocabulary. The
// stream header fixes daemon and channel ownership; payloads therefore carry
// operation data and correlation only.

// AllocRequest is home→daemon: "prepare to receive/hold bytes at coord"
// (§4.7's first frame). The door already resolved placement + wrote a
// durable reservation (accessdoor.StorageControl.AllocRequest, §4.3) before
// this frame is ever sent — coord is the SERVER-generated opaque storage
// handle (§1.6), never daemon-chosen. Dir marks a directory-shaped create
// (mkdir vs touch); a content-bearing create's actual bytes never ride this
// frame (§8.1) — they arrive later through the daemon-local write route (§5),
// staged under this SAME coord.
type AllocRequest struct {
	RequestID string `json:"request_id"`
	Coord     string `json:"coord"`
	Dir       bool   `json:"dir"`
}

// AllocReply is daemon→home: the Allocator's verdict (§4.1) — OK on a
// successful (or idempotently-already-done) mkdir/touch, else Reason names
// the failure (a Go-error-shaped string, not an access verdict — this RPC
// plane carries no authorization decision of its own, the door already made
// one before sending AllocRequest).
//
// NotReady is the third disposition, and it is NOT a verdict: the lane exists
// but is not yet bound to a built compartment, so the daemon did not attempt
// the mkdir/touch and holds no opinion about whether it would have succeeded.
// The home may reach a lane in that state at any time — the lane starts
// carrying frames the moment it is admitted, and its compartment is built
// afterwards — and the daemon deliberately projects no readiness state into
// the home's ledger, so the home cannot know in advance. Folding this into
// !OK would make a refusal the daemon never issued indistinguishable from one
// it did.
type AllocReply struct {
	RequestID string `json:"request_id"`
	OK        bool   `json:"ok"`
	NotReady  bool   `json:"not_ready,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

// Committed is daemon→home: create-outbox's landing signal (§1.5/§1.7,
// §4.7's second frame) — sent after the daemon's staging→fsync→rename
// completes for a content-bearing create. It carries ONLY the reservation
// id, never a creator (§1.7 P0-2: "daemon 不报 creator" — the daemon has no
// truth to report one FROM; the home looks the door-authenticated creator
// up in its OWN reservation row).
type Committed struct {
	RequestID     string `json:"request_id"`
	ReservationID string `json:"reservation_id"`
	Ticket        string `json:"ticket"`
}

// CommittedReply is home→daemon: CommitReservation's outcome relayed back —
// Found=false is Committed's replay-safe no-op (already landed, or the
// reservation was lost to a same-resource_id race and already cleaned up by
// the loser-cleanup path, §1.7's trigger ②); Lost=true (only meaningful when
// Found=true) tells the daemon its own staged bytes are now orphaned (some
// OTHER reservation landed the resource id first) and must be swept, never
// retried.
type CommittedReply struct {
	RequestID string `json:"request_id"`
	Found     bool   `json:"found"`
	Lost      bool   `json:"lost"`
	Reason    string `json:"reason,omitempty"`
}

// ReclaimAck is daemon→home: delete-outbox's closure signal (§1.8/§4.7's
// third frame) — sent after the Reclaimer confirms the tombstone's bytes are
// collected.
type ReclaimAck struct {
	RequestID   string `json:"request_id"`
	TombstoneID string `json:"tombstone_id"`
}

// ReclaimAckReply is home→daemon: ClearTombstone's outcome relayed back.
// Found=false is ReclaimAck's replay-safe no-op (already cleared).
type ReclaimAckReply struct {
	RequestID string `json:"request_id"`
	Found     bool   `json:"found"`
	Reason    string `json:"reason,omitempty"`
}

// ReconcilePull is daemon→home: the Scrubber's periodic (ticker-driven,
// level-triggered) pull for restart/crash recovery state (§4.1/§4.7's
// fourth frame) — the daemon holds no local truth, so EVERYTHING it needs to
// reconcile its directory against comes back in the reply.
//
// ActiveCoords is 期11 review's own narrowing addition: every coord this
// daemon currently has an OPEN local WriteHandle for (drivers/devicehost/
// internal/storagehost.Host.ActiveWriteCoords), snapshotted fresh on every
// pull. The
// home's TouchReservationsByCoords liveness bump reads ONLY this list —
// "this daemon is still polling" is deliberately NOT treated as "every
// reservation this daemon owns is alive" (that blanket form let an abandoned
// reservation's daemon merely staying online forever suppress its age-sweep
// forever). The empty slice is indistinguishable from "explicitly nothing
// active", which is exactly what an idle daemon should send.
type ReconcilePull struct {
	RequestID    string   `json:"request_id"`
	ActiveCoords []string `json:"active_coords,omitempty"`
}

// ReclaimRequest is home→daemon: "collect coord's already-landed local bytes"
// (期11 review §2.5 #B) — the content-less create loser's synchronous reclaim,
// riding the SAME home-initiated control channel AllocRequest uses. A
// content-less create allocates live/<coord> up front (AllocRequest's own
// mkdir/touch) but moves no bytes, so its loser has NO Committed round trip on
// which to piggyback a CommittedReply.Lost→ReclaimCoord signal (the
// with-content path's mechanism). This frame is that signal for the
// synchronous path: the door, on ErrReservationLost, tells the placement
// daemon to reclaim the orphaned empty coord directly. Idempotent on the
// daemon side (ReclaimCoord no-ops an already-empty coord).
type ReclaimRequest struct {
	RequestID string `json:"request_id"`
	Coord     string `json:"coord"`
}

// ReclaimReply is daemon→home: the reclaim verdict — OK once the coord's local
// bytes are gone (or were already absent), else Reason names the failure (a
// Go-error-shaped string, not an access verdict — same discipline as
// AllocReply). NotReady carries the same not-a-verdict meaning it does on
// AllocReply: the lane is not bound to a built compartment yet, so nothing was
// attempted.
type ReclaimReply struct {
	RequestID string `json:"request_id"`
	OK        bool   `json:"ok"`
	NotReady  bool   `json:"not_ready,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

// ReconcileResource is one row of ReconcilePullReply's "应有资源清单" —
// landed file resources this daemon is the placement for (registry↔
// directory diff input: a coord on disk with no matching entry here is an
// orphan; an entry here with no matching coord on disk is a lost-byte
// anomaly). No Dir flag: whether coord is a directory or a regular file is
// NOT persisted registry state (resourcespec.ResourceMeta carries no such
// column — a directory create's "dir-ness" lives entirely as a fact ABOUT
// the daemon's own filesystem, §4.2) — the Scrubber observes it locally
// (a stat on coord's path), it never needs the registry to assert it.
type ReconcileResource struct {
	Coord string `json:"coord"`
}

// ReconcileReservation is one row of ReconcilePullReply's "挂起 reservation"
// — a content-bearing create whose Committed has not yet landed, from this
// daemon's OWN reservations (server-side pre-filtered, §4.7's sender-auth
// read discipline: a daemon never sees another daemon's rows).
type ReconcileReservation struct {
	ReservationID string `json:"reservation_id"`
	Coord         string `json:"coord"`
}

// ReconcileTombstone is one row of ReconcilePullReply's "待收 tombstone" —
// bytes this daemon still owes a ReclaimAck for.
type ReconcileTombstone struct {
	TombstoneID string `json:"tombstone_id"`
	Coord       string `json:"coord"`
}

// ReconcilePullReply is home→daemon: the Scrubber's full recovery picture,
// pre-filtered to THIS daemon alone (§4.7: "ReconcilePull 只返回该 sender
// 名下的 rows/reservations/tombstones — 不泄他 daemon 的 coord 清单").
type ReconcilePullReply struct {
	RequestID           string                 `json:"request_id"`
	Resources           []ReconcileResource    `json:"resources,omitempty"`
	PendingReservations []ReconcileReservation `json:"pending_reservations,omitempty"`
	PendingTombstones   []ReconcileTombstone   `json:"pending_tombstones,omitempty"`
	Reason              string                 `json:"reason,omitempty"`
}

func (m AllocRequest) validate() error {
	if err := requiredControlField("alloc_request.request_id", m.RequestID); err != nil {
		return err
	}
	return requiredControlField("alloc_request.coord", m.Coord)
}

func (m AllocReply) validate() error {
	if err := requiredControlField("alloc_reply.request_id", m.RequestID); err != nil {
		return err
	}
	if m.OK && m.NotReady {
		return errors.New("link: alloc_reply.not_ready contradicts ok")
	}
	if !m.OK {
		return requiredControlField("alloc_reply.reason", m.Reason)
	}
	return nil
}

func (m Committed) validate() error {
	if err := requiredControlField("committed.request_id", m.RequestID); err != nil {
		return err
	}
	if err := requiredControlField("committed.reservation_id", m.ReservationID); err != nil {
		return err
	}
	return requiredControlField("committed.ticket", m.Ticket)
}

func (m CommittedReply) validate() error {
	if err := requiredControlField("committed_reply.request_id", m.RequestID); err != nil {
		return err
	}
	if m.Lost && !m.Found {
		return errors.New("link: committed_reply.lost requires found")
	}
	return nil
}

func (m ReclaimAck) validate() error {
	if err := requiredControlField("reclaim_ack.request_id", m.RequestID); err != nil {
		return err
	}
	return requiredControlField("reclaim_ack.tombstone_id", m.TombstoneID)
}

func (m ReclaimAckReply) validate() error {
	return requiredControlField("reclaim_ack_reply.request_id", m.RequestID)
}

func (m ReconcilePull) validate() error {
	if err := requiredControlField("reconcile_pull.request_id", m.RequestID); err != nil {
		return err
	}
	for i, coord := range m.ActiveCoords {
		if coord == "" {
			return fmt.Errorf("link: reconcile_pull.active_coords[%d] is required", i)
		}
	}
	return nil
}

func (m ReclaimRequest) validate() error {
	if err := requiredControlField("reclaim_request.request_id", m.RequestID); err != nil {
		return err
	}
	return requiredControlField("reclaim_request.coord", m.Coord)
}

func (m ReclaimReply) validate() error {
	if err := requiredControlField("reclaim_reply.request_id", m.RequestID); err != nil {
		return err
	}
	if m.OK && m.NotReady {
		return errors.New("link: reclaim_reply.not_ready contradicts ok")
	}
	if !m.OK {
		return requiredControlField("reclaim_reply.reason", m.Reason)
	}
	return nil
}

func (m ReconcilePullReply) validate() error {
	if err := requiredControlField("reconcile_pull_reply.request_id", m.RequestID); err != nil {
		return err
	}
	for i, row := range m.Resources {
		if row.Coord == "" {
			return fmt.Errorf("link: reconcile_pull_reply.resources[%d].coord is required", i)
		}
	}
	for i, row := range m.PendingReservations {
		if row.ReservationID == "" || row.Coord == "" {
			return fmt.Errorf("link: reconcile_pull_reply.pending_reservations[%d] is incomplete", i)
		}
	}
	for i, row := range m.PendingTombstones {
		if row.TombstoneID == "" || row.Coord == "" {
			return fmt.Errorf("link: reconcile_pull_reply.pending_tombstones[%d] is incomplete", i)
		}
	}
	return nil
}
