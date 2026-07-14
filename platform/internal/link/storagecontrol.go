package link

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// storagecontrol.go is the daemon storage host's control-RPC WIRE FORMAT
// (期11 spec §4.7): four request/response frame pairs riding the SAME
// stream-0 control plane attach/attach_reply already uses (controlFrame,
// controlKind — control.go), extended additively. §4.7's own text names
// yamux's eventual dedicated control stream as the PRIMARY carrier once §5
// lands it ("对齐§5.2主选yamux...fallback触发时才落stream-0新增control
// kind") — this section builds before §5 exists, so it lands on the named
// FALLBACK carrier (stream-0) now. The message SHAPES below are carrier-
// agnostic (plain structs, JSON-encoded exactly like AttachRequest/Reply
// already are) — §5 migrating the carrier to a yamux control stream is a
// transport swap underneath these types, not a redesign of them.

// AllocRequest is home→daemon: "prepare to receive/hold bytes at coord"
// (§4.7's first frame). The door already resolved placement + wrote a
// durable reservation (accessdoor.StorageControl.AllocRequest, §4.3) before
// this frame is ever sent — coord is the SERVER-generated opaque storage
// handle (§1.6), never daemon-chosen. Dir marks a directory-shaped create
// (mkdir vs touch); a content-bearing create's actual bytes never ride this
// frame (§8.1) — they arrive later via the lane (§5), staged under this
// SAME coord.
type AllocRequest struct {
	RequestID string `json:"request_id"`
	ChannelID string `json:"channel_id"`
	Coord     string `json:"coord"`
	Dir       bool   `json:"dir"`
}

// AllocReply is daemon→home: the Allocator's verdict (§4.1) — OK on a
// successful (or idempotently-already-done) mkdir/touch, else Reason names
// the failure (a Go-error-shaped string, not an access verdict — this RPC
// plane carries no authorization decision of its own, the door already made
// one before sending AllocRequest).
type AllocReply struct {
	RequestID string `json:"request_id"`
	OK        bool   `json:"ok"`
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
// third frame) — sent after the Reclaimer confirms the axis-allocated
// tombstone's bytes are collected.
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
// daemon currently has an OPEN local WriteHandle for (cmd/daemon/internal/
// storagehost.Host.ActiveWriteCoords), snapshotted fresh on every pull. The
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
// AllocReply).
type ReclaimReply struct {
	RequestID string `json:"request_id"`
	OK        bool   `json:"ok"`
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
// bytes this daemon still owes a ReclaimAck for. Provenance is the plain-string
// wire mirror of resourcespec.Provenance; its current closed set contains only
// "axis-allocated". This package deliberately carries the string form because
// it sits outside the runtime tree (archtest confines resourcespec's own type
// to runtime/*). The daemon-side Reclaimer removes bytes only for that known
// value and treats an unrecognized value as a fail-safe no-op. Any future
// provenance addition must update both sides of this wire together.
type ReconcileTombstone struct {
	TombstoneID string `json:"tombstone_id"`
	Coord       string `json:"coord"`
	Provenance  string `json:"provenance"`
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

// controlKind additions (additive to control.go's closed set — attach/
// attach_reply are untouched, this is a widening not a redefinition).
const (
	ctrlAllocRequest       controlKind = "alloc_request"
	ctrlAllocReply         controlKind = "alloc_reply"
	ctrlCommitted          controlKind = "committed"
	ctrlCommittedReply     controlKind = "committed_reply"
	ctrlReclaimAck         controlKind = "reclaim_ack"
	ctrlReclaimAckReply    controlKind = "reclaim_ack_reply"
	ctrlReconcilePull      controlKind = "reconcile_pull"
	ctrlReconcilePullReply controlKind = "reconcile_pull_reply"
	ctrlReclaimRequest     controlKind = "reclaim_request"
	ctrlReclaimReply       controlKind = "reclaim_reply"
)

// newRequestID mints a fresh correlation id for one control-RPC round trip
// (§4.7: "correlation 带单调 request_id 应答回携" — uuid is used here as the
// uniqueness source; the spec's "单调" concern is about NOT re-using an id
// mid-flight, which a fresh uuid per call trivially satisfies, not literal
// increasing-integer monotonicity).
func newRequestID() string { return uuid.NewString() }

// errPendingCancelled is delivered to a pendingReplies waiter whose entry was
// cancelled (e.g. the link tore down) rather than answered.
var errPendingCancelled = errors.New("link: control RPC cancelled (link closed before a reply arrived)")

// pendingReplies is the generic correlation table BOTH directions of the
// storage control-RPC plane share: the Acceptor uses one (keyed by
// AllocReply.RequestID) to await a daemon's alloc_reply; the Dialer uses
// three (Committed/ReclaimAck/ReconcilePull reply kinds) to await the home's
// responses to its own outbound sends. One request_id may have at most one
// waiter at a time — a second register under a live id would silently
// orphan the first (never done by any caller here: each call mints a fresh
// id via newRequestID).
type pendingReplies[T any] struct {
	mu      sync.Mutex
	waiters map[string]chan T
}

func newPendingReplies[T any]() *pendingReplies[T] {
	return &pendingReplies[T]{waiters: map[string]chan T{}}
}

// register opens a fresh waiter slot for id. The channel is buffered 1 so a
// deliver that races a caller's own timeout/cancel never blocks the
// delivering goroutine (the control-frame read loop).
func (p *pendingReplies[T]) register(id string) chan T {
	ch := make(chan T, 1)
	p.mu.Lock()
	p.waiters[id] = ch
	p.mu.Unlock()
	return ch
}

// deliver hands v to id's waiter, if one is still registered. A delivery
// with no live waiter (already timed out / cancelled, or a stray/duplicate
// reply) is silently dropped — best-effort, matching every other advisory
// arm in this package (obs/cancel-forward).
func (p *pendingReplies[T]) deliver(id string, v T) {
	p.mu.Lock()
	ch := p.waiters[id]
	delete(p.waiters, id)
	p.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- v:
	default:
	}
}

// cancel drops id's waiter without delivering — used by the caller's own
// ctx/timeout path so a late reply after the caller gave up cannot leak the
// map entry.
func (p *pendingReplies[T]) cancel(id string) {
	p.mu.Lock()
	delete(p.waiters, id)
	p.mu.Unlock()
}

// controlRPCTimeout bounds every storage control-RPC round trip on both
// sides (AllocRequest waiting for AllocReply; the daemon's Committed/
// ReclaimAck/ReconcilePull waiting for their replies) — a wedged peer must
// not hang the caller forever. Generous relative to leasePing/leaseTTL
// (10s/30s) since a real Allocator mkdir or Scrubber reconcile pull may
// legitimately take longer than a bare control-frame round trip.
var controlRPCTimeout = 20 * time.Second

// wait blocks for id's reply on ch, honoring ctx, the link's done channel,
// and controlRPCTimeout — the shared tail every storage control-RPC send
// (both directions) runs after writing its request frame. It cancels id's
// registration on every non-success exit so a late/never-arriving reply
// cannot leak the pendingReplies map entry.
func (p *pendingReplies[T]) wait(ctx context.Context, id string, ch chan T, done <-chan struct{}) (T, error) {
	var zero T
	timeout := time.NewTimer(controlRPCTimeout)
	defer timeout.Stop()
	select {
	case v := <-ch:
		return v, nil
	case <-ctx.Done():
		p.cancel(id)
		return zero, ctx.Err()
	case <-done:
		p.cancel(id)
		return zero, errPendingCancelled
	case <-timeout.C:
		p.cancel(id)
		return zero, fmt.Errorf("link: control RPC %s: %w", id, errControlRPCTimeout)
	}
}

var errControlRPCTimeout = errors.New("timed out waiting for a reply")

// encodeStorageControl / decodeStorageControl mirror encodeControl/
// decodeControl's JSON discipline for the four new frame kinds — kept
// separate from control.go's controlFrame struct only to avoid bloating its
// single struct with eight more optional pointer fields; the wire encoding
// (one JSON object per control substream payload) is identical.
type storageControlFrame struct {
	Kind               controlKind         `json:"kind"`
	AllocRequest       *AllocRequest       `json:"alloc_request,omitempty"`
	AllocReply         *AllocReply         `json:"alloc_reply,omitempty"`
	Committed          *Committed          `json:"committed,omitempty"`
	CommittedReply     *CommittedReply     `json:"committed_reply,omitempty"`
	ReclaimAck         *ReclaimAck         `json:"reclaim_ack,omitempty"`
	ReclaimAckReply    *ReclaimAckReply    `json:"reclaim_ack_reply,omitempty"`
	ReconcilePull      *ReconcilePull      `json:"reconcile_pull,omitempty"`
	ReconcilePullReply *ReconcilePullReply `json:"reconcile_pull_reply,omitempty"`
	ReclaimRequest     *ReclaimRequest     `json:"reclaim_request,omitempty"`
	ReclaimReply       *ReclaimReply       `json:"reclaim_reply,omitempty"`
}

func encodeStorageControl(f storageControlFrame) ([]byte, error) { return json.Marshal(f) }

func decodeStorageControl(b []byte) (storageControlFrame, error) {
	var f storageControlFrame
	if err := json.Unmarshal(b, &f); err != nil {
		return storageControlFrame{}, fmt.Errorf("link: decode storage control: %w", err)
	}
	return f, nil
}
