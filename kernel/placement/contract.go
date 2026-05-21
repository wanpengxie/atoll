// Package placement declares the channel-placement state machine, the
// ACK-frame field set, the state transition matrix, and the kernel-side
// interfaces server.placements / runtime.lifecycle implement.
//
// Authoritative spec: L2 §1.4.11 (channel placement 原子所有权协议 +
// fencing token + ACK 完整字段匹配).
package placement

import (
	"context"
	"crypto/rand"
	"encoding/hex"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
)

// State is the channel placement state per L2 §1.4.11.2 state machine
// (M1.5 closed set: creating / active / orphan / stale).
type State string

const (
	StateCreating State = "creating"
	StateActive   State = "active"
	StateOrphan   State = "orphan"
	StateStale    State = "stale"
)

// AllStates lists every state in spec order — used by the
// state_machine_test to assert closed-set coverage.
var AllStates = []State{
	StateCreating,
	StateActive,
	StateOrphan,
	StateStale,
}

// String returns the wire form.
func (s State) String() string { return string(s) }

// OwnerEpoch is the monotonic ownership counter per channel — incremented
// whenever placement is re-claimed (orphan → creating). Decoupled from
// FencingToken: epoch is the monotonic ordering invariant used to detect
// stale create/reclaim attempts; fencing token is the unguessable opaque
// secret used to gate writes (proto-foundation §3.6.1 + proto-layer1
// §6.2). The two values are generated together but carry independent
// semantics.
type OwnerEpoch int64

// FencingToken is the SQL-layer guard value that gates daemon writes to
// the channel sqlite (L2 §1.4.11.6). Per proto-foundation §3.6.1 +
// proto-layer1 §6.2: an opaque, cryptographically unguessable random
// token generated fresh on every channel creation / reclaim. Decoupled
// from OwnerEpoch (two independent values; equality match is the only
// legal comparison — ordering uses OwnerEpoch).
type FencingToken string

// String returns the wire form.
func (f FencingToken) String() string { return string(f) }

// NewFencingToken generates a fresh opaque fencing token per
// proto-foundation §3.6.1 + proto-layer1 §6.2. Uses crypto/rand to
// produce 16 bytes of randomness, encoded as lowercase hex (32 chars).
// MUST be called on every channel creation + reclaim; reusing a value
// across reclaims violates F.fencing (the L1 fencing invariant).
func NewFencingToken() (FencingToken, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return FencingToken(hex.EncodeToString(buf[:])), nil
}

// ConnectionEpoch is the daemonbus connection epoch. Placement rows store
// the same value carried by daemonbus frame headers.
type ConnectionEpoch int64

// DaemonID is the stable daemon identifier (assigned at registration,
// survives restarts).
type DaemonID string

// String returns the wire form.
func (d DaemonID) String() string { return string(d) }

// DaemonEpoch is the daemon process identifier (assigned at process
// start, changes on every restart). Used for audit + cross-checking the
// `_lock` table during phase-1 daemon startup (L2 §10.1 step 1.3).
type DaemonEpoch int64

// CreateRequestID is the server-allocated UUID idempotency key for one
// `control.create_channel` flow (L2 §1.4.11.3 step 1).
type CreateRequestID string

// String returns the wire form.
func (c CreateRequestID) String() string { return string(c) }

// UserID is the server identity user identifier carried in placement
// bootstrap member payloads.
type UserID string

// String returns the wire form.
func (u UserID) String() string { return string(u) }

// TenantID is the multi-tenant scope identifier reserved per
// .dalek/pm/m1.5-tickets.md §T10. M1.5 demo uses TenantID("") /
// TenantID("default") and treats placements as a single shared pool;
// M2+ SaaS deployments will scope placement selection / quota by
// TenantID without changing the placement state machine.
type TenantID string

// String returns the wire form.
func (t TenantID) String() string { return string(t) }

// Placement mirrors the channel_placements row from L2 §1.4.11.1 plus
// the three federation / tenancy columns reserved by
// .dalek/pm/m1.5-tickets.md §T10 ("placements 表预留 federation 字段").
//
// HostActorID / FederatedOrigin / TenantID are zero-value ("") in
// M1.5 demo deployments and stored as NULL in sqlite. Populating them
// is M1.4 / federation / SaaS work and does NOT change the M1.5 state
// machine — the columns are skipped by every M1.5 CAS / SELECT helper
// that doesn't care about them.
type Placement struct {
	ChannelID             channel.ID
	DaemonID              DaemonID
	State                 State
	OwnerEpoch            OwnerEpoch
	FencingToken          FencingToken // opaque random; independent of OwnerEpoch
	CreateRequestID       CreateRequestID
	DaemonConnectionEpoch ConnectionEpoch // 0 until first connection
	LastHeartbeatAt       int64           // 0 until first heartbeat
	CreatedAt             int64
	ActivatedAt           int64 // 0 until state advances to Active
	EnteredStateAt        int64 // timestamp when State was last entered

	// Federation / tenancy reservation columns per m1.5-tickets §T10.
	// All three are "" / NULL in M1.5 demo deployments.
	HostActorID     string   // M1.4 channel-as-actor: which channel-local actor exposes this channel externally
	FederatedOrigin string   // M2+ federation: remote origin this channel mirrors (empty for native channels)
	TenantID        TenantID // M2+ multi-tenant scope; "" / "default" in demo
}

// CreateChannelRequest is the payload of `control.create_channel` frame
// per proto-foundation §3.3.3 Phase 1 + impl-layer2 §3.2.1. The server
// reserves the placement row in state='creating' with owner_epoch=0 and
// an empty fencing_token, then pushes this frame to the candidate
// daemon. The daemon is the trust root for fencing — it generates a
// fresh unguessable fencing_token and sets owner_epoch=1 during Phase 2
// bootstrap, and returns those values to the server in
// CreateChannelAck (§3.2.2). This struct therefore MUST NOT carry
// fencing_token / owner_epoch / connection_epoch_expected — those are
// daemon-side obligations.
type CreateChannelRequest struct {
	ChannelID       channel.ID      `json:"channel_id"`
	CreateRequestID CreateRequestID `json:"create_request_id"`
	// InitialMembers carries the channel's bootstrap members so daemon
	// can populate actor_registry in the same bootstrap saga (T1.9 / L2
	// §12.1). Each entry is an opaque JSON object the daemon decodes
	// against its own schema.
	InitialMembers []InitialMember `json:"initial_members,omitempty"`
	// ChannelType carries the L4 channel-template key (catalog.Channel.Type)
	// the server resolved at reserve time — typically `"group"` (no
	// template) or `"xhs-creator"` (M1.6-T5 first template). The daemon
	// uses it to look up the per-type ChannelTemplate (actor seeds /
	// workdir subdirs / domain prompt) so the bootstrap saga can specialise
	// the new channel without touching the envelope schema.
	//
	// Empty string is treated as the legacy "no template" path — the
	// saga only seeds system + initial members and OnChannelBoot does
	// not install any domain adapters. This preserves backward
	// compatibility for pre-M1.6-T5 daemons.
	//
	// TODO: impl-layer2 §3.2.1 lists only initial_members/initial_types/
	// initial_config/metadata as wire fields; channel_type should fold
	// into one of those (likely initial_config) in a follow-up.
	ChannelType string `json:"channel_type,omitempty"`
}

// InitialMember mirrors one `initial_members[*]` entry per L2 §12.1.
type InitialMember struct {
	UserID        UserID        `json:"user_id"`
	MemberActorID actor.ActorID `json:"member_actor_id"`
	Kind          actor.Kind    `json:"kind"` // 'human' for L2 §12.1 channel.create flow
	DisplayName   string        `json:"display_name"`
	Role          string        `json:"role"` // 'owner' | 'member'
}

// AckStatus is the closed status set inside CreateChannelAck (L2
// §1.4.11.3 step 4 — `status: bound | rejected`).
type AckStatus string

const (
	AckBound    AckStatus = "bound"
	AckRejected AckStatus = "rejected"
)

// CreateChannelAck is the payload of `control.create_channel_ack` frame
// per proto-foundation §3.3.3 Phase 2 + impl-layer2 §3.2.2. The daemon
// is the trust root for fencing: it generates a fresh OwnerEpoch=1 and
// an unguessable FencingToken during bootstrap and returns them inside
// this ack. The server's Phase 3 CAS reads OwnerEpoch / FencingToken /
// DaemonID FROM the ack and writes them into the placement row using
// `(channel_id, create_request_id, state='creating')` as the CAS
// predicate.
//
// Pre-check (Match) only verifies the saga identifiers
// (channel_id + create_request_id) plus the candidate daemon — the
// fencing tuple is the daemon's authoritative output and not a
// comparison input.
type CreateChannelAck struct {
	FrameID         string          `json:"frame_id"`
	ChannelID       channel.ID      `json:"channel_id"`
	CreateRequestID CreateRequestID `json:"create_request_id"`
	OwnerEpoch      OwnerEpoch      `json:"owner_epoch"`
	FencingToken    FencingToken    `json:"fencing_token"`
	DaemonID        DaemonID        `json:"daemon_id"`
	DaemonEpoch     DaemonEpoch     `json:"daemon_epoch"`
	Status          AckStatus       `json:"status"`
	Reason          string          `json:"reason,omitempty"`
}

// Match verifies the ACK targets the same saga the server reserved for
// — channel_id + create_request_id + daemon_id must match the placement
// candidate. OwnerEpoch / FencingToken are daemon-side outputs (the
// daemon is the trust root for fencing — proto-foundation §3.3.3
// Phase 2) and therefore MUST NOT be checked here; they are written
// into the placement row by the Phase 3 CAS, not compared against a
// pre-existing value.
//
// Used as a fast pre-check before hitting sqlite; tests use it to
// assert the saga-identifier match rule.
func (a CreateChannelAck) Match(p Placement) bool {
	return a.ChannelID == p.ChannelID &&
		a.CreateRequestID == p.CreateRequestID &&
		a.DaemonID == p.DaemonID
}

// ReclaimRequest mirrors `control.daemon_reclaim` payload per L2
// §1.4.11.4 — daemon reports the channels it claims to still own after
// reconnecting. server validates each row against the placement table.
type ReclaimRequest struct {
	DaemonID    DaemonID         `json:"daemon_id"`
	DaemonEpoch DaemonEpoch      `json:"daemon_epoch"`
	Channels    []ReclaimChannel `json:"channels"`
}

// ReclaimChannel is one entry in ReclaimRequest.Channels — the daemon
// reports the (channel_id, fencing_token, owner_epoch) triple it has on
// disk. Server validates against the placement record.
type ReclaimChannel struct {
	ChannelID    channel.ID   `json:"channel_id"`
	FencingToken FencingToken `json:"fencing_token"`
	OwnerEpoch   OwnerEpoch   `json:"owner_epoch"`
}

// ReclaimDecision is server's per-channel response to a reclaim request
// (L2 §1.4.11.4 step 2 — accepted vs rejected).
type ReclaimDecision struct {
	ChannelID channel.ID `json:"channel_id"`
	Accepted  bool       `json:"accepted"`
	Reason    string     `json:"reason,omitempty"` // populated only when Accepted == false
}

// Store is the server-side placement store contract (L2 §1.4.11.1
// channel_placements 表). server/placements implements it on top of
// sqlite; tests can wire an in-memory implementation.
type Store interface {
	// Reserve performs the L2 §1.4.11.3 step 1 INSERT — INSERT a new
	// placement row in StateCreating with caller-provided values.
	// Returns the resulting Placement on success.
	//
	// Returns an error wrapping placement-already-exists when the
	// channel_id PRIMARY KEY collides — caller may inspect via
	// errors.Is(err, ErrPlacementExists).
	Reserve(ctx context.Context, p Placement) (Placement, error)

	// Get returns the placement row for channelID (ok=false when
	// missing). Used by reclaim path + reconcile loop.
	Get(ctx context.Context, channelID channel.ID) (Placement, bool, error)

	// CASActivate runs the L2 §1.4.11.3 step 5 SQL — UPDATE state to
	// Active iff every field in the WHERE clause matches the ACK.
	// Returns ok=true when the CAS succeeded; ok=false (no error)
	// when the CAS lost (caller should treat as "ACK rejected, leave
	// reconcile loop to advance to orphan").
	CASActivate(
		ctx context.Context,
		ack CreateChannelAck,
		newConnectionEpoch ConnectionEpoch,
		nowMs int64,
	) (ok bool, err error)

	// MarkStale transitions an active placement to Stale (used by
	// reconcile loop on heartbeat timeout — L2 §11.5). Idempotent.
	MarkStale(ctx context.Context, channelID channel.ID, nowMs int64) error

	// MarkOrphan transitions a creating placement to Orphan on create
	// timeout (L2 §11.5). Idempotent.
	MarkOrphan(ctx context.Context, channelID channel.ID, nowMs int64) error

	// AcceptReclaim updates daemon_connection_epoch + last_heartbeat_at
	// when a daemon's reclaim is accepted by the server (L2 §1.4.11.4
	// step 2 accepted branch). Returns ok=false when validation fails
	// (caller should reject reclaim and let stale/orphan reconcile
	// advance the row).
	//
	// daemonID is the WS-authenticated owner identifier from
	// Connection.DaemonID — the SQL CAS pins it into the WHERE clause
	// alongside (channel_id, owner_epoch, fencing_token) so a different
	// daemon presenting the same (epoch, token) tuple cannot hijack
	// ownership (FIX-T4 / L2 §1.4.11.4 invariant — covers spec
	// requirement T1.4 "AcceptReclaim matches daemon_id,
	// fencing_token, owner_epoch").
	AcceptReclaim(
		ctx context.Context,
		channelID channel.ID,
		daemonID DaemonID,
		req ReclaimChannel,
		newConnectionEpoch ConnectionEpoch,
		nowMs int64,
	) (ok bool, err error)
}

// ErrPlacementExists is returned by Store.Reserve when the channel_id
// already has a placement row. Sentinel so callers can errors.Is on it
// without string matching.
type ErrPlacementExists struct {
	ChannelID channel.ID
}

// Error implements the error interface.
func (e *ErrPlacementExists) Error() string {
	return "placement already exists for channel " + string(e.ChannelID)
}

// CanTransition reports whether a transition from `from` to `to` is
// legal per the L2 §1.4.11.2 state machine. Used by tests to assert the
// full transition matrix (see kernel/placement/state_machine_test.go).
func CanTransition(from, to State) bool {
	switch from {
	case "":
		// Empty (∅) → creating only (server reserve).
		return to == StateCreating
	case StateCreating:
		// creating → active (ACK match) | orphan (create_timeout).
		return to == StateActive || to == StateOrphan
	case StateActive:
		// active → stale (heartbeat timeout) | ∅ (server unbind + ACK).
		return to == StateStale || to == ""
	case StateOrphan:
		// orphan → creating (retry new daemon, owner_epoch+1).
		return to == StateCreating
	case StateStale:
		// stale → active (original daemon reclaim, fencing match) |
		// orphan (stale_timeout — M2+ migration trigger).
		return to == StateActive || to == StateOrphan
	}
	return false
}
