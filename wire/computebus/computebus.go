// Package computebus is the home↔compute wire contract (v2): the carrier
// protocol by which an attached compute (daemon) connects to the channel home
// (server), receives dispatched envelopes for the business cells it hosts, and
// emits their output back up to the home harness. Pure schema — depends only on
// kernel. Replaces the v1 daemonbus/viewsync/devicetransit split (collapsed:
// server IS truth, one home↔compute hop).
package computebus

import (
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// AttachDeclaration is the actor identity a compute ships on attach so the home
// can register the actor into membership (the compute holds NO truth —
// registration is home-side). Type catalog is domain, not wire (type non-first-class).
type AttachDeclaration struct {
	ActorID actor.ActorID
	Kind    actor.Kind
	Binding actor.Binding
}

// AttachRequest is sent by a compute to join a channel home (lightcone-style:
// one api-key, one URL). The home authenticates the key and records the
// actor→compute assignment.
type AttachRequest struct {
	APIKey       string
	ComputeID    string
	Declarations []AttachDeclaration
}

// AttachReply confirms the attach and carries the assigned channel.
type AttachReply struct {
	ChannelID channel.ID
	Accepted  bool
	Reason    string
}

// Heartbeat is a keepalive probe (not a fencing-lease). Missed heartbeats →
// readLoop EOF → death (second source).
type Heartbeat struct {
	ComputeID string
	// Present lists the actor ids whose cells are live on this compute.
	Present []actor.ActorID
}

// DispatchFrame carries one envelope DOWN from the home harness to a compute's
// hosted cell mailbox.
type DispatchFrame struct {
	Target   actor.ActorID
	Envelope *message.Envelope
}

// EmitFrame carries one envelope UP from a compute cell to the home harness to
// be written into channel truth.
type EmitFrame struct {
	Source   actor.ActorID
	Envelope *message.Envelope
}

// DeathFrame propagates a cell death (DOWN) from compute to home so the
// caller's caller-scoped closure collapses to receiver_unavailable across the
// wire (redesign §5 — death signal crosses the home↔compute boundary).
type DeathFrame struct {
	Actor actor.ActorID
	Cause string
}
