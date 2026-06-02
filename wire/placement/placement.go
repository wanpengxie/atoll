// Package placement is the actor↔compute assignment + lease contract (v2). It
// is where v2 fencing lives: each business actor has ≤1 host compute, arbitrated
// by an actor-host lease (single-writer + fencing, Kleppmann/k8s node-lease —
// NOT consensus). Pure schema (kernel only). Replaces v1 channel-level
// placement-saga/reclaim (collapsed: the channel home is fixed at the server;
// only per-actor host slots are leased).
package placement

import (
	"github.com/wanpengxie/ActOS/kernel/actor"
)

// HostToken is the opaque fencing guard for one actor-host slot. A stale token
// (from a superseded compute) is rejected by the home when it stamps an emit.
type HostToken string

// Lease binds an actor to its current host compute for a bounded term. A fresh
// Heartbeat re-arms ExpiresAtMs; expiry frees the slot for reassignment.
type Lease struct {
	Actor       actor.ActorID
	ComputeID   string
	Token       HostToken
	ExpiresAtMs int64
}

// Assignment is the home's decision to place an actor on a compute.
type Assignment struct {
	Actor     actor.ActorID
	ComputeID string
	Token     HostToken
}

// IsFresh reports whether the lease is still valid at nowMs.
func (l Lease) IsFresh(nowMs int64) bool { return nowMs < l.ExpiresAtMs }
