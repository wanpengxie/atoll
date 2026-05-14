// Package actor defines the channel-actor identity model: ActorID type,
// Sender re-export, ActorRegistry interface (channel-local query
// contract per L1 §12).
//
// `actor_registry` is a channel-local control-plane store table; this
// package only declares the **interface** kernel-level callers (harness,
// scheduler, type_registry install) depend on. The sqlite implementation
// lives in runtime/store/actors.go (T3) and the federation/postgres
// alternative will live in runtime/store/<other>.go — neither imported
// here.
package actor

// ActorID is the channel-local sender identifier. It is identical to
// envelope `sender.id` (L0 §2.1 — same namespace, see L1 §3.2).
//
// Naming convention reference (informative — non-normative; L1 §3.2):
//
//	user:<short-id>      - human members
//	system               - the channel-local system actor (fixed id)
//	agent:<role-name>    - channel agents / sub-agents
//	tool:<adapter-name>  - tool/adapter actors (in_process /
//	                       outbound_http / via_server_transit per
//	                       L1 §11.7)
//
// Cross-channel uniqueness is NOT guaranteed; mapping a real-world user
// to multiple channels is out of scope for the channel-local registry
// (L1 §12.5).
type ActorID string

// Well-known fixed actor ids per L1 §3.2.
const (
	// SystemActorID is the channel-local system actor — every channel
	// has exactly one, written by the bootstrap saga (L2 §1.4.7 step 3).
	SystemActorID ActorID = "system"
)

// String returns the wire form of the actor id.
func (a ActorID) String() string { return string(a) }
