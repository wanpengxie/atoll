// Package actor holds the v4/v5 actor identity model:
//
//   - SenderKind closed-set enum (L0 §2.3)
//   - Sender struct (envelope.sender object)
//   - ActorId scalar type and naming convention
//   - ActorRegistry interface (in-memory abstraction; sqlite backend
//     lives in runtime/store per T3)
//
// kernel/actor MUST stay free of IO. T1 will land the full type
// definitions migrated out of pkg/v4types; this file is the T2
// skeleton.
package actor

// SenderKind is the actor physical-position classifier on
// envelope.sender.kind. Authoritative spec: L0 §2.3 — 4-value closed
// set.
//
// TODO(T1): mirror pkg/v4types.SenderKind here once spec migration
// lands; pkg/v4types will re-export this type via type alias.
type SenderKind string

// SenderKind enum — closed set per L0 §2.3.
const (
	SenderHuman  SenderKind = "human"
	SenderAgent  SenderKind = "agent"
	SenderSystem SenderKind = "system"
	SenderTool   SenderKind = "tool"
)

// Sender is the nested sender object inside an envelope.
//
// TODO(T1): expand to spec-final shape and align canonical hash inputs.
type Sender struct {
	Kind SenderKind `json:"kind"`
	ID   string     `json:"id"`
	Name string     `json:"name"`
}
