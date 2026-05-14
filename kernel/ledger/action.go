package ledger

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/actor"
)

// Status mirrors the action_ledger.status column from L2 §1.4.10.1. Two
// states only: reserved (envelope.id allocated, harness write pending /
// in-progress) and committed (harness write returned ok).
type Status string

const (
	StatusReserved  Status = "reserved"
	StatusCommitted Status = "committed"
)

// Entry mirrors the action_ledger row schema from L2 §1.4.10.1.
type Entry struct {
	Key         Key
	TurnID      string
	ActorID     actor.ActorID
	EnvelopeID  string
	Status      Status
	ReservedAt  int64
	CommittedAt int64 // 0 until Commit; non-zero after harness append succeeds
}

// Ledger is the action_ledger reserve/commit two-phase contract from
// L2 §1.4.10.1. Implementations live in runtime/store/ledger.go and
// must back the table in the same channel sqlite as messages so the
// reserve↔commit↔harness-append all share one transaction.
type Ledger interface {
	// Find looks up an entry by ledger key. Returns ok=false if the key
	// has never been reserved. Used by the L2 §1.4.10.1 Phase 1 reserve
	// branch ("existing → reuse envelope_id").
	Find(ctx context.Context, key Key) (Entry, bool, error)

	// Reserve atomically allocates a new entry (status = reserved) IF
	// the key is not already present. Returns the resulting entry —
	// when the key already existed, the existing row is returned
	// unchanged (idempotent reserve).
	//
	// Caller is responsible for picking envelopeID before calling
	// Reserve (per L2 §1.4.10.1 — caller decides envelope.id outside
	// the ledger so the ledger row can record it).
	Reserve(ctx context.Context, e Entry) (Entry, error)

	// Commit transitions a reserved entry to committed status. Idempotent:
	// committing an already-committed key returns nil. Committing a key
	// that does not exist returns an error (caller bug).
	Commit(ctx context.Context, key Key, committedAt int64) error
}
