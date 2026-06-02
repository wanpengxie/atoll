package storespec

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/ledger"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// Status mirrors the action_ledger.status column (L2 §1.4.10.1).
type Status string

const (
	StatusReserved  Status = "reserved"
	StatusCommitted Status = "committed"
)

// TurnID is the idempotency turn identifier persisted in action_ledger.
type TurnID string

// String returns the wire form.
func (t TurnID) String() string { return string(t) }

// Entry mirrors the action_ledger row schema (L2 §1.4.10.1). The ledger key
// type + derivation stay in kernel/ledger (pure); the stateful row + contract
// live here.
type Entry struct {
	Key         ledger.Key
	TurnID      TurnID
	ActorID     actor.ActorID
	EnvelopeID  message.ID
	Status      Status
	ReservedAt  int64
	CommittedAt int64 // 0 until Commit
}

// Ledger is the action_ledger reserve/commit two-phase contract (L2
// §1.4.10.1). Impl in runtime/store/ledger.go.
type Ledger interface {
	Find(ctx context.Context, key ledger.Key) (Entry, bool, error)
	Reserve(ctx context.Context, e Entry) (Entry, error)
	Commit(ctx context.Context, key ledger.Key, committedAt int64) error
}
