package ledger

import "context"

// Reservation is the result of a successful Reserve call. Holding a
// Reservation grants the caller the right to commit (or roll back) the
// message-id slot identified by Key.
//
// TODO(T1): expand with deadline + canonical_hash field once the L2
// §1.4.10 spec migration lands.
type Reservation struct {
	Key     Key
	HoldsAt int64
}

// ActionLedger is the channel-local idempotency ledger (L2 §1.4.10).
//
// Reserve takes the caller-supplied ledger_key and either:
//
//   - returns (Reservation, false, nil) — new key, slot held; caller
//     proceeds with harness + write
//   - returns (Reservation, true, nil) — duplicate; the existing
//     committed envelope's message_id is in the Reservation's metadata
//     and caller MUST return the original response (idempotent replay)
//   - returns (_, _, err) — IO error (treated as transient)
//
// Commit promotes a Reservation to a permanent ledger row pinned to the
// concrete message_id.
//
// kernel/ledger only defines the contract; sqlite-backed implementation
// lives in runtime/store per T3.
type ActionLedger interface {
	Reserve(ctx context.Context, key Key) (Reservation, bool, error)
	Commit(ctx context.Context, r Reservation, messageID string) error
	Rollback(ctx context.Context, r Reservation) error
}
