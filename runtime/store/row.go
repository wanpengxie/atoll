package store

import "github.com/wanpengxie/ActOS/kernel/message"

// StoredRow wraps a protocol Envelope (17 content+metadata fields) with the
// store-derived columns that kernel deliberately keeps OUT of the pure
// Envelope (kernel-construction-spec §1.2 / target-state §3.7). The channel
// messages table persists Envelope ⊕ these columns; read paths return
// StoredRow, write paths take *message.Envelope and the store allocates seq /
// computes is_terminal / canonical_hash inside the append transaction.
//
// (DeliveredAt / LastError stay on message.Envelope — they are L0 §2.5
// delivery metadata, part of the wire envelope, not store-derived.)
type StoredRow struct {
	Envelope message.Envelope

	// Seq is the store-allocated monotonic position (messages.seq).
	Seq int64
	// IsTerminal mirrors the is_terminal column (computed from
	// payload.status at insert, proto-layer0 §2.5.1).
	IsTerminal bool
	// CanonicalHash is the StepDedupe hash over the sender-provided
	// envelope (pre-normalize), stored for O(1) retry dedupe.
	CanonicalHash string
	// Attempts / DeliveryFailedAt are delivery-retry scheduling
	// diagnostics (L1 §6.1 backoff gate).
	Attempts         int64
	DeliveryFailedAt *int64
}
