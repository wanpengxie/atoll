// Package log declares the channel-local message append-only log
// contract (L2 §1.4.1 messages 表 + L2 §1.4.4 atomic primitives) and the
// Seq / Cursor types that drive cursor advancement (L1 §6.3.4 / L2
// §1.4.3 actor_cursors).
package log

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/actor"
)

// Seq is the messages.seq column — store-allocated monotonic per-channel
// sequence (L2 §1.4.1 PRIMARY KEY AUTOINCREMENT). Type alias makes
// arithmetic / ordering checks intent-clear without sacrificing int64
// efficiency.
type Seq int64

// Cursor mirrors actor_cursors row (L2 §1.4.3). Position metric is
// LastConsumedSeq — `LastConsumedID` is informational only.
type Cursor struct {
	ActorID         actor.ActorID
	LastConsumedSeq Seq
	LastConsumedID  string // informational; ordering uses Seq
	UpdatedAt       int64
}

// Cursors is the actor_cursors query / mutation contract.
//
// Concrete sqlite implementation lives in runtime/store/actors.go (T3)
// and MUST implement the L1 §6.3.4.3 monotonic CAS rule for Advance.
type Cursors interface {
	// Get returns the cursor for actorID. Returns ok=false when the
	// row is missing (caller may treat as cursor 0). Existence is
	// guaranteed for actors that went through actor_registry Insert
	// (L2 §1.4.6 invariant — same-tx seed).
	Get(ctx context.Context, actorID actor.ActorID) (Cursor, bool, error)

	// Advance pushes the cursor forward via monotonic CAS (L1 §6.3.4.3
	// — store rejects newSeq <= currentSeq silently). Returns ok=true
	// when the CAS succeeded; ok=false (no error) when the CAS lost
	// because another writer already advanced past newSeq.
	Advance(
		ctx context.Context,
		actorID actor.ActorID,
		newSeq Seq,
		newID string,
		nowMs int64,
	) (ok bool, err error)
}
