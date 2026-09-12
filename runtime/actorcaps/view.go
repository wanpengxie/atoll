package actorcaps

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/protocol/message"
)

// LedgerRow is the actor-facing projection of a stored message. The storage
// contract stays behind the assembly root; actors receive only the immutable
// envelope and its ledger ordering facts.
type LedgerRow struct {
	Envelope   message.Envelope
	Seq        int64
	IsTerminal bool
}

// LedgerRead limits one complete snapshot read. Zero limits use the hard
// defaults; callers can only lower them.
type LedgerRead struct {
	// Session is retained only for the migration to consumer-owned projection.
	// It is removed with the driver step.
	Session  string
	MaxRows  int
	MaxBytes int
}

const MaxLedgerRows = 4096
const MaxLedgerBytes = 16 << 20
const MaxVisiblePageRows = 256

var ErrLedgerLimit = errors.New("ledger_read_limit_exceeded")

type LedgerSnapshot struct {
	Rows    []LedgerRow
	HeadSeq int64
}

// LedgerView is the read-only channel-ledger capability supplied to an actor.
// It exposes storage-neutral snapshots and visible cursor pages only; session
// and turn projection are consumer policy layered above this capability.
type LedgerView interface {
	// Read returns a complete prefix at one head, or an error with no partial rows.
	Read(context.Context, LedgerRead) (LedgerSnapshot, error)
	ReadVisibleAfterSeq(context.Context, int64, int) ([]LedgerRow, int64, error)
	ReadVisibleBeforeSeq(context.Context, int64, int) ([]LedgerRow, int64, bool, error)
	// Transitional methods removed once session projection moves to agentbase.
	Session(context.Context, string, message.ID) ([]LedgerRow, error)
	BuildSession(context.Context, LedgerSnapshot, string, message.ID) ([]LedgerRow, error)
	Tail(context.Context, int64, int) ([]LedgerRow, int64, error)
}
