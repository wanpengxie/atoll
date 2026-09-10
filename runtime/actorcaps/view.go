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

// LedgerRead limits the entire storage scan, including rows outside Session.
// Zero limits use the hard defaults; callers can only lower them.
// Session selects own rows without expanding ancestors. Empty selects all rows.
type LedgerRead struct {
	Session  string
	MaxRows  int
	MaxBytes int
}

const MaxLedgerRows = 4096
const MaxLedgerBytes = 16 << 20

var ErrLedgerLimit = errors.New("ledger_read_limit_exceeded")

type LedgerSnapshot struct {
	Rows    []LedgerRow
	HeadSeq int64
}

// LedgerView is the read-only ledger capability supplied to an actor. Session
// returns an ancestor-expanded session prefix; Tail follows the channel log.
type LedgerView interface {
	// Read returns a complete prefix at one head, or an error with no partial rows.
	Read(context.Context, LedgerRead) (LedgerSnapshot, error)
	Session(context.Context, string, message.ID) ([]LedgerRow, error)
	// BuildSession projects an existing snapshot without another history read.
	// The supplied platform View implements session semantics.
	BuildSession(context.Context, LedgerSnapshot, string, message.ID) ([]LedgerRow, error)
	Tail(context.Context, int64, int) ([]LedgerRow, int64, error)
}
