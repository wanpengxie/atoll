package actorcaps

import (
	"context"

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

// LedgerView is the read-only ledger capability supplied to an actor. Session
// returns an ancestor-expanded session prefix; Tail follows the channel log.
type LedgerView interface {
	Session(context.Context, string, message.ID) ([]LedgerRow, error)
	Tail(context.Context, int64, int) ([]LedgerRow, int64, error)
}
