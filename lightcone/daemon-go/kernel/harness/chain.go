package harness

import (
	"context"

	"github.com/coagent-ai/daemon-go/kernel/message"
)

// Chain runs an ordered list of Step values against an in-flight
// envelope (L1 §10 — 9-step Message-Write Harness).
//
// kernel/harness owns the contract; the concrete 9-step composition
// (auth → required-field → kind → sender → schema → audience → terminal
// dedupe → fencing → message-id-conflict) lives in runtime per T3.
type Chain interface {
	Run(ctx context.Context, env *message.Envelope) (*message.Envelope, *RejectError, error)
}
