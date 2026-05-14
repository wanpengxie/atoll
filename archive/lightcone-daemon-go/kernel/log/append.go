package log

import (
	"context"

	"github.com/coagent-ai/daemon-go/kernel/message"
)

// MessageLog is the append-only channel-local log (L2 §1.4.1).
//
// Append writes one envelope atomically, allocating a new monotonic
// Seq. Implementations MUST guarantee uniqueness on (channel_id,
// message_id) and reject duplicate writes via the
// HarnessMessageIDConflict path (caller-facing).
//
// FindByID / FindParent / FindTerminalResponse are read-side accessors
// used by harness steps and the long-pending scheduler.
//
// kernel/log only defines the contract; the sqlite-backed
// implementation lives in runtime/store per T3.
type MessageLog interface {
	Append(ctx context.Context, env *message.Envelope) (Seq, error)
	FindByID(ctx context.Context, id string) (*message.Envelope, error)
	FindParent(ctx context.Context, id string) (*message.Envelope, error)
	FindTerminalResponse(ctx context.Context, parentID string) (*message.Envelope, error)
}
