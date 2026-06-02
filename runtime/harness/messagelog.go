package harness

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/log"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/fence"
)

// FencingTuple is the explicit daemon ownership token a channel-local
// MessageLog append must present when the concrete store enforces fence.
// A zero tuple means "no fencing supplied"; unfenced test stores may ignore
// it, but fenced stores reject it as stale rather than reading hidden state
// from context.Context.
//
// Lives in runtime/harness (the consumer that owns the write chain) rather
// than kernel/log: it is engine write-path state, not a protocol type, and
// keeping the MessageLog contract here avoids a runtime/store ↔ runtime/harness
// import cycle (store implements harness.MessageLog; harness never imports
// store).
type FencingTuple struct {
	Token fence.FencingToken
	Epoch fence.DaemonEpoch
}

// MessageLog is the channel-local messages-table append contract (L2 §1.4.1).
// Append is the only mutation entry point; reads are not declared as the sole
// path because L2 §2.2 allows agents / scheduler / trigger gateway to read
// messages directly via sqlite query.
//
// Concrete sqlite implementation lives in runtime/store/messages.go and MUST
// enforce the L2 §1.4.1 invariants (id UNIQUE; parent_id terminal-response
// UNIQUE INDEX; same-transaction is_terminal computation). The engine append
// ACL (L2 §1.4.5) is layered ABOVE this interface — only the harness chain may
// call Append.
type MessageLog interface {
	// Append writes the envelope to the channel-local messages table.
	// Returns *log.AppendError for protocol-level rejects (terminal
	// duplicate / id conflict), or a generic error for IO failures. On
	// success the implementation MAY patch env.Seq + env.IsTerminal +
	// env.TSReceived in-place.
	Append(ctx context.Context, env *message.Envelope, fencing FencingTuple) (log.AppendResult, error)

	// FindByID returns the row identified by envelope.id, or ok=false when
	// no such row exists.
	FindByID(ctx context.Context, channelID channel.ID, id message.ID) (message.Envelope, bool, error)

	// LookupCanonicalHash returns the stored canonical_hash of the row
	// identified by envelope.id (ok=false when absent). Used by StepDedupe.
	LookupCanonicalHash(ctx context.Context, channelID channel.ID, id message.ID) (hash string, ok bool, err error)

	// HasFinalResponse reports whether the channel log already contains a
	// kind=response row pointing at parentID with payload.status in the
	// Layer 1 final closed set. Non-authoritative pre-check for Step 8.
	HasFinalResponse(ctx context.Context, channelID channel.ID, parentID message.ID) (bool, error)

	// FinalResponseSender returns the sender.id of the existing Layer 1 final
	// response for parentID (ok=false when none). Used by Step 8 to tell a
	// caller self-close (unanswered_timeout) from a genuine receiver final so
	// a LATE receiver final can be rewritten to a response.late_final
	// observability event (closure-revision Δ4) rather than rejected.
	FinalResponseSender(ctx context.Context, channelID channel.ID, parentID message.ID) (actor.ActorID, bool, error)
}
