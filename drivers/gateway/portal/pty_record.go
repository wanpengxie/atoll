package portal

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/platform/terminal"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

// TerminalCommandType is the ledger type for one command run in a terminal.
//
// It is an EVENT, not a request: nobody is being asked to do anything, the
// command already ran. Events carry no deadline and expect no reply.
const TerminalCommandType = "terminal.command"

// NewRecorder returns the Recorder the terminal manager writes rows through.
//
// The row is delivered through the HUMAN'S OWN subject slot — the same path a
// message they typed takes. That is the whole mechanism behind the claim this
// line exists to make: the sender is stamped by the runtime from the slot it
// came out of, never filled in by this code (terminal-line-design.md §4.2).
// A row that pretended to be authored by someone else would be unforgeable
// here even if this function tried.
func NewRecorder(acquire func(context.Context, channel.ID) (channelhost.Bundle, error)) terminal.Recorder {
	return func(ctx context.Context, chID channel.ID, caller actor.ActorID, rec terminal.Record) {
		if acquire == nil {
			return
		}
		bundle, err := acquire(ctx, chID)
		if err != nil {
			return
		}
		slot, ok := bundle.Gateway().SubjectSlotFor(caller)
		if !ok {
			return
		}
		raw, err := json.Marshal(rec)
		if err != nil {
			return
		}
		frame, err := subjectgate.NewFrame(subjectgate.FrameSubmit, uuid.NewString(), subjectgate.SubmitPayload{
			ChannelID:  string(chID),
			ID:         uuid.NewString(),
			MsgType:    TerminalCommandType,
			Kind:       string(message.KindEvent),
			Visibility: string(message.VisibilityPublic),
			Payload:    raw,
		})
		if err != nil {
			return
		}
		// Best effort by design: a terminal must not stall because the ledger
		// is momentarily busy. A dropped row is a gap in the record, never a
		// hang in the shell — the stream half恒不需精准 (§4.3) and the
		// command half恒不得阻塞人的手.
		_, _ = slot.Deliver(ctx, frame)
	}
}
