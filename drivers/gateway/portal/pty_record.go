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

// TerminalSessionType marks a session's own lifecycle rows.
//
// 开会话是这条线上**唯一一次真判权动作**（design §7：判权粒度恒是"开不开
// 会话"）。若账本只记被授权之后的命令、而漏掉授权本身，那份记录就答不出
// "谁在何时被允许开了这个终端"——恒不可只留下行为而抹掉许可。
const TerminalSessionType = "terminal.session"

// NewRecorder returns the Recorder the terminal manager writes rows through.
//
// The row is delivered through the HUMAN'S OWN subject slot — the same path a
// message they typed takes. That is the whole mechanism behind the claim this
// line exists to make: the sender is stamped by the runtime from the slot it
// came out of, never filled in by this code (terminal-line-design.md §4.2).
// A row that pretended to be authored by someone else would be unforgeable
// here even if this function tried.
func NewRecorder(acquire func(context.Context, channel.ID) (channelhost.Bundle, error)) terminal.Recorder {
	return func(ctx context.Context, chID channel.ID, caller actor.ActorID, rec terminal.Record) string {
		if acquire == nil {
			return ""
		}
		bundle, err := acquire(ctx, chID)
		if err != nil {
			return ""
		}
		slot, ok := bundle.Gateway().SubjectSlotFor(caller)
		if !ok {
			return ""
		}
		raw, err := json.Marshal(rec)
		if err != nil {
			return ""
		}
		id := uuid.NewString()
		msgType := TerminalCommandType
		if rec.Event != "" {
			msgType = TerminalSessionType
		}
		frame, err := subjectgate.NewFrame(subjectgate.FrameSubmit, uuid.NewString(), subjectgate.SubmitPayload{
			ChannelID: string(chID),
			ID:        id,
			MsgType:   msgType,
			Kind:      string(message.KindEvent),
			// 挂到会话的开启行。humancell 恒会拿它去账本核实——查不到就拒，
			// 故这里恒不可能凭空造出一棵树来（cause 恒不采信客户端的话）。
			ParentID:   rec.Parent,
			Visibility: string(message.VisibilityPublic),
			Payload:    raw,
		})
		if err != nil {
			return ""
		}
		// Best effort by design: a terminal must not stall because the ledger
		// is momentarily busy. A dropped row is a gap in the record, never a
		// hang in the shell — the stream half恒不需精准 (§4.3) and the
		// command half恒不得阻塞人的手.
		if _, err := slot.Deliver(ctx, frame); err != nil {
			return ""
		}
		return id
	}
}
