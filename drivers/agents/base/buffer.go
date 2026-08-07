package base

import (
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/message"
)

const (
	defaultBufferMaxCount = 128
	defaultBufferMaxBytes = 8 << 20
	defaultBatchMaxCount  = 32
)

type requestItem struct {
	msg         actorbase.Msg
	trigger     Trigger
	bytes       int
	explicitCAS bool
	closed      bool
}

func newRequestItem(msg actorbase.Msg, index int) *requestItem {
	return &requestItem{
		msg: msg,
		trigger: Trigger{
			Envelope:      envelopeFromMsg(msg),
			CorrelationID: behavior.CorrelationID(msg.CorrelationID, msg.ID),
			Index:         index,
		},
		bytes: len(msg.Payload),
	}
}

type requestBuffer struct {
	items    []*requestItem
	bytes    int
	maxCount int
	maxBytes int
}

func (b *requestBuffer) push(item *requestItem) bool {
	if len(b.items)+1 > b.maxCount || b.bytes+item.bytes > b.maxBytes {
		return false
	}
	b.items = append(b.items, item)
	b.bytes += item.bytes
	return true
}

func (b *requestBuffer) remove(id string) *requestItem {
	for i, item := range b.items {
		if string(item.msg.ID) != id {
			continue
		}
		b.items = append(b.items[:i], b.items[i+1:]...)
		b.bytes -= item.bytes
		return item
	}
	return nil
}

// popBatch consumes one consecutive same-sender batch. Closed entries are
// discarded before grouping; a different sender is a hard batch boundary.
func (b *requestBuffer) popBatch(max int) []*requestItem {
	for len(b.items) > 0 && b.items[0].closed {
		b.bytes -= b.items[0].bytes
		b.items = b.items[1:]
	}
	if len(b.items) == 0 {
		return nil
	}
	sender := b.items[0].msg.Sender.ID
	n := 0
	for n < len(b.items) && n < max && b.items[n].msg.Sender.ID == sender {
		if b.items[n].closed {
			break
		}
		n++
	}
	out := append([]*requestItem(nil), b.items[:n]...)
	for _, item := range out {
		b.bytes -= item.bytes
	}
	b.items = b.items[n:]
	return out
}

func envelopeFromMsg(m actorbase.Msg) (env message.Envelope) {
	env.ID, env.TS, env.ChannelID, env.Sender = m.ID, m.TS, m.ChannelID, m.Sender
	env.Kind, env.Type, env.Payload = m.Kind, m.Type, m.Payload
	env.ParentID, env.CorrelationID = m.ParentID, m.CorrelationID
	env.Visibility, env.Audience, env.ExpiresAt = m.Visibility, m.Audience, m.ExpiresAt
	return env
}
