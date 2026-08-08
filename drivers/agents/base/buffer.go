package base

import (
	"encoding/json"
	"strings"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
)

const (
	defaultBufferMaxCount = 128
	defaultBufferMaxBytes = 8 << 20
	defaultBatchMaxCount  = 32
)

type requestItem struct {
	msg          actorbase.Msg
	input        RuntimeInput
	scope        EffectScope
	bytes        int
	explicitCAS  bool
	expectedTurn TurnID
	closed       bool
}

func newRequestItem(msg actorbase.Msg) *requestItem {
	corr := behavior.CorrelationID(msg.CorrelationID, msg.ID)
	return &requestItem{msg: msg, input: RuntimeInput{SourceID: string(msg.ID), Type: msg.Type, Sender: string(msg.Sender.ID), Payload: append(json.RawMessage(nil), msg.Payload...), Text: messageText(msg.Payload)}, scope: NewEffectScope(string(msg.ID), string(corr)), bytes: len(msg.Payload)}
}
func messageText(raw json.RawMessage) string {
	var p struct {
		Text *string `json:"text"`
	}
	if json.Unmarshal(raw, &p) == nil && p.Text != nil {
		return *p.Text
	}
	return string(raw)
}
func steerInput(item *requestItem) RuntimeInput {
	x := item.input
	x.Type = ""
	x.Payload = nil
	x.Text = strings.TrimSpace(messageText(item.msg.Payload))
	return x
}

type requestBuffer struct {
	items                     []*requestItem
	bytes, maxCount, maxBytes int
}

func (b *requestBuffer) push(i *requestItem) bool {
	if len(b.items)+1 > b.maxCount || b.bytes+i.bytes > b.maxBytes {
		return false
	}
	b.items = append(b.items, i)
	b.bytes += i.bytes
	return true
}
func (b *requestBuffer) remove(id string) *requestItem {
	for n, i := range b.items {
		if string(i.msg.ID) == id {
			b.items = append(b.items[:n], b.items[n+1:]...)
			b.bytes -= i.bytes
			return i
		}
	}
	return nil
}
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
	for n < len(b.items) && n < max && b.items[n].msg.Sender.ID == sender && !b.items[n].closed {
		n++
	}
	out := append([]*requestItem(nil), b.items[:n]...)
	for _, i := range out {
		b.bytes -= i.bytes
	}
	b.items = b.items[n:]
	return out
}
