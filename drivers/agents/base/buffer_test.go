package base

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

func bufferedMsg(id string, sender actor.ActorID, payloadBytes int) *requestItem {
	payload := make([]byte, payloadBytes)
	env := message.Envelope{
		ID:      message.ID(id),
		Kind:    message.KindRequest,
		Type:    "user.text",
		Sender:  message.Sender{ID: sender},
		Payload: payload,
	}
	return newRequestItem(testEnvelopeMsg(env), 0)
}

func testEnvelopeMsg(env message.Envelope) actorbase.Msg {
	return actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), env)
}

func TestRequestBufferBatchesOnlyConsecutiveSameSender(t *testing.T) {
	b := requestBuffer{maxCount: 8, maxBytes: 1024}
	for _, item := range []*requestItem{
		bufferedMsg("1", "actor:a", 1),
		bufferedMsg("2", "actor:a", 1),
		bufferedMsg("3", "actor:b", 1),
		bufferedMsg("4", "actor:a", 1),
	} {
		if !b.push(item) {
			t.Fatal("unexpected buffer rejection")
		}
	}
	if got := b.popBatch(8); len(got) != 2 {
		t.Fatalf("first batch size = %d, want 2", len(got))
	}
	if got := b.popBatch(8); len(got) != 1 || got[0].msg.Sender.ID != "actor:b" {
		t.Fatalf("second batch = %#v, want actor:b singleton", got)
	}
}

func TestRequestBufferEnforcesCountAndBytes(t *testing.T) {
	byCount := requestBuffer{maxCount: 1, maxBytes: 1024}
	if !byCount.push(bufferedMsg("1", "actor:a", 1)) || byCount.push(bufferedMsg("2", "actor:a", 1)) {
		t.Fatal("count bound was not enforced")
	}
	byBytes := requestBuffer{maxCount: 8, maxBytes: 2}
	if !byBytes.push(bufferedMsg("1", "actor:a", 2)) || byBytes.push(bufferedMsg("2", "actor:a", 1)) {
		t.Fatal("byte bound was not enforced")
	}
}

func TestSameSenderBatchMergeTerminals(t *testing.T) {
	l, e := newUnitLoop()
	l.state = stateStarting
	for _, id := range []string{"1", "2", "3"} {
		l.enqueue(bufferedMsg(id, "actor:a", 1), true)
	}
	l.state = stateIdle
	l.startNext()
	if e.starts != 1 || len(e.batches) != 1 || len(e.batches[0]) != 3 {
		t.Fatalf("starts=%d batches=%#v", e.starts, e.batches)
	}
	terms := l.sys.(*testSys).terms
	if len(terms) != 2 || terms[0].id != "1" || terms[1].id != "2" {
		t.Fatalf("merge terminals=%#v", terms)
	}
	for _, term := range terms {
		value, ok := term.value.(map[string]any)
		if !ok || value["merged_into"] != message.ID("3") {
			t.Fatalf("merge terminal=%#v", term)
		}
	}
	if got := l.committing; len(got) != 1 {
		t.Fatalf("committing=%#v, want tail only", got)
	}
}

func TestBufferCountAndByteLimitsRejectNewWithOverloaded(t *testing.T) {
	for _, tt := range []struct {
		name     string
		maxCount int
		maxBytes int
		first    int
		second   int
	}{
		{name: "count", maxCount: 1, maxBytes: 100, first: 1, second: 1},
		{name: "bytes", maxCount: 10, maxBytes: 2, first: 2, second: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l, _ := newUnitLoop()
			l.state = stateStarting
			l.buffer = requestBuffer{maxCount: tt.maxCount, maxBytes: tt.maxBytes}
			l.enqueue(bufferedMsg("old", "actor:a", tt.first), true)
			beforeCount, beforeBytes := len(l.buffer.items), l.buffer.bytes
			l.enqueue(bufferedMsg("new", "actor:a", tt.second), true)
			if len(l.buffer.items) != beforeCount || l.buffer.bytes != beforeBytes || l.buffer.items[0].msg.ID != "old" {
				t.Fatalf("old queue changed: %#v bytes=%d", l.buffer.items, l.buffer.bytes)
			}
			terms := l.sys.(*testSys).terms
			if len(terms) != 1 || terms[0].id != "new" || terms[0].code != "overloaded" {
				t.Fatalf("overflow terminals=%#v", terms)
			}
		})
	}
}
