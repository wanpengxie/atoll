package echo

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/message"
)

// fakeSys is a minimal actorbase.Sys double: it embeds the (nil) interface so
// every verb this Proc never touches stays unimplemented (a call would nil-
// panic, failing the test loudly rather than silently no-op'ing), and
// overrides only the three verbs echo's run actually calls: Recv, Reply,
// Fail. It feeds a fixed sequence of Msg deliveries then returns errStop (the
// Recv-error loop-termination contract, spec §1.3).
type fakeSys struct {
	actorbase.Sys

	queue   []actorbase.Msg
	at      int
	replies []replyCall
	fails   []failCall
}

type replyCall struct {
	msg actorbase.Msg
	v   any
}

type failCall struct {
	msg          actorbase.Msg
	code, detail string
}

var errStop = errors.New("fakeSys: queue drained")

func (f *fakeSys) Recv() (actorbase.Msg, error) {
	if f.at >= len(f.queue) {
		return actorbase.Msg{}, errStop
	}
	m := f.queue[f.at]
	f.at++
	return m, nil
}

func (f *fakeSys) Reply(msg actorbase.Msg, v any) (message.ID, error) {
	f.replies = append(f.replies, replyCall{msg, v})
	return msg.ID, nil
}

func (f *fakeSys) Fail(msg actorbase.Msg, code, detail string) (message.ID, error) {
	f.fails = append(f.fails, failCall{msg, code, detail})
	return msg.ID, nil
}

var _ actorbase.Sys = (*fakeSys)(nil)

func requestMsg(id, typ string, payload any) actorbase.Msg {
	raw, _ := json.Marshal(payload)
	return actorbase.NewMsg(context.Background(), message.Envelope{
		ID:      message.ID(id),
		Kind:    message.KindRequest,
		Type:    typ,
		Payload: raw,
	})
}

func TestRun_SayRepliesWithPayloadVerbatim(t *testing.T) {
	msg := requestMsg("req-1", TypeSay, map[string]any{"text": "ping"})
	sys := &fakeSys{queue: []actorbase.Msg{msg}}

	if err := run(sys); !errors.Is(err, errStop) {
		t.Fatalf("run returned %v, want errStop", err)
	}
	if len(sys.replies) != 1 {
		t.Fatalf("replies = %d, want 1", len(sys.replies))
	}
	if len(sys.fails) != 0 {
		t.Fatalf("fails = %d, want 0", len(sys.fails))
	}
	got := sys.replies[0]
	if got.msg.ID != msg.ID {
		t.Fatalf("reply msg id = %q, want %q", got.msg.ID, msg.ID)
	}
	raw, ok := got.v.(json.RawMessage)
	if !ok {
		t.Fatalf("reply value type = %T, want json.RawMessage", got.v)
	}
	if string(raw) != string(msg.Payload) {
		t.Fatalf("reply payload = %s, want verbatim %s", raw, msg.Payload)
	}
}

func TestRun_UnknownTypeFailsTypeUnsupported(t *testing.T) {
	msg := requestMsg("req-2", "echo.nope", map[string]any{})
	sys := &fakeSys{queue: []actorbase.Msg{msg}}

	if err := run(sys); !errors.Is(err, errStop) {
		t.Fatalf("run returned %v, want errStop", err)
	}
	if len(sys.fails) != 1 {
		t.Fatalf("fails = %d, want 1", len(sys.fails))
	}
	if len(sys.replies) != 0 {
		t.Fatalf("replies = %d, want 0", len(sys.replies))
	}
	got := sys.fails[0]
	if got.code != "type_unsupported" {
		t.Fatalf("fail code = %q, want type_unsupported", got.code)
	}
}

func TestRun_LoopEndsByPropagatingRecvError(t *testing.T) {
	sys := &fakeSys{}
	if err := run(sys); !errors.Is(err, errStop) {
		t.Fatalf("run returned %v, want errStop on an empty queue", err)
	}
}
