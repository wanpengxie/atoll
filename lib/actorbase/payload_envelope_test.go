package actorbase

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

func TestRequestWritersUseOnePayloadEnvelopeAndNewMsgUnwrapsBody(t *testing.T) {
	argsCases := []struct {
		name string
		args any
	}{
		{name: "object", args: map[string]any{"x": float64(1)}},
		{name: "array", args: []any{"x", float64(2)}},
		{name: "scalar", args: "x"},
		{name: "null", args: nil},
		{name: "body context key", args: map[string]any{"_context": "application"}},
		{name: "body caller key", args: map[string]any{"caller": "application"}},
	}
	writers := []struct {
		name       string
		withCaller bool
		write      func(*engine, json.RawMessage, any) error
	}{
		{name: "Call", write: func(e *engine, _ json.RawMessage, args any) error {
			_, err := e.Call("tool:echo:1", "echo.say", args)
			return err
		}},
		{name: "Post", write: func(e *engine, raw json.RawMessage, _ any) error {
			_, err := e.Post(behavior.RequestSpec{Type: "echo.say", Audience: message.Audience{"tool:echo:1"}, Payload: raw})
			return err
		}},
		{name: "JobTable.Submit", write: func(e *engine, raw json.RawMessage, _ any) error {
			_, err := e.Submit(behavior.RequestSpec{Type: "echo.say", Audience: message.Audience{"tool:echo:1"}, Payload: raw})
			return err
		}},
		{name: "CallFor", withCaller: true, write: func(e *engine, _ json.RawMessage, args any) error {
			_, err := e.CallFor(harness.Caller{Channel: "c0", Actor: "agent:caller:1"}, "tool:echo:1", "echo.say", args)
			return err
		}},
	}
	for _, writer := range writers {
		for _, tc := range argsCases {
			t.Run(writer.name+"/"+tc.name, func(t *testing.T) {
				pen := &fakePen{self: "agent:sender:1"}
				e := newTestEngine(t, pen, Hooks{ResolveTarget: func(target string) (actor.ActorID, error) {
					return actor.ActorID(target), nil
				}}, 8, 8)
				raw, err := json.Marshal(tc.args)
				if err != nil {
					t.Fatal(err)
				}
				if err := writer.write(e, raw, tc.args); err != nil {
					t.Fatal(err)
				}
				env := pen.last()
				if env == nil {
					t.Fatal("request was not written")
				}
				var outer map[string]json.RawMessage
				if err := json.Unmarshal(env.Payload, &outer); err != nil {
					t.Fatalf("payload is not an object: %v", err)
				}
				if len(outer) != 1 && !(writer.withCaller && len(outer) == 2) {
					t.Fatalf("payload keys=%v", reflect.ValueOf(outer).MapKeys())
				}
				if !jsonSemanticallyEqual(t, outer["body"], raw) {
					t.Fatalf("body=%s args=%s", outer["body"], raw)
				}
				msg := NewMsg(OriginMailbox, context.Background(), *env)
				if !jsonSemanticallyEqual(t, msg.Payload, raw) {
					t.Fatalf("unwrapped=%s args=%s", msg.Payload, raw)
				}
				caller, ok := msg.Caller()
				if ok != writer.withCaller {
					t.Fatalf("Caller ok=%v want %v", ok, writer.withCaller)
				}
				if writer.withCaller && (caller.Channel != "c0" || caller.Actor != "agent:caller:1") {
					t.Fatalf("caller=%+v", caller)
				}
			})
		}
	}
}

func TestEmitDoesNotUseRequestPayloadEnvelope(t *testing.T) {
	pen := &fakePen{self: "agent:sender:1"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	if _, err := e.Emit(behavior.EventSpec{Type: "echo.event", Payload: json.RawMessage(`{"x":1}`)}); err != nil {
		t.Fatal(err)
	}
	if got := string(pen.last().Payload); got != `{"x":1}` {
		t.Fatalf("event payload=%s", got)
	}
}

func TestEffectiveCallerPrefersContextAndFallsBackToEnvelope(t *testing.T) {
	for _, test := range []struct {
		name    string
		payload json.RawMessage
		want    harness.Caller
	}{
		{name: "context", payload: json.RawMessage(`{"_context":{"caller":{"channel":"remote","actor":"human:alice:1"}},"body":{}}`), want: harness.Caller{Channel: "remote", Actor: "human:alice:1"}},
		{name: "envelope fallback", payload: json.RawMessage(`{"body":{}}`), want: harness.Caller{Channel: "local", Actor: "agent:sender:1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			msg := NewMsg(OriginMailbox, context.Background(), message.Envelope{
				ChannelID: "local", Kind: message.KindRequest,
				Sender: message.Sender{ID: "agent:sender:1"}, Payload: test.payload,
			})
			if got := EffectiveCaller(msg); got != test.want {
				t.Fatalf("EffectiveCaller=%+v want %+v", got, test.want)
			}
		})
	}
}

func jsonSemanticallyEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		t.Fatal(err)
	}
	return reflect.DeepEqual(av, bv)
}
