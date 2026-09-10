package actorbase

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

func contextEngine(t *testing.T) (*engine, *fakePen) {
	t.Helper()
	pen := &fakePen{self: "agent:worker:1"}
	e := newTestEngine(t, pen, Hooks{ResolveTarget: func(target string) (actor.ActorID, error) { return actor.ActorID(target), nil }}, 32, 32)
	return e, pen
}

func TestOutgoingVerbsCarryContextWithoutReceivingParent(t *testing.T) {
	app := harness.Context{Session: "session-A", Caller: &harness.Caller{Channel: "c", Actor: "human:alice:1"}}
	msg := NewBodyMsgContext(OriginMailbox, t.Context(), app, message.Envelope{ID: "request", CorrelationID: "tree", Payload: []byte(`{}`)})
	for _, verb := range []string{"emit", "post", "call", "submit", "call-for"} {
		t.Run(verb, func(t *testing.T) {
			e, pen := contextEngine(t)
			// This engine has never received or indexed the parent. All information
			// needed for the child must arrive through separate Cause and Context values.
			spec := behavior.RequestSpec{Cause: msg.Cause(), Type: "work", Audience: message.Audience{"tool:worker:1"}, Payload: []byte(`{}`), Context: msg.Context()}
			var err error
			switch verb {
			case "emit":
				_, err = e.Emit(behavior.EventSpec{Cause: msg.Cause(), Type: "progress", Payload: []byte(`{}`), Context: msg.Context()})
			case "post":
				_, err = e.Post(spec)
			case "call":
				_, err = e.Call(msg.Cause(), msg.Context(), "tool:worker:1", "work", map[string]any{})
			case "submit":
				_, err = e.Submit(spec)
			default:
				_, err = e.CallFor(msg.Cause(), msg.Context(), harness.Caller{Channel: "c", Actor: "agent:relay:1"}, "tool:worker:1", "work", map[string]any{})
			}
			if err != nil {
				t.Fatal(err)
			}
			env := pen.last()
			got, _, err := harness.UnwrapPayload(env.Payload)
			want := app.Clone()
			if verb == "call-for" {
				want.Caller = &harness.Caller{Channel: "c", Actor: "agent:relay:1"}
			}
			if err != nil || !reflect.DeepEqual(got, want) || env.ParentID != "request" || env.CorrelationID != "tree" {
				t.Fatalf("context=%+v envelope=%+v err=%v", got, env, err)
			}
		})
	}
}

func TestResponseCanCauseToolCallAfterRequestScopeEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	app := harness.Context{Session: "S", Caller: &harness.Caller{Channel: "c", Actor: "human:alice:1"}}
	raw, _ := harness.WrapPayload(app, json.RawMessage(`{"status":"completed"}`))
	response := NewMsg(OriginMailbox, ctx, message.Envelope{ID: "model-response", ParentID: "generate", CorrelationID: "turn", Kind: message.KindResponse, Payload: raw})
	cause := response.Cause()
	cancel()
	e, pen := contextEngine(t)
	if _, err := e.Post(behavior.RequestSpec{Cause: cause, Type: "tool.run", Audience: message.Audience{"tool:worker:1"}, Payload: []byte(`{}`), Context: response.Context()}); err != nil {
		t.Fatal(err)
	}
	got, _, err := harness.UnwrapPayload(pen.last().Payload)
	if err != nil || !reflect.DeepEqual(got, app) || pen.last().ParentID != "model-response" {
		t.Fatalf("lost response scope: %+v %v", got, err)
	}
}

func TestParallelScopesDoNotShareOrMutateContext(t *testing.T) {
	e, pen := contextEngine(t)
	var wg sync.WaitGroup
	for i := 0; i < 128; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := message.ID(fmt.Sprintf("request-%d", i))
			msg := NewBodyMsgContext(OriginMailbox, t.Context(), harness.Context{Session: string(id)}, message.Envelope{ID: id, Payload: []byte(`{}`)})
			if _, err := e.Post(behavior.RequestSpec{Cause: msg.Cause(), Type: "work", Audience: message.Audience{"tool:worker:1"}, Payload: []byte(`{}`), Context: msg.Context()}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	pen.mu.Lock()
	defer pen.mu.Unlock()
	for _, env := range pen.written {
		app, _, err := harness.UnwrapPayload(env.Payload)
		if err != nil || app.Session != string(env.ParentID) {
			t.Fatalf("scopes crossed: %+v %v", env, err)
		}
	}
}

func TestIDOnlyCauseCarriesExplicitContext(t *testing.T) {
	app := harness.Context{Session: "explicit"}
	for _, verb := range []string{"post", "emit", "call"} {
		e, pen := contextEngine(t)
		cause := message.Anchored("old-request", "tree")
		var err error
		switch verb {
		case "post":
			_, err = e.Post(behavior.RequestSpec{Cause: cause, Type: "work", Audience: message.Audience{"tool:worker:1"}, Payload: []byte(`{}`), Context: app})
		case "emit":
			_, err = e.Emit(behavior.EventSpec{Cause: cause, Type: "event", Payload: []byte(`{}`), Context: app})
		default:
			_, err = e.Call(cause, app, "tool:worker:1", "work", map[string]any{})
		}
		if err != nil || pen.last() == nil {
			t.Fatalf("%s failed: %v", verb, err)
		}
		got, _, err := harness.UnwrapPayload(pen.last().Payload)
		if err != nil || got.Session != app.Session {
			t.Fatalf("%s lost explicit context: %+v %v", verb, got, err)
		}
	}
}

func TestContextAssignmentIsAnImmutableRequestValue(t *testing.T) {
	original := NewBodyMsgContext(OriginMailbox, t.Context(), harness.Context{Session: "old", Caller: &harness.Caller{Channel: "c", Actor: "human:a:1"}}, message.Envelope{ID: "ask", Payload: []byte(`{}`)})
	contextCopy := original.Context()
	contextCopy.Session = "new-session"
	next := original.WithContext(contextCopy)
	cause := next.Cause()
	if cause != original.Cause() {
		t.Fatal("changing session changed message causality")
	}
	exposed := next.Context()
	exposed.Caller.Actor = "human:changed:1"
	app := next.Context()
	if app.Session != "new-session" || app.Caller.Actor != "human:a:1" || original.Context().Session != "old" {
		t.Fatalf("mutated request scope: %+v", app)
	}
}

// A body field named session is opaque to the generic sender, including new.
func TestSendingDoesNotInterpretSessionInBody(t *testing.T) {
	for _, verb := range []string{"post", "emit", "call"} {
		t.Run(verb, func(t *testing.T) {
			e, pen := contextEngine(t)
			app := harness.Context{Session: "existing"}
			body := json.RawMessage(`{"session":"new","text":"hello"}`)
			var err error
			switch verb {
			case "post":
				_, err = e.Post(behavior.RequestSpec{Cause: message.Root(), Context: app, Type: "work", Audience: message.Audience{"tool:worker:1"}, Payload: body})
			case "emit":
				_, err = e.Emit(behavior.EventSpec{Cause: message.Root(), Context: app, Type: "event", Payload: body})
			case "call":
				_, err = e.Call(message.Root(), app, "tool:worker:1", "work", body)
			}
			if err != nil {
				t.Fatal(err)
			}
			got, raw, err := harness.UnwrapPayload(pen.last().Payload)
			var value map[string]any
			if err != nil {
				t.Fatal(err)
			}
			if json.Unmarshal(raw, &value) != nil || value["session"] != "new" || got.Session != "existing" {
				t.Fatalf("sender changed application data: %+v %s", got, raw)
			}
		})
	}
}
