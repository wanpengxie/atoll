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
			// needed for the child must arrive through the in-hand request's Cause.
			spec := behavior.RequestSpec{Cause: msg.Cause(), Type: "work", Audience: message.Audience{"tool:worker:1"}, Payload: []byte(`{}`)}
			var err error
			switch verb {
			case "emit":
				_, err = e.Emit(behavior.EventSpec{Cause: msg.Cause(), Type: "progress", Payload: []byte(`{}`)})
			case "post":
				_, err = e.Post(spec)
			case "call":
				_, err = e.Call(msg.Cause(), "tool:worker:1", "work", map[string]any{})
			case "submit":
				_, err = e.Submit(spec)
			default:
				_, err = e.CallFor(msg.Cause(), harness.Caller{Channel: "c", Actor: "agent:relay:1"}, "tool:worker:1", "work", map[string]any{})
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
	if _, err := e.Post(behavior.RequestSpec{Cause: cause, Type: "tool.run", Audience: message.Audience{"tool:worker:1"}, Payload: []byte(`{}`)}); err != nil {
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
			if _, err := e.Post(behavior.RequestSpec{Cause: msg.Cause(), Type: "work", Audience: message.Audience{"tool:worker:1"}, Payload: []byte(`{}`)}); err != nil {
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

func TestIDOnlyCauseFailsBeforeWriting(t *testing.T) {
	for _, verb := range []string{"post", "emit", "call"} {
		e, pen := contextEngine(t)
		cause := message.Anchored("old-request", "tree")
		var err error
		switch verb {
		case "post":
			_, err = e.Post(behavior.RequestSpec{Cause: cause, Type: "work", Audience: message.Audience{"tool:worker:1"}, Payload: []byte(`{}`)})
		case "emit":
			_, err = e.Emit(behavior.EventSpec{Cause: cause, Type: "event", Payload: []byte(`{}`)})
		default:
			_, err = e.Call(cause, "tool:worker:1", "work", map[string]any{})
		}
		if err == nil || pen.last() != nil {
			t.Fatalf("%s silently wrote with lost context: %v", verb, err)
		}
	}
}

func TestSessionAssignmentIsAnImmutableRequestValue(t *testing.T) {
	original := NewBodyMsgContext(OriginMailbox, t.Context(), harness.Context{Session: "old", Caller: &harness.Caller{Channel: "c", Actor: "human:a:1"}}, message.Envelope{ID: "ask", Payload: []byte(`{}`)})
	next, err := original.WithSession("new-session")
	if err != nil {
		t.Fatal(err)
	}
	cause := next.Cause()
	exposed := next.Context()
	exposed.Caller.Actor = "human:changed:1"
	app, err := cause.Context()
	if err != nil || app.Session != "new-session" || app.Caller.Actor != "human:a:1" || original.Context().Session != "old" {
		t.Fatalf("mutated request scope: %+v %v", app, err)
	}
}

func TestPreparedRootKeepsAllocatedSessionThroughAudit(t *testing.T) {
	e, pen := contextEngine(t)
	root, body, err := PrepareRoot(harness.Context{Caller: &harness.Caller{Channel: "c", Actor: "human:a:1"}}, []byte(`{"session":"new","text":"hello"}`))
	if err != nil {
		t.Fatal(err)
	}
	app, err := root.Context()
	if err != nil || app.Session == "" || app.Session == "new" {
		t.Fatalf("context=%+v err=%v", app, err)
	}
	id, err := e.Post(behavior.RequestSpec{Cause: root, Type: "work", Audience: message.Audience{"tool:worker:1"}, Payload: body})
	if err != nil {
		t.Fatal(err)
	}
	requestApp, _, err := harness.UnwrapPayload(pen.last().Payload)
	if err != nil || !reflect.DeepEqual(requestApp, app) {
		t.Fatalf("request context changed: %+v %v", requestApp, err)
	}
	_, err = e.Emit(behavior.EventSpec{Cause: message.Anchored(id, id).WithContext(app), Type: "audit", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	auditApp, _, err := harness.UnwrapPayload(pen.last().Payload)
	if err != nil || !reflect.DeepEqual(auditApp, app) {
		t.Fatalf("audit context changed: %+v %v", auditApp, err)
	}
}
