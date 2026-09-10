package agentlooper

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	contextproto "github.com/wanpengxie/atoll/drivers/tools/agentcontext/api"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	llmproto "github.com/wanpengxie/atoll/drivers/tools/pillm/api"
	workspaceproto "github.com/wanpengxie/atoll/drivers/tools/piworkspace/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

type reliabilitySys struct {
	loopSys
	t           *testing.T
	outputs     []json.RawMessage
	requests    [][]json.RawMessage
	toolFailure error
	toolPending actorbase.Pending
	cancel      context.CancelFunc
}

func completed(value any) actorbase.Pending {
	raw, _ := json.Marshal(value)
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	fields["status"] = json.RawMessage(`"completed"`)
	raw, _ = json.Marshal(fields)
	return immediatePending{actorbase.NewBodyMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{Kind: message.KindResponse, Payload: raw})}
}
func (s *reliabilitySys) Call(cause message.Cause, app harness.Context, target actor.ActorID, typ string, value any) (actorbase.Pending, error) {
	if typ == llmproto.TypeGenerate {
		messages := testContextMessages("session:test")
		if err := validateHistory(messages); err != nil {
			s.t.Fatalf("model received unpaired history: %v", err)
		}
		s.requests = append(s.requests, messages)
		if len(s.outputs) == 0 {
			s.t.Fatal("unexpected model invocation")
		}
		output := s.outputs[0]
		s.outputs = s.outputs[1:]
		s.llmCalls++
		return completed(llmproto.GenerateResponse{Message: output}), nil
	}
	if typ == workspaceproto.TypeBash {
		s.toolCalls++
		if s.toolPending != nil {
			return s.toolPending, nil
		}
		if s.cancel != nil {
			s.cancel()
			return nil, context.Canceled
		}
		if s.toolFailure != nil {
			return nil, s.toolFailure
		}
		return completed(map[string]any{"content": []any{map[string]any{"type": "text", "text": "ok"}}}), nil
	}
	if typ == contextproto.TypeBuild {
		req := value.(contextproto.BuildRequest)
		history := append([]json.RawMessage(nil), req.Prior...)
		for _, in := range req.Inputs {
			history = append(history, mustRaw(map[string]any{"role": "user", "content": in.Text}))
		}
		return completed(contextproto.Artifact{Messages: history, SystemPrompt: "test"}), nil
	}
	return s.loopSys.Call(cause, app, target, typ, value)
}

func runReliability(t *testing.T, s *reliabilitySys, ctx context.Context) agentloop.ReportRequest {
	t.Helper()
	_, _ = sharedLooperResources.Delete("ctx/session:test")
	s.t = t
	a := &assignment{start: agentloop.StartRequest{SessionID: "session:test", WorkID: "w", AssignmentID: "a", ControllerActor: "controller", ContextActor: "context", LLMActor: "llm", WorkspaceActor: "workspace", MaxTurns: 5, ToolTimeoutMS: 5}, cause: message.Root(), inputs: []agentloop.Input{{ID: "i", Seq: 1, Text: "run"}}}
	l := &looper{active: map[string]*assignment{"a": a}}
	l.drive(ctx, s, a)
	var report agentloop.ReportRequest
	if len(s.posts) != 1 || json.Unmarshal(s.posts[0].Payload, &report) != nil {
		t.Fatalf("reports=%v", s.posts)
	}
	history := testContextMessages("session:test")
	if len(history) > 0 {
		if err := validateHistory(history); err != nil {
			t.Fatalf("saved history invalid: %v", err)
		}
	}
	return report
}

const finalAssistant = `{"role":"assistant","stopReason":"stop","content":[{"type":"text","text":"done"}]}`

func callAssistant(reason string) json.RawMessage {
	return json.RawMessage(`{"role":"assistant","stopReason":"` + reason + `","content":[{"type":"toolCall","id":"one","name":"bash","arguments":{"command":"true"}},{"type":"toolCall","id":"two","name":"absent","arguments":{}},{"type":"toolCall","id":"three","name":"bash","arguments":[]}]}`)
}
func TestReliabilityPairsMixedFailuresAndReusedIDsAcrossThreeTurns(t *testing.T) {
	s := &reliabilitySys{outputs: []json.RawMessage{callAssistant("toolUse"), callAssistant("toolUse"), json.RawMessage(finalAssistant)}, toolFailure: context.DeadlineExceeded}
	r := runReliability(t, s, context.Background())
	if r.State != "completed" || s.toolCalls != 2 || s.llmCalls != 3 {
		t.Fatalf("report=%+v tools=%d models=%d", r, s.toolCalls, s.llmCalls)
	}
	if !strings.Contains(string(mustRaw(testContextMessages("session:test"))), "Looper: tool was not executed") {
		t.Fatal("pre-ledger rejection was not represented as unexecuted")
	}
}
func TestReliabilityLengthNeverExecutesAndClosesEveryCall(t *testing.T) {
	s := &reliabilitySys{outputs: []json.RawMessage{callAssistant("length"), json.RawMessage(finalAssistant)}}
	r := runReliability(t, s, context.Background())
	if r.State != "completed" || s.toolCalls != 0 {
		t.Fatalf("report=%+v tools=%d", r, s.toolCalls)
	}
}
func TestReliabilityCancelClosesBatchWithoutFurtherCalls(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &reliabilitySys{outputs: []json.RawMessage{callAssistant("toolUse")}, cancel: cancel}
	r := runReliability(t, s, ctx)
	if r.State != "cancelled" || s.toolCalls != 1 || s.llmCalls != 1 {
		t.Fatalf("report=%+v", r)
	}
}
func TestReliabilityErrorAssistantNeverExecutesPartialCalls(t *testing.T) {
	for _, reason := range []string{"error", "aborted"} {
		t.Run(reason, func(t *testing.T) {
			s := &reliabilitySys{outputs: []json.RawMessage{callAssistant(reason)}, toolFailure: errors.New("must not run")}
			r := runReliability(t, s, context.Background())
			if r.State != "failed" || s.toolCalls != 0 || len(testContextMessages("session:test")) == 0 {
				t.Fatalf("report=%+v", r)
			}
		})
	}
}

func TestReliabilityRetainsOpaqueProviderFieldsWithoutNarrowRoundTrip(t *testing.T) {
	raw := json.RawMessage(`{"role":"assistant","provider":"openai","api":"openai-responses","model":"fixture","stopReason":"stop","content":[{"type":"thinking","thinking":"","thinkingSignature":"{\"type\":\"reasoning\",\"encrypted_content\":\"opaque\"}"},{"type":"text","text":"done","textSignature":"opaque-text"}],"futureProviderField":{"opaque":"preserve"}}`)
	s := &reliabilitySys{outputs: []json.RawMessage{raw}}
	_ = runReliability(t, s, context.Background())
	history := testContextMessages("session:test")
	if string(history[len(history)-1]) != string(raw) {
		t.Fatalf("raw assistant changed: %s", history[len(history)-1])
	}
}

func TestReliabilityActualPendingDeadlineRecordsRequestWithoutFakeToolResponse(t *testing.T) {
	s := &reliabilitySys{outputs: []json.RawMessage{callAssistant("toolUse"), json.RawMessage(finalAssistant)}, toolPending: cancelledPending{}}
	r := runReliability(t, s, context.Background())
	if r.State != "completed" || !strings.Contains(string(mustRaw(testContextMessages("session:test"))), `"request_id":"blocked"`) || !strings.Contains(string(mustRaw(testContextMessages("session:test"))), `"error_code":"deadline_exceeded"`) {
		t.Fatalf("report=%+v", r)
	}
	// This Sys receives only the Controller report: no synthetic Actor response
	// was posted on behalf of the tool whose actual response is still unknown.
	for _, post := range s.posts {
		if post.Type != agentloop.TypeReport {
			t.Fatalf("unexpected fabricated delivery: %+v", post)
		}
	}
}

func TestReliabilityAmbiguousToolIdentityCannotExecute(t *testing.T) {
	for _, id := range []string{"", "same"} {
		raw := mustRaw(map[string]any{"role": "assistant", "stopReason": "toolUse", "content": []any{map[string]any{"type": "toolCall", "id": id, "name": "bash", "arguments": map[string]any{}}, map[string]any{"type": "toolCall", "id": id, "name": "bash", "arguments": map[string]any{}}}})
		s := &reliabilitySys{outputs: []json.RawMessage{raw}}
		r := runReliability(t, s, context.Background())
		if r.State != "failed" || s.toolCalls != 0 {
			t.Fatalf("report=%+v calls=%d", r, s.toolCalls)
		}
	}
}

type terminalOpenProgress struct {
	immediatePending
	updates chan actorbase.Msg
}

func (p terminalOpenProgress) Progress() <-chan actorbase.Msg { return p.updates }

type terminalOpenProgressSys struct {
	looperTestBase
	p actorbase.Pending
}

func (s terminalOpenProgressSys) Call(message.Cause, harness.Context, actor.ActorID, string, any) (actorbase.Pending, error) {
	return s.p, nil
}
func TestTerminalDoesNotWaitForeverForProgressClosure(t *testing.T) {
	p := terminalOpenProgress{immediatePending: immediatePending{actorbase.NewBodyMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{Kind: message.KindResponse, Payload: json.RawMessage(`{"status":"completed"}`)})}, updates: make(chan actorbase.Msg)}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	_, err := call(ctx, terminalOpenProgressSys{p: p}, message.Root(), harness.Context{}, "tool:fixture:1", "fixture", nil)
	if err != nil || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("terminal blocked by progress closure: %v", err)
	}
}
