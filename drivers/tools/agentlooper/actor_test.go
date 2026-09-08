package agentlooper

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	contextproto "github.com/wanpengxie/atoll/drivers/tools/agentcontext/api"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	llmproto "github.com/wanpengxie/atoll/drivers/tools/pillm/api"
	workspaceproto "github.com/wanpengxie/atoll/drivers/tools/piworkspace/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

type immediatePending struct{ msg actorbase.Msg }

func (p immediatePending) RequestID() message.ID { return "child" }
func (p immediatePending) Progress() <-chan actorbase.Msg {
	ch := make(chan actorbase.Msg)
	close(ch)
	return ch
}
func (p immediatePending) Wait(context.Context, time.Duration) (actorbase.Msg, error) {
	return p.msg, nil
}
func (p immediatePending) Cancel() error { return nil }

type progressGatedPending struct {
	progress chan actorbase.Msg
	drained  chan struct{}
}

func (p *progressGatedPending) RequestID() message.ID          { return "gated" }
func (p *progressGatedPending) Progress() <-chan actorbase.Msg { return p.progress }
func (p *progressGatedPending) Wait(ctx context.Context, _ time.Duration) (actorbase.Msg, error) {
	select {
	case <-p.drained:
		return actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{Kind: message.KindResponse, Payload: json.RawMessage(`{"status":"completed","value":1}`)}), nil
	case <-ctx.Done():
		return actorbase.Msg{}, ctx.Err()
	}
}
func (p *progressGatedPending) Cancel() error { return nil }

type progressGatedSys struct {
	actorbase.Sys
	pending *progressGatedPending
}

func (s *progressGatedSys) Call(message.Cause, actor.ActorID, string, any) (actorbase.Pending, error) {
	return s.pending, nil
}

func TestInternalCallDrainsProgressBeforeWaitingForTerminal(t *testing.T) {
	p := &progressGatedPending{progress: make(chan actorbase.Msg), drained: make(chan struct{})}
	go func() {
		p.progress <- actorbase.Msg{}
		close(p.progress)
		close(p.drained)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := call(ctx, &progressGatedSys{pending: p}, message.Root(), "tool:x:1", "x", map[string]any{}); err != nil {
		t.Fatal(err)
	}
}

func TestLooperAcceptsBoundedConcurrentAssignments(t *testing.T) {
	cfg, err := parseConfig(json.RawMessage(`{"controller_actor":"native-agent","max_assignments":32}`))
	if err != nil || cfg.MaxAssignments != 32 {
		t.Fatalf("multi-assignment config=%+v err=%v", cfg, err)
	}
	for _, value := range []int{0, 10001} {
		raw, _ := json.Marshal(map[string]any{"controller_actor": "native-agent", "max_assignments": value})
		if _, err := parseConfig(raw); err == nil {
			t.Fatalf("out-of-range max_assignments=%d was accepted", value)
		}
	}
}

type cancelledPending struct{}

func (cancelledPending) RequestID() message.ID { return "blocked" }
func (cancelledPending) Progress() <-chan actorbase.Msg {
	ch := make(chan actorbase.Msg)
	close(ch)
	return ch
}
func (cancelledPending) Wait(ctx context.Context, _ time.Duration) (actorbase.Msg, error) {
	<-ctx.Done()
	return actorbase.Msg{}, ctx.Err()
}
func (cancelledPending) Cancel() error { return nil }

type multiAssignmentSys struct {
	actorbase.Sys
	life context.Context
	mu   sync.Mutex
}

func (s *multiAssignmentSys) Life() context.Context { return s.life }
func (s *multiAssignmentSys) Call(message.Cause, actor.ActorID, string, any) (actorbase.Pending, error) {
	return cancelledPending{}, nil
}
func (s *multiAssignmentSys) Reply(actorbase.Msg, any) (message.ID, error) {
	return "reply", nil
}
func (s *multiAssignmentSys) Fail(actorbase.Msg, string, string, ...map[string]any) (message.ID, error) {
	return "failure", nil
}
func (s *multiAssignmentSys) Post(behavior.RequestSpec) (message.ID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return "report", nil
}

func internalRequest(id, typ string, body any) actorbase.Msg {
	raw, _ := json.Marshal(body)
	wrapped, _ := json.Marshal(map[string]any{"body": json.RawMessage(raw)})
	return actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID: message.ID(id), Sender: message.Sender{ID: "agent:controller:1"}, Kind: message.KindRequest, Type: typ, Payload: wrapped,
	})
}

func TestOneLooperRunsSeveralLoopsAndStopsOnlyTheAddressedOne(t *testing.T) {
	life, cancelLife := context.WithCancel(context.Background())
	sys := &multiAssignmentSys{life: life}
	l := &looper{cfg: Config{ControllerActor: "controller", MaxAssignments: 2}, active: map[string]*assignment{}}
	for index, workID := range []agentloop.StartRequest{
		{WorkID: "w-1", AssignmentID: "a-1", ControllerActor: "agent:controller:1", ContextActor: "context", LLMActor: "llm", Inputs: []agentloop.Input{{ID: "i-1", Seq: 1, Text: "one"}}},
		{WorkID: "w-2", AssignmentID: "a-2", ControllerActor: "agent:controller:1", ContextActor: "context", LLMActor: "llm", Inputs: []agentloop.Input{{ID: "i-2", Seq: 1, Text: "two"}}},
	} {
		l.start(sys, internalRequest(fmt.Sprintf("start-%d", index), agentloop.TypeStart, workID))
	}
	l.mu.Lock()
	active := len(l.active)
	l.mu.Unlock()
	if active != 2 {
		cancelLife()
		l.wg.Wait()
		t.Fatalf("one Looper active assignments=%d, want 2", active)
	}
	l.stop(sys, internalRequest("stop-1", agentloop.TypeStop, agentloop.StopRequest{WorkID: "w-1", AssignmentID: "a-1"}))
	deadline := time.Now().Add(time.Second)
	for {
		l.mu.Lock()
		first, second := l.active["w-1"], l.active["w-2"]
		l.mu.Unlock()
		if first == nil {
			if second == nil {
				t.Fatal("stopping w-1 also stopped w-2")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("addressed assignment did not stop")
		}
		time.Sleep(time.Millisecond)
	}
	cancelLife()
	l.wg.Wait()
}

func TestMalformedModelToolCallCannotReachWorkspace(t *testing.T) {
	for _, raw := range []json.RawMessage{
		json.RawMessage(`{"role":"assistant","content":[{"type":"toolCall","name":"write","arguments":{"path":"x"}}]}`),
		json.RawMessage(`{"role":"assistant","content":[{"type":"toolCall","id":"tc","name":"write","arguments":null}]}`),
		json.RawMessage(`{"role":"user","content":[]}`),
	} {
		if _, _, err := assistantParts(raw); err == nil {
			t.Fatalf("malformed model proposal was accepted: %s", raw)
		}
	}
}

func TestToolResultPreservesPiContentDetailsAndUsage(t *testing.T) {
	raw := toolResultRaw(toolCall{ID: "tc", Name: "read"}, json.RawMessage(`{
		"content":[{"type":"image","data":"abc","mimeType":"image/png"}],
		"details":{"truncated":true},
		"usage":{"input":1},
		"addedToolNames":["extra"]
	}`))
	var got map[string]json.RawMessage
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"content", "details", "usage", "addedToolNames"} {
		if len(got[field]) == 0 {
			t.Fatalf("tool result lost %s: %s", field, raw)
		}
	}
}

func TestChannelCallTargetsBodyHandleInsteadOfWorkspace(t *testing.T) {
	start := agentloop.StartRequest{WorkspaceActor: "workspace", HostActor: "host"}
	if target, word := toolTarget(start, "channel_call"); target != "host" || word != channelCallWord {
		t.Fatalf("channel call target=%q word=%q", target, word)
	}
	if target, word := toolTarget(start, "read"); target != "workspace" || word != workspaceproto.TypeRead {
		t.Fatalf("workspace call target=%q word=%q", target, word)
	}
	definitions := toolDefinitions(true, true)
	if len(definitions) != 5 {
		t.Fatalf("tool definitions=%d, want workspace four plus channel_call", len(definitions))
	}
	var host struct {
		Name       string          `json:"name"`
		Parameters json.RawMessage `json:"parameters"`
	}
	if err := json.Unmarshal(definitions[4], &host); err != nil || host.Name != "channel_call" || !json.Valid(host.Parameters) {
		t.Fatalf("host tool=%s err=%v", definitions[4], err)
	}
}

func TestResultTextExcerptIsBoundedWithoutBreakingUTF8(t *testing.T) {
	input := strings.Repeat("界", maxResultTextBytes)
	got, truncated := boundedResultText(input)
	if !truncated || len(got) > maxResultTextBytes || !utf8.ValidString(got) {
		t.Fatalf("bytes=%d truncated=%v valid=%v", len(got), truncated, utf8.ValidString(got))
	}
}

type loopSys struct {
	actorbase.Sys
	llmCalls  int
	toolCalls int
	posts     []behavior.RequestSpec
}

func (s *loopSys) Call(_ message.Cause, _ actor.ActorID, typ string, _ any) (actorbase.Pending, error) {
	var body any
	switch typ {
	case contextproto.TypeBuild:
		body = contextproto.Artifact{ArtifactID: "ctx-1", WorkID: "w-1", AssignmentID: "a-1", InputThrough: 1, SystemPrompt: "test", Messages: []json.RawMessage{json.RawMessage(`{"role":"user","content":"do it","timestamp":1}`)}}
	case llmproto.TypeGenerate:
		s.llmCalls++
		if s.llmCalls == 1 {
			body = llmproto.GenerateResponse{Provider: "faux", Model: "faux-1", Message: json.RawMessage(`{"role":"assistant","content":[{"type":"toolCall","id":"tc-1","name":"bash","arguments":{"command":"true"}}],"timestamp":2}`)}
		} else {
			body = llmproto.GenerateResponse{Provider: "faux", Model: "faux-1", Message: json.RawMessage(`{"role":"assistant","content":[{"type":"text","text":"done"}],"timestamp":3}`)}
		}
	case workspaceproto.TypeBash:
		s.toolCalls++
		body = map[string]any{"content": []map[string]any{{"type": "text", "text": "ok"}}}
	default:
		body = map[string]any{}
	}
	raw, _ := json.Marshal(body)
	var m map[string]json.RawMessage
	_ = json.Unmarshal(raw, &m)
	m["status"] = json.RawMessage(`"completed"`)
	payload, _ := json.Marshal(m)
	env := message.Envelope{ID: "response", Kind: message.KindResponse, Payload: payload}
	return immediatePending{msg: actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), env)}, nil
}
func (s *loopSys) Post(spec behavior.RequestSpec) (message.ID, error) {
	s.posts = append(s.posts, spec)
	return "report", nil
}

func TestLooperOwnsOneCompleteToolLoopAndReportsProposal(t *testing.T) {
	sys := &loopSys{}
	l := &looper{cfg: Config{MaxAssignments: 1}, active: map[string]*assignment{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := &assignment{start: agentloop.StartRequest{WorkID: "w-1", AssignmentID: "a-1", ControllerActor: "agent:controller:1", ContextActor: "context", LLMActor: "llm", WorkspaceActor: "workspace", MaxTurns: 4}, cause: message.Root(), cancel: cancel, inputs: []agentloop.Input{{ID: "i-1", Seq: 1, Text: "do it"}}}
	l.active["w-1"] = a
	l.drive(ctx, sys, a)
	if sys.llmCalls != 2 || sys.toolCalls != 1 {
		t.Fatalf("llm=%d tool=%d", sys.llmCalls, sys.toolCalls)
	}
	if len(sys.posts) != 1 || sys.posts[0].Type != agentloop.TypeReport {
		t.Fatalf("posts=%+v", sys.posts)
	}
	var report agentloop.ReportRequest
	if err := json.Unmarshal(sys.posts[0].Payload, &report); err != nil {
		t.Fatal(err)
	}
	if report.State != "completed" || report.ConsumedThrough != 1 || !json.Valid(report.Result) {
		t.Fatalf("report=%+v", report)
	}
	if string(report.Result) == "" || !contains(string(report.Result), `"text":"done"`) {
		t.Fatalf("result=%s", report.Result)
	}
	if l.active["w-1"] != nil {
		t.Fatal("terminal assignment still occupies the lane after its report")
	}
}
func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
