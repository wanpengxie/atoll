package agentlooper

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
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
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/harness"
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
		return actorbase.NewBodyMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{Kind: message.KindResponse, Payload: json.RawMessage(`{"status":"completed","value":1}`)}), nil
	case <-ctx.Done():
		return actorbase.Msg{}, ctx.Err()
	}
}
func (p *progressGatedPending) Cancel() error { return nil }

type progressGatedSys struct {
	looperTestBase
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
	looperTestBase
	life context.Context
	mu   sync.Mutex
}

func (s *multiAssignmentSys) View() actorcaps.LedgerView {
	return (&historyTestSys{rows: []ledgerRow{
		{Seq: 1, ID: "i-1", Session: "session:one", Body: mustJSON(map[string]any{"text": "one"})},
		{Seq: 2, ID: "i-2", Session: "session:two", Body: mustJSON(map[string]any{"text": "two"})},
	}}).View()
}
func (s *multiAssignmentSys) Life() context.Context { return s.life }
func (s *multiAssignmentSys) Call(_ message.Cause, _ actor.ActorID, word string, payload any) (actorbase.Pending, error) {
	return cancelledPending{}, nil
}
func (s *multiAssignmentSys) Emit(behavior.EventSpec) (message.ID, error) { return "opened", nil }
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
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	var session string
	_ = json.Unmarshal(fields["session"], &session)
	delete(fields, "session")
	raw, _ = json.Marshal(fields)
	return actorbase.NewBodyMsgContext(actorbase.OriginMailbox, context.Background(), harness.Context{Session: session}, message.Envelope{
		ID: message.ID(id), Sender: message.Sender{ID: "agent:controller:1"}, Kind: message.KindRequest, Type: typ, Payload: raw,
	})
}

func TestOneLooperRunsSeveralLoopsAndStopsOnlyTheAddressedOne(t *testing.T) {
	life, cancelLife := context.WithCancel(context.Background())
	sys := &multiAssignmentSys{life: life}
	l := &looper{cfg: Config{ControllerActor: "controller", LLMActor: "llm", MaxAssignments: 2}, active: map[string]*assignment{}}
	for index, workID := range []agentloop.StartRequest{
		{Open: &agentloop.OpenRequest{}, SessionID: "session:one", WorkID: "w-1", AssignmentID: "a-1", ControllerActor: "agent:controller:1", ContextActor: "context", LLMActor: "llm", Inputs: []agentloop.Input{{ID: "i-1", Seq: 1, Text: "one"}}},
		{Open: &agentloop.OpenRequest{}, SessionID: "session:two", WorkID: "w-2", AssignmentID: "a-2", ControllerActor: "agent:controller:1", ContextActor: "context", LLMActor: "llm", Inputs: []agentloop.Input{{ID: "i-2", Seq: 1, Text: "two"}}},
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
	l.stop(sys, internalRequest("stop-1", agentloop.TypeStop, agentloop.StopRequest{SessionID: "session:one", WorkID: "w-1", AssignmentID: "a-1"}))
	deadline := time.Now().Add(time.Second)
	for {
		l.mu.Lock()
		first, second := l.active["a-1"], l.active["a-2"]
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

func TestToolResultPreservesExplicitIsErrorAndSpillDetails(t *testing.T) {
	raw := json.RawMessage(`{"content":[{"type":"text","text":"tail"}],"details":{"fullOutputPath":"/tmp/pi-spill","truncation":{"truncated":true}},"isError":true}`)
	result := (&looper{}).toolResult(context.Background(), &resultStoreSys{}, &assignment{}, toolCall{ID: "tc", Name: "bash"}, resolvedTool{Word: workspaceproto.TypeBash}, raw)
	if !strings.Contains(string(result), `"isError":true`) || !strings.Contains(string(result), "/tmp/pi-spill") || !strings.Contains(string(result), "truncation") {
		t.Fatalf("Pi failure/spill metadata lost: %s", result)
	}
}

func TestDefaultToolsBindHostAndWorkspaceWithoutAddingSearchTools(t *testing.T) {
	start := agentloop.StartRequest{WorkspaceActor: "workspace", HostActor: "host"}
	tools := configuredTools(start)
	if len(tools) != 7 || tools[0].Actor != "workspace" || tools[0].Word != workspaceproto.TypeRead || tools[4].Actor != "host" || tools[4].Word != channelCallWord {
		t.Fatalf("default tools=%+v", tools)
	}
	for _, tool := range tools {
		if tool.Name == "grep" || tool.Name == "find" || tool.Name == "ls" {
			t.Fatalf("new search tool was silently added to compatibility defaults: %+v", tools)
		}
	}
}

type customToolSys struct {
	looperTestBase
	llmCalls     int
	customCalls  int
	definitions  []json.RawMessage
	posts        []behavior.RequestSpec
	toolCallName string
}

func (s *customToolSys) Call(_ message.Cause, target actor.ActorID, typ string, value any) (actorbase.Pending, error) {
	var body any
	switch typ {
	case contextproto.TypeBuild:
		body = contextproto.Artifact{ArtifactID: "ctx", Messages: []json.RawMessage{json.RawMessage(`{"role":"user","content":"run","timestamp":1}`)}}
	case "actor.describe":
		if target != "custom" {
			return nil, fmt.Errorf("unexpected describe target %s", target)
		}
		body = map[string]any{"class": "custom", "interfaces": []string{"actor"}, "capabilities": map[string]bool{}, "words": map[string]any{
			"custom.run": map[string]any{"description": "manifest description", "input_schema": json.RawMessage(`{"type":"object","required":["value"],"properties":{"value":{"type":"string"}},"additionalProperties":false}`)},
		}}
	case llmproto.TypeGenerate:
		s.llmCalls++
		raw, _ := json.Marshal(value)
		var req llmproto.GenerateRequest
		_ = json.Unmarshal(raw, &req)
		s.definitions = req.Tools
		if s.llmCalls == 1 {
			name := s.toolCallName
			if name == "" {
				name = "custom"
			}
			body = llmproto.GenerateResponse{Message: mustRaw(map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "toolCall", "id": "tc", "name": name, "arguments": map[string]any{"value": "x"}}}, "timestamp": 2})}
		} else {
			body = llmproto.GenerateResponse{Message: json.RawMessage(`{"role":"assistant","content":[{"type":"text","text":"done"}],"timestamp":3}`)}
		}
	case "custom.run":
		if target != "custom" {
			return nil, fmt.Errorf("unexpected custom target %s", target)
		}
		s.customCalls++
		body = map[string]any{"content": []map[string]any{{"type": "text", "text": "custom result"}}}
	default:
		return nil, fmt.Errorf("unexpected call %s", typ)
	}
	raw, _ := json.Marshal(body)
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	fields["status"] = json.RawMessage(`"completed"`)
	payload, _ := json.Marshal(fields)
	return immediatePending{msg: actorbase.NewBodyMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{Kind: message.KindResponse, Payload: payload})}, nil
}

func (s *customToolSys) Post(spec behavior.RequestSpec) (message.ID, error) {
	s.posts = append(s.posts, spec)
	return "report", nil
}

func TestManifestDefinedCustomToolRunsWithoutWorkspace(t *testing.T) {
	sys := &customToolSys{}
	bindings := []agentloop.ToolBinding{{Name: "custom", Actor: "custom", Word: "custom.run"}}
	a := &assignment{start: agentloop.StartRequest{SessionID: "session:test", WorkID: "w", AssignmentID: "a", ContextActor: "context", LLMActor: "llm", Tools: &bindings, MaxTurns: 3}, cause: message.Root(), inputs: []agentloop.Input{{ID: "i", Seq: 1, Text: "run"}}}
	(&looper{}).drive(context.Background(), sys, a)
	if sys.llmCalls != 2 || sys.customCalls != 1 || len(sys.definitions) != 1 || !strings.Contains(string(sys.definitions[0]), "manifest description") {
		t.Fatalf("llm=%d custom=%d definitions=%s", sys.llmCalls, sys.customCalls, sys.definitions)
	}
}

func TestExplicitEmptyToolListProducesNoDefinitions(t *testing.T) {
	empty := []agentloop.ToolBinding{}
	if got := configuredTools(agentloop.StartRequest{WorkspaceActor: "workspace", HostActor: "host", Tools: &empty}); len(got) != 0 {
		t.Fatalf("explicit empty tools gained defaults: %+v", got)
	}
}

func TestUnopenedToolNameNeverReachesTarget(t *testing.T) {
	sys := &customToolSys{toolCallName: "not_allowed"}
	bindings := []agentloop.ToolBinding{{Name: "custom", Actor: "custom", Word: "custom.run"}}
	a := &assignment{start: agentloop.StartRequest{SessionID: "session:test", WorkID: "w", AssignmentID: "a", ContextActor: "context", LLMActor: "llm", Tools: &bindings, MaxTurns: 3}, cause: message.Root(), inputs: []agentloop.Input{{ID: "i", Seq: 1, Text: "run"}}}
	(&looper{}).drive(context.Background(), sys, a)
	if sys.customCalls != 0 || sys.llmCalls != 2 {
		t.Fatalf("unopened tool calls=%d llm=%d", sys.customCalls, sys.llmCalls)
	}
}

func TestInvalidManifestFailsBeforeInference(t *testing.T) {
	sys := &customToolSys{}
	bindings := []agentloop.ToolBinding{{Name: "custom", Actor: "custom", Word: "missing.run"}}
	a := &assignment{start: agentloop.StartRequest{SessionID: "session:test", WorkID: "w", AssignmentID: "a", ContextActor: "context", LLMActor: "llm", Tools: &bindings}, cause: message.Root(), inputs: []agentloop.Input{{ID: "i", Seq: 1, Text: "run"}}}
	(&looper{}).drive(context.Background(), sys, a)
	if sys.llmCalls != 0 || len(sys.posts) != 1 {
		t.Fatalf("invalid manifest reached inference: llm=%d posts=%d", sys.llmCalls, len(sys.posts))
	}
	var report agentloop.ReportRequest
	_ = json.Unmarshal(sys.posts[0].Payload, &report)
	if report.State != "failed" {
		t.Fatalf("report=%+v", report)
	}
}

type refreshManifestSys struct {
	looperTestBase
	calls int
}

func (s *refreshManifestSys) Call(_ message.Cause, _ actor.ActorID, typ string, _ any) (actorbase.Pending, error) {
	if typ != "actor.describe" {
		return nil, fmt.Errorf("unexpected %s", typ)
	}
	s.calls++
	raw, _ := json.Marshal(map[string]any{"class": "custom", "interfaces": []string{"actor"}, "capabilities": map[string]bool{}, "words": map[string]any{"custom.run": map[string]any{"description": fmt.Sprintf("version-%d", s.calls), "input_schema": json.RawMessage(`{"type":"object"}`)}}})
	return completedPending(raw), nil
}

func TestEachEpisodeReadsAFreshManifest(t *testing.T) {
	sys := &refreshManifestSys{}
	bindings := []agentloop.ToolBinding{{Name: "custom", Actor: "custom", Word: "custom.run"}}
	for want := 1; want <= 2; want++ {
		a := &assignment{start: agentloop.StartRequest{Tools: &bindings}, cause: message.Root()}
		tools, err := resolveTools(context.Background(), sys, a)
		if err != nil || len(tools) != 1 || !strings.Contains(string(tools[0].Definition), fmt.Sprintf("version-%d", want)) {
			t.Fatalf("episode %d tools=%+v err=%v", want, tools, err)
		}
	}
}

func TestInvalidInputSchemasAreRejected(t *testing.T) {
	for _, raw := range []json.RawMessage{nil, json.RawMessage(`false`), json.RawMessage(`{"type":"string"}`), json.RawMessage(`{"type":"object","properties":42}`), json.RawMessage(`{"type":`)} {
		if err := validInputSchema(raw); err == nil {
			t.Fatalf("invalid schema accepted: %s", raw)
		}
	}
}

func TestResultTextExcerptIsBoundedWithoutBreakingUTF8(t *testing.T) {
	input := strings.Repeat("界", maxResultTextBytes)
	got, truncated := boundedResultText(input)
	if !truncated || len(got) > maxResultTextBytes || !utf8.ValidString(got) {
		t.Fatalf("bytes=%d truncated=%v valid=%v", len(got), truncated, utf8.ValidString(got))
	}
}

type resultStoreSys struct {
	looperTestBase
	target  actor.ActorID
	word    string
	request workspaceproto.WriteRequest
	err     error
}

func (s *resultStoreSys) Call(_ message.Cause, target actor.ActorID, word string, value any) (actorbase.Pending, error) {
	if s.err != nil {
		return nil, s.err
	}
	s.target, s.word = target, word
	raw, _ := json.Marshal(value)
	_ = json.Unmarshal(raw, &s.request)
	return completedPending(json.RawMessage(`{"content":[{"type":"text","text":"saved"}]}`)), nil
}

func TestLongToolTextIsBoundedDeterministically(t *testing.T) {
	sys := &resultStoreSys{}
	full := "αβγ\n" + strings.Repeat("long", 100)
	a := &assignment{start: agentloop.StartRequest{WorkspaceActor: "store", ToolResultMaxLines: 10, ToolResultMaxBytes: 256, ToolImageMaxBytes: 1024}, cause: message.Root()}
	raw, _ := json.Marshal(map[string]any{"content": []map[string]any{{"type": "text", "text": full}}, "details": map[string]any{"upstream": true}})
	result := (&looper{}).toolResult(context.Background(), sys, a, toolCall{ID: "tc", Name: "custom"}, resolvedTool{Word: "custom.run"}, raw)
	if sys.target != "" || !strings.Contains(string(result), "Output truncated to configured limit") || strings.Contains(string(result), `"path"`) {
		t.Fatalf("tool rendering performed a non-ledger-replayable side effect: target=%s result=%s", sys.target, result)
	}
	if !utf8.Valid(result) || !strings.Contains(string(result), "atoll_output") || !strings.Contains(string(result), "upstream") {
		t.Fatalf("bounded result=%s", result)
	}
	var message struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	_ = json.Unmarshal(result, &message)
	if len(message.Content) != 1 || len(message.Content[0].Text) > a.start.ToolResultMaxBytes || strings.Count(message.Content[0].Text, "\n") >= a.start.ToolResultMaxLines {
		t.Fatalf("model text escaped configured boundary: %+v", message.Content)
	}
}

func TestLongToolTextDoesNotDependOnOutputStore(t *testing.T) {
	full := strings.Repeat("x", 100)
	raw, _ := json.Marshal(map[string]any{"content": []map[string]any{{"type": "text", "text": full}}})
	var first string
	for _, tc := range []struct {
		name string
		a    *assignment
		sys  *resultStoreSys
	}{
		{"no store", &assignment{start: agentloop.StartRequest{ToolResultMaxLines: 10, ToolResultMaxBytes: 10}}, &resultStoreSys{}},
		{"save failure", &assignment{start: agentloop.StartRequest{WorkspaceActor: "store", ToolResultMaxLines: 10, ToolResultMaxBytes: 10}}, &resultStoreSys{err: errors.New("disk full")}},
	} {
		result := (&looper{}).toolResult(context.Background(), tc.sys, tc.a, toolCall{ID: "tc", Name: "custom"}, resolvedTool{Word: "custom.run"}, raw)
		if first == "" {
			first = string(result)
		}
		if string(result) != first || !strings.Contains(string(result), `"truncated":true`) {
			t.Fatalf("%s result=%s", tc.name, result)
		}
	}
}

func TestLargeOrdinaryJSONResultUsesTheSameBoundary(t *testing.T) {
	sys := &resultStoreSys{}
	raw, _ := json.Marshal(map[string]any{"status": "completed", "value": strings.Repeat("json", 100)})
	a := &assignment{start: agentloop.StartRequest{WorkspaceActor: "store", ToolResultMaxLines: 20, ToolResultMaxBytes: 256}, cause: message.Root()}
	result := (&looper{}).toolResult(context.Background(), sys, a, toolCall{ID: "tc", Name: "custom"}, resolvedTool{Word: "custom.run"}, raw)
	if sys.request.Content != "" || !strings.Contains(string(result), "atoll_output") {
		t.Fatalf("unexpected side effect=%q result=%s", sys.request.Content, result)
	}
}

func TestShellTextRetainsTailAndImageBlocksAreNotTextualized(t *testing.T) {
	if got, truncated := boundedToolText("head\nmiddle\ntail", 2, 100, true); !truncated || got != "middle\ntail" {
		t.Fatalf("tail=%q truncated=%v", got, truncated)
	}
	pixel := base64.StdEncoding.EncodeToString([]byte("png"))
	a := &assignment{start: agentloop.StartRequest{ToolImageMaxBytes: 10}}
	raw, _ := json.Marshal(map[string]any{"content": []map[string]any{{"type": "text", "text": "image"}, {"type": "image", "data": pixel, "mimeType": "image/png"}}})
	result := (&looper{}).toolResult(context.Background(), &resultStoreSys{}, a, toolCall{ID: "tc", Name: "read"}, resolvedTool{Word: workspaceproto.TypeRead}, raw)
	if !strings.Contains(string(result), `"type":"image"`) || !strings.Contains(string(result), pixel) {
		t.Fatalf("image block lost: %s", result)
	}
	a.start.ToolImageMaxBytes = 2
	result = (&looper{}).toolResult(context.Background(), &resultStoreSys{}, a, toolCall{ID: "tc", Name: "read"}, resolvedTool{Word: workspaceproto.TypeRead}, raw)
	if !strings.Contains(string(result), `"isError":true`) || !strings.Contains(string(result), "exceeds 2 bytes") {
		t.Fatalf("oversized image result=%s", result)
	}
}

type loopSys struct {
	looperTestBase
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
	case "actor.describe":
		body = map[string]any{"class": "test", "interfaces": []string{"actor"}, "capabilities": map[string]bool{}, "words": map[string]any{
			workspaceproto.TypeRead:  map[string]any{"description": "read", "input_schema": json.RawMessage(workspaceproto.ReadInputSchema)},
			workspaceproto.TypeWrite: map[string]any{"description": "write", "input_schema": json.RawMessage(workspaceproto.WriteInputSchema)},
			workspaceproto.TypeEdit:  map[string]any{"description": "edit", "input_schema": json.RawMessage(workspaceproto.EditInputSchema)},
			workspaceproto.TypeBash:  map[string]any{"description": "bash", "input_schema": json.RawMessage(workspaceproto.BashInputSchema)},
		}}
	default:
		body = map[string]any{}
	}
	raw, _ := json.Marshal(body)
	var m map[string]json.RawMessage
	_ = json.Unmarshal(raw, &m)
	m["status"] = json.RawMessage(`"completed"`)
	payload, _ := json.Marshal(m)
	env := message.Envelope{ID: "response", Kind: message.KindResponse, Payload: payload}
	return immediatePending{msg: actorbase.NewBodyMsg(actorbase.OriginMailbox, context.Background(), env)}, nil
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
	a := &assignment{start: agentloop.StartRequest{SessionID: "session:test", WorkID: "w-1", AssignmentID: "a-1", ControllerActor: "agent:controller:1", ContextActor: "context", LLMActor: "llm", WorkspaceActor: "workspace", MaxTurns: 4}, cause: message.Root(), cancel: cancel, inputs: []agentloop.Input{{ID: "i-1", Seq: 1, Text: "do it"}}}
	l.active["a-1"] = a
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
	if report.State != "completed" {
		t.Fatalf("report=%+v", report)
	}
	context := testContextMessages("session:test")
	if len(context) == 0 || !contains(string(context[len(context)-1]), `"text":"done"`) {
		t.Fatalf("context=%s", mustRaw(context))
	}
	if l.active["a-1"] != nil {
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
