package agentlooper

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	contextproto "github.com/wanpengxie/atoll/drivers/tools/agentcontext/api"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	"github.com/wanpengxie/atoll/drivers/tools/pibridge"
	llmproto "github.com/wanpengxie/atoll/drivers/tools/pillm/api"
	workspaceproto "github.com/wanpengxie/atoll/drivers/tools/piworkspace/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

type piLoopSys struct {
	actorbase.Sys
	bridge       *pibridge.Bridge
	cwd          string
	llmCall      int
	posts        []behavior.RequestSpec
	toolName     string
	toolArgs     map[string]any
	sawImage     bool
	expectedMime string
}

func completedPending(value json.RawMessage) actorbase.Pending {
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(value, &fields)
	fields["status"] = json.RawMessage(`"completed"`)
	payload, _ := json.Marshal(fields)
	return immediatePending{msg: actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{ID: "pi-response", Kind: message.KindResponse, Payload: payload})}
}

func (s *piLoopSys) Call(_ message.Cause, _ actor.ActorID, typ string, value any) (actorbase.Pending, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	switch typ {
	case contextproto.TypeBuild:
		artifact, _ := json.Marshal(contextproto.Artifact{
			ArtifactID: "ctx-pi", WorkID: "w-pi", AssignmentID: "a-pi", InputThrough: 1,
			SystemPrompt: "Use the available tool, then report completion.",
			Messages:     []json.RawMessage{json.RawMessage(`{"role":"user","content":"write result.txt","timestamp":1}`)},
		})
		return completedPending(artifact), nil
	case llmproto.TypeGenerate:
		s.llmCall++
		var req llmproto.GenerateRequest
		raw, _ := json.Marshal(value)
		_ = json.Unmarshal(raw, &req)
		if s.llmCall == 1 {
			name, args := s.toolName, s.toolArgs
			if name == "" {
				name = "write"
				args = map[string]any{"path": "result.txt", "content": "written by Pi loop\n"}
			}
			req.Options, _ = json.Marshal(map[string]any{"faux_tool_call": map[string]any{"id": "tc-pi", "name": name, "arguments": args}})
		} else {
			messages, _ := json.Marshal(req.Messages)
			s.sawImage = strings.Contains(string(messages), `"type":"image"`) && strings.Contains(string(messages), `"mimeType":"`+s.expectedMime+`"`)
			req.Options = json.RawMessage(`{"faux_response":"finished after tool result"}`)
		}
		result, err := s.bridge.Call(ctx, llmproto.TypeGenerate, req, s.cwd, nil)
		if err != nil {
			return nil, err
		}
		return completedPending(result), nil
	case "actor.describe":
		value, _ := json.Marshal(map[string]any{"class": "workspace", "interfaces": []string{"actor"}, "capabilities": map[string]bool{}, "words": map[string]any{
			workspaceproto.TypeRead:  map[string]any{"description": "read", "input_schema": json.RawMessage(workspaceproto.ReadInputSchema)},
			workspaceproto.TypeWrite: map[string]any{"description": "write", "input_schema": json.RawMessage(workspaceproto.WriteInputSchema)},
			workspaceproto.TypeEdit:  map[string]any{"description": "edit", "input_schema": json.RawMessage(workspaceproto.EditInputSchema)},
			workspaceproto.TypeBash:  map[string]any{"description": "bash", "input_schema": json.RawMessage(workspaceproto.BashInputSchema)},
		}})
		return completedPending(value), nil
	default:
		raw, err := s.bridge.Call(ctx, typ, value, s.cwd, nil)
		if err != nil {
			return nil, err
		}
		return completedPending(raw), nil
	}
}

func TestPNGAndJPEGPassThroughWorkspaceLooperAndFauxProvider(t *testing.T) {
	png, err := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=")
	if err != nil {
		t.Fatal(err)
	}
	fixtures := []struct {
		name, mime string
		data       []byte
	}{{"pixel.png", "image/png", png}, {"pixel.jpg", "image/jpeg", []byte{0xff, 0xd8, 0xff, 0xd9}}}
	for _, fixture := range fixtures {
		t.Run(fixture.mime, func(t *testing.T) {
			cwd := t.TempDir()
			if err := os.WriteFile(filepath.Join(cwd, fixture.name), fixture.data, 0o600); err != nil {
				t.Fatal(err)
			}
			life, cancelLife := context.WithCancel(context.Background())
			bridge, err := pibridge.Start(life, "node", cwd, nil)
			if err != nil {
				cancelLife()
				t.Fatal(err)
			}
			t.Cleanup(func() { bridge.Close(); cancelLife() })
			sys := &piLoopSys{bridge: bridge, cwd: cwd, toolName: "read", toolArgs: map[string]any{"path": fixture.name}, expectedMime: fixture.mime}
			bindings := []agentloop.ToolBinding{{Name: "read", Actor: "workspace", Word: workspaceproto.TypeRead}}
			a := &assignment{start: agentloop.StartRequest{WorkID: "w-image", AssignmentID: "a-image", ContextActor: "context", LLMActor: "llm", WorkspaceActor: "workspace", Tools: &bindings, Model: "faux/faux-1", MaxTurns: 4}, cause: message.Root(), inputs: []agentloop.Input{{ID: "i", Seq: 1, Text: "read image"}}}
			(&looper{}).drive(context.Background(), sys, a)
			if sys.llmCall != 2 || !sys.sawImage || len(sys.posts) != 1 {
				t.Fatalf("llm=%d saw_image=%v reports=%d", sys.llmCall, sys.sawImage, len(sys.posts))
			}
		})
	}
}

type bridgeStoreSys struct {
	actorbase.Sys
	bridge *pibridge.Bridge
	cwd    string
	calls  int
}

func (s *bridgeStoreSys) Call(_ message.Cause, _ actor.ActorID, typ string, value any) (actorbase.Pending, error) {
	s.calls++
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	raw, err := s.bridge.Call(ctx, typ, value, s.cwd, nil)
	if err != nil {
		return nil, err
	}
	return completedPending(raw), nil
}

func TestLongCustomResultIsSavedByteForByteThroughWorkspaceWrite(t *testing.T) {
	cwd := t.TempDir()
	life, cancelLife := context.WithCancel(context.Background())
	bridge, err := pibridge.Start(life, "node", cwd, nil)
	if err != nil {
		cancelLife()
		t.Fatal(err)
	}
	t.Cleanup(func() { bridge.Close(); cancelLife() })
	sys := &bridgeStoreSys{bridge: bridge, cwd: cwd}
	full := strings.Repeat("多字节-long-line", 100)
	raw, _ := json.Marshal(map[string]any{"content": []map[string]any{{"type": "text", "text": full}}})
	a := &assignment{start: agentloop.StartRequest{WorkspaceActor: "workspace", ToolResultMaxLines: 2000, ToolResultMaxBytes: 256}, cause: message.Root()}
	result := (&looper{}).toolResult(context.Background(), sys, a, toolCall{ID: "tc", Name: "custom"}, resolvedTool{Word: "custom.run"}, raw)
	var message struct {
		Details struct {
			Atoll struct {
				Path string `json:"path"`
			} `json:"atoll_output"`
		} `json:"details"`
	}
	if err := json.Unmarshal(result, &message); err != nil || message.Details.Atoll.Path == "" {
		t.Fatalf("result=%s err=%v", result, err)
	}
	saved, err := os.ReadFile(filepath.Join(cwd, message.Details.Atoll.Path))
	if err != nil || string(saved) != full || sys.calls != 1 {
		t.Fatalf("saved bytes=%d calls=%d err=%v", len(saved), sys.calls, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	read, err := bridge.Call(ctx, workspaceproto.TypeRead, workspaceproto.ReadRequest{Path: message.Details.Atoll.Path}, cwd, nil)
	var readResult struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	_ = json.Unmarshal(read, &readResult)
	if err != nil || len(readResult.Content) != 1 || !strings.HasPrefix(readResult.Content[0].Text, full) {
		t.Fatalf("reference read=%s err=%v", read, err)
	}
}

func (s *piLoopSys) Post(spec behavior.RequestSpec) (message.ID, error) {
	s.posts = append(s.posts, spec)
	return "pi-report", nil
}

func TestLooperRunsActualPinnedPiProviderAndWorkspaceTool(t *testing.T) {
	cwd := t.TempDir()
	life, cancelLife := context.WithCancel(context.Background())
	bridge, err := pibridge.Start(life, "node", cwd, nil)
	if err != nil {
		cancelLife()
		t.Fatal(err)
	}
	t.Cleanup(func() { bridge.Close(); cancelLife() })

	sys := &piLoopSys{bridge: bridge, cwd: cwd}
	l := &looper{cfg: Config{MaxAssignments: 1}, active: map[string]*assignment{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := &assignment{
		start: agentloop.StartRequest{WorkID: "w-pi", AssignmentID: "a-pi", ControllerActor: "agent:controller:1", ContextActor: "context", LLMActor: "llm", WorkspaceActor: "workspace", Model: "faux/faux-1", MaxTurns: 4},
		cause: message.Root(), cancel: cancel, inputs: []agentloop.Input{{ID: "i-pi", Seq: 1, Text: "write result.txt"}},
	}
	l.active["w-pi"] = a
	l.drive(ctx, sys, a)

	data, err := os.ReadFile(filepath.Join(cwd, "result.txt"))
	if err != nil || string(data) != "written by Pi loop\n" {
		t.Fatalf("workspace result=%q err=%v", data, err)
	}
	if sys.llmCall != 2 || len(sys.posts) != 1 {
		t.Fatalf("llm calls=%d reports=%d", sys.llmCall, len(sys.posts))
	}
	var report agentloop.ReportRequest
	if err := json.Unmarshal(sys.posts[0].Payload, &report); err != nil || report.State != "completed" || !contains(string(report.Result), "finished after tool result") || len(report.History) != 4 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
}
