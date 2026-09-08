package agentlooper

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	contextproto "github.com/wanpengxie/atoll/drivers/tools/agentcontext/api"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	"github.com/wanpengxie/atoll/drivers/tools/pibridge"
	llmproto "github.com/wanpengxie/atoll/drivers/tools/pillm/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

type piLoopSys struct {
	actorbase.Sys
	bridge  *pibridge.Bridge
	cwd     string
	llmCall int
	posts   []behavior.RequestSpec
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
			req.Options = json.RawMessage(`{"faux_tool_call":{"id":"tc-pi","name":"write","arguments":{"path":"result.txt","content":"written by Pi loop\n"}}}`)
		} else {
			req.Options = json.RawMessage(`{"faux_response":"finished after tool result"}`)
		}
		result, err := s.bridge.Call(ctx, llmproto.TypeGenerate, req, s.cwd, nil)
		if err != nil {
			return nil, err
		}
		return completedPending(result), nil
	default:
		raw, err := s.bridge.Call(ctx, typ, value, s.cwd, nil)
		if err != nil {
			return nil, err
		}
		return completedPending(raw), nil
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
