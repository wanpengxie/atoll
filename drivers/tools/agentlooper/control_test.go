package agentlooper

import (
	"context"
	"encoding/json"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	llmproto "github.com/wanpengxie/atoll/drivers/tools/pillm/api"
	workspaceproto "github.com/wanpengxie/atoll/drivers/tools/piworkspace/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"sync"
	"testing"
)

func TestSealAndSteerHaveOneLinearizationPoint(t *testing.T) {
	for i := 0; i < 200; i++ {
		a := &assignment{inputs: []agentloop.Input{{ID: "first", Seq: 1}}, consumed: 1}
		ready := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var sealed bool
		var d agentloop.ControlResult
		var err error
		go func() { defer wg.Done(); <-ready; sealed = a.seal() }()
		go func() {
			defer wg.Done()
			<-ready
			d, err = a.acceptInput(agentloop.InputRequest{ControlID: "s", Inputs: []agentloop.Input{{ID: "second", Seq: 2, Text: "continue"}}})
		}()
		close(ready)
		wg.Wait()
		if err != nil || (sealed && d.Disposition != "target_gone") || (!sealed && d.Disposition != "accepted") {
			t.Fatalf("sealed=%v decision=%+v err=%v", sealed, d, err)
		}
	}
}
func TestControlRetryDoesNotInjectTwiceAndRejectsChangedPayload(t *testing.T) {
	a := &assignment{inputs: []agentloop.Input{{ID: "first", Seq: 1}}, consumed: 1}
	req := agentloop.InputRequest{ControlID: "s", Inputs: []agentloop.Input{{ID: "second", Seq: 2, Text: "continue"}}}
	for i := 0; i < 2; i++ {
		if d, err := a.acceptInput(req); err != nil || d.Disposition != "accepted" {
			t.Fatal(d, err)
		}
	}
	if len(a.inputs) != 2 || len(a.pendingInputs()) != 1 {
		t.Fatal("duplicate injection")
	}
	req.Inputs[0].Text = "changed"
	if _, err := a.acceptInput(req); err == nil {
		t.Fatal("changed retry accepted")
	}
}

// Inject while the outbound model/tool request is being handled, before its
// response. The same execution must consume it after a legal tool boundary.
type steeringSys struct {
	reliabilitySys
	a        *assignment
	trigger  string
	injected bool
}

func (s *steeringSys) Call(c message.Cause, target actor.ActorID, typ string, v any) (actorbase.Pending, error) {
	if typ == s.trigger && !s.injected {
		s.injected = true
		d, err := s.a.acceptInput(agentloop.InputRequest{ControlID: "steer", Inputs: []agentloop.Input{{ID: "next", Seq: 2, Text: "new constraint"}}})
		if err != nil || d.Disposition != "accepted" {
			s.t.Fatal(d, err)
		}
		if s.a.consumed != 1 {
			s.t.Fatal("accepted input claimed consumed during wait")
		}
	}
	return s.reliabilitySys.Call(c, target, typ, v)
}
func TestSteerDuringModelAndToolWaitStaysInSameExecution(t *testing.T) {
	for _, trigger := range []string{llmproto.TypeGenerate, workspaceproto.TypeBash} {
		t.Run(trigger, func(t *testing.T) {
			a := &assignment{start: agentloop.StartRequest{SessionID: "session:v", WorkID: "w", AssignmentID: "e", ControllerActor: "controller", ContextActor: "context", LLMActor: "llm", WorkspaceActor: "workspace", MaxTurns: 4}, cause: message.Root(), inputs: []agentloop.Input{{ID: "first", Seq: 1, Text: "run"}}}
			first := json.RawMessage(finalAssistant)
			if trigger == workspaceproto.TypeBash {
				first = json.RawMessage(`{"role":"assistant","stopReason":"toolUse","content":[{"type":"toolCall","id":"tool","name":"bash","arguments":{"command":"true"}}]}`)
			}
			s := &steeringSys{reliabilitySys: reliabilitySys{t: t, outputs: []json.RawMessage{first, json.RawMessage(finalAssistant)}}, a: a, trigger: trigger}
			l := &looper{active: map[string]*assignment{"e": a}}
			l.drive(context.Background(), s, a)
			if len(s.posts) != 1 {
				t.Fatal("unexpected execution report count")
			}
			var report agentloop.ReportRequest
			_ = json.Unmarshal(s.posts[0].Payload, &report)
			if report.TurnID != "e" || report.State != "completed" || s.llmCalls != 2 || len(a.controls) != 1 {
				t.Fatalf("report=%+v models=%d", report, s.llmCalls)
			}
			if err := validateHistory(testContextMessages("session:v")); err != nil {
				t.Fatal(err)
			}
		})
	}
}
