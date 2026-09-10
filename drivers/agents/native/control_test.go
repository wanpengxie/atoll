package native

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	agentproto "github.com/wanpengxie/atoll/drivers/agents/workapi"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/runtime/harness"
)

func controlFixture(t *testing.T) (*testSys, *controller, *workRecord) {
	t.Helper()
	sys := newTestSys(newTestState())
	c := &controller{cfg: Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 32, MaxTurns: 4, MaxInputsPerWork: 128, MaxOperationKeys: 256, MaxAssignmentsPerLooper: 4}, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	attachTestInbox(c)
	c.handleAsk(sys, testRequest("q", agentproto.TypeAsk, map[string]any{"text": "first", "session_id": "session:v", "delivery": "receipt", "submission_key": "one"}))
	w := c.data.Works[c.data.Order[0]]
	w.Stage, w.ExecutionState = "thinking", "confirmed_running"
	return sys, c, w
}
func attachTestInbox(c *controller) {
	c.local = context.Background()
	c.inbox = make(chan controllerEvent, controllerInboxCapacity)
}
func drainControl(t *testing.T, _ *testSys, c *controller) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case event := <-c.inbox:
			if event.control != nil {
				return
			}
		case <-deadline:
			t.Fatal("control delivery did not terminate")
		}
	}
}
func admitControl(t *testing.T, sys *testSys, c *controller, s *session) agentloop.ControlResult {
	t.Helper()
	drainControl(t, sys, c)
	if s.Control == nil {
		t.Fatal("missing pending control")
	}
	d := agentloop.ControlResult{ControlID: s.Control.ID, Disposition: "accepted", Inputs: s.Control.Request.Inputs}
	c.settleControl(sys, s, d, false)
	return d
}
func TestSteerTransfersOwnerOnceAndReportBeforeAck(t *testing.T) {
	sys, c, old := controlFixture(t)
	s := c.sessions[old.SessionID]
	execution := s.Execution
	c.steer(sys, testRequest("steer", agentproto.TypeSteer, map[string]any{"work_id": old.ID, "text": "second", "expected_turn_id": execution, "operation_key": "op"}))
	if s.Owner != old.ID || sys.replies["steer"] != nil {
		t.Fatal("Post was treated as acceptance")
	}
	drainControl(t, sys, c)
	pc := s.Control
	d := agentloop.ControlResult{ControlID: pc.ID, Disposition: "accepted", Inputs: pc.Request.Inputs}
	c.settleControl(sys, s, d, false)
	deliverTestReport(c, sys, testReport("done", "tool:loop-a:9", agentloop.ReportRequest{SessionID: s.ID, TurnID: execution, State: "completed"}))
	next := c.data.Works[string(pc.Targets[0])]
	if old.State != agentproto.WorkClosed || next.Outcome != agentproto.OutcomeCompleted || next.AssignmentID != execution || s.Control != nil {
		t.Fatalf("ownership old=%+v new=%+v", old, next)
	}
	c.steer(sys, testRequest("again", agentproto.TypeSteer, map[string]any{"work_id": old.ID, "text": "second", "expected_turn_id": execution, "operation_key": "op"}))
	if sys.replies["again"] == nil || len(c.data.Works) != 2 {
		t.Fatal("idempotent retry changed ownership")
	}
	c.steer(sys, testRequest("conflict", agentproto.TypeSteer, map[string]any{"work_id": old.ID, "text": "different", "operation_key": "op"}))
	if sys.fails["conflict"] != "operation_conflict" {
		t.Fatal("operation conflict hidden")
	}
}
func TestSteerRejectionRestoresOriginalQueueOrder(t *testing.T) {
	sys, c, w := controlFixture(t)
	s := c.sessions[w.SessionID]
	for _, id := range []string{"a", "b", "c"} {
		c.handleAsk(sys, testRequest(id, agentproto.TypeAsk, map[string]any{"text": id, "session_id": s.ID, "delivery": "receipt", "submission_key": id}))
	}
	original := append([]agentproto.WorkID(nil), s.Buffer...)
	c.steer(sys, testRequest("all", agentproto.TypeSteer, map[string]any{"session_id": s.ID, "all": true}))
	drainControl(t, sys, c)
	c.settleControl(sys, s, agentloop.ControlResult{ControlID: s.Control.ID, Disposition: "target_gone"}, false)
	if string(mustJSON(original)) != string(mustJSON(s.Buffer)) {
		t.Fatal("queue reordered")
	}
}
func TestSteerAllOnlyIncludesCallerAndView(t *testing.T) {
	sys, c, w := controlFixture(t)
	s := c.sessions[w.SessionID]
	c.handleAsk(sys, testRequest("a", agentproto.TypeAsk, map[string]any{"text": "alice", "session_id": s.ID}))
	c.handleAsk(sys, testRequestFrom("b", agentproto.TypeAsk, "c", "human:bob:1", map[string]any{"text": "bob", "session_id": s.ID}))
	c.handleAsk(sys, testRequest("other", agentproto.TypeAsk, map[string]any{"text": "other", "session_id": "view:other"}))
	c.steer(sys, testRequest("all", agentproto.TypeSteer, map[string]any{"session_id": s.ID, "all": true}))
	drainControl(t, sys, c)
	if len(s.Control.Targets) != 1 || c.data.Works[string(s.Control.Targets[0])].SourceRequest != "a" || len(s.Buffer) != 1 {
		t.Fatal("steer swept other caller or view")
	}
	c.settleControl(sys, s, agentloop.ControlResult{ControlID: s.Control.ID, Disposition: "target_gone"}, false)
}

func TestForgedCallerCannotAcquireWorkOwnership(t *testing.T) {
	sys, c, _ := controlFixture(t)
	c.handleAsk(sys, testRequest("owned", agentproto.TypeAsk, map[string]any{"text": "alice", "session_id": "session:v"}))
	w := c.requestWork("owned")
	if w == nil || w.Submitter != "human:alice:1" {
		t.Fatalf("work=%+v", w)
	}
	forged := testRequestFrom("hold-forged", agentproto.TypeHold, "c", "human:bob:1", map[string]any{"session_id": "session:v", "target": "owned"})
	app := forged.Context()
	app.Caller = &harness.Caller{Channel: "c", Actor: "human:alice:1"}
	forged = forged.WithContext(app)
	c.editControl(sys, forged)
	if sys.fails[forged.ID] != "target_not_owned" {
		t.Fatalf("forged caller authorized hold: replies=%v failures=%v", sys.replies, sys.fails)
	}
}
func TestViewControlsFreezeCASAndReplacementIdentity(t *testing.T) {
	sys, c, w := controlFixture(t)
	s := c.sessions[w.SessionID]
	c.handleAsk(sys, testRequest("queued", agentproto.TypeAsk, map[string]any{"text": "old", "session_id": s.ID}))
	c.editControl(sys, testRequestFrom("edit", agentproto.TypeReplace, "c", "human:bob:1", map[string]any{"target": "queued", "old_text": "old", "new_text": "new"}))
	replacement := c.data.Works[string(s.Buffer[0])]
	if replacement.Owner.Actor != "human:bob:1" || replacement.Inputs[0].CallerActor != replacement.Owner.Actor || c.requestWork("queued").State != agentproto.WorkClosed {
		t.Fatal("replace lost sender or queue position")
	}
	c.editControl(sys, testRequest("bad", agentproto.TypeReplace, map[string]any{"target": "edit", "old_text": "old", "new_text": "bad"}))
	if sys.fails["bad"] != "cas_mismatch" {
		t.Fatal("CAS missing")
	}
	s.Freeze = "interrupt"
	c.editControl(sys, testRequest("unhold", agentproto.TypeUnhold, map[string]any{"session_id": s.ID}))
	if s.Freeze != "interrupt" {
		t.Fatal("unhold cleared interrupt")
	}
	c.editControl(sys, testRequest("hold", agentproto.TypeHold, map[string]any{"session_id": s.ID}))
	c.editControl(sys, testRequest("release", agentproto.TypeUnhold, map[string]any{"session_id": s.ID}))
	if s.Freeze != "interrupt" {
		t.Fatal("prior interrupt not restored")
	}
	c.handleAsk(sys, testRequest("resume", agentproto.TypeAsk, map[string]any{"text": "continue", "session_id": s.ID}))
	if s.Freeze != "" {
		t.Fatal("ask did not release freeze")
	}
}
func TestHoldOwnerRequeuesOnlyAfterExecutionStops(t *testing.T) {
	sys, c, w := controlFixture(t)
	s := c.sessions[w.SessionID]
	execution := s.Execution
	c.editControl(sys, testRequest("hold", agentproto.TypeHold, map[string]any{"target": "q"}))
	if len(s.Buffer) != 0 || !s.Rebuffer || w.Stage != "stopping" {
		t.Fatal("owner requeued before stopping")
	}
	deliverTestReport(c, sys, testReport("done", "tool:loop-a:9", agentloop.ReportRequest{SessionID: s.ID, WorkID: w.ID, AssignmentID: execution, State: "cancelled", ConsumedThrough: 1, History: []json.RawMessage{json.RawMessage(`{"role":"user","content":"first"}`)}}))
	if len(s.Buffer) != 1 || s.Execution != "" || !w.Resumed || s.Freeze != "hold" {
		t.Fatal("hold did not preserve frozen editable owner")
	}
	c.editControl(sys, testRequest("edit", agentproto.TypeReplace, map[string]any{"target": "q", "old_text": w.Inputs[0].Text, "new_text": "revised"}))
	if sys.fails["edit"] != "" {
		t.Fatal("stopped owner not editable")
	}
}
func TestControlAmbiguousSessionAndQueuedInterruptIsolation(t *testing.T) {
	sys, c, w := controlFixture(t)
	s := c.sessions[w.SessionID]
	c.handleAsk(sys, testRequest("queued", agentproto.TypeAsk, map[string]any{"text": "later", "session_id": s.ID}))
	queued := c.requestWork("queued")
	c.handleAsk(sys, testRequest("other", agentproto.TypeAsk, map[string]any{"text": "other", "session_id": "view:other"}))
	c.editControl(sys, testRequest("ambiguous", agentproto.TypeHold, map[string]any{}))
	if sys.fails["ambiguous"] != "scope_required" {
		t.Fatal("ambiguous scope guessed")
	}
	c.steer(sys, testRequest("missing", agentproto.TypeSteer, map[string]any{"session_id": s.ID, "target": "missing"}))
	if sys.fails["missing"] != "work_not_found" {
		t.Fatal("view hid invalid target")
	}
	c.handleInterrupt(sys, testRequest("stop", agentproto.TypeInterrupt, map[string]any{"work_id": queued.ID}))
	if queued.State != agentproto.WorkClosed || w.Stage == "stopping" || s.Freeze != "" {
		t.Fatal("queued cancellation stopped executing owner")
	}
	c.editControl(sys, testRequestFrom("foreign", agentproto.TypeHold, "elsewhere", "human:alice:1", map[string]any{"session_id": "s-missing"}))
	if sys.fails["foreign"] != "session_not_found" {
		t.Fatal("control on a session that does not exist was allowed")
	}
}
func TestSteerLimitsSurviveOwnerTransfer(t *testing.T) {
	sys, c, w := controlFixture(t)
	c.cfg.MaxInputsPerWork = 2
	c.cfg.MaxOperationKeys = 1
	s := c.sessions[w.SessionID]
	c.steer(sys, testRequest("steer", agentproto.TypeSteer, map[string]any{"work_id": w.ID, "text": "second", "operation_key": "op"}))
	admitControl(t, sys, c, s)
	c.steer(sys, testRequest("extra", agentproto.TypeSteer, map[string]any{"work_id": s.Owner, "text": "third"}))
	if sys.fails["extra"] != "limit_exceeded" {
		t.Fatal("input limit bypassed")
	}
	c.handleInterrupt(sys, testRequest("stop", agentproto.TypeInterrupt, map[string]any{"work_id": s.Owner, "operation_key": "op2"}))
	if sys.fails["stop"] != "limit_exceeded" {
		t.Fatal("operation limit bypassed")
	}
}

func TestMissingViewDoesNotCreateIndependentSession(t *testing.T) {
	sys, c, _ := controlFixture(t)
	msg := testRequest("unscoped", agentproto.TypeAsk, map[string]any{"text": "new"})
	msg = actorbase.NewBodyMsgContext(actorbase.OriginMailbox, context.Background(), harness.Context{}, msg.Envelope)
	before := len(c.sessions)
	c.handleAsk(sys, msg)
	if sys.fails["unscoped"] != "scope_required" || len(c.sessions) != before {
		t.Fatal("missing scope created unrelated history")
	}
}
func TestIdleTargetSteerUnfreezesAndPreservesOperationReceipt(t *testing.T) {
	sys, c, w := controlFixture(t)
	s := c.sessions[w.SessionID]
	c.cfg.MaxAssignmentsPerLooper = 1
	c.handleAsk(sys, testRequest("later", agentproto.TypeAsk, map[string]any{"text": "later", "session_id": s.ID, "delivery": "receipt", "submission_key": "later"}))
	s.Freeze = "hold"
	deliverTestReport(c, sys, testReport("done", "tool:loop-a:9", agentloop.ReportRequest{SessionID: s.ID, WorkID: w.ID, AssignmentID: w.AssignmentID, State: "completed", ConsumedThrough: 1, Result: json.RawMessage(`{"text":"done"}`)}))
	body := map[string]any{"target": "later", "operation_key": "prioritize"}
	c.steer(sys, testRequest("pick", agentproto.TypeSteer, body))
	if s.Execution == "" || s.Freeze != "" || sys.replies["pick"] == nil {
		t.Fatal("idle steer did not resume")
	}
	before := len(sys.posts)
	c.steer(sys, testRequest("retry", agentproto.TypeSteer, body))
	if len(sys.posts) != before || sys.replies["retry"] == nil {
		t.Fatal("idle steer retry lost receipt")
	}
}
func TestPendingControlDoesNotBlockAnotherView(t *testing.T) {
	sys, c, w := controlFixture(t)
	s := c.sessions[w.SessionID]
	c.steer(sys, testRequest("steer", agentproto.TypeSteer, map[string]any{"session_id": s.ID, "text": "wait"}))
	drainControl(t, sys, c)
	for _, id := range []string{"b", "c", "d"} {
		c.handleAsk(sys, testRequest(id, agentproto.TypeAsk, map[string]any{"session_id": "view:" + id, "text": id, "delivery": "receipt", "submission_key": id}))
	}
	for _, id := range []string{"b", "c", "d"} {
		if c.sessions["view:"+id].Execution == "" {
			t.Fatal("pending control blocked independent view", id)
		}
	}
	c.settleControl(sys, s, agentloop.ControlResult{ControlID: s.Control.ID, Disposition: "target_gone"}, false)
}
func TestUnknownControlIsNotAutomaticallyRequeued(t *testing.T) {
	sys, c, w := controlFixture(t)
	s := c.sessions[w.SessionID]
	c.handleAsk(sys, testRequest("waiting", agentproto.TypeAsk, map[string]any{"session_id": s.ID, "text": "later"}))
	c.steer(sys, testRequest("target", agentproto.TypeSteer, map[string]any{"target": "waiting"}))
	drainControl(t, sys, c)
	c.settleControl(sys, s, agentloop.ControlResult{ControlID: s.Control.ID}, true)
	if len(s.Buffer) != 0 || s.Freeze != "interrupt" || w.Stage != "stopping" || c.requestWork("waiting").ExecutionState != "control_unknown" {
		t.Fatal("unknown admission was replayed or hidden")
	}
}

func TestStaleControlCompletionIsIgnoredPrivately(t *testing.T) {
	sys, c, w := controlFixture(t)
	s := c.sessions[w.SessionID]
	stale := controlDone{Session: s.ID, Execution: "old", ID: "old"}
	c.controlDone(sys, stale)
	if len(sys.replies) != 1 || len(sys.fails) != 0 || s.Control != nil {
		t.Fatalf("stale private completion changed protocol state: replies=%v fails=%v control=%+v", sys.replies, sys.fails, s.Control)
	}
}

func TestAgentInterruptCancelsAllBufferedWorksWithoutSkipping(t *testing.T) {
	sys, c, w := controlFixture(t)
	s := c.sessions[w.SessionID]
	for _, id := range []string{"a", "b", "c", "d"} {
		c.handleAsk(sys, testRequest(id, agentproto.TypeAsk, map[string]any{"text": id, "session_id": s.ID, "delivery": "receipt", "submission_key": id}))
	}
	c.handleInterrupt(sys, testRequest("all", agentproto.TypeInterrupt, map[string]any{}))
	for _, id := range []string{"a", "b", "c", "d"} {
		if c.requestWork(id).State != agentproto.WorkClosed {
			t.Fatal("buffer cancellation skipped", id)
		}
	}
	if len(s.Buffer) != 0 || w.Stage != "stopping" {
		t.Fatal("active or queued work escaped interrupt")
	}
}
