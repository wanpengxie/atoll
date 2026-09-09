package native

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	agentproto "github.com/wanpengxie/atoll/drivers/agents/workapi"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/harness"
)

type testState struct {
	mu     sync.Mutex
	values map[resource.ResourceID][]byte
}

func newTestState() *testState { return &testState{values: map[resource.ResourceID][]byte{}} }
func (s *testState) Get(id resource.ResourceID) (accessdoor.Outcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.values[id]
	if !ok {
		return accessdoor.Outcome{RejectReason: access.ResourceNotFound}, nil
	}
	return accessdoor.Outcome{Found: true, Value: append([]byte(nil), raw...)}, nil
}
func (s *testState) Put(id resource.ResourceID, raw []byte) (accessdoor.Outcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[id] = append([]byte(nil), raw...)
	return accessdoor.Outcome{}, nil
}
func (s *testState) Del(id resource.ResourceID) (accessdoor.Outcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.values, id)
	return accessdoor.Outcome{}, nil
}

type testSys struct {
	actorbase.Sys
	state          *testState
	self           actor.ActorID
	replies        map[message.ID]any
	fails          map[message.ID]string
	progress       map[message.ID][]any
	posts          []behavior.RequestSpec
	events         []behavior.EventSpec
	controlResults chan behavior.RequestSpec
}

type testPending struct{ msg actorbase.Msg }

func (p testPending) RequestID() message.ID { return "system-query" }
func (p testPending) Progress() <-chan actorbase.Msg {
	ch := make(chan actorbase.Msg)
	close(ch)
	return ch
}
func (p testPending) Wait(context.Context, time.Duration) (actorbase.Msg, error) { return p.msg, nil }
func (p testPending) Cancel() error                                              { return nil }

func newTestSys(state *testState) *testSys {
	return &testSys{controlResults: make(chan behavior.RequestSpec, 16), state: state, self: "agent:native:1", replies: map[message.ID]any{}, fails: map[message.ID]string{}, progress: map[message.ID][]any{}}
}
func (s *testSys) State() actorbase.StateHandle { return s.state }
func (s *testSys) Self() actor.ActorID          { return s.self }
func (s *testSys) Life() context.Context        { return context.Background() }
func (s *testSys) Reply(msg actorbase.Msg, v any) (message.ID, error) {
	s.replies[msg.ID] = v
	return "reply", nil
}
func (s *testSys) Fail(msg actorbase.Msg, code, _ string, _ ...map[string]any) (message.ID, error) {
	s.fails[msg.ID] = code
	return "fail", nil
}
func (s *testSys) Progress(msg actorbase.Msg, _ string, v any) (message.ID, error) {
	s.progress[msg.ID] = append(s.progress[msg.ID], v)
	return "progress", nil
}
func (s *testSys) Post(spec behavior.RequestSpec) (message.ID, error) {
	if spec.Type == controlDoneType && s.controlResults != nil {
		s.controlResults <- spec
		return "control-done", nil
	}

	s.posts = append(s.posts, spec)
	return message.ID("post"), nil
}
func (s *testSys) Emit(spec behavior.EventSpec) (message.ID, error) {
	s.events = append(s.events, spec)
	return "event", nil
}
func (s *testSys) Call(_ message.Cause, target actor.ActorID, typ string, _ any) (actorbase.Pending, error) {
	if target != actor.SystemActorID || typ != message.TypeSystemLogQuery {
		return nil, fmt.Errorf("unexpected call %s %s", target, typ)
	}
	turns := make([]logQueryTurn, 0)
	for i := len(s.events) - 1; i >= 0; i-- {
		event := s.events[i]
		if event.Type != recoveryEventType {
			continue
		}
		turns = append(turns, logQueryTurn{Messages: []logMessage{{Seq: int64(i + 1), Sender: message.Sender{ID: s.self}, MessageType: event.Type, PayloadText: string(event.Payload)}}})
	}
	payload, _ := json.Marshal(logQueryResponse{Turns: turns, HeadSeq: int64(len(s.events))})
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(payload, &fields)
	fields["status"] = json.RawMessage(`"completed"`)
	payload, _ = json.Marshal(fields)
	return testPending{msg: actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{ID: "system-result", Kind: message.KindResponse, Payload: payload})}, nil
}

func testRequest(id, typ string, body any) actorbase.Msg {
	return testRequestFrom(id, typ, "c", "human:alice:1", body)
}
func testRequestFrom(id, typ, channelID, sender string, body any) actorbase.Msg {
	if typ == agentproto.TypeAsk {
		raw, _ := json.Marshal(body)
		var fields map[string]any
		_ = json.Unmarshal(raw, &fields)
		if _, ok := fields["view_id"]; !ok && fields["related_work_id"] == nil {
			key, _ := fields["submission_key"].(string)
			if key == "" {
				key = id
			}
			fields["view_id"] = "view:" + key
		}
		body = fields
	}
	raw, _ := json.Marshal(body)
	wrapped, _ := json.Marshal(map[string]any{"body": json.RawMessage(raw)})
	return actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{ID: message.ID(id), ChannelID: channel.ID(channelID), Sender: message.Sender{ID: actor.ActorID(sender)}, Kind: message.KindRequest, Type: typ, Payload: wrapped})
}

func TestOneLooperQueuesSecondWorkUntilFirstCloses(t *testing.T) {
	state := newTestState()
	sys := newTestSys(state)
	cfg := Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxTurns: 4}
	c := &controller{cfg: cfg, data: newSnapshot(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	c.handleAsk(sys, testRequest("q1", agentproto.TypeAsk, map[string]any{"text": "one", "delivery": "receipt", "submission_key": "one"}))
	c.handleAsk(sys, testRequest("q2", agentproto.TypeAsk, map[string]any{"text": "two", "delivery": "receipt", "submission_key": "two"}))
	if len(sys.posts) != 1 {
		t.Fatalf("start posts=%d", len(sys.posts))
	}
	first, second := c.data.Works[c.data.Order[0]], c.data.Works[c.data.Order[1]]
	if second.Stage != "queued" || second.ExecutionState != "waiting_capacity" || second.AssignmentID != "" {
		t.Fatalf("queued second=%+v", second)
	}
	c.handleReport(sys, testReport("done", "tool:loop-a:9", agentloop.ReportRequest{WorkID: first.ID, AssignmentID: first.AssignmentID, State: "completed", ConsumedThrough: 1, Result: json.RawMessage(`{"text":"done"}`), ExecutionState: "confirmed"}))
	if len(sys.posts) != 2 || second.AssignmentID == "" || second.Stage != "dispatching" {
		t.Fatalf("second was not scheduled after release: posts=%d work=%+v", len(sys.posts), second)
	}
}

func TestOneLooperDispatchesSeveralIndependentLoopsWithinItsBound(t *testing.T) {
	state := newTestState()
	sys := newTestSys(state)
	cfg := Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxAssignmentsPerLooper: 3, MaxTurns: 4}
	c := &controller{cfg: cfg, data: newSnapshot(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	for i, key := range []string{"one", "two", "three", "four"} {
		c.handleAsk(sys, testRequest(fmt.Sprintf("q%d", i), agentproto.TypeAsk, map[string]any{"text": key, "delivery": "receipt", "submission_key": key}))
	}
	if len(sys.posts) != 3 {
		t.Fatalf("one Looper should receive three concurrent starts, posts=%d", len(sys.posts))
	}
	for i := 0; i < 3; i++ {
		w := c.data.Works[c.data.Order[i]]
		if w.Looper != "loop-a" || w.AssignmentID == "" || w.Stage != "dispatching" {
			t.Fatalf("work %d was not dispatched through the shared Looper: %+v", i, w)
		}
	}
	queued := c.data.Works[c.data.Order[3]]
	if queued.AssignmentID != "" || queued.ExecutionState != "waiting_capacity" {
		t.Fatalf("work beyond the Looper bound was not queued: %+v", queued)
	}
}

func TestWorkControlIsSharedInsideCallingChannelButIsolatedAcrossChannels(t *testing.T) {
	state := newTestState()
	sys := newTestSys(state)
	cfg := Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxTurns: 4}
	c := &controller{cfg: cfg, data: newSnapshot(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	c.handleAsk(sys, testRequestFrom("q", agentproto.TypeAsk, "shared", "human:alice:1", map[string]any{"text": "one", "delivery": "receipt", "submission_key": "one"}))
	w := c.data.Works[c.data.Order[0]]
	c.handleStatus(sys, testRequestFrom("same-channel", agentproto.TypeStatus, "shared", "human:bob:2", map[string]any{"work_id": w.ID}))
	if sys.fails["same-channel"] != "" || sys.replies["same-channel"] == nil {
		t.Fatal("a second member of the calling Channel cannot inspect its Channel work")
	}
	c.handleStatus(sys, testRequestFrom("other-channel", agentproto.TypeStatus, "other", "human:alice:3", map[string]any{"work_id": w.ID}))
	if sys.fails["other-channel"] != "work_not_found" {
		t.Fatalf("cross-channel visibility failure=%q", sys.fails["other-channel"])
	}
}

func TestStatusCursorIsStableWhenNewerWorkArrives(t *testing.T) {
	sys := newTestSys(newTestState())
	c := &controller{cfg: Config{}, data: newSnapshot(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	owner := harness.Caller{Channel: "c", Actor: "human:alice:1"}
	for i, id := range []agentproto.WorkID{"w-1", "w-2", "w-3"} {
		w := &workRecord{ID: id, Owner: owner, State: agentproto.WorkClosed, CreatedAt: int64(i + 1)}
		c.data.Works[string(id)] = w
		c.data.Order = append(c.data.Order, string(id))
	}
	c.handleStatus(sys, testRequest("page-1", agentproto.TypeStatus, map[string]any{"limit": 1}))
	first, ok := sys.replies["page-1"].(agentproto.StatusResponse)
	if !ok || len(first.Works) != 1 || first.Works[0].WorkID != "w-3" || first.NextCursor == "" {
		t.Fatalf("first page=%+v", sys.replies["page-1"])
	}
	newer := &workRecord{ID: "w-4", Owner: owner, State: agentproto.WorkOpen, CreatedAt: 4}
	c.data.Works["w-4"] = newer
	c.data.Order = append(c.data.Order, "w-4")
	c.handleStatus(sys, testRequest("page-2", agentproto.TypeStatus, map[string]any{"limit": 1, "cursor": first.NextCursor}))
	second, ok := sys.replies["page-2"].(agentproto.StatusResponse)
	if !ok || len(second.Works) != 1 || second.Works[0].WorkID != "w-2" {
		t.Fatalf("new work shifted cursor page: %+v", sys.replies["page-2"])
	}
}

func TestStatusReconcilesAcceptedInputDisposition(t *testing.T) {
	sys := newTestSys(newTestState())
	c := &controller{cfg: Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxInputsPerWork: 128, MaxOperationKeys: 256, MaxTurns: 4}, data: newSnapshot(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	c.handleAsk(sys, testRequest("q", agentproto.TypeAsk, map[string]any{"text": "one", "delivery": "receipt", "submission_key": "one"}))
	w := c.data.Works[c.data.Order[0]]
	c.handleStatus(sys, testRequest("assigned", agentproto.TypeStatus, map[string]any{"work_id": w.ID}))
	assigned := sys.replies["assigned"].(agentproto.StatusResponse)
	if len(assigned.Work.Inputs) != 1 || assigned.Work.Inputs[0].Disposition != "assigned" {
		t.Fatalf("assigned status=%+v", assigned.Work)
	}
	c.handleReport(sys, testReport("done", "tool:loop-a:9", agentloop.ReportRequest{WorkID: w.ID, AssignmentID: w.AssignmentID, State: "completed", ConsumedThrough: 1, Result: json.RawMessage(`{"text":"done"}`), ExecutionState: "confirmed"}))
	c.handleStatus(sys, testRequest("included", agentproto.TypeStatus, map[string]any{"work_id": w.ID}))
	included := sys.replies["included"].(agentproto.StatusResponse)
	if len(included.Work.Inputs) != 1 || included.Work.Inputs[0].Disposition != "included" {
		t.Fatalf("included status=%+v", included.Work)
	}
}

func TestAskCarriesCallerOriginAndAttachmentsIntoLooperInput(t *testing.T) {
	sys := newTestSys(newTestState())
	c := &controller{cfg: Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxTurns: 4}, data: newSnapshot(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	attachment := map[string]any{"address": "daemon://local-device/c0.agent/uploads/report.md", "name": "report.md", "media_type": "text/markdown"}
	c.handleAsk(sys, testRequestFrom("q", agentproto.TypeAsk, "calling", "human:alice:1", map[string]any{"text": "read", "delivery": "receipt", "submission_key": "one", "origin": map[string]string{"session": "browser", "label": "Mac"}, "attachments": []any{attachment}}))
	if len(sys.posts) != 1 {
		t.Fatalf("posts=%d", len(sys.posts))
	}
	var start agentloop.StartRequest
	if err := json.Unmarshal(sys.posts[0].Payload, &start); err != nil || len(start.Inputs) != 1 {
		t.Fatalf("start=%+v err=%v", start, err)
	}
	in := start.Inputs[0]
	if in.CallerChannel != "calling" || in.CallerActor != "human:alice:1" || in.Origin == nil || in.Origin.Session != "browser" || len(in.Attachments) != 1 {
		t.Fatalf("input lost request context: %+v", in)
	}
}

func TestDuplicateWaitersShareOneWorkAndBothReceiveResult(t *testing.T) {
	state := newTestState()
	sys := newTestSys(state)
	cfg := Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxTurns: 4}
	c := &controller{cfg: cfg, data: newSnapshot(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	body := map[string]any{"text": "one", "submission_key": "same"}
	c.handleAsk(sys, testRequest("q1", agentproto.TypeAsk, body))
	c.handleAsk(sys, testRequest("q2", agentproto.TypeAsk, body))
	w := c.data.Works[c.data.Order[0]]
	c.handleReport(sys, testReport("done", "tool:loop-a:9", agentloop.ReportRequest{WorkID: w.ID, AssignmentID: w.AssignmentID, State: "completed", ConsumedThrough: 1, Result: json.RawMessage(`{"text":"done"}`), ExecutionState: "confirmed"}))
	if len(c.data.Works) != 1 || sys.replies["q1"] == nil || sys.replies["q2"] == nil {
		t.Fatalf("works=%d q1=%v q2=%v", len(c.data.Works), sys.replies["q1"], sys.replies["q2"])
	}
}

func TestWaitWorkStopsOnlyAfterItsLastCallerLeaves(t *testing.T) {
	sys := newTestSys(newTestState())
	c := &controller{cfg: Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxTurns: 4}, data: newSnapshot(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	body := map[string]any{"text": "one", "submission_key": "same"}
	c.handleAsk(sys, testRequest("q1", agentproto.TypeAsk, body))
	c.handleAsk(sys, testRequest("q2", agentproto.TypeAsk, body))
	w := c.data.Works[c.data.Order[0]]
	closeWait := func(id, requestID string) {
		c.handleWaitClosed(sys, testRequestFrom(id, waitClosedType, "c", sys.self.String(), map[string]any{"work_id": w.ID, "request_id": requestID}))
	}
	closeWait("gone-1", "q1")
	if w.Stage == "stopping" || len(c.wait[w.ID]) != 1 {
		t.Fatalf("first departing waiter stopped shared work: stage=%s waiters=%d", w.Stage, len(c.wait[w.ID]))
	}
	closeWait("gone-2", "q2")
	if w.Stage != "stopping" || len(c.wait[w.ID]) != 0 {
		t.Fatalf("last departing waiter did not stop foreground work: stage=%s waiters=%d", w.Stage, len(c.wait[w.ID]))
	}
	unauthorized := testRequestFrom("forged", waitClosedType, "c", "human:alice:1", map[string]any{"work_id": w.ID, "request_id": "q2"})
	c.handleWaitClosed(sys, unauthorized)
	if sys.fails["forged"] != "permission_denied" {
		t.Fatalf("forged lifecycle report failure=%q", sys.fails["forged"])
	}
}

func TestUnconsumedInputFailsWithoutRedispatch(t *testing.T) {
	sys, c, w := controlFixture(t)
	w.Inputs = append(w.Inputs, inputRecord{Input: agentloop.Input{ID: "late", Seq: 2, Text: "late"}, Disposition: "assigned"})
	w.AssignedThrough = 2
	count := len(sys.posts)
	c.handleReport(sys, testReport("done", "tool:loop-a:9", agentloop.ReportRequest{ViewID: w.ViewID, WorkID: w.ID, AssignmentID: w.AssignmentID, State: "completed", ConsumedThrough: 1, Result: json.RawMessage(`{"text":"done"}`)}))
	if w.Outcome != agentproto.OutcomeFailed || w.ExecutionState != "unconsumed_input" || len(sys.posts) != count {
		t.Fatal("unconsumed input was replayed or hidden")
	}
}

func TestOversizedWorkInputIsRejectedBeforeDurableAcceptance(t *testing.T) {
	sys := newTestSys(newTestState())
	c := &controller{cfg: Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxInputsPerWork: 128, MaxOperationKeys: 256, MaxTurns: 4}, data: newSnapshot(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	c.handleAsk(sys, testRequest("large", agentproto.TypeAsk, map[string]any{"text": strings.Repeat("x", maxWorkInputBytes)}))
	if sys.fails["large"] != "limit_exceeded" || len(c.data.Works) != 0 || len(sys.events) != 0 {
		t.Fatalf("failure=%q works=%d events=%d", sys.fails["large"], len(c.data.Works), len(sys.events))
	}
}

func testReport(id, looper string, body any) actorbase.Msg {
	raw, _ := json.Marshal(body)
	wrapped, _ := json.Marshal(map[string]any{"body": json.RawMessage(raw)})
	return actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{ID: message.ID(id), ChannelID: "c", Sender: message.Sender{ID: actor.ActorID(looper)}, Kind: message.KindRequest, Type: agentloop.TypeReport, Payload: wrapped})
}

func TestIndependentWorksCanCloseOutOfOrder(t *testing.T) {
	state := newTestState()
	sys := newTestSys(state)
	cfg := Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxAssignmentsPerLooper: 2, MaxTurns: 4}
	c := &controller{cfg: cfg, data: newSnapshot(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	one := testRequest("q1", agentproto.TypeAsk, map[string]any{"text": "long"})
	two := testRequest("q2", agentproto.TypeAsk, map[string]any{"text": "short"})
	c.handleAsk(sys, one)
	c.handleAsk(sys, two)
	if len(c.data.Works) != 2 || len(sys.posts) != 2 {
		t.Fatalf("works=%d posts=%d", len(c.data.Works), len(sys.posts))
	}
	var starts []agentloop.StartRequest
	for _, post := range sys.posts {
		var req agentloop.StartRequest
		if json.Unmarshal(post.Payload, &req) != nil {
			t.Fatal("bad start")
		}
		starts = append(starts, req)
	}
	if starts[0].WorkID == starts[1].WorkID || starts[0].AssignmentID == starts[1].AssignmentID {
		t.Fatal("work/assignment identity reused")
	}
	second := starts[1]
	c.handleReport(sys, testReport("r2", "tool:loop-a:9", agentloop.ReportRequest{WorkID: second.WorkID, AssignmentID: second.AssignmentID, State: "completed", ConsumedThrough: 1, Result: json.RawMessage(`{"text":"short done"}`), ExecutionState: "confirmed"}))
	if _, ok := sys.replies["q2"]; !ok {
		t.Fatal("second work did not answer while first remained open")
	}
	if _, ok := sys.replies["q1"]; ok {
		t.Fatal("first work answered before its report")
	}
	if c.data.Works[string(starts[0].WorkID)].State != agentproto.WorkOpen {
		t.Fatal("closing w2 changed w1")
	}
}

func TestTargetedInterruptDoesNotFreezeSibling(t *testing.T) {
	state := newTestState()
	sys := newTestSys(state)
	cfg := Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxAssignmentsPerLooper: 2, MaxTurns: 4}
	c := &controller{cfg: cfg, data: newSnapshot(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	c.handleAsk(sys, testRequest("q1", agentproto.TypeAsk, map[string]any{"text": "one", "delivery": "receipt", "submission_key": "one"}))
	c.handleAsk(sys, testRequest("q2", agentproto.TypeAsk, map[string]any{"text": "two", "delivery": "receipt", "submission_key": "two"}))
	first := c.data.Works[c.data.Order[0]]
	second := c.data.Works[c.data.Order[1]]
	c.handleReport(sys, testReport("accepted-1", "tool:loop-a:9", agentloop.ReportRequest{WorkID: first.ID, AssignmentID: first.AssignmentID, State: "accepted", ExecutionState: "confirmed_running"}))
	c.handleReport(sys, testReport("accepted-2", "tool:loop-a:9", agentloop.ReportRequest{WorkID: second.ID, AssignmentID: second.AssignmentID, State: "accepted", ExecutionState: "confirmed_running"}))
	before := len(sys.posts)
	c.handleInterrupt(sys, testRequest("stop", agentproto.TypeInterrupt, map[string]any{"work_id": first.ID}))
	if first.Stage != "stopping" || second.Stage != "thinking" {
		t.Fatalf("first=%s second=%s", first.Stage, second.Stage)
	}
	if len(sys.posts) != before+1 || sys.posts[len(sys.posts)-1].Type != agentloop.TypeStop {
		t.Fatal("targeted loop.stop not posted")
	}
}

func TestTargetedInterruptOperationKeyDoesNotPostStopTwice(t *testing.T) {
	sys := newTestSys(newTestState())
	c := &controller{cfg: Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxTurns: 4}, data: newSnapshot(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	c.handleAsk(sys, testRequest("q", agentproto.TypeAsk, map[string]any{"text": "one", "delivery": "receipt", "submission_key": "one"}))
	w := c.data.Works[c.data.Order[0]]
	body := map[string]any{"work_id": w.ID, "operation_key": "stop-1"}
	c.handleInterrupt(sys, testRequest("stop-1", agentproto.TypeInterrupt, body))
	c.handleInterrupt(sys, testRequest("stop-2", agentproto.TypeInterrupt, body))
	stops := 0
	for _, post := range sys.posts {
		if post.Type == agentloop.TypeStop {
			stops++
		}
	}
	if stops != 1 || sys.replies["stop-1"] == nil || sys.replies["stop-2"] == nil {
		t.Fatalf("stops=%d replies=%+v", stops, sys.replies)
	}
	c.steer(sys, testRequest("conflict", agentproto.TypeSteer, map[string]any{"work_id": w.ID, "text": "more", "operation_key": "stop-1"}))
	if sys.fails["conflict"] != "operation_conflict" {
		t.Fatalf("conflict=%q", sys.fails["conflict"])
	}
}

func TestAgentWideInterruptDoesNotDispatchAnotherWorkMidBatch(t *testing.T) {
	sys := newTestSys(newTestState())
	c := &controller{cfg: Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxTurns: 4}, data: newSnapshot(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	c.handleAsk(sys, testRequest("q1", agentproto.TypeAsk, map[string]any{"text": "one", "delivery": "receipt", "submission_key": "one"}))
	c.handleAsk(sys, testRequest("q2", agentproto.TypeAsk, map[string]any{"text": "two", "delivery": "receipt", "submission_key": "two"}))
	c.handleInterrupt(sys, testRequest("stop-all", agentproto.TypeInterrupt, map[string]any{}))
	starts := 0
	for _, post := range sys.posts {
		if post.Type == agentloop.TypeStart {
			starts++
		}
	}
	if starts != 1 {
		t.Fatalf("global interrupt dispatched a work it was about to cancel: starts=%d posts=%+v", starts, sys.posts)
	}
	for _, w := range c.data.Works {
		if w.State == agentproto.WorkOpen && w.Stage != "stopping" {
			t.Fatalf("work left runnable after global interrupt: %+v", w)
		}
	}
}

func TestReceiptIsIdempotentAndRestartMarksUnknownInsteadOfReplaying(t *testing.T) {
	state := newTestState()
	sys := newTestSys(state)
	cfg := Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxTurns: 4}
	c := &controller{cfg: cfg, data: newSnapshot(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	body := map[string]any{"text": "do it", "delivery": "receipt", "submission_key": "stable"}
	c.handleAsk(sys, testRequest("q1", agentproto.TypeAsk, body))
	c.handleAsk(sys, testRequest("q2", agentproto.TypeAsk, body))
	if len(c.data.Works) != 1 {
		t.Fatalf("duplicate receipt created %d works", len(c.data.Works))
	}
	restarted := &controller{cfg: cfg, data: newSnapshot(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	if err := restarted.recoverLedger(sys); err != nil {
		t.Fatal(err)
	}
	postCount := len(sys.posts)
	restarted.reconcileAfterRestart(sys)
	w := restarted.data.Works[restarted.data.Order[0]]
	if w.State != agentproto.WorkClosed || w.Outcome != agentproto.OutcomeFailed || w.ExecutionState != "execution_unknown_after_restart" {
		t.Fatalf("recovered work=%+v", w)
	}
	if len(sys.posts) != postCount {
		t.Fatal("an already-dispatched assignment was blindly replayed")
	}
}

func TestQueuedContinuationClosesUnknownWithoutReplay(t *testing.T) {
	sys := newTestSys(newTestState())
	c := &controller{cfg: Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxTurns: 4}, data: newSnapshot(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	w := &workRecord{
		ID: "w-cont", Owner: harness.Caller{Channel: "c", Actor: "human:alice:1"}, State: agentproto.WorkOpen,
		Stage: "queued", ExecutionState: "not_started", Continuation: true, CreatedAt: 1, UpdatedAt: 1,
		Inputs: []inputRecord{
			{Input: agentloop.Input{ID: "i-1", Seq: 1, Text: "first"}, Disposition: "included"},
			{Input: agentloop.Input{ID: "i-2", Seq: 2, Text: "second"}, Disposition: "accepted"},
		},
		Context: []json.RawMessage{json.RawMessage(`{"role":"user","content":"first"}`), json.RawMessage(`{"role":"assistant","content":[{"type":"text","text":"done"}]}`)},
	}
	c.data.Works[string(w.ID)] = w
	c.data.Order = []string{string(w.ID)}
	if err := c.commit(sys, message.Root(), w); err != nil {
		t.Fatal(err)
	}
	restarted := &controller{cfg: c.cfg, data: newSnapshot(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	if err := restarted.recoverLedger(sys); err != nil {
		t.Fatal(err)
	}
	if err := restarted.reconcileAfterRestart(sys); err != nil {
		t.Fatal(err)
	}
	recovered := restarted.data.Works[string(w.ID)]
	if recovered.State != agentproto.WorkClosed || recovered.ExecutionState != "execution_unknown_after_restart" || len(sys.posts) != 0 {
		t.Fatalf("unexpected replay: %+v", recovered)
	}

}

func TestRelatedWorkContinuesExistingView(t *testing.T) {
	sys := newTestSys(newTestState())
	c := &controller{cfg: Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxInputsPerWork: 128, MaxOperationKeys: 256, MaxTurns: 4}, data: newSnapshot(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	prior := []json.RawMessage{json.RawMessage(`{"role":"user","content":"root"}`), json.RawMessage(`{"role":"assistant","content":[{"type":"text","text":"checkpoint"}]}`)}
	source := &workRecord{ID: "w-source", ViewID: "view:source", Owner: harness.Caller{Channel: "c", Actor: "human:alice:1"}, State: agentproto.WorkClosed, Outcome: agentproto.OutcomeCompleted, Context: prior, CreatedAt: 1, UpdatedAt: 1}
	c.sessions = map[string]*session{"view:source": {ID: "view:source", Scope: "c", History: prior}}
	c.sessionOrder = []string{"view:source"}
	c.data.Works[string(source.ID)] = source
	c.data.Order = []string{string(source.ID)}
	c.handleAsk(sys, testRequest("branch", agentproto.TypeAsk, map[string]any{
		"text": "try another approach", "delivery": "receipt", "submission_key": "branch-1", "related_work_id": source.ID,
	}))
	if sys.fails["branch"] != "" || len(sys.posts) != 1 {
		t.Fatalf("failure=%q posts=%d", sys.fails["branch"], len(sys.posts))
	}
	var start agentloop.StartRequest
	if err := json.Unmarshal(sys.posts[0].Payload, &start); err != nil || len(start.Prior) != len(prior) {
		t.Fatalf("start=%+v err=%v", start, err)
	}
	branched := c.data.Works[string(start.WorkID)]
	if branched.RelatedWorkID != source.ID || branched.ViewID != source.ViewID {
		t.Fatalf("branch=%+v", branched)
	}
	if len(cloneRecoveryWork(source).Context) != len(prior) {
		t.Fatal("closed checkpoint was dropped from recovery snapshot")
	}
}

func TestRelatedWorkWithoutCheckpointIsRejectedHonestly(t *testing.T) {
	sys := newTestSys(newTestState())
	c := &controller{cfg: Config{MaxOpenWorks: 8}, data: newSnapshot(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	source := &workRecord{ID: "w-source", Owner: harness.Caller{Channel: "c", Actor: "human:alice:1"}, State: agentproto.WorkOpen}
	c.data.Works[string(source.ID)] = source
	c.handleAsk(sys, testRequest("branch", agentproto.TypeAsk, map[string]any{"text": "branch", "related_work_id": source.ID}))
	if sys.fails["branch"] != "context_unavailable" {
		t.Fatalf("failure=%q", sys.fails["branch"])
	}
}
