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
	"github.com/wanpengxie/atoll/runtime/actorcaps"
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
	state    *testState
	self     actor.ActorID
	replies  map[message.ID]any
	fails    map[message.ID]string
	progress map[message.ID][]any
	posts    []behavior.RequestSpec
	events   []behavior.EventSpec
	ledger   []logMessage
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
	return &testSys{state: state, self: "agent:native:1", replies: map[message.ID]any{}, fails: map[message.ID]string{}, progress: map[message.ID][]any{}}
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
	s.posts = append(s.posts, spec)
	return message.ID("post"), nil
}
func (s *testSys) Emit(spec behavior.EventSpec) (message.ID, error) {
	s.events = append(s.events, spec)
	return "event", nil
}
func (s *testSys) Call(_ message.Cause, app harness.Context, target actor.ActorID, typ string, payload any) (actorbase.Pending, error) {
	if typ == agentloop.TypeStart || typ == agentloop.TypeInput || typ == agentloop.TypeInspect || typ == agentloop.TypeStop {
		raw, _ := json.Marshal(payload)
		s.posts = append(s.posts, behavior.RequestSpec{Type: typ, Audience: message.Audience{target}, Payload: raw})
		disposition := "accepted"
		if typ == agentloop.TypeInspect {
			disposition = "observed"
		}
		body, _ := json.Marshal(map[string]any{"status": "completed", "disposition": disposition})
		if typ == agentloop.TypeStart {
			var start agentloop.StartRequest
			_ = json.Unmarshal(raw, &start)
			id := message.ID(fmt.Sprintf("start-%d", len(s.ledger)+1))
			s.appendLedger(logMessage{ID: id, Kind: message.KindRequest, MessageType: typ, Sender: message.Sender{ID: s.self}, Audience: message.Audience{target}, PayloadText: string(raw)}, start.SessionID)
			s.appendLedger(logMessage{ID: id + "-ack", ParentID: id, Kind: message.KindResponse, MessageType: typ, Sender: message.Sender{ID: target}, PayloadText: string(body)}, start.SessionID)
		}
		return testPending{msg: actorbase.NewBodyMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{ID: "call-result", Kind: message.KindResponse, Payload: body})}, nil
	}
	return nil, fmt.Errorf("unexpected call %s %s", target, typ)
}

type nativeTestView struct {
	actorcaps.LedgerView
	read func(context.Context, actorcaps.LedgerRead) (actorcaps.LedgerSnapshot, error)
}

func (v nativeTestView) Read(ctx context.Context, q actorcaps.LedgerRead) (actorcaps.LedgerSnapshot, error) {
	return v.read(ctx, q)
}
func (s *testSys) View() actorcaps.LedgerView {
	return nativeTestView{read: func(ctx context.Context, q actorcaps.LedgerRead) (actorcaps.LedgerSnapshot, error) {
		if err := ctx.Err(); err != nil {
			return actorcaps.LedgerSnapshot{}, err
		}
		out := actorcaps.LedgerSnapshot{HeadSeq: int64(len(s.ledger))}
		for _, r := range s.ledger {
			app, _, err := harness.UnwrapPayload(json.RawMessage(r.PayloadText))
			if err != nil {
				return actorcaps.LedgerSnapshot{}, err
			}
			if q.Session != "" && q.Session != app.Session {
				continue
			}
			out.Rows = append(out.Rows, actorcaps.LedgerRow{Seq: r.Seq, IsTerminal: r.Terminal, Envelope: message.Envelope{ID: r.ID, Sender: r.Sender, Audience: r.Audience, Kind: r.Kind, Type: r.MessageType, ParentID: r.ParentID, TSReceived: r.TSReceived, Payload: json.RawMessage(r.PayloadText)}})
		}
		return out, nil
	}}
}

func testRequest(id, typ string, body any) actorbase.Msg {
	return testRequestFrom(id, typ, "c", "human:alice:1", body)
}
func testRequestFrom(id, typ, channelID, sender string, body any) actorbase.Msg {
	raw, _ := json.Marshal(body)
	var fields map[string]any
	_ = json.Unmarshal(raw, &fields)
	session, _ := fields["session_id"].(string)
	delete(fields, "session_id")
	if typ == agentproto.TypeAsk && session == "" && fields["related_work_id"] == nil {
		key, _ := fields["submission_key"].(string)
		if key == "" {
			key = id
		}
		session = "view:" + key
	}
	raw, _ = json.Marshal(fields)
	return actorbase.NewBodyMsgContext(actorbase.OriginMailbox, context.Background(), harness.Context{Session: session}, message.Envelope{ID: message.ID(id), ChannelID: channel.ID(channelID), Sender: message.Sender{ID: actor.ActorID(sender)}, Kind: message.KindRequest, Type: typ, Payload: raw})
}

func TestOneLooperQueuesSecondWorkUntilFirstCloses(t *testing.T) {
	state := newTestState()
	sys := newTestSys(state)
	cfg := Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxTurns: 4}
	c := &controller{cfg: cfg, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	c.handleAsk(sys, testRequest("q1", agentproto.TypeAsk, map[string]any{"text": "one", "delivery": "receipt", "submission_key": "one"}))
	c.handleAsk(sys, testRequest("q2", agentproto.TypeAsk, map[string]any{"text": "two", "delivery": "receipt", "submission_key": "two"}))
	if len(sys.posts) != 1 {
		t.Fatalf("start posts=%d", len(sys.posts))
	}
	first, second := c.data.Works[c.data.Order[0]], c.data.Works[c.data.Order[1]]
	if second.Stage != "queued" || second.ExecutionState != "waiting_capacity" || second.AssignmentID != "" {
		t.Fatalf("queued second=%+v", second)
	}
	deliverTestReport(c, sys, testReport("done", "tool:loop-a:9", agentloop.ReportRequest{WorkID: first.ID, AssignmentID: first.AssignmentID, State: "completed", ConsumedThrough: 1, Result: json.RawMessage(`{"text":"done"}`), ExecutionState: "confirmed"}))
	if len(sys.posts) != 2 || second.AssignmentID == "" || second.Stage != "dispatching" {
		t.Fatalf("second was not scheduled after release: posts=%d work=%+v", len(sys.posts), second)
	}
}

func TestOneLooperDispatchesSeveralIndependentLoopsWithinItsBound(t *testing.T) {
	state := newTestState()
	sys := newTestSys(state)
	cfg := Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxAssignmentsPerLooper: 3, MaxTurns: 4}
	c := &controller{cfg: cfg, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
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

func TestWorkControlIsSharedInsideAgentInstance(t *testing.T) {
	state := newTestState()
	sys := newTestSys(state)
	cfg := Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxTurns: 4}
	c := &controller{cfg: cfg, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	c.handleAsk(sys, testRequestFrom("q", agentproto.TypeAsk, "shared", "human:alice:1", map[string]any{"text": "one", "delivery": "receipt", "submission_key": "one"}))
	w := c.data.Works[c.data.Order[0]]
	c.handleStatus(sys, testRequestFrom("other-channel", agentproto.TypeStatus, "elsewhere", "human:bob:2", map[string]any{"work_id": w.ID}))
	if sys.fails["other-channel"] != "" || sys.replies["other-channel"] == nil {
		t.Fatal("another reported caller channel could not inspect instance work")
	}
	c.handleStatus(sys, testRequestFrom("unknown-work", agentproto.TypeStatus, "shared", "human:alice:3", map[string]any{"work_id": "no-such-work"}))
	if sys.fails["unknown-work"] != "work_not_found" {
		t.Fatalf("unknown work failure=%q", sys.fails["unknown-work"])
	}
}

func TestSessionIsSharedAcrossReportedCallerChannels(t *testing.T) {
	state := newTestState()
	sys := newTestSys(state)
	cfg := Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxTurns: 4}
	c := &controller{cfg: cfg, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	c.handleAsk(sys, testRequestFrom("q", agentproto.TypeAsk, "shared", "human:alice:1", map[string]any{"text": "one", "session_id": "s-1"}))
	if c.sessions["s-1"] == nil {
		t.Fatal("ask did not open the named session")
	}
	c.handleSession(sys, testRequestFrom("get-other-channel", agentproto.TypeSessionGet, "elsewhere", "human:bob:2", map[string]any{"session_id": "s-1"}))
	if sys.fails["get-other-channel"] != "" || sys.replies["get-other-channel"] == nil {
		t.Fatalf("a caller reported from another channel could not read the session: %q", sys.fails["get-other-channel"])
	}
	c.handleAsk(sys, testRequestFrom("continue", agentproto.TypeAsk, "elsewhere", "human:bob:2", map[string]any{"text": "two", "session_id": "s-1"}))
	if sys.fails["continue"] != "" {
		t.Fatalf("continuing the named session failed: %q", sys.fails["continue"])
	}
	if len(c.sessions) != 1 {
		t.Fatalf("continuing an existing name opened a second session: %d", len(c.sessions))
	}
	c.handleSession(sys, testRequestFrom("get-missing", agentproto.TypeSessionGet, "shared", "human:alice:1", map[string]any{"session_id": "s-missing"}))
	if sys.fails["get-missing"] != "session_not_found" {
		t.Fatalf("unknown session failure=%q", sys.fails["get-missing"])
	}
}

func TestSessionContextAndExplicitIDMustAgree(t *testing.T) {
	c := &controller{data: newWorkTable(), sessions: map[string]*session{"s-1": {ID: "s-1"}, "s-2": {ID: "s-2"}}}
	c.data.Works["w-2"] = &workRecord{ID: "w-2", SessionID: "s-2"}
	msg := testRequest("context", agentproto.TypeSteer, map[string]any{"session_id": "s-1"})

	if _, err := c.selectSession(msg, "s-2", "", ""); err == nil || err.Error() != "invalid_args" {
		t.Fatalf("context/explicit session mismatch error=%v", err)
	}
	if _, err := c.selectSession(msg, "", "w-2", ""); err == nil || err.Error() != "invalid_args" {
		t.Fatalf("context/work session mismatch error=%v", err)
	}
}

func TestStatusCursorIsStableWhenNewerWorkArrives(t *testing.T) {
	sys := newTestSys(newTestState())
	c := &controller{cfg: Config{}, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
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
	c := &controller{cfg: Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxInputsPerWork: 128, MaxOperationKeys: 256, MaxTurns: 4}, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	c.handleAsk(sys, testRequest("q", agentproto.TypeAsk, map[string]any{"text": "one", "delivery": "receipt", "submission_key": "one"}))
	w := c.data.Works[c.data.Order[0]]
	c.handleStatus(sys, testRequest("assigned", agentproto.TypeStatus, map[string]any{"work_id": w.ID}))
	assigned := sys.replies["assigned"].(agentproto.StatusResponse)
	if len(assigned.Work.Inputs) != 1 || assigned.Work.Inputs[0].Disposition != "assigned" {
		t.Fatalf("assigned status=%+v", assigned.Work)
	}
	deliverTestReport(c, sys, testReport("done", "tool:loop-a:9", agentloop.ReportRequest{WorkID: w.ID, AssignmentID: w.AssignmentID, State: "completed", ConsumedThrough: 1, Result: json.RawMessage(`{"text":"done"}`), ExecutionState: "confirmed"}))
	c.handleStatus(sys, testRequest("included", agentproto.TypeStatus, map[string]any{"work_id": w.ID}))
	included := sys.replies["included"].(agentproto.StatusResponse)
	if len(included.Work.Inputs) != 1 || included.Work.Inputs[0].Disposition != "included" {
		t.Fatalf("included status=%+v", included.Work)
	}
}

func TestAskCarriesCallerOriginAndAttachmentsIntoLooperInput(t *testing.T) {
	sys := newTestSys(newTestState())
	c := &controller{cfg: Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxTurns: 4}, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	attachment := map[string]any{"address": "daemon://local-device/c0.agent/uploads/report.md", "name": "report.md", "media_type": "text/markdown"}
	c.handleAsk(sys, testRequestFrom("q", agentproto.TypeAsk, "calling", "human:alice:1", map[string]any{"text": "read", "delivery": "receipt", "submission_key": "one", "origin": map[string]string{"session": "browser", "label": "Mac"}, "attachments": []any{attachment}}))
	if len(sys.posts) != 1 {
		t.Fatalf("posts=%d", len(sys.posts))
	}
	var start agentloop.StartRequest
	if err := json.Unmarshal(sys.posts[0].Payload, &start); err != nil || len(start.Inputs) != 1 {
		t.Fatalf("start=%+v err=%v", start, err)
	}
	if start.Inputs[0].ID != "q" {
		t.Fatalf("start did not reference the ask ledger row: %+v", start.Inputs)
	}
	in := c.data.Works[c.data.Order[0]].Inputs[0].Input
	if in.CallerChannel != "calling" || in.CallerActor != "human:alice:1" || in.Origin == nil || in.Origin.Session != "browser" || len(in.Attachments) != 1 {
		t.Fatalf("accepted work lost request context: %+v", in)
	}
}

func TestDuplicateWaitersShareOneWorkAndBothReceiveResult(t *testing.T) {
	state := newTestState()
	sys := newTestSys(state)
	cfg := Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxTurns: 4}
	c := &controller{cfg: cfg, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	body := map[string]any{"text": "one", "submission_key": "same"}
	c.handleAsk(sys, testRequest("q1", agentproto.TypeAsk, body))
	c.handleAsk(sys, testRequest("q2", agentproto.TypeAsk, body))
	w := c.data.Works[c.data.Order[0]]
	deliverTestReport(c, sys, testReport("done", "tool:loop-a:9", agentloop.ReportRequest{WorkID: w.ID, AssignmentID: w.AssignmentID, State: "completed", ConsumedThrough: 1, Result: json.RawMessage(`{"text":"done"}`), ExecutionState: "confirmed"}))
	if len(c.data.Works) != 1 || sys.replies["q1"] == nil || sys.replies["q2"] == nil {
		t.Fatalf("works=%d q1=%v q2=%v", len(c.data.Works), sys.replies["q1"], sys.replies["q2"])
	}
}

func TestWaitWorkStopsOnlyAfterItsLastCallerLeaves(t *testing.T) {
	sys := newTestSys(newTestState())
	c := &controller{cfg: Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxTurns: 4}, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	body := map[string]any{"text": "one", "submission_key": "same"}
	c.handleAsk(sys, testRequest("q1", agentproto.TypeAsk, body))
	c.handleAsk(sys, testRequest("q2", agentproto.TypeAsk, body))
	w := c.data.Works[c.data.Order[0]]
	closeWait := func(requestID string) {
		c.handleWaitClosed(sys, waitClosed{WorkID: w.ID, RequestID: message.ID(requestID)})
	}
	closeWait("q1")
	if w.Stage == "stopping" || len(c.wait[w.ID]) != 1 {
		t.Fatalf("first departing waiter stopped shared work: stage=%s waiters=%d", w.Stage, len(c.wait[w.ID]))
	}
	closeWait("q2")
	if w.Stage != "stopping" || len(c.wait[w.ID]) != 0 {
		t.Fatalf("last departing waiter did not stop foreground work: stage=%s waiters=%d", w.Stage, len(c.wait[w.ID]))
	}
}

func TestMinimalTerminalReportIncludesAllAssignedInputs(t *testing.T) {
	sys, c, w := controlFixture(t)
	w.Inputs = append(w.Inputs, inputRecord{Input: agentloop.Input{ID: "late", Seq: 2, Text: "late"}, Disposition: "assigned"})
	w.AssignedThrough = 2
	count := len(sys.posts)
	deliverTestReport(c, sys, testReport("done", "tool:loop-a:9", agentloop.ReportRequest{SessionID: w.SessionID, WorkID: w.ID, AssignmentID: w.AssignmentID, State: "completed", ConsumedThrough: 1, Result: json.RawMessage(`{"text":"done"}`)}))
	if w.Outcome != agentproto.OutcomeCompleted || w.Inputs[1].Disposition != "included" || len(sys.posts) != count {
		t.Fatal("minimal boundary did not settle the assigned input prefix")
	}
}

func TestOversizedWorkInputIsRejectedBeforeDurableAcceptance(t *testing.T) {
	sys := newTestSys(newTestState())
	c := &controller{cfg: Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxInputsPerWork: 128, MaxOperationKeys: 256, MaxTurns: 4}, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	c.handleAsk(sys, testRequest("large", agentproto.TypeAsk, map[string]any{"text": strings.Repeat("x", maxWorkInputBytes)}))
	if sys.fails["large"] != "limit_exceeded" || len(c.data.Works) != 0 || len(sys.events) != 0 {
		t.Fatalf("failure=%q works=%d events=%d", sys.fails["large"], len(c.data.Works), len(sys.events))
	}
}

func testReport(id, looper string, body any) actorbase.Msg {
	raw, _ := json.Marshal(body)
	wrapped := raw
	return actorbase.NewBodyMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{ID: message.ID(id), ChannelID: "c", Sender: message.Sender{ID: actor.ActorID(looper)}, Kind: message.KindRequest, Type: agentloop.TypeReport, Payload: wrapped})
}

func TestIndependentWorksCanCloseOutOfOrder(t *testing.T) {
	state := newTestState()
	sys := newTestSys(state)
	cfg := Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxAssignmentsPerLooper: 2, MaxTurns: 4}
	c := &controller{cfg: cfg, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
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
	if starts[0].TurnID == starts[1].TurnID {
		t.Fatal("work/assignment identity reused")
	}
	second := c.data.Works[c.data.Order[1]]
	deliverTestReport(c, sys, testReport("r2", "tool:loop-a:9", agentloop.ReportRequest{SessionID: second.SessionID, TurnID: second.AssignmentID, State: "completed"}))
	if _, ok := sys.replies["q2"]; !ok {
		t.Fatal("second work did not answer while first remained open")
	}
	if _, ok := sys.replies["q1"]; ok {
		t.Fatal("first work answered before its report")
	}
	if c.data.Works[c.data.Order[0]].State != agentproto.WorkOpen {
		t.Fatal("closing w2 changed w1")
	}
}

func TestTargetedInterruptDoesNotFreezeSibling(t *testing.T) {
	state := newTestState()
	sys := newTestSys(state)
	cfg := Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxAssignmentsPerLooper: 2, MaxTurns: 4}
	c := &controller{cfg: cfg, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	c.handleAsk(sys, testRequest("q1", agentproto.TypeAsk, map[string]any{"text": "one", "delivery": "receipt", "submission_key": "one"}))
	c.handleAsk(sys, testRequest("q2", agentproto.TypeAsk, map[string]any{"text": "two", "delivery": "receipt", "submission_key": "two"}))
	first := c.data.Works[c.data.Order[0]]
	second := c.data.Works[c.data.Order[1]]
	deliverTestReport(c, sys, testReport("accepted-1", "tool:loop-a:9", agentloop.ReportRequest{WorkID: first.ID, AssignmentID: first.AssignmentID, State: "accepted", ExecutionState: "confirmed_running"}))
	deliverTestReport(c, sys, testReport("accepted-2", "tool:loop-a:9", agentloop.ReportRequest{WorkID: second.ID, AssignmentID: second.AssignmentID, State: "accepted", ExecutionState: "confirmed_running"}))
	before := len(sys.posts)
	c.handleInterrupt(sys, testRequest("stop", agentproto.TypeInterrupt, map[string]any{"work_id": first.ID}))
	if first.Stage != "stopping" || second.Stage != "dispatching" {
		t.Fatalf("first=%s second=%s", first.Stage, second.Stage)
	}
	if len(sys.posts) != before+1 || sys.posts[len(sys.posts)-1].Type != agentloop.TypeStop {
		t.Fatal("targeted loop.stop not posted")
	}
}

func TestTargetedInterruptOperationKeyDoesNotPostStopTwice(t *testing.T) {
	sys := newTestSys(newTestState())
	c := &controller{cfg: Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxTurns: 4}, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
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
	c := &controller{cfg: Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxTurns: 4}, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
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

func TestReceiptIsIdempotentWithinControllerProcess(t *testing.T) {
	state := newTestState()
	sys := newTestSys(state)
	cfg := Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxTurns: 4}
	c := &controller{cfg: cfg, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	body := map[string]any{"text": "do it", "delivery": "receipt", "submission_key": "stable"}
	c.handleAsk(sys, testRequest("q1", agentproto.TypeAsk, body))
	c.handleAsk(sys, testRequest("q2", agentproto.TypeAsk, body))
	if len(c.data.Works) != 1 {
		t.Fatalf("duplicate receipt created %d works", len(c.data.Works))
	}
}

func TestRelatedWorkContinuesExistingView(t *testing.T) {
	sys := newTestSys(newTestState())
	c := &controller{cfg: Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxInputsPerWork: 128, MaxOperationKeys: 256, MaxTurns: 4}, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	source := &workRecord{ID: "w-source", SessionID: "view:source", BoundaryID: "boundary-source", Owner: harness.Caller{Channel: "c", Actor: "human:alice:1"}, State: agentproto.WorkClosed, Outcome: agentproto.OutcomeCompleted, CreatedAt: 1, UpdatedAt: 1}
	c.sessions = map[string]*session{"view:source": {ID: "view:source"}}
	c.sessionOrder = []string{"view:source"}
	c.data.Works[string(source.ID)] = source
	c.data.Order = []string{string(source.ID)}
	branch := testRequest("branch", agentproto.TypeAsk, map[string]any{
		"text": "try another approach", "delivery": "receipt", "submission_key": "branch-1", "related_work_id": source.ID,
	})
	branch = actorbase.NewBodyMsgContext(actorbase.OriginMailbox, context.Background(), harness.Context{Session: "view:branch"}, branch.Envelope)
	c.handleAsk(sys, branch)
	if sys.fails["branch"] != "" || len(sys.posts) != 1 {
		t.Fatalf("failure=%q posts=%d", sys.fails["branch"], len(sys.posts))
	}
	var start agentloop.StartRequest
	if err := json.Unmarshal(sys.posts[0].Payload, &start); err != nil || start.Open == nil || start.Open.Base == nil || start.Open.Base.Session != source.SessionID || start.Open.Base.At != source.BoundaryID {
		t.Fatalf("start=%+v err=%v", start, err)
	}
	branched := c.requestWork("branch")
	if branched.RelatedWorkID != source.ID || branched.SessionID != "view:branch" {
		t.Fatalf("branch=%+v", branched)
	}
}

func TestRelatedWorkWithoutCheckpointIsRejectedHonestly(t *testing.T) {
	sys := newTestSys(newTestState())
	c := &controller{cfg: Config{MaxOpenWorks: 8}, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	source := &workRecord{ID: "w-source", Owner: harness.Caller{Channel: "c", Actor: "human:alice:1"}, State: agentproto.WorkOpen}
	c.data.Works[string(source.ID)] = source
	c.handleAsk(sys, testRequest("branch", agentproto.TypeAsk, map[string]any{"text": "branch", "related_work_id": source.ID}))
	if sys.fails["branch"] != "context_unavailable" {
		t.Fatalf("failure=%q", sys.fails["branch"])
	}
}

func TestAskAgainstArchivedSessionForksAndAssignsFreshSession(t *testing.T) {
	sys := newTestSys(newTestState())
	c := &controller{cfg: Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxInputsPerWork: 128, MaxTurns: 4}, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	c.sessions = map[string]*session{"s-old": {ID: "s-old", Archived: true, Opened: true, LastBoundary: "report-old", ForkPoint: "report-old", Merge: "auto"}}
	c.sessionOrder = []string{"s-old"}
	request := actorbase.NewBodyMsgContext(actorbase.OriginMailbox, context.Background(), harness.Context{Session: "s-old"}, message.Envelope{ID: "reply-old", ChannelID: "c", Sender: message.Sender{ID: "human:alice:1"}, Kind: message.KindRequest, Type: agentproto.TypeAsk, Payload: json.RawMessage(`{"text":"continue","delivery":"receipt","submission_key":"reply-old"}`)})

	c.handleAsk(sys, request)

	w := c.requestWork(string(request.ID))
	if w == nil {
		t.Fatal("request did not create work")
	}
	app := w.SourceContext
	assigned := app.Session
	if assigned == "" || assigned == "s-old" {
		t.Fatalf("assigned session=%q", assigned)
	}
	if w.SessionID != assigned {
		t.Fatalf("work=%+v assigned=%q", w, assigned)
	}
	if len(sys.posts) != 1 {
		t.Fatalf("start calls=%d", len(sys.posts))
	}
	var start agentloop.StartRequest
	if err := json.Unmarshal(sys.posts[0].Payload, &start); err != nil || start.Open == nil || start.Open.Base == nil || start.Open.Base.Session != "s-old" || start.Open.Base.At != "report-old" {
		t.Fatalf("start=%+v err=%v", start, err)
	}
}

func TestClosedWorkReleasesRequestScope(t *testing.T) {
	c := &controller{}
	w := &workRecord{State: agentproto.WorkClosed, SourceCause: message.Anchored("ask", "tree"), SourceContext: harness.Context{Session: "S"}}
	c.finishWaiters(nil, w)
	if w.SourceCause.Stated() {
		t.Fatal("completed receipt retained request scope")
	}
}

func TestControllerOwnsNewSessionAndRetryKeepsIt(t *testing.T) {
	sys := newTestSys(newTestState())
	c := &controller{cfg: Config{Loopers: []string{"loop-a"}, ContextActor: "context", LLMActor: "llm", MaxOpenWorks: 8, MaxInputsPerWork: 128, MaxTurns: 4}, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	for _, id := range []message.ID{"new-ask", "retry"} {
		request := actorbase.NewBodyMsgContext(actorbase.OriginMailbox, context.Background(), harness.Context{}, message.Envelope{ID: id, ChannelID: "c", Sender: message.Sender{ID: "human:alice:1"}, Kind: message.KindRequest, Type: agentproto.TypeAsk, Payload: json.RawMessage(`{"session":"new","text":"hello","delivery":"receipt","submission_key":"same"}`)})
		c.handleAsk(sys, request)
		if sys.fails[id] != "" {
			t.Fatalf("ask rejected: %s", sys.fails[id])
		}
	}
	if len(c.data.Order) != 1 || len(c.sessions) != 1 || len(sys.posts) != 1 {
		t.Fatalf("new session retry duplicated work: %+v", c.data.Order)
	}
	w := c.data.Works[c.data.Order[0]]
	if !strings.HasPrefix(w.SessionID, "s-") || w.SourceContext.Session != w.SessionID {
		t.Fatalf("assigned context: %+v", w)
	}
	for _, id := range []message.ID{"new-ask", "retry"} {
		reply, ok := sys.replies[id].(map[string]any)
		if !ok || reply["session_id"] != w.SessionID {
			t.Fatalf("reply=%+v", sys.replies[id])
		}
	}
}
func TestControllerRejectsConflictingSessionSelector(t *testing.T) {
	sys := newTestSys(newTestState())
	request := actorbase.NewBodyMsgContext(actorbase.OriginMailbox, context.Background(), harness.Context{Session: "existing"}, message.Envelope{ID: "conflict", Type: agentproto.TypeAsk, Payload: json.RawMessage(`{"session":"new","text":"hello"}`)})
	c := &controller{}
	c.handleAsk(sys, request)
	if sys.fails[request.ID] != "invalid_args" {
		t.Fatalf("conflict=%s", sys.fails[request.ID])
	}
	if len(c.sessions) != 0 {
		t.Fatal("created a session before validating the selector")
	}
}
