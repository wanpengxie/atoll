package native

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	agentproto "github.com/wanpengxie/atoll/drivers/agents/workapi"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// Any State access on these paths would reintroduce work snapshot recovery.
type noWorkStateSys struct{ *pulseTestSys }

func (*noWorkStateSys) State() actorbase.StateHandle { panic("unexpected work State access") }

func addOldSession(sys *testSys) {
	add := func(id, session, typ, body string, sender actor.ActorID, audience message.Audience, kind message.Kind, parent message.ID) {
		sys.appendLedger(logMessage{ID: message.ID(id), Kind: kind, MessageType: typ, Sender: message.Sender{ID: sender}, Audience: audience, ParentID: parent, PayloadText: body}, session)
	}
	add("opened", "branch", agentloop.TypeSessionOpened, `{"base":{"session":"main","at":"main-boundary"}}`, "tool:loop-a:9", nil, message.KindEvent, "")
	add("old-start", "branch", agentloop.TypeStart, `{"turn_id":"old-turn","session_id":"branch"}`, sys.Self(), message.Audience{"tool:loop-a:9"}, message.KindRequest, "")
	add("old-ack", "branch", agentloop.TypeStart, `{"status":"completed","disposition":"accepted"}`, "tool:loop-a:9", nil, message.KindResponse, "old-start")
	add("track", "main", "session.track", `{"from_session":"branch","merge":"manual"}`, "tool:main:1", nil, message.KindEvent, "")
}

func TestRestartIgnoresOldWorkAndDoesNotContactLooper(t *testing.T) {
	base := newTestSys(newTestState())
	addOldSession(base)
	// A previous binary may have left either valid or corrupt State behind.
	base.state.values["native-agent.work-projection.v1"] = []byte(`{"version":1,"works":{"old-work":{"work_id":"old-work","state":"open","assignment_id":"old-turn","session_id":"branch","looper":"tool:loop-a:9"}},"order":["old-work"]}`)
	sys := &noWorkStateSys{&pulseTestSys{testSys: base}}
	sys.inbox = []actorbase.Msg{
		testRequest("status", agentproto.TypeStatus, map[string]any{"work_id": "old-work"}),
		testRequest("result", agentproto.TypeResult, map[string]any{"work_id": "old-work"}),
		testRequest("list", agentproto.TypeSessionList, map[string]any{}),
		testReport("late-report", "tool:loop-a:9", agentloop.ReportRequest{SessionID: "branch", TurnID: "old-turn", State: "completed"}),
	}
	if err := run(sys, Config{Loopers: []string{"loop-a"}, Archive: ArchiveConfig{IdleMS: 1}}); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	for _, id := range []message.ID{"status", "result"} {
		if base.fails[id] != "work_not_found" {
			t.Fatalf("%s revived old work: failures=%v replies=%v", id, base.fails, base.replies)
		}
	}
	if base.fails["late-report"] != "stale_assignment" || base.replies["list"] == nil || sys.arms != 3 {
		t.Fatalf("failures=%v replies=%v timers=%d", base.fails, base.replies, sys.arms)
	}
	if len(base.posts) != 0 || len(base.events) != 0 {
		t.Fatalf("restart sent commands or settled old work: posts=%v events=%v", base.posts, base.events)
	}
}

func TestNewSubmissionAfterRestartCreatesNewAssignmentWithoutWorkState(t *testing.T) {
	sys := newTestSys(newTestState())
	cfg := Config{Loopers: []string{"loop-a"}, LLMActor: "llm", MaxOpenWorks: 8}
	first := &controller{cfg: cfg, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	body := map[string]any{"session_id": "branch", "text": "work", "delivery": "receipt", "submission_key": "key"}
	first.handleAsk(sys, testRequest("first", agentproto.TypeAsk, body))
	old := first.requestWork("first")
	if old == nil || old.AssignmentID == "" {
		t.Fatal("first submission did not start")
	}
	restarted := &controller{cfg: cfg, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	if err := restarted.refreshSessionRelations(sys); err != nil {
		t.Fatal(err)
	}
	restarted.handleAsk(sys, testRequest("new", agentproto.TypeAsk, body))
	current := restarted.requestWork("new")
	if current == nil || current.ID == old.ID || current.AssignmentID == "" || current.AssignmentID == old.AssignmentID || len(sys.posts) != 2 {
		t.Fatalf("new submission reused old execution: old=%+v new=%+v posts=%v", old, current, sys.posts)
	}
	// A late old report cannot query history or disturb the new assignment.
	rejecting := &failedControlSys{testSys: sys, ctx: context.Background()}
	restarted.handleReport(rejecting, testReport("late", "tool:loop-a:9", agentloop.ReportRequest{SessionID: old.SessionID, TurnID: old.AssignmentID, State: "completed"}))
	if sys.fails["late"] != "stale_assignment" || len(rejecting.calls) != 0 || restarted.sessions["branch"].Execution != current.AssignmentID {
		t.Fatalf("old report affected current work: failures=%v calls=%v", sys.fails, rejecting.calls)
	}
	deliverTestReport(restarted, sys, testReport("current-report", "tool:loop-a:9", agentloop.ReportRequest{SessionID: current.SessionID, TurnID: current.AssignmentID, State: "completed", Result: mustJSON(map[string]any{"text": "done"})}))
	if current.State != agentproto.WorkClosed || current.Outcome != agentproto.OutcomeCompleted {
		t.Fatalf("new work did not complete: %+v failures=%v", current, sys.fails)
	}
	if len(sys.state.values) != 0 {
		t.Fatalf("work was persisted: %v", sys.state.values)
	}
}

func TestRelationRefreshNeverRebuildsOrOverwritesExecution(t *testing.T) {
	base := newTestSys(newTestState())
	addOldSession(base)
	c := &controller{data: newWorkTable()}
	if err := c.refreshSessionRelations(base); err != nil {
		t.Fatal(err)
	}
	s := c.sessions["branch"]
	if s == nil || s.Base == nil || s.Base.Session != "main" || s.Holder != "tool:loop-a:9" || s.Merge != "manual" || s.Execution != "" || s.Owner != "" || len(s.Buffer) != 0 || len(c.data.Works) != 0 {
		t.Fatalf("relation projection revived execution or lost relationship: %+v", s)
	}
	// Even an old accepted terminal report is history, not a current completion.
	base.appendLedger(logMessage{ID: "old-report", Kind: message.KindRequest, MessageType: agentloop.TypeReport, Sender: message.Sender{ID: "tool:loop-a:9"}, PayloadText: `{"turn_id":"old-turn","state":"completed"}`}, "branch")
	s.Execution, s.Owner = "old-turn", "local-owner"
	s.Buffer, s.Freeze, s.Rebuffer = []agentproto.WorkID{"waiting"}, "hold", true
	s.Control = &pendingControl{ID: "current-control"}
	control := s.Control
	c.handleSession(base, testRequest("list", agentproto.TypeSessionList, map[string]any{}))
	if s.Execution != "old-turn" || s.Owner != "local-owner" || !reflect.DeepEqual(s.Buffer, []agentproto.WorkID{"waiting"}) || s.Control != control || s.Freeze != "hold" || !s.Rebuffer {
		t.Fatalf("history query overwrote current state: %+v", s)
	}
	if s.LastBoundary != "old-report" || len(c.data.Works) != 0 || len(base.posts) != 0 {
		t.Fatalf("history query did not stay a relationship read: session=%+v posts=%v", s, base.posts)
	}
	// Clearing process memory does not establish that the independent Looper is idle.
	c = &controller{data: newWorkTable(), cfg: Config{Archive: ArchiveConfig{IdleMS: 1}}}
	if err := c.refreshSessionRelations(base); err != nil {
		t.Fatal(err)
	}
	s = c.sessions["branch"]
	s.LastUsed = 1
	pulseSys := &pulseTestSys{testSys: base}
	if err := c.pulse(pulseSys, pulseEvent(base.Self(), pulseType)); err != nil {
		t.Fatal(err)
	}
	if len(base.posts) != 0 || s.Archived {
		t.Fatalf("timer stopped historical holder: posts=%v session=%+v", base.posts, s)
	}
	// Normal idle archival remains available after this process handles a report.
	c.data.Works["current"] = &workRecord{ID: "current", SessionID: s.ID, State: agentproto.WorkClosed, BoundaryID: s.LastBoundary}
	if err := c.pulse(pulseSys, pulseEvent(base.Self(), pulseType)); err != nil {
		t.Fatal(err)
	}
	if len(base.posts) != 1 || base.posts[0].Type != agentloop.TypeStop || !s.Archived {
		t.Fatalf("normal idle archive stopped working: posts=%v session=%+v", base.posts, s)
	}
}

type failedControlSys struct {
	*testSys
	ctx     context.Context
	calls   []string
	pending actorbase.Pending
}

func (s *failedControlSys) Life() context.Context { return s.ctx }
func (s *failedControlSys) Call(_ message.Cause, _ actor.ActorID, word string, _ any) (actorbase.Pending, error) {
	s.calls = append(s.calls, word)
	if s.pending != nil {
		return s.pending, nil
	}
	return nil, errors.New("unavailable")
}

func TestFailedControlDoesNotInspectLooper(t *testing.T) {
	sys := &failedControlSys{testSys: newTestSys(newTestState()), ctx: context.Background()}
	deliverControl(sys, "branch", "loop-a", &pendingControl{ID: "control", Execution: "turn", Message: testRequest("steer", agentproto.TypeSteer, map[string]any{})})
	if !reflect.DeepEqual(sys.calls, []string{agentloop.TypeInput}) || len(sys.controlResults) != 1 {
		t.Fatalf("failed control triggered recovery: calls=%v completions=%d", sys.calls, len(sys.controlResults))
	}
}

func TestContextRefusalClosesCurrentWorkWithoutRetry(t *testing.T) {
	sys := newTestSys(newTestState())
	c := &controller{cfg: Config{Loopers: []string{"loop-a"}, LLMActor: "llm", MaxOpenWorks: 8}, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	c.handleAsk(sys, testRequest("ask", agentproto.TypeAsk, map[string]any{"text": "continue", "delivery": "receipt", "submission_key": "one"}))
	w := c.requestWork("ask")
	done := startDone{Session: w.SessionID, Turn: w.AssignmentID, Looper: w.Looper, Error: "session_context_unavailable", Detail: "History exceeds 4096 messages; open a new session."}
	msg := actorbase.NewBodyMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{ID: "start-refusal", Sender: message.Sender{ID: sys.Self()}, Payload: mustJSON(done)})
	c.startDone(sys, msg)
	c.scheduleQueued(sys, message.Root())
	if w.State != agentproto.WorkClosed || w.Outcome != agentproto.OutcomeFailed || len(sys.posts) != 1 || c.sessions[w.SessionID].Execution != "" {
		t.Fatalf("context refusal was retried: work=%+v posts=%v", w, sys.posts)
	}
	var fields map[string]string
	if err := json.Unmarshal(w.Result, &fields); err != nil {
		t.Fatal(err)
	}
	if fields["error_code"] != done.Error || fields["detail"] != done.Detail || !strings.Contains(fields["guidance"], "new session") {
		t.Fatalf("context failure guidance was lost: %s", w.Result)
	}
}

type cancelledLifePending struct {
	testPending
	cancelled bool
}

func (p *cancelledLifePending) Wait(ctx context.Context, _ time.Duration) (actorbase.Msg, error) {
	<-ctx.Done()
	return actorbase.Msg{}, ctx.Err()
}
func (p *cancelledLifePending) Cancel() error { p.cancelled = true; return nil }

func TestControllerExitDoesNotCancelRemoteRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, word := range []string{agentloop.TypeStart, agentloop.TypeInput} {
		t.Run(word, func(t *testing.T) {
			pending := &cancelledLifePending{}
			sys := &failedControlSys{testSys: newTestSys(newTestState()), ctx: ctx, pending: pending}
			if word == agentloop.TypeStart {
				awaitStart(sys, message.Root(), "branch", "turn", "loop-a", pending)
			} else {
				deliverControl(sys, "branch", "loop-a", &pendingControl{ID: "control", Execution: "turn", Message: testRequest("steer", agentproto.TypeSteer, map[string]any{})})
			}
			if pending.cancelled || len(sys.controlResults) != 0 || len(sys.posts) != 0 {
				t.Fatalf("Controller exit propagated to another actor: cancelled=%v posts=%v", pending.cancelled, sys.posts)
			}
		})
	}
}
