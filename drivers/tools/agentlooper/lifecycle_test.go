package agentlooper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	agentbase "github.com/wanpengxie/atoll/drivers/agents/base"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	llmproto "github.com/wanpengxie/atoll/drivers/tools/pillm/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/registry"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/harness"
)

type historyTestSys struct {
	looperTestBase
	rows         []ledgerRow
	queries      int
	posts        int
	inbox        []actorbase.Msg
	code, detail string
	replies      int
	readErr      error
	resourceOnce sync.Once
	resource     actorbase.ResourceHandle
}

func (s *historyTestSys) Resource() actorbase.ResourceHandle {
	s.resourceOnce.Do(func() {
		if s.resource == nil {
			s.resource = &looperTestResource{values: map[resource.ResourceID][]byte{}}
		}
	})
	return s.resource
}
func (*historyTestSys) Self() actor.ActorID { return "tool:loop:1" }
func (s *historyTestSys) Recv() (actorbase.Msg, error) {
	if len(s.inbox) == 0 {
		return actorbase.Msg{}, io.EOF
	}
	m := s.inbox[0]
	s.inbox = s.inbox[1:]
	return m, nil
}
func (s *historyTestSys) Reply(actorbase.Msg, any) (message.ID, error) {
	s.replies++
	return "reply", nil
}
func (s *historyTestSys) Fail(_ actorbase.Msg, code, detail string, _ ...map[string]any) (message.ID, error) {
	s.code, s.detail = code, detail
	return "failure", nil
}
func (s *historyTestSys) Post(behavior.RequestSpec) (message.ID, error) {
	s.posts++
	return "post", nil
}
func (s *historyTestSys) Call(_ message.Cause, app harness.Context, target actor.ActorID, word string, payload any) (actorbase.Pending, error) {
	return nil, fmt.Errorf("unexpected call %s %s", target, word)
}

type looperTestView struct {
	actorcaps.LedgerView
	read  func(context.Context, actorcaps.LedgerRead) (actorcaps.LedgerSnapshot, error)
	build func(context.Context, actorcaps.LedgerSnapshot, string, message.ID) ([]actorcaps.LedgerRow, error)
}

// Tests compose the real platform projection with a controlled ledger reader.
// The Looper receives only the LedgerView interface, just as in production.
func (v looperTestView) BuildSession(ctx context.Context, snapshot actorcaps.LedgerSnapshot, session string, upto message.ID) ([]actorcaps.LedgerRow, error) {
	if v.build != nil {
		return v.build(ctx, snapshot, session, upto)
	}
	return (home.View{}).BuildSession(ctx, snapshot, session, upto)
}
func (v looperTestView) Read(ctx context.Context, q actorcaps.LedgerRead) (actorcaps.LedgerSnapshot, error) {
	return v.read(ctx, q)
}
func (s *historyTestSys) View() actorcaps.LedgerView {
	return looperTestView{read: func(ctx context.Context, q actorcaps.LedgerRead) (actorcaps.LedgerSnapshot, error) {
		s.queries++
		if s.readErr != nil {
			return actorcaps.LedgerSnapshot{}, s.readErr
		}
		if err := ctx.Err(); err != nil {
			return actorcaps.LedgerSnapshot{}, err
		}
		out := actorcaps.LedgerSnapshot{HeadSeq: 100}
		for _, r := range s.rows {
			raw, _ := harness.WrapPayload(harness.Context{Session: r.Session}, r.Body)
			out.Rows = append(out.Rows, actorcaps.LedgerRow{Seq: r.Seq, IsTerminal: r.Terminal, Envelope: message.Envelope{ID: r.ID, Sender: message.Sender{ID: r.Sender}, Audience: r.Audience, Kind: r.Kind, Type: r.Type, ParentID: r.Parent, Payload: raw}})
		}
		return out, nil
	}}
}

func closedHistory() []ledgerRow {
	return []ledgerRow{
		{Seq: 1, ID: "opened", Session: "s", Type: agentloop.TypeSessionOpened, Kind: message.KindEvent, Body: mustJSON(map[string]any{})},
		{Seq: 2, ID: "input", Session: "s", Type: "agent.ask", Kind: message.KindRequest, Body: mustJSON(map[string]any{"text": "old question"})},
		{Seq: 3, ID: "start", Session: "s", Type: agentloop.TypeStart, Kind: message.KindRequest, Sender: "agent:controller:1", Audience: message.Audience{"tool:loop:1"}, Body: mustJSON(agentloop.StartRequest{SessionID: "s", TurnID: "old", Inputs: []agentloop.Input{{ID: "input", Seq: 1}}})},
		{Seq: 4, ID: "ack", Session: "s", Type: agentloop.TypeStart, Kind: message.KindResponse, Parent: "start", Body: mustJSON(map[string]any{"status": "completed", "disposition": "accepted"})},
		{Seq: 5, ID: "generate", Session: "s", Type: llmproto.TypeGenerate, Kind: message.KindRequest, Parent: "start", Body: mustJSON(map[string]any{})},
		{Seq: 6, ID: "answer", Session: "s", Type: llmproto.TypeGenerate, Kind: message.KindResponse, Parent: "generate", Body: mustJSON(llmproto.GenerateResponse{Message: mustJSON(map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "text", "text": "old answer"}}})})},
		{Seq: 7, ID: "report", Session: "s", Type: agentloop.TypeReport, Kind: message.KindRequest, Sender: "tool:loop:1", Body: mustJSON(agentloop.ReportRequest{TurnID: "old", State: "completed"})},
	}
}

func TestLooperStartupAndStaleCommandsNeverAttach(t *testing.T) {
	for _, word := range []string{"", agentloop.TypeInput, agentloop.TypeStop, agentloop.TypeInspect} {
		t.Run(word, func(t *testing.T) {
			sys := &historyTestSys{rows: closedHistory()[:6]}
			if word != "" {
				sys.inbox = []actorbase.Msg{internalRequest("command", word, map[string]any{"session": "s", "assignment_id": "old"})}
			}
			if err := proc(Config{ControllerActor: "controller", LLMActor: "llm"}, registry.Deps{})(sys); !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
			if sys.queries != 0 || sys.posts != 0 {
				t.Fatalf("startup/command recovered old work: queries=%d posts=%d", sys.queries, sys.posts)
			}
		})
	}
}

func TestUnclosedSessionStartIsRejectedWithNewSessionAdvice(t *testing.T) {
	sys := &historyTestSys{rows: closedHistory()[:6]}
	l := &looper{cfg: Config{ControllerActor: "controller", MaxAssignments: 2}, active: map[string]*assignment{}}
	l.start(sys, internalRequest("new-start", agentloop.TypeStart, agentloop.StartRequest{SessionID: "s", TurnID: "new", Inputs: []agentloop.Input{{ID: "new-input", Text: "continue"}}}))
	if sys.code != sessionContextUnavailable || !strings.Contains(sys.detail, "open a new session") || len(l.active) != 0 || sys.posts != 0 || sys.replies != 0 {
		t.Fatalf("unclosed session was accepted or repaired: code=%s detail=%s active=%d posts=%d", sys.code, sys.detail, len(l.active), sys.posts)
	}
}

func TestHistoricalTurnIDCannotStartAnotherExecution(t *testing.T) {
	sys := &historyTestSys{rows: closedHistory()}
	l := &looper{cfg: Config{ControllerActor: "controller", MaxAssignments: 2}, active: map[string]*assignment{}}
	l.start(sys, internalRequest("retry-old-turn", agentloop.TypeStart, agentloop.StartRequest{SessionID: "s", TurnID: "old", Inputs: []agentloop.Input{{ID: "new-input", Text: "again"}}}))
	if sys.code != sessionContextUnavailable || !strings.Contains(sys.detail, "turn_id") || len(l.active) != 0 || sys.posts != 0 {
		t.Fatalf("historical turn ID was reused: code=%s detail=%s", sys.code, sys.detail)
	}
}

func TestClosedHistoryIsReadOnceAndMissingInputsAreRejected(t *testing.T) {
	for _, missing := range []bool{false, true} {
		sys := &historyTestSys{rows: closedHistory()}
		if missing {
			sys.rows = append(sys.rows[:1], sys.rows[2:]...)
		}
		object, opened, err := sessionContext(context.Background(), sys, message.Root(), "s")
		if missing {
			if err == nil {
				t.Fatal("missing input was silently dropped")
			}
		} else if err != nil || !opened || len(object.Messages) != 2 || !strings.Contains(string(object.Messages[0]), "old question") || !strings.Contains(string(object.Messages[1]), "old answer") {
			t.Fatalf("closed history mismatch: object=%+v opened=%v err=%v", object, opened, err)
		}
		if sys.queries != 1 || sys.posts != 0 {
			t.Fatalf("history was rescanned or settled: queries=%d posts=%d", sys.queries, sys.posts)
		}
	}
}

func TestSessionContextUsesOnlyMatchingValidRuntimeCache(t *testing.T) {
	sys := &historyTestSys{rows: closedHistory()}
	ledger, opened, err := sessionContext(t.Context(), sys, message.Root(), "s")
	if err != nil || !opened || len(ledger.Messages) != 2 || ledger.Version == "" {
		t.Fatalf("ledger context=%+v opened=%v err=%v", ledger, opened, err)
	}
	runtimeOnly := mustJSON(map[string]any{"role": "user", "content": environmentHintPrefix + "cwd=/runtime"})
	cached := cloneContextObject(ledger)
	cached.Messages = append(cached.Messages, runtimeOnly)
	if _, err := agentbase.WriteContext(sys, "s", cached); err != nil {
		t.Fatal(err)
	}
	got, _, err := sessionContext(t.Context(), sys, message.Root(), "s")
	if err != nil || len(got.Messages) != 3 || string(got.Messages[2]) != string(runtimeOnly) {
		t.Fatalf("matching runtime context was not restored: context=%+v err=%v", got, err)
	}

	stale := cloneContextObject(cached)
	stale.Version = "stale"
	if _, err := agentbase.WriteContext(sys, "s", stale); err != nil {
		t.Fatal(err)
	}
	got, _, err = sessionContext(t.Context(), sys, message.Root(), "s")
	if err != nil || len(got.Messages) != len(ledger.Messages) || got.Version != ledger.Version {
		t.Fatalf("stale runtime context replaced ledger: context=%+v err=%v", got, err)
	}

	id, _ := agentbase.ContextResource("s")
	if _, err := sys.Resource().Write(id, []byte(`{"messages":`)); err != nil {
		t.Fatal(err)
	}
	got, _, err = sessionContext(t.Context(), sys, message.Root(), "s")
	if err != nil || len(got.Messages) != len(ledger.Messages) || got.Version != ledger.Version {
		t.Fatalf("malformed runtime context blocked ledger recovery: context=%+v err=%v", got, err)
	}
}

func TestHistoryViewFailureRejectsStart(t *testing.T) {
	for _, err := range []error{actorcaps.ErrLedgerLimit, context.DeadlineExceeded, errors.New("storage unavailable")} {
		sys := &historyTestSys{readErr: err}
		l := &looper{cfg: Config{ControllerActor: "controller", MaxAssignments: 2}, active: map[string]*assignment{}}
		l.start(sys, internalRequest("start", agentloop.TypeStart, agentloop.StartRequest{SessionID: "s", TurnID: "new", Inputs: []agentloop.Input{{ID: "in", Text: "work"}}}))
		if sys.code != sessionContextUnavailable || sys.replies != 0 || sys.posts != 0 || len(l.active) != 0 {
			t.Fatalf("read failure accepted: %+v", sys)
		}
	}
}

func TestAncestorContextUsesOneViewSnapshot(t *testing.T) {
	sys := &historyTestSys{rows: append(closedHistory(), ledgerRow{Seq: 8, ID: "fork", Session: "branch", Type: agentloop.TypeSessionOpened, Body: mustJSON(agentloop.Opened{Base: &agentloop.BoundaryRef{Session: "s", At: "report"}})})}
	object, err := materializeSession(context.Background(), sys, message.Root(), "branch", "")
	if err != nil || len(object.Messages) != 2 || sys.queries != 1 {
		t.Fatalf("object=%+v err=%v reads=%d", object, err, sys.queries)
	}
}

func TestLocalSessionExclusionDoesNotReadHistory(t *testing.T) {
	sys := &historyTestSys{}
	l := &looper{cfg: Config{ControllerActor: "controller", MaxAssignments: 2}, active: map[string]*assignment{"old": {start: agentloop.StartRequest{SessionID: "s"}}}}
	l.start(sys, internalRequest("new-start", agentloop.TypeStart, agentloop.StartRequest{SessionID: "s", TurnID: "new", Inputs: []agentloop.Input{{ID: "input", Text: "work"}}}))
	if sys.code != "busy" || sys.queries != 0 || len(l.active) != 1 {
		t.Fatalf("session exclusion failed: code=%s queries=%d active=%d", sys.code, sys.queries, len(l.active))
	}
}

func TestContextSnapshotLimitAndMainHistory(t *testing.T) {
	tooMany := agentbase.ContextObject{Messages: make([]json.RawMessage, maxSessionHistoryMessages+1)}
	if err := validateSessionContext(tooMany); err == nil {
		t.Fatal("oversized snapshot accepted")
	}
	sys := &historyTestSys{rows: []ledgerRow{
		{Seq: 1, ID: "main-open", Session: "main", Kind: message.KindEvent, Type: agentloop.TypeSessionOpened, Body: mustJSON(map[string]any{})},
		{Seq: 2, ID: "merge", Session: "main", Kind: message.KindEvent, Type: "session.merge", Body: mustJSON(map[string]any{"decision": "merged", "summary": map[string]any{"role": "user", "content": "main summary"}})},
	}}
	object, err := materializeSession(context.Background(), sys, message.Root(), "main", "merge")
	if err != nil || len(object.Messages) != 1 || !strings.Contains(string(object.Messages[0]), "main summary") {
		t.Fatalf("main base lost its merged history: object=%+v err=%v", object, err)
	}
}

type exitPending struct {
	cancelledPending
	cancelLife context.CancelFunc
	cancelled  bool
}

func (p *exitPending) Wait(context.Context, time.Duration) (actorbase.Msg, error) {
	p.cancelLife()
	return actorbase.Msg{}, context.Canceled
}
func (p *exitPending) Cancel() error { p.cancelled = true; return nil }

type exitCallSys struct {
	*historyTestSys
	life context.Context
	p    actorbase.Pending
}

func (s *exitCallSys) Life() context.Context { return s.life }
func (s *exitCallSys) Call(message.Cause, harness.Context, actor.ActorID, string, any) (actorbase.Pending, error) {
	return s.p, nil
}

func TestLooperExitDoesNotCancelRemoteCallsOrSendReport(t *testing.T) {
	life, stop := context.WithCancel(context.Background())
	defer stop()
	p := &exitPending{cancelLife: stop}
	sys := &exitCallSys{historyTestSys: &historyTestSys{}, life: life, p: p}
	_, _ = call(life, sys, message.Root(), harness.Context{}, "tool:remote:1", "execute", map[string]any{})
	(&looper{}).report(sys, &assignment{}, "cancelled", 0, nil, "", "", "")
	if p.cancelled || sys.posts != 0 || sys.replies != 0 {
		t.Fatalf("exit affected remote work: cancelled=%v posts=%d replies=%d", p.cancelled, sys.posts, sys.replies)
	}
}

func TestInputBatchSharesSnapshotButNextBatchSeesNewRows(t *testing.T) {
	sys := &historyTestSys{rows: []ledgerRow{
		{Seq: 1, ID: "one", Session: "s", Body: mustJSON(map[string]any{"text": "first"})},
		{Seq: 2, ID: "two", Session: "s", Body: mustJSON(map[string]any{"text": "second"})},
	}}
	inputs, err := hydrateInputs(context.Background(), sys, message.Root(), []agentloop.Input{{ID: "one"}, {ID: "two"}})
	if err != nil || len(inputs) != 2 || inputs[0].Text != "first" || inputs[1].Text != "second" || sys.queries != 1 {
		t.Fatalf("inputs=%v reads=%d err=%v", inputs, sys.queries, err)
	}
	sys.rows = append(sys.rows, ledgerRow{Seq: 3, ID: "three", Session: "s", Body: mustJSON(map[string]any{"text": "third"})})
	inputs, err = hydrateInputs(context.Background(), sys, message.Root(), []agentloop.Input{{ID: "three"}})
	if err != nil || inputs[0].Text != "third" || sys.queries != 2 {
		t.Fatalf("stale insertion snapshot: inputs=%v reads=%d err=%v", inputs, sys.queries, err)
	}
}

type projectionTestSys struct {
	*historyTestSys
	view actorcaps.LedgerView
}

func (s projectionTestSys) View() actorcaps.LedgerView { return s.view }

func TestMaterializeDelegatesSessionExpansionToView(t *testing.T) {
	sys := &historyTestSys{rows: closedHistory()}
	denied := errors.New("platform session projection rejected")
	calls := 0
	view := looperTestView{
		read: sys.View().Read,
		build: func(ctx context.Context, snapshot actorcaps.LedgerSnapshot, session string, upto message.ID) ([]actorcaps.LedgerRow, error) {
			calls++
			if len(snapshot.Rows) != len(sys.rows) || session != "s" || upto != "report" {
				t.Fatalf("wrong projection input: rows=%d session=%s upto=%s", len(snapshot.Rows), session, upto)
			}
			return nil, denied
		},
	}
	_, err := materializeSession(t.Context(), projectionTestSys{sys, view}, message.Root(), "s", "report")
	if !errors.Is(err, denied) || calls != 1 || sys.queries != 1 {
		t.Fatalf("View bypassed or re-read: err=%v build=%d reads=%d", err, calls, sys.queries)
	}
}
