package agentlooper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	agentbase "github.com/wanpengxie/atoll/drivers/agents/base"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	llmproto "github.com/wanpengxie/atoll/drivers/tools/pillm/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
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
	query        func(map[string]any) map[string]any
}

func (*historyTestSys) Resource() actorbase.ResourceHandle { panic("must not trust context KV") }
func (*historyTestSys) Self() actor.ActorID                { return "tool:loop:1" }
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
func (s *historyTestSys) Call(_ message.Cause, target actor.ActorID, word string, payload any) (actorbase.Pending, error) {
	if target != actor.SystemActorID || word != message.TypeSystemLogQuery {
		return nil, fmt.Errorf("unexpected call %s %s", target, word)
	}
	s.queries++
	req := payload.(map[string]any)
	var result map[string]any
	if s.query != nil {
		result = s.query(req)
	} else {
		var turns []any
		for _, row := range s.rows {
			if session, _ := req["session_id"].(string); session != row.Session {
				continue
			}
			raw, _ := harness.WrapPayload(harness.Context{Session: row.Session}, row.Body)
			turns = append(turns, map[string]any{"messages": []any{map[string]any{
				"seq": row.Seq, "id": row.ID, "sender": message.Sender{ID: row.Sender}, "audience": row.Audience,
				"kind": row.Kind, "message_type": row.Type, "parent_id": row.Parent, "payload_text": string(raw),
			}}})
		}
		result = map[string]any{"head_seq": 100, "turns": turns}
	}
	result["status"] = "completed"
	return immediatePending{msg: actorbase.NewBodyMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{Payload: mustJSON(result)})}, nil
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
			if err := proc(Config{ControllerActor: "controller"})(sys); !errors.Is(err, io.EOF) {
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
	l := &looper{cfg: Config{ControllerActor: "controller", LLMActor: "llm", MaxAssignments: 2}, active: map[string]*assignment{}}
	l.start(sys, internalRequest("new-start", agentloop.TypeStart, agentloop.StartRequest{SessionID: "s", TurnID: "new", Inputs: []agentloop.Input{{ID: "new-input", Text: "continue"}}}))
	if sys.code != sessionContextUnavailable || !strings.Contains(sys.detail, "open a new session") || len(l.active) != 0 || sys.posts != 0 || sys.replies != 0 {
		t.Fatalf("unclosed session was accepted or repaired: code=%s detail=%s active=%d posts=%d", sys.code, sys.detail, len(l.active), sys.posts)
	}
}

func TestHistoricalTurnIDCannotStartAnotherExecution(t *testing.T) {
	sys := &historyTestSys{rows: closedHistory()}
	l := &looper{cfg: Config{ControllerActor: "controller", LLMActor: "llm", MaxAssignments: 2}, active: map[string]*assignment{}}
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

func TestHistoryBudgetIncludesSparseScansAndRecursiveBases(t *testing.T) {
	sys := &historyTestSys{}
	sys.query = func(map[string]any) map[string]any {
		return map[string]any{"head_seq": 1_000_000, "has_more": true, "next_before_seq": 1_000_000 - sys.queries*512, "scanned": 512}
	}
	if _, err := readSessionRows(context.Background(), sys, message.Root(), "sparse"); err == nil || sys.queries != 8 {
		t.Fatalf("sparse history scan was unbounded: calls=%d err=%v", sys.queries, err)
	}
	ctx, cancel := newHistoryContext(context.Background())
	defer cancel()
	budget := ctx.Value(historyBudgetKey{}).(*historyBudget)
	budget.rows = maxSessionHistoryMessages
	budget.cache["branch"] = []ledgerRow{{ID: "fork", Session: "branch", Type: agentloop.TypeSessionOpened, Body: mustJSON(agentloop.Opened{Base: &agentloop.BoundaryRef{Session: "base"}})}}
	sys = &historyTestSys{}
	if _, err := materializeSession(ctx, sys, message.Root(), "branch", "", nil); err == nil || sys.queries != 0 {
		t.Fatalf("recursive base escaped the shared message limit: queries=%d err=%v", sys.queries, err)
	}
}

func TestHistoryMessageLimit4096(t *testing.T) {
	for _, total := range []int{4096, 1_000_000} {
		t.Run(fmt.Sprint(total), func(t *testing.T) {
			read := 0
			sys := &historyTestSys{}
			payload, _ := harness.WrapPayload(harness.Context{Session: "s"}, json.RawMessage(`{}`))
			sys.query = func(req map[string]any) map[string]any {
				var turns []any
				for i := 0; i < req["limit"].(int) && read < total; i++ {
					var rows []any
					for j := 0; j < 2; j++ {
						read++
						rows = append(rows, map[string]any{"seq": total - read + 1, "id": fmt.Sprint(read), "payload_text": string(payload)})
					}
					turns = append(turns, map[string]any{"messages": rows})
				}
				return map[string]any{"head_seq": total, "turns": turns, "scanned": len(turns), "has_more": read < total, "next_before_seq": total - read + 1}
			}
			rows, err := readSessionRows(context.Background(), sys, message.Root(), "s")
			if read != maxSessionHistoryMessages {
				t.Fatalf("history limit bypassed: read=%d err=%v", read, err)
			}
			if total == maxSessionHistoryMessages {
				if err != nil || len(rows) != total {
					t.Fatalf("complete bounded history rejected: rows=%d err=%v", len(rows), err)
				}
			} else if err == nil || rows != nil {
				t.Fatal("partial history was returned as complete")
			}
		})
	}
}

func TestLocalSessionExclusionDoesNotReadHistory(t *testing.T) {
	sys := &historyTestSys{}
	l := &looper{cfg: Config{ControllerActor: "controller", LLMActor: "llm", MaxAssignments: 2}, active: map[string]*assignment{"old": {start: agentloop.StartRequest{SessionID: "s"}}}}
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
	object, err := materializeSession(context.Background(), sys, message.Root(), "main", "merge", nil)
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
func (s *exitCallSys) Call(message.Cause, actor.ActorID, string, any) (actorbase.Pending, error) {
	return s.p, nil
}

func TestLooperExitDoesNotCancelRemoteCallsOrSendReport(t *testing.T) {
	life, stop := context.WithCancel(context.Background())
	defer stop()
	p := &exitPending{cancelLife: stop}
	sys := &exitCallSys{historyTestSys: &historyTestSys{}, life: life, p: p}
	_, _ = call(life, sys, message.Root(), "tool:remote:1", "execute", map[string]any{})
	(&looper{}).report(sys, &assignment{}, "cancelled", 0, nil, "", "", "")
	if p.cancelled || sys.posts != 0 || sys.replies != 0 {
		t.Fatalf("exit affected remote work: cancelled=%v posts=%d replies=%d", p.cancelled, sys.posts, sys.replies)
	}
}
