package agentmain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

var errMainRead = errors.New("injected main read failure")

type mainFailureResource struct {
	actorbase.ResourceHandle
	outcome           accessdoor.Outcome
	readErr, writeErr error
	writes            int
}

func (r *mainFailureResource) Read(resource.ResourceID) (accessdoor.Outcome, error) {
	return r.outcome, r.readErr
}
func (r *mainFailureResource) Write(_ resource.ResourceID, value []byte) (accessdoor.Outcome, error) {
	r.writes++
	if r.writeErr != nil {
		return accessdoor.Outcome{}, r.writeErr
	}
	r.outcome = accessdoor.Outcome{Found: true, Value: append([]byte(nil), value...)}
	return accessdoor.Outcome{}, nil
}

type mainFailureSys struct {
	pulseTestSys
	res      mainFailureResource
	query    func(map[string]any) (actorbase.Pending, error)
	emitted  []behavior.EventSpec
	emitErr  error
	failures []map[string]any
	replies  int
}

func (s *mainFailureSys) Resource() actorbase.ResourceHandle { return &s.res }
func (s *mainFailureSys) Call(_ message.Cause, target actor.ActorID, typ string, payload any) (actorbase.Pending, error) {
	if target != actor.SystemActorID || typ != message.TypeSystemLogQuery {
		return nil, fmt.Errorf("unexpected call: %s", typ)
	}
	s.scans++
	if s.query != nil {
		return s.query(payload.(map[string]any))
	}
	return pulsePending{}, nil
}
func (s *mainFailureSys) Emit(spec behavior.EventSpec) (message.ID, error) {
	s.emitted = append(s.emitted, spec)
	return message.ID(fmt.Sprintf("event-%d", len(s.emitted))), s.emitErr
}
func (s *mainFailureSys) Reply(_ actorbase.Msg, _ any) (message.ID, error) {
	s.replies++
	return "reply", nil
}
func (s *mainFailureSys) Fail(_ actorbase.Msg, code, detail string, fields ...map[string]any) (message.ID, error) {
	result := map[string]any{"code": code, "detail": detail}
	for _, f := range fields {
		for k, v := range f {
			result[k] = v
		}
	}
	s.failures = append(s.failures, result)
	return "failure", nil
}

type mainFailurePending struct {
	actorbase.Pending
	body json.RawMessage
	err  error
}

func (p mainFailurePending) Wait(context.Context, time.Duration) (actorbase.Msg, error) {
	return actorbase.NewBodyMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{Kind: message.KindResponse, Payload: p.body}), p.err
}
func failedMainQuery(mode string) (actorbase.Pending, error) {
	switch mode {
	case "call":
		return nil, errMainRead
	case "wait":
		return mainFailurePending{err: errMainRead}, nil
	case "response":
		return mainFailurePending{body: json.RawMessage(`{"status":"failed","error_code":"ledger_unavailable","detail":"injected"}`)}, nil
	default:
		return mainFailurePending{body: json.RawMessage(`{"status":"completed","turns":false}`)}, nil
	}
}
func mainQueryRows(items ...row) actorbase.Pending {
	var messages []map[string]any
	for _, r := range items {
		messages = append(messages, map[string]any{"seq": r.Seq, "id": r.ID, "parent_id": r.Parent, "kind": r.Kind, "message_type": r.Type, "sender": message.Sender{ID: r.Sender}, "audience": r.To, "payload_text": string(mainTestBody(map[string]any{"_context": map[string]any{}, "body": r.Body}))})
	}
	return mainFailurePending{body: mainTestBody(map[string]any{"status": "completed", "turns": []any{map[string]any{"messages": messages}}})}
}

func TestMainInitializationReadFailuresDoNotWriteOrStart(t *testing.T) {
	for _, mode := range []string{"resource", "denied", "invalid_context", "call", "wait", "response", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			s := &mainFailureSys{}
			switch mode {
			case "resource":
				s.res.readErr = errMainRead
			case "denied":
				s.res.outcome.RejectReason = access.AccessDenied
			case "invalid_context":
				s.res.outcome = accessdoor.Outcome{Found: true, Value: []byte(`{`)}
			default:
				s.query = func(map[string]any) (actorbase.Pending, error) { return failedMainQuery(mode) }
			}
			if err := proc(Config{Session: "main"})(s); err == nil || errors.Is(err, io.EOF) {
				t.Fatalf("startup error=%v", err)
			}
			if len(s.emitted) != 0 || s.res.writes != 0 || s.arms != 0 {
				t.Fatalf("startup had side effects: emits=%d writes=%d timers=%d", len(s.emitted), s.res.writes, s.arms)
			}
		})
	}
}

func TestMainInitializationMissingAndFailedWrites(t *testing.T) {
	for _, mode := range []string{"missing", "opened_failure", "context_failure"} {
		t.Run(mode, func(t *testing.T) {
			s := &mainFailureSys{}
			s.res.outcome.RejectReason = access.ResourceNotFound
			if mode == "opened_failure" {
				s.emitErr = errMainRead
			}
			if mode == "context_failure" {
				s.res.writeErr = errMainRead
			}
			err := ensureMain(s, Config{Session: "main"})
			if (err != nil) != (mode != "missing") {
				t.Fatalf("err=%v", err)
			}
			wantWrites := 1
			if mode == "opened_failure" {
				wantWrites = 0
			}
			if len(s.emitted) != 1 || s.emitted[0].Type != "session.opened" || s.res.writes != wantWrites {
				t.Fatalf("emits=%v writes=%d", s.emitted, s.res.writes)
			}
		})
	}
}

func TestMergeMainReadFailurePreventsAllWrites(t *testing.T) {
	for _, mode := range []string{"call", "wait", "response", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			s := &mainFailureSys{}
			s.query = func(req map[string]any) (actorbase.Pending, error) {
				if req["session_id"] == "branch" {
					return pulsePending{}, nil
				}
				return failedMainQuery(mode)
			}
			merged, err := mergeBranch(s, Config{Session: "main"}, "branch", "manual")
			if err == nil || merged || s.scans != 2 || len(s.emitted) != 0 || s.res.writes != 0 {
				t.Fatalf("merged=%v err=%v scans=%d writes=%d emits=%d", merged, err, s.scans, s.res.writes, len(s.emitted))
			}
		})
	}
}

func TestMergeFailureDoesNotStopActorOrTimer(t *testing.T) {
	s := &mainFailureSys{}
	s.res.outcome = accessdoor.Outcome{Found: true, Value: []byte(`{"messages":[]}`)}
	s.query = func(map[string]any) (actorbase.Pending, error) { return nil, errMainRead }
	for _, typ := range []string{"agent.main.merge_all", "agent.main.merge", "agent.main.track"} {
		s.inbox = append(s.inbox, actorbase.NewBodyMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{Kind: message.KindRequest, Type: typ, Payload: []byte(`{"from_session":"branch","merge":"manual"}`)}))
	}
	// merge accepts only from_session.
	s.inbox[1] = actorbase.NewBodyMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{Kind: message.KindRequest, Type: "agent.main.merge", Payload: []byte(`{"from_session":"branch"}`)})
	if err := proc(Config{Session: "main"})(s); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if len(s.failures) != 2 || s.replies != 1 || s.arms != 3 || len(s.emitted) != 1 || s.emitted[0].Type != "session.track" {
		t.Fatalf("failures=%v replies=%d arms=%d events=%v", s.failures, s.replies, s.arms, s.emitted)
	}
	for _, f := range s.failures {
		if f["code"] != "merge_failed" || !strings.Contains(f["detail"].(string), errMainRead.Error()) {
			t.Fatalf("failure=%v", f)
		}
	}
	if s.failures[0]["count"] != 0 {
		t.Fatalf("batch failure=%v", s.failures[0])
	}
}

func TestMergeAllReportsBranchFailure(t *testing.T) {
	s := &mainFailureSys{}
	s.query = func(req map[string]any) (actorbase.Pending, error) {
		switch req["session_id"] {
		case "main":
			return mainQueryRows(row{Seq: 1, ID: "track", Type: "session.track", Body: mainTestBody(map[string]any{"from_session": "branch", "merge": "manual"})}), nil
		case "branch":
			return nil, errMainRead
		default:
			return mainQueryRows(
				row{Seq: 2, ID: "start", Kind: message.KindRequest, Type: "loop.start", Sender: "agent:controller:1", To: message.Audience{"tool:loop:1"}, Body: mainTestBody(map[string]any{"session_id": "branch"})},
				row{Seq: 3, ID: "ack", Parent: "start", Kind: message.KindResponse, Body: mainTestBody(map[string]any{"disposition": "accepted"})},
				row{Seq: 4, ID: "stop", Kind: message.KindRequest, Type: "loop.stop", Sender: "agent:controller:1", To: message.Audience{"tool:loop:1"}, Body: mainTestBody(map[string]any{"session_id": "branch", "archive": true})},
			), nil
		}
	}
	mergeAll(s, actorbase.Msg{}, Config{Session: "main"})
	if len(s.failures) != 1 || s.failures[0]["from_session"] != "branch" || s.failures[0]["count"] != 0 || s.replies != 0 || len(s.emitted) != 0 {
		t.Fatalf("failures=%v replies=%d events=%v", s.failures, s.replies, s.emitted)
	}
}

func TestMainInitializationRebuildsExistingHistoryWithoutReopening(t *testing.T) {
	s := &mainFailureSys{}
	summary := mainTestBody(map[string]any{"role": "assistant", "content": "existing summary"})
	s.query = func(map[string]any) (actorbase.Pending, error) {
		return mainQueryRows(
			row{Seq: 1, ID: "opened", Type: "session.opened", Body: mainTestBody(map[string]any{"session_id": "main"})},
			row{Seq: 2, ID: "merged", Type: "session.merge", Body: mainTestBody(map[string]any{"decision": "merged", "summary": summary})},
		), nil
	}
	if err := ensureMain(s, Config{Session: "main"}); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Messages []json.RawMessage `json:"messages"`
		Version  string            `json:"version"`
	}
	if err := json.Unmarshal(s.res.outcome.Value, &got); err != nil {
		t.Fatal(err)
	}
	if len(s.emitted) != 0 || s.res.writes != 1 || len(got.Messages) != 1 || string(got.Messages[0]) != string(summary) || got.Version != "merged" {
		t.Fatalf("context=%s emits=%v writes=%d", s.res.outcome.Value, s.emitted, s.res.writes)
	}
}

func TestMainInitializationIncompleteHistoryDoesNotWrite(t *testing.T) {
	for _, mode := range []string{"later_page", "invalid_row"} {
		t.Run(mode, func(t *testing.T) {
			s := &mainFailureSys{}
			s.query = func(req map[string]any) (actorbase.Pending, error) {
				if mode == "invalid_row" {
					return mainFailurePending{body: json.RawMessage(`{"status":"completed","turns":[{"messages":[{"id":"bad","payload_text":"{}"}]}]}`)}, nil
				}
				if req["before_seq"] != nil {
					return nil, errMainRead
				}
				return mainFailurePending{body: json.RawMessage(`{"status":"completed","turns":[],"has_more":true,"head_seq":100,"next_before_seq":50}`)}, nil
			}
			if err := ensureMain(s, Config{Session: "main"}); err == nil {
				t.Fatal("accepted incomplete history")
			}
			if len(s.emitted) != 0 || s.res.writes != 0 {
				t.Fatalf("emits=%v writes=%d", s.emitted, s.res.writes)
			}
		})
	}
}
