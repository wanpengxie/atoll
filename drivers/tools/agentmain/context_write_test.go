package agentmain

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	agentbase "github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
)

func TestMainContextWritesRetryThenDelete(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		failWrites              int
		deleteErr               error
		wantWrites, wantDeletes int
		wantErr                 bool
	}{
		{"first", -1, nil, 1, 0, false},
		{"third", 2, nil, 3, 0, false},
		{"deleted", 0, nil, 3, 1, true},
		{"delete-failed", 0, errMainRead, 3, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &mainFailureSys{}
			s.res.writeErr, s.res.failWrites, s.res.deleteErr = errMainRead, tc.failWrites, tc.deleteErr
			object := agentbase.ContextObject{Messages: []json.RawMessage{json.RawMessage(`{"role":"assistant","content":"B"}`)}, Version: "merge-B"}
			err := writeMainContext(s, Config{Session: "main"}, object)
			if errors.Is(err, errMainContextWrite) != tc.wantErr || s.res.writes != tc.wantWrites || s.res.deletes != tc.wantDeletes {
				t.Fatalf("err=%v writes=%d deletes=%d", err, s.res.writes, s.res.deletes)
			}
			if !tc.wantErr {
				got, found, err := agentbase.LoadContext(s, "main")
				if err != nil || !found || got.Version != object.Version || len(got.Messages) != 1 {
					t.Fatalf("context=%+v found=%v err=%v", got, found, err)
				}
			}
		})
	}
}

// No LLM Call implementation: retrying a committed merge must not summarize again.
func committedMainSys() *mainFailureSys {
	s := &mainFailureSys{}
	s.res.outcome = accessdoor.Outcome{Found: true, Value: []byte(`{"messages":[{"role":"assistant","content":"A"}],"version":"old"}`)}
	s.query = func(q actorcaps.LedgerRead) (actorcaps.LedgerSnapshot, error) {
		if q.Session == "main" {
			return mainQueryRows(
				row{Seq: 1, ID: "opened", Type: "session.opened", Body: mainTestBody(map[string]any{})},
				row{Seq: 2, ID: "merge-A", Type: "session.merge", Body: mainTestBody(map[string]any{"decision": "merged", "summary": map[string]any{"role": "assistant", "content": "A"}})},
				row{Seq: 6, ID: "merge-B", Type: "session.merge", Body: mainTestBody(map[string]any{"decision": "merged", "refs": []string{"report"}, "summary": map[string]any{"role": "assistant", "content": "B"}})},
			), nil
		}
		if q.Session == "branch" {
			return mainQueryRows(
				row{Seq: 3, ID: "start", Kind: message.KindRequest, Type: "loop.start", Sender: "agent:controller:1", To: message.Audience{"tool:loop:1"}, Body: mainTestBody(map[string]any{"session_id": "branch", "turn_id": "turn"})},
				row{Seq: 4, ID: "ack", Parent: "start", Kind: message.KindResponse, Type: "loop.start", Body: mainTestBody(map[string]any{"disposition": "accepted"})},
				row{Seq: 5, ID: "report", Kind: message.KindRequest, Type: "loop.report", Sender: "tool:loop:1", Body: mainTestBody(map[string]any{"turn_id": "turn", "state": "completed"})},
			), nil
		}
		return actorcaps.LedgerSnapshot{}, nil
	}
	return s
}

func TestCommittedMergeRebuildsAfterFailedWriteAndDelete(t *testing.T) {
	for _, deleteFails := range []bool{false, true} {
		s := committedMainSys()
		s.res.writeErr = errMainRead
		if deleteFails {
			s.res.deleteErr = errMainRead
		}
		merged, err := mergeBranch(s, Config{Session: "main"}, "branch", "manual")
		if merged || !errors.Is(err, errMainContextWrite) || s.res.writes != 3 || s.res.deletes != 1 || len(s.emitted) != 0 {
			t.Fatalf("merged=%v err=%v writes=%d deletes=%d events=%v", merged, err, s.res.writes, s.res.deletes, s.emitted)
		}
		s.res.writeErr, s.res.deleteErr = nil, nil
		merged, err = mergeBranch(s, Config{Session: "main"}, "branch", "manual")
		object, found, readErr := agentbase.LoadContext(s, "main")
		if merged || err != nil || readErr != nil || !found || len(object.Messages) != 2 || object.Version != "merge-B" || !strings.Contains(string(object.Messages[1]), `"B"`) || len(s.emitted) != 0 {
			t.Fatalf("lost committed summary: object=%+v found=%v err=%v readErr=%v", object, found, err, readErr)
		}
	}
}

func TestFailedWriteAndDeleteDoNotStopMain(t *testing.T) {
	s := committedMainSys()
	s.res.writeErr, s.res.deleteErr = errMainRead, errMainRead
	for _, typ := range []string{"agent.main.merge", "agent.main.track"} {
		body := []byte(`{"from_session":"branch"}`)
		if typ == "agent.main.track" {
			body = []byte(`{"from_session":"branch","merge":"manual"}`)
		}
		s.inbox = append(s.inbox, actorbase.NewBodyMsg(actorbase.OriginMailbox, t.Context(), message.Envelope{Kind: message.KindRequest, Type: typ, Payload: body}))
	}
	if err := proc(Config{Session: "main"})(s); !errors.Is(err, io.EOF) {
		t.Fatalf("main exited on storage failure: %v", err)
	}
	if len(s.failures) != 1 || s.replies != 1 || s.arms != 3 || s.res.deletes != 1 {
		t.Fatalf("main stopped serving: failures=%v replies=%d timers=%d deletes=%d", s.failures, s.replies, s.arms, s.res.deletes)
	}
}

func TestStartupContextWriteFailureKeepsMainReceiving(t *testing.T) {
	s := &mainFailureSys{}
	s.res.writeErr, s.res.deleteErr = errMainRead, errMainRead
	s.inbox = []actorbase.Msg{actorbase.NewBodyMsg(actorbase.OriginMailbox, t.Context(), message.Envelope{Kind: message.KindRequest, Type: "agent.main.track", Payload: []byte(`{"from_session":"branch","merge":"manual"}`)})}
	if err := proc(Config{Session: "main"})(s); !errors.Is(err, io.EOF) {
		t.Fatalf("startup KV failure stopped main: %v", err)
	}
	if s.res.writes != 3 || s.res.deletes != 1 || s.replies != 1 || s.arms != 3 {
		t.Fatalf("writes=%d deletes=%d replies=%d timers=%d", s.res.writes, s.res.deletes, s.replies, s.arms)
	}
}
