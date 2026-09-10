package native

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

type relationQuerySys struct {
	*testSys
	calls int
	query func(map[string]any) logQueryResponse
}

func (s *relationQuerySys) Call(_ message.Cause, target actor.ActorID, word string, body any) (actorbase.Pending, error) {
	if target != actor.SystemActorID || word != message.TypeSystemLogQuery {
		return nil, fmt.Errorf("unexpected call %s %s", target, word)
	}
	s.calls++
	response := s.query(body.(map[string]any))
	raw := mustJSON(response)
	// The response status is outside the projection payload fields.
	raw = append([]byte(`{"status":"completed",`), raw[1:]...)
	return testPending{msg: actorbase.NewBodyMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{Payload: raw})}, nil
}

func TestRelationHistoryHasHardMessageLimit(t *testing.T) {
	for _, total := range []int{1000, 1_000_000} {
		t.Run(fmt.Sprint(total), func(t *testing.T) {
			read := 0
			sys := &relationQuerySys{testSys: newTestSys(newTestState())}
			sys.query = func(q map[string]any) logQueryResponse {
				limit := q["limit"].(int)
				page := logQueryResponse{HeadSeq: int64(total)}
				for i := 0; i < limit && read < total; i++ {
					page.Turns = append(page.Turns, logQueryTurn{Messages: []logMessage{
						{Seq: int64(total - read), MessageType: "unrelated", PayloadText: `{}`},
						{Seq: int64(total - read - 1), MessageType: "unrelated", PayloadText: `{}`},
					}})
					read += 2
				}
				page.NextBeforeSeq, page.HasMore = int64(total-read), read < total
				return page
			}
			branch := &session{ID: "branch", Holder: "old-holder", Merge: "manual", Execution: "current-turn"}
			c := &controller{sessions: map[string]*session{"branch": branch}}
			err := c.refreshSessionRelations(sys)
			if total == 1000 {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				if !errors.Is(err, errRelationHistoryLimit) {
					t.Fatalf("million-row history was not rejected: %v", err)
				}
				if branch.Holder != "old-holder" || branch.Merge != "manual" || branch.Execution != "current-turn" {
					t.Fatalf("incomplete history replaced the existing projection: %+v", branch)
				}
			}
			if read != maxRelationHistoryMessages || sys.calls != 25 {
				t.Fatalf("unbounded read: messages=%d queries=%d", read, sys.calls)
			}
		})
	}
}

func TestRelationHistoryBudgetsIncludeEmptyPagesAndFullReads(t *testing.T) {
	for _, mode := range []string{"empty-pages", "full-read", "bytes", "irrelevant-body"} {
		t.Run(mode, func(t *testing.T) {
			sys := &relationQuerySys{testSys: newTestSys(newTestState())}
			sys.query = func(q map[string]any) logQueryResponse {
				switch mode {
				case "empty-pages":
					return logQueryResponse{HeadSeq: 1_000_000, HasMore: true, NextBeforeSeq: int64(1_000_000 - sys.calls)}
				case "bytes":
					return logQueryResponse{Turns: []logQueryTurn{{Messages: []logMessage{{PayloadText: strings.Repeat("x", maxRelationHistoryBytes+1)}}}}}
				default:
					if sys.calls == 1 {
						word := agentloop.TypeStart
						if mode == "irrelevant-body" {
							word = "llm.generate"
						}
						return logQueryResponse{HeadSeq: 1, Turns: []logQueryTurn{{Messages: []logMessage{{Seq: 1, MessageType: word, Truncated: true, PayloadText: "{"}}}}}
					}
					offset := q["offset"].(int) + 1
					return logQueryResponse{Message: &logMessage{PayloadText: "x", NextOffset: &offset}}
				}
			}
			_, _, err := readRelationHistory(sys)
			if mode == "irrelevant-body" {
				if err != nil || sys.calls != 1 {
					t.Fatalf("expanded an unrelated body: calls=%d err=%v", sys.calls, err)
				}
			} else if !errors.Is(err, errRelationHistoryLimit) || sys.calls > maxRelationHistoryQueries {
				t.Fatalf("budget bypassed: calls=%d err=%v", sys.calls, err)
			}
		})
	}
}

func TestRelationQueryDeadlineStopsBeforeAnotherCall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sys := &relationQuerySys{testSys: newTestSys(newTestState())}
	_, err := callSystemQueryContext(sys, ctx, map[string]any{})
	if !errors.Is(err, context.Canceled) || sys.calls != 0 {
		t.Fatalf("expired budget still queried history: calls=%d err=%v", sys.calls, err)
	}
}
