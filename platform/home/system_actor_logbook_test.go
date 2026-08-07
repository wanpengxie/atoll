package home

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/internal/sysactor"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestSystemActorLogbookRecentThroughRealHome(t *testing.T) {
	h := openClosureHome(t, "system-actor-logbook")
	writer := routingAgent(t, h, closureCallerDecl)
	reader := routingAgent(t, h, closureReceiverDecl)
	contextRequest := writeHomeRequest(t, h, writer, reader, "user.context", json.RawMessage(`{"text":"remember me"}`))

	first := callLogbookRecent(t, h, reader)
	assertLogbookContainsOnlyContext(t, first, contextRequest.ID)
	second := callLogbookRecent(t, h, reader)
	assertLogbookContainsOnlyContext(t, second, contextRequest.ID)
}

func writeHomeRequest(t *testing.T, h *Home, sender, receiver actor.ActorID, typ string, payload json.RawMessage) message.Envelope {
	t.Helper()
	term, _ := serverTerm(t, h, sender)
	basis, err := h.controller.PenBasis(sender, term)
	if err != nil {
		t.Fatal(err)
	}
	pen := h.minter.MintAuthority(basis.Run, basis.Kind)
	env, err := behavior.BuildRequest(time.Now, behavior.RequestSpec{Type: typ, Payload: payload, Audience: message.Audience{receiver}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := pen.Write(context.Background(), env)
	if err != nil || !result.Accepted() {
		t.Fatalf("write %s: result=%+v err=%v", typ, result, err)
	}
	return *env
}

func callLogbookRecent(t *testing.T, h *Home, caller actor.ActorID) sysactor.LogbookRecentResponse {
	t.Helper()
	request := writeHomeRequest(t, h, caller, actor.SystemActorID, platform.TypeLogbookRecent, json.RawMessage(`{"limit":5}`))
	var terminal message.Envelope
	restartEventually(t, "system actor logbook response", func() bool {
		rows, err := h.query.ReadAfterSeq(context.Background(), 0, 2000)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if row.Envelope.Kind == message.KindResponse && row.Envelope.ParentID == request.ID {
				terminal = row.Envelope
				return true
			}
		}
		return false
	})
	var payload struct {
		Status   string                    `json:"status"`
		Messages []sysactor.LogbookMessage `json:"messages"`
	}
	if err := json.Unmarshal(terminal.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Status != string(message.StatusCompleted) {
		t.Fatalf("logbook terminal=%s", terminal.Payload)
	}
	return sysactor.LogbookRecentResponse{Messages: payload.Messages}
}

func assertLogbookContainsOnlyContext(t *testing.T, response sysactor.LogbookRecentResponse, contextID message.ID) {
	t.Helper()
	if len(response.Messages) != 1 || response.Messages[0].Envelope.ID != contextID {
		t.Fatalf("logbook messages=%+v", response.Messages)
	}
	if response.Messages[0].Envelope.Type == platform.TypeLogbookRecent {
		t.Fatal("logbook query polluted its own result")
	}
}
