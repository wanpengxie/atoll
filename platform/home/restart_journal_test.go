package home

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
)

func TestRestartJournalRetryCannotRetireSuccessorTicket(t *testing.T) {
	h := openWhiteboxHome(t)
	h.disablePoke.Store(true)
	ctx := context.Background()
	id, err := h.Admit(ctx, actor.KindHuman, "restart-journal")
	if err != nil {
		t.Fatal(err)
	}
	unlock := h.actorGates.lock(id)
	locked := true
	defer func() {
		if locked {
			unlock()
		}
	}()
	_, _ = h.liveness.Retire(id, false)
	oldTicket, verdict := h.liveness.BeginEnsure(id, 1)
	if verdict != transitionApplied {
		t.Fatalf("old BeginEnsure=%v", verdict)
	}
	if verdict := h.liveness.PublishLocal(id, oldTicket, noInc, &testCarrier{}); verdict != transitionApplied {
		t.Fatalf("old publish=%v", verdict)
	}

	const jobID = int64(77)
	expected, applied, err := h.cs.RestartJournal.ClaimRestartAttempt(ctx, jobID, id, string(oldTicket), h.nowMs())
	if err != nil || applied || expected != string(oldTicket) {
		t.Fatalf("claim=(%q,%v,%v)", expected, applied, err)
	}
	if _, retired := h.liveness.RetireIfTicketMatches(id, oldTicket, true); !retired {
		state, _ := h.liveness.stateForTest(id)
		t.Fatalf("simulated first attempt did not retire old ticket: expected=%q state=%+v", oldTicket, state)
	}
	newTicket, verdict := h.liveness.BeginEnsure(id, 1)
	if verdict != transitionApplied || newTicket == oldTicket {
		t.Fatalf("successor BeginEnsure=(%q,%v)", newTicket, verdict)
	}
	if verdict := h.liveness.PublishLocal(id, newTicket, noInc, &testCarrier{}); verdict != transitionApplied {
		t.Fatalf("successor publish=%v", verdict)
	}
	unlock()
	locked = false

	version, marked, err := h.ApplyRestartTarget(ctx, jobID, id)
	if err != nil || !marked || version != 1 {
		t.Fatalf("retry=(version=%d marked=%v err=%v)", version, marked, err)
	}
	intent := h.liveness.AttachmentIntent(id)
	standing, ok := h.liveness.WakeStanding(id)
	if !ok || !intent.Present || intent.Ticket != newTicket || standing.Occ != occRunning {
		t.Fatalf("retry retired successor: intent=%+v standing=%+v ok=%v", intent, standing, ok)
	}
}
