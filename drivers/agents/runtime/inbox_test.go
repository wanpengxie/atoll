package runtime

import (
	"log/slog"
	"testing"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

func TestObservationPressureDropsObservationButPreservesLifecycleFact(t *testing.T) {
	q := newInbox(Policy{IngressCapacity: 1}.normalized())
	sink := &generationSink{generation: 1, queue: q, gate: &hostAdmission{}, logger: slog.New(slog.DiscardHandler)}
	target := driverproto.WorkerTurnTarget{Attempt: 1, Native: "native"}
	if !sink.Publish(driverproto.Tool{Target: target, CallID: "one"}) {
		t.Fatal("first observation rejected")
	}
	if !sink.Publish(driverproto.Diagnostic{Code: "dropped"}) {
		t.Fatal("pressure drop incorrectly sealed the sink")
	}
	if !sink.Publish(driverproto.WorkerEnded{Detail: "done"}) {
		t.Fatal("lifecycle fact was blocked by observation pressure")
	}
	first, ok := q.pop()
	if !ok {
		t.Fatal("missing retained observation")
	}
	if fact, ok := first.value.(driverFact); !ok {
		t.Fatalf("first=%T want driverFact", first.value)
	} else if _, ok := fact.event.(driverproto.Tool); !ok {
		t.Fatalf("first event=%T want Tool", fact.event)
	}
	second, ok := q.pop()
	if !ok {
		t.Fatal("missing lifecycle fact")
	}
	if fact, ok := second.value.(driverFact); !ok {
		t.Fatalf("second=%T want driverFact", second.value)
	} else if _, ok := fact.event.(driverproto.WorkerEnded); !ok {
		t.Fatalf("second event=%T want WorkerEnded", fact.event)
	}
	if _, ok := q.pop(); ok {
		t.Fatal("dropped diagnostic remained in inbox")
	}
}

func TestInboxCoalescedActivityKeepsAdmissionOrder(t *testing.T) {
	q := newInbox(Policy{IngressCapacity: 2, CommandCapacity: 1, CallbackCapacity: 1}.normalized())
	target := driverproto.WorkerTurnTarget{Attempt: 1, Native: "native"}
	if !q.pushActivity(1, target) || !q.push(classTimer, timerFact{kind: timerWatchdog}) || !q.pushActivity(1, target) {
		t.Fatal("admission failed")
	}
	first, ok := q.pop()
	if !ok {
		t.Fatal("missing first fact")
	}
	if _, ok := first.value.(driverFact); !ok {
		t.Fatalf("first=%T want driverFact", first.value)
	}
	second, ok := q.pop()
	if !ok {
		t.Fatal("missing second fact")
	}
	if _, ok := second.value.(timerFact); !ok {
		t.Fatalf("second=%T want timerFact", second.value)
	}
}

func TestInboxCommandCreditRejectsWithoutAdmission(t *testing.T) {
	q := newInbox(Policy{IngressCapacity: 1, CommandCapacity: 1, CallbackCapacity: 1}.normalized())
	if !q.push(classCommand, command{kind: commandStart}) {
		t.Fatal("first command rejected")
	}
	if q.push(classCommand, command{kind: commandTerminate}) {
		t.Fatal("command admitted without credit")
	}
	if got, ok := q.pop(); !ok || got.value.(command).kind != commandStart {
		t.Fatalf("admitted command changed: %#v", got)
	}
}
