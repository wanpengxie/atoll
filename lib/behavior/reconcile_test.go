package behavior

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
)

// presenceSet is a PresenceProbe backed by a fixed present-set.
type presenceSet map[actor.ActorID]bool

func (p presenceSet) Present(id actor.ActorID) bool { return p[id] }

// The reconciler closes ONLY the receivers the substrate reports absent; a live
// receiver (still present) is skipped — it can still answer, no closure owed.
func TestReconcile_ClosesOnlyAbsentReceivers(t *testing.T) {
	w := &recordingWriter{}
	q := &queryStub{
		receivers: []actor.ActorID{"dead", "alive"},
		// queryStub.OpenRequestsForActor ignores the actor and returns these rows
		// for whichever receiver is drained. Only the absent one ("dead") is
		// drained, so exactly one request is closed.
		rows: envsToRows([]*message.Envelope{newRequest("r1", nil)}),
	}
	present := presenceSet{"alive": true} // "dead" absent

	err := ReconcileReceiverUnavailable(context.Background(), w, q, present,
		fixedClock(1), sysSender(), nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if w.count() != 1 {
		t.Fatalf("want 1 terminal (only the absent receiver), got %d", w.count())
	}
}

// Idempotency: a re-scan whose per-request write is rejected as a terminal
// duplicate (the UNIQUE-index collision the store raises) produces NO new
// closure and surfaces NO fault — repeated scans are safe.
func TestReconcile_IdempotentOnDuplicateTerminal(t *testing.T) {
	w := &recordingWriter{duplicate: true} // every write rejected as terminal-duplicate
	q := &queryStub{
		receivers: []actor.ActorID{"dead"},
		rows:      envsToRows([]*message.Envelope{newRequest("r1", nil)}),
	}
	present := presenceSet{} // dead absent

	var faults int
	err := ReconcileReceiverUnavailable(context.Background(), w, q, present,
		fixedClock(1), sysSender(), func(message.ID, error) { faults++ })
	if err != nil {
		t.Fatalf("a duplicate-terminal collision must not surface a top-level error: %v", err)
	}
	if faults != 0 {
		t.Fatalf("a duplicate terminal is the idempotent no-op, NOT a fault, got %d faults", faults)
	}
}

// A scan-query failure is the loudest fault (no orphan can be enumerated):
// returns the error, writes nothing.
func TestReconcile_ScanQueryFailureReturnsError(t *testing.T) {
	w := &recordingWriter{}
	q := &queryStub{recvErr: errors.New("store down")}

	err := ReconcileReceiverUnavailable(context.Background(), w, q, presenceSet{},
		fixedClock(1), sysSender(), nil)
	if err == nil {
		t.Fatal("a distinct-receivers scan failure must return an error")
	}
	if w.count() != 0 {
		t.Fatalf("no terminal must be written on scan failure, got %d", w.count())
	}
}

// A per-receiver drain failure surfaces onFault (that receiver's callers are
// black holes) but does not strand the rest of the scan.
func TestReconcile_PerReceiverDrainFaultContinues(t *testing.T) {
	w := &recordingWriter{}
	q := &queryStub{
		receivers: []actor.ActorID{"dead-1", "dead-2"},
		err:       errors.New("drain boom"), // OpenRequestsForActor fails for both
	}

	var faults int
	err := ReconcileReceiverUnavailable(context.Background(), w, q, presenceSet{},
		fixedClock(1), sysSender(), func(message.ID, error) { faults++ })
	if err != nil {
		t.Fatalf("per-receiver drain faults must not return a top-level error: %v", err)
	}
	if faults != 2 {
		t.Fatalf("onFault must fire once per failed receiver, got %d", faults)
	}
}
