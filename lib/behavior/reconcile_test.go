package behavior

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// deregSet is a ClosedForever predicate backed by a fixed closed-forever set: an
// id in the set is deregistered / never a member (closure owed), anything else is
// still a registered member.
func deregSet(closed map[actor.ActorID]bool) ClosedForever {
	return func(_ context.Context, id actor.ActorID) (bool, error) { return closed[id], nil }
}

// The reconciler closes ONLY the receivers the predicate reports closed forever
// (deregistered); a still-registered receiver is skipped — it may get a
// successor, so its callers wait for the request deadline, no closure owed.
func TestReconcile_ClosesOnlyDeregisteredReceivers(t *testing.T) {
	w := &recordingWriter{}
	q := &queryStub{
		receivers: []actor.ActorID{"dead", "alive"},
		// queryStub.OpenRequestsForActor ignores the actor and returns these rows
		// for whichever receiver is drained. Only the deregistered one ("dead") is
		// drained, so exactly one request is closed.
		rows: envsToRows([]*message.Envelope{newRequest("r1", nil)}),
	}
	closed := deregSet(map[actor.ActorID]bool{"dead": true}) // "alive" still a member

	err := ReconcileReceiverUnavailable(context.Background(), w, q, closed,
		fixedClock(1), nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if w.count() != 1 {
		t.Fatalf("want 1 terminal (only the deregistered receiver), got %d", w.count())
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
	closed := deregSet(map[actor.ActorID]bool{"dead": true})

	var faults int
	err := ReconcileReceiverUnavailable(context.Background(), w, q, closed,
		fixedClock(1), func(message.ID, error) { faults++ })
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

	err := ReconcileReceiverUnavailable(context.Background(), w, q, deregSet(nil),
		fixedClock(1), nil)
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
	closed := deregSet(map[actor.ActorID]bool{"dead-1": true, "dead-2": true})

	var faults int
	err := ReconcileReceiverUnavailable(context.Background(), w, q, closed,
		fixedClock(1), func(message.ID, error) { faults++ })
	if err != nil {
		t.Fatalf("per-receiver drain faults must not return a top-level error: %v", err)
	}
	if faults != 2 {
		t.Fatalf("onFault must fire once per failed receiver, got %d", faults)
	}
}

// C7 新测④ (behavior 层): a closure-predicate LOOKUP failure must NEVER be treated
// as closed — the round skips that receiver (no write), surfaces a fault, and
// returns no top-level error; a later round, once the predicate recovers, closes
// it. Locks the failure policy (错误绝不当注销) + next-round convergence.
func TestReconcile_PredicateFailureSkipsThenRecovers(t *testing.T) {
	w := &recordingWriter{}
	q := &queryStub{
		receivers: []actor.ActorID{"dead"},
		rows:      envsToRows([]*message.Envelope{newRequest("r1", nil)}),
	}
	boom := errors.New("registry lookup down")
	fail := true
	predicate := func(_ context.Context, _ actor.ActorID) (bool, error) {
		if fail {
			return false, boom
		}
		return true, nil // recovered: the receiver is confirmed deregistered.
	}

	// Round 1: predicate fails → no close, one fault, no top-level error.
	var faults int
	if err := ReconcileReceiverUnavailable(context.Background(), w, q, predicate,
		fixedClock(1), func(message.ID, error) { faults++ }); err != nil {
		t.Fatalf("a predicate failure must not return a top-level error: %v", err)
	}
	if w.count() != 0 {
		t.Fatalf("a predicate lookup failure must NOT close anyone (错误当注销=误杀), got %d writes", w.count())
	}
	if faults != 1 {
		t.Fatalf("predicate failure must surface exactly one fault, got %d", faults)
	}

	// Round 2: predicate recovered → the receiver is closed.
	fail = false
	if err := ReconcileReceiverUnavailable(context.Background(), w, q, predicate,
		fixedClock(1), func(message.ID, error) { faults++ }); err != nil {
		t.Fatalf("recovered round unexpected err: %v", err)
	}
	if w.count() != 1 {
		t.Fatalf("recovered round must close the deregistered receiver, got %d writes", w.count())
	}
}
