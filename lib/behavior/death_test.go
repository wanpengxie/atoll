package behavior

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// queryStub satisfies storespec.MessageQuery for tests.
type queryStub struct {
	rows      []storespec.StoredRow
	err       error
	receivers []actor.ActorID
	recvErr   error
}

func (q *queryStub) MaxSeq(context.Context) (int64, error) { return 0, nil }
func (q *queryStub) LatestBySenderAndType(context.Context, actor.ActorID, string) (storespec.StoredRow, bool, error) {
	return storespec.StoredRow{}, false, nil
}
func (q *queryStub) ReadAfterSeq(context.Context, int64, int) ([]storespec.StoredRow, error) {
	return nil, nil
}
func (q *queryStub) OpenRequestsForActor(_ context.Context, _ actor.ActorID) ([]storespec.StoredRow, error) {
	return q.rows, q.err
}
func (q *queryStub) DistinctOpenRequestReceivers(context.Context) ([]actor.ActorID, error) {
	return q.receivers, q.recvErr
}

func envsToRows(envs []*message.Envelope) []storespec.StoredRow {
	var rows []storespec.StoredRow
	for _, e := range envs {
		if e == nil {
			continue
		}
		rows = append(rows, storespec.StoredRow{Envelope: *e})
	}
	return rows
}

// MaterialiseReceiverUnavailable writes one receiver_unavailable terminal for
// each in-flight request to the dead actor, and skips nil entries.
func TestMaterialise_WritesPerRequest(t *testing.T) {
	w := &recordingWriter{}
	q := &queryStub{rows: envsToRows([]*message.Envelope{
		newRequest("a", nil),
		newRequest("b", nil),
	})}
	err := MaterialiseReceiverUnavailable(context.Background(), w, q, fixedClock(1), actor.ActorID("dead"), nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if w.count() != 2 {
		t.Fatalf("want 2 terminals, got %d", w.count())
	}
	for _, term := range w.writes {
		if term.Kind != message.KindResponse {
			t.Fatalf("terminal kind = %q, want response", term.Kind)
		}
		// The system identity is welded onto the (system) pen, not set by the
		// builder. The relay stub does not inject it, so the built terminal keeps
		// a zero Sender (sealed-pen).
		if term.Sender != (message.Sender{}) {
			t.Fatalf("terminal sender = %+v, want zero (pen-injected)", term.Sender)
		}
		var p struct {
			Status string `json:"status"`
			Reason string `json:"reason"`
		}
		_ = json.Unmarshal(term.Payload, &p)
		if p.Status != "failed" || p.Reason != string(message.TerminalReceiverUnavailable) {
			t.Fatalf("terminal payload = %+v, want failed/receiver_unavailable", p)
		}
	}
}

// A drain-query failure is the loudest fault: returns the error, writes nothing.
func TestMaterialise_DrainQueryFailureReturnsError(t *testing.T) {
	w := &recordingWriter{}
	q := &queryStub{err: errors.New("store down")}
	err := MaterialiseReceiverUnavailable(context.Background(), w, q, fixedClock(1), actor.ActorID("dead"), nil)
	if err == nil {
		t.Fatal("a drain-query failure must return an error")
	}
	if w.count() != 0 {
		t.Fatalf("no terminal must be written on drain failure, got %d", w.count())
	}
}

// A per-request write failure invokes onFault and continues to the rest — one
// bad request must not strand the others.
func TestMaterialise_PerRequestWriteFaultContinues(t *testing.T) {
	w := &recordingWriter{err: errors.New("write boom")}
	q := &queryStub{rows: envsToRows([]*message.Envelope{newRequest("a", nil), newRequest("b", nil)})}

	var faults []message.ID
	err := MaterialiseReceiverUnavailable(context.Background(), w, q, fixedClock(1), actor.ActorID("dead"),
		func(reqID message.ID, ferr error) { faults = append(faults, reqID) })
	if err != nil {
		t.Fatalf("per-request faults must not return a top-level error: %v", err)
	}
	if len(faults) != 2 {
		t.Fatalf("onFault must fire per failed request, got %d", len(faults))
	}
}

// A per-request write failure with a nil onFault is silently ignored (no panic)
// and the loop still continues.
func TestMaterialise_NilOnFaultIgnored(t *testing.T) {
	w := &recordingWriter{err: errors.New("write boom")}
	q := &queryStub{rows: envsToRows([]*message.Envelope{newRequest("a", nil), newRequest("b", nil)})}
	err := MaterialiseReceiverUnavailable(context.Background(), w, q, fixedClock(1), actor.ActorID("dead"), nil)
	if err != nil {
		t.Fatalf("nil onFault must not surface an error: %v", err)
	}
}
