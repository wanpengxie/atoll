package behavior

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// openReqsStub is an OpenRequests double: returns a fixed set or an error.
type openReqsStub struct {
	reqs []*message.Envelope
	err  error
}

func (o *openReqsStub) OpenRequestsForActor(_ context.Context, _ actor.ActorID) ([]*message.Envelope, error) {
	return o.reqs, o.err
}

func sysSender() message.Sender {
	return message.Sender{Kind: actor.Kind("system"), ID: actor.ActorID("channel-sys")}
}

// MaterialiseReceiverUnavailable writes one receiver_unavailable terminal for
// each in-flight request to the dead actor, and skips nil entries.
func TestMaterialise_WritesPerRequest(t *testing.T) {
	w := &recordingWriter{}
	or := &openReqsStub{reqs: []*message.Envelope{
		newRequest("a", nil),
		nil, // must be skipped, not panic
		newRequest("b", nil),
	}}
	err := MaterialiseReceiverUnavailable(context.Background(), w, or, fixedClock(1), sysSender(), actor.ActorID("dead"), nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if w.count() != 2 {
		t.Fatalf("want 2 terminals (nil skipped), got %d", w.count())
	}
	for _, term := range w.writes {
		if term.Kind != message.KindResponse {
			t.Fatalf("terminal kind = %q, want response", term.Kind)
		}
		if term.Sender != sysSender() {
			t.Fatalf("terminal sender = %+v, want system", term.Sender)
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
	or := &openReqsStub{err: errors.New("store down")}
	err := MaterialiseReceiverUnavailable(context.Background(), w, or, fixedClock(1), sysSender(), actor.ActorID("dead"), nil)
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
	or := &openReqsStub{reqs: []*message.Envelope{newRequest("a", nil), newRequest("b", nil)}}

	var faults []message.ID
	err := MaterialiseReceiverUnavailable(context.Background(), w, or, fixedClock(1), sysSender(), actor.ActorID("dead"),
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
	or := &openReqsStub{reqs: []*message.Envelope{newRequest("a", nil), newRequest("b", nil)}}
	err := MaterialiseReceiverUnavailable(context.Background(), w, or, fixedClock(1), sysSender(), actor.ActorID("dead"), nil)
	if err != nil {
		t.Fatalf("nil onFault must not surface an error: %v", err)
	}
}
