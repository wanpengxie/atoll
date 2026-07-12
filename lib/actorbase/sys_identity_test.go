package actorbase

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// TestRespondEnvelopeAcrossIncarnation: response authority系于 identity, not the
// per-life serve projection (D1). A fresh incarnation that never Recv'd the
// request — its serve ledger is empty — still answers a request recovered from
// the log, and the response lands with parent_id == the request id. The empty
// ledger is untouched (no entry = zero account action).
func TestRespondEnvelopeAcrossIncarnation(t *testing.T) {
	t.Parallel()
	pen := &fakePen{self: "user:alice"}
	e := runningEngine(t, pen)

	// A request this incarnation never admitted (recovered from the log).
	req := newRequestEnv("r-crosslife", -1)
	req.Sender.ID = "agent:worker" // the response must address the original sender

	if e.serve.len() != 0 {
		t.Fatalf("fresh incarnation serve ledger len = %d, want 0", e.serve.len())
	}

	id, err := e.RespondEnvelope(req, behavior.ResponseSpec{Status: message.StatusCompleted})
	if err != nil {
		t.Fatalf("RespondEnvelope across incarnation = %v, want nil", err)
	}
	if id == "" {
		t.Fatal("RespondEnvelope returned empty receipt id")
	}
	resp := pen.last()
	if resp == nil {
		t.Fatal("RespondEnvelope wrote no envelope")
	}
	if resp.Kind != message.KindResponse {
		t.Fatalf("written kind = %q, want response", resp.Kind)
	}
	if resp.ParentID != req.ID {
		t.Fatalf("response parent_id = %q, want %q", resp.ParentID, req.ID)
	}
	// No serve-ledger entry existed, so closing it is a no-op — the projection
	// stays empty, the log carried the truth.
	if e.serve.len() != 0 {
		t.Fatalf("serve ledger len after cross-life respond = %d, want 0", e.serve.len())
	}

	// nil req is a defended error (drive.go:141 现行).
	if _, err := e.RespondEnvelope(nil, behavior.ResponseSpec{Status: message.StatusCompleted}); err == nil {
		t.Fatal("RespondEnvelope(nil) accepted, want error")
	}
}

// TestAfterIdentityPayloadVerbatim guards裁决 6: the timer payload rides as
// json.RawMessage byte-for-byte — NO []byte→base64 marshal that would corrupt
// what the fired timer's recipient parses. Also pins the BindIdentity value
// (identity-bound durable timer, D7).
func TestAfterIdentityPayloadVerbatim(t *testing.T) {
	t.Parallel()
	pen := &fakePen{self: "user:alice"}
	e := runningEngine(t, pen)
	rec := &recordingSchedule{}
	e.sched = rec

	payload := json.RawMessage(`{"remind":"call mom","n":42,"nested":{"k":"v"}}`)
	id, err := e.AfterIdentity(time.Minute, "reminder.note", payload)
	if err != nil {
		t.Fatalf("AfterIdentity = %v", err)
	}
	if id != schedule.TimerID("t-1") {
		t.Fatalf("AfterIdentity timer id = %q, want t-1", id)
	}
	if len(rec.reqs) != 1 {
		t.Fatalf("schedule saw %d reqs, want 1", len(rec.reqs))
	}
	got := rec.reqs[0]
	if string(got.Payload) != string(payload) {
		t.Fatalf("timer payload = %q, want verbatim %q (base64 回潮?)", string(got.Payload), string(payload))
	}
	if got.Bind != schedule.BindIdentity {
		t.Fatalf("AfterIdentity Bind = %v, want BindIdentity", got.Bind)
	}
	if got.Type != "reminder.note" {
		t.Fatalf("timer type = %q, want reminder.note", got.Type)
	}
}
