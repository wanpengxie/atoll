package sysactor

// The reserved actor.describe answer: the system actor self-describes in the
// introspect contract shape (identity + documented reserved queries), answers
// the single-type selector form, and does not synthesize for an unknown
// selector — the caller's closure reaps unanswered questions.

import (
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
)

func newDescribeReq() actorbase.Msg {
	return requestMsg("q-desc", introspect.QueryDescribe, nil)
}

// TestRespondDescribe proves the system actor self-answers the reserved
// actor.describe in the introspect contract shape: identity + the single
// reserved query it serves (actor.list) documented in Types.
func TestRespondDescribe(t *testing.T) {
	req := newDescribeReq()
	sys := &fakeSys{}
	s := New(Deps{Authority: fakeRegistry{}})

	s.handle(sys, req)
	if len(sys.replies) != 1 {
		t.Fatalf("expected 1 reply, got %d", len(sys.replies))
	}
	raw, err := json.Marshal(sys.replies[0].v)
	if err != nil {
		t.Fatalf("marshal reply: %v", err)
	}
	var d introspect.Describe
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatalf("unmarshal describe: %v", err)
	}
	if d.ActorID != string(actor.SystemActorID) {
		t.Fatalf("describe actor_id=%q, want %q", d.ActorID, actor.SystemActorID)
	}
	meta, ok := d.Types[introspect.QueryList]
	if !ok || meta.Description == "" {
		t.Fatalf("describe types=%+v, want documented %q", d.Types, introspect.QueryList)
	}
}

// TestRespondDescribe_TypeSelector proves the single-type selector form
// answers with the introspect DescribeType shape.
func TestRespondDescribe_TypeSelector(t *testing.T) {
	req := requestMsg("q-desc", introspect.QueryDescribe, []byte(`{"type":"actor.list"}`))
	sys := &fakeSys{}
	s := New(Deps{Authority: fakeRegistry{}})

	s.handle(sys, req)
	if len(sys.replies) != 1 {
		t.Fatalf("expected 1 reply, got %d", len(sys.replies))
	}
	raw, err := json.Marshal(sys.replies[0].v)
	if err != nil {
		t.Fatalf("marshal reply: %v", err)
	}
	var dt introspect.DescribeType
	if err := json.Unmarshal(raw, &dt); err != nil {
		t.Fatalf("unmarshal describe type: %v", err)
	}
	if dt.Type != introspect.QueryList || dt.Description == "" {
		t.Fatalf("describe type answer=%+v", dt)
	}
}

// TestRespondDescribe_UnknownSelector proves an unknown type selector is not
// synthesized (no reply): the caller closure reaps it.
func TestRespondDescribe_UnknownSelector(t *testing.T) {
	req := requestMsg("q-desc", introspect.QueryDescribe, []byte(`{"type":"nope"}`))
	sys := &fakeSys{}
	s := New(Deps{Authority: fakeRegistry{}})

	s.handle(sys, req)
	if len(sys.replies) != 0 {
		t.Fatalf("expected no reply, got %d", len(sys.replies))
	}
}
