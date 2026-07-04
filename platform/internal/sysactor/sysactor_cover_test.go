package sysactor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// errRegistry fails ListActive, exercising the registry-error early return in
// respondList (a substrate read failure writes nothing — the same "does not
// synthesize" posture as an unrouted type, never a bogus empty directory).
type errRegistry struct{ err error }

func (e errRegistry) Lookup(context.Context, actor.ActorID) (storespec.Record, bool, error) {
	return storespec.Record{}, false, nil
}
func (e errRegistry) Exists(context.Context, actor.ActorID) (bool, error) { return false, nil }
func (e errRegistry) ListActive(context.Context) ([]storespec.Record, error) {
	return nil, e.err
}

func newDescribeReq() actorbase.Msg {
	return requestMsg("q-desc", introspect.QueryDescribe, nil)
}

// TestRespondDescribe proves the system actor self-answers the reserved
// actor.describe in the introspect contract shape: identity + the single
// reserved query it serves (actor.list) documented in Types.
func TestRespondDescribe(t *testing.T) {
	req := newDescribeReq()
	sys := &fakeSys{}
	s := New(Deps{Registry: fakeRegistry{}})

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
	s := New(Deps{Registry: fakeRegistry{}})

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
	s := New(Deps{Registry: fakeRegistry{}})

	s.handle(sys, req)
	if len(sys.replies) != 0 {
		t.Fatalf("expected no reply, got %d", len(sys.replies))
	}
}

// TestReceive_NonRequestIgnored proves the system actor does not synthesize for
// anything but the two reserved requests: events and other reserved requests
// are silently left (no reply), so the caller's closure times out instead.
func TestReceive_NonRequestIgnored(t *testing.T) {
	cases := []struct {
		kind message.Kind
		typ  string
	}{
		{message.KindEvent, "some.event"},
		{message.KindRequest, "actor.other"},
		{message.KindResponse, introspect.QueryList},
	}
	for _, c := range cases {
		sys := &fakeSys{}
		s := New(Deps{Registry: fakeRegistry{}})
		msg := actorbase.NewMsg(context.Background(), message.Envelope{ID: "e1", Kind: c.kind, Type: c.typ})
		s.handle(sys, msg)
		if len(sys.replies) != 0 {
			t.Fatalf("Receive(%s/%s) wrote %d replies, want 0 (not synthesized)", c.kind, c.typ, len(sys.replies))
		}
	}
}

// TestRespondList_RegistryError proves a substrate read failure (ListActive)
// writes no reply — never swallowed into a bogus empty directory.
func TestRespondList_RegistryError(t *testing.T) {
	listReq := requestMsg("q1", introspect.QueryList, nil)
	sys := &fakeSys{}
	s := New(Deps{Registry: errRegistry{err: errors.New("registry down")}})

	s.handle(sys, listReq)
	if len(sys.replies) != 0 {
		t.Fatalf("expected no reply on registry error, got %d", len(sys.replies))
	}
}

// TestObs_NilStat proves the nil-seam contract: with no liveness seam wired,
// actor.list composes everyone absent with zero uptime (advisory, never a gate).
func TestObs_NilStat(t *testing.T) {
	reg := fakeRegistry{rows: []storespec.Record{
		{ID: "actor:a", Kind: actor.KindAgent, Binding: actor.BindingEmbedded},
	}}
	listReq := requestMsg("q1", introspect.QueryList, nil)
	sys := &fakeSys{}
	// Stat left nil → everyone absent.
	s := New(Deps{Registry: reg})

	s.handle(sys, listReq)
	raw, err := json.Marshal(sys.replies[0].v)
	if err != nil {
		t.Fatalf("marshal reply: %v", err)
	}
	var cat introspect.Catalog
	if err := json.Unmarshal(raw, &cat); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(cat.Actors) != 1 {
		t.Fatalf("catalog has %d actors, want 1", len(cat.Actors))
	}
	if cat.Actors[0].Present || cat.Actors[0].UptimeMs != 0 {
		t.Fatalf("nil stat row=%+v, want absent/zero uptime", cat.Actors[0])
	}
}

// TestObs_PresentZeroStartedAt proves the present-but-zero-bind-instant case:
// when the seam reports present with a zero StartedAt, uptime stays 0 (the
// !startedAt.IsZero() guard) while present is still true.
func TestObs_PresentZeroStartedAt(t *testing.T) {
	reg := fakeRegistry{rows: []storespec.Record{
		{ID: "actor:a", Kind: actor.KindAgent, Binding: actor.BindingEmbedded},
	}}
	listReq := requestMsg("q1", introspect.QueryList, nil)
	sys := &fakeSys{}
	// present=true but started=zero time → uptime guarded to 0.
	s := New(Deps{
		Registry: reg,
		Stat:     fakeStat{present: map[actor.ActorID]bool{"actor:a": true}},
		Clock:    func() time.Time { return time.Unix(1000, 0) },
	})

	s.handle(sys, listReq)
	raw, err := json.Marshal(sys.replies[0].v)
	if err != nil {
		t.Fatalf("marshal reply: %v", err)
	}
	var cat introspect.Catalog
	if err := json.Unmarshal(raw, &cat); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !cat.Actors[0].Present {
		t.Fatalf("present=false, want true")
	}
	if cat.Actors[0].UptimeMs != 0 {
		t.Fatalf("uptime=%d, want 0 (zero StartedAt guarded)", cat.Actors[0].UptimeMs)
	}
}
