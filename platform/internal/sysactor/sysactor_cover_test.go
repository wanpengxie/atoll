package sysactor_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/lib/introspect"
	"github.com/wanpengxie/ActOS/platform/internal/sysactor"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// errRegistry fails ListActive, exercising the registry-error early return in
// respondList (substrate read failure is propagated, never swallowed).
type errRegistry struct{ err error }

func (e errRegistry) Lookup(context.Context, actor.ActorID) (storespec.Record, bool, error) {
	return storespec.Record{}, false, nil
}
func (e errRegistry) Exists(context.Context, actor.ActorID) (bool, error) { return false, nil }
func (e errRegistry) ListActive(context.Context) ([]storespec.Record, error) {
	return nil, e.err
}

// errLookup fails FindByID, exercising the lookup-error return in respondReserved.
type errLookup struct{ err error }

func (e errLookup) FindByID(context.Context, message.ID) (*message.Envelope, bool, error) {
	return nil, false, e.err
}

// missLookup reports the request not found (ok=false), exercising the
// not-found defensive branch in respondReserved.
type missLookup struct{}

func (missLookup) FindByID(context.Context, message.ID) (*message.Envelope, bool, error) {
	return nil, false, nil
}

func newDescribeReq() *message.Envelope {
	return &message.Envelope{
		ID: "q-desc", ChannelID: "ch", Kind: message.KindRequest, Type: introspect.QueryDescribe,
		Sender:   message.Sender{Kind: actor.KindAgent, ID: "caller"},
		Audience: message.Audience{actor.SystemActorID},
	}
}

// TestRespondDescribe proves the system actor self-answers the reserved
// actor.describe in the introspect contract shape: identity + the single
// reserved query it serves (actor.list) documented in Types.
func TestRespondDescribe(t *testing.T) {
	req := newDescribeReq()
	fc := &fakeWriter{}
	s := sysactor.New(sysactor.Deps{
		Registry: fakeRegistry{}, Writer: fc, Lookup: fakeLookup{req: req},
	})

	if err := s.Receive(context.Background(), req); err != nil {
		t.Fatalf("actor.describe: %v", err)
	}
	if len(fc.written) != 1 {
		t.Fatalf("expected 1 response written, got %d", len(fc.written))
	}
	var d introspect.Describe
	if err := json.Unmarshal(fc.written[0].Payload, &d); err != nil {
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
	req := newDescribeReq()
	req.Payload = []byte(`{"type":"actor.list"}`)
	fc := &fakeWriter{}
	s := sysactor.New(sysactor.Deps{
		Registry: fakeRegistry{}, Writer: fc, Lookup: fakeLookup{req: req},
	})

	if err := s.Receive(context.Background(), req); err != nil {
		t.Fatalf("actor.describe selector: %v", err)
	}
	if len(fc.written) != 1 {
		t.Fatalf("expected 1 response written, got %d", len(fc.written))
	}
	var dt introspect.DescribeType
	if err := json.Unmarshal(fc.written[0].Payload, &dt); err != nil {
		t.Fatalf("unmarshal describe type: %v", err)
	}
	if dt.Type != introspect.QueryList || dt.Description == "" {
		t.Fatalf("describe type answer=%+v", dt)
	}
}

// TestRespondDescribe_UnknownSelector proves an unknown type selector is not
// synthesized (no write): the caller closure reaps it.
func TestRespondDescribe_UnknownSelector(t *testing.T) {
	req := newDescribeReq()
	req.Payload = []byte(`{"type":"nope"}`)
	fc := &fakeWriter{}
	s := sysactor.New(sysactor.Deps{
		Registry: fakeRegistry{}, Writer: fc, Lookup: fakeLookup{req: req},
	})

	if err := s.Receive(context.Background(), req); err != nil {
		t.Fatalf("actor.describe unknown selector: %v", err)
	}
	if len(fc.written) != 0 {
		t.Fatalf("expected no response written, got %d", len(fc.written))
	}
}

// TestReceive_NonRequestIgnored proves the system actor does not synthesize for
// anything but the two reserved requests: events and other reserved requests
// are silently left (no write), so the caller's closure times out instead.
func TestReceive_NonRequestIgnored(t *testing.T) {
	cases := []*message.Envelope{
		{ID: "e1", Kind: message.KindEvent, Type: "some.event"},
		{ID: "r1", Kind: message.KindRequest, Type: "actor.other"},
		{ID: "p1", Kind: message.KindResponse, Type: introspect.QueryList},
	}
	for _, env := range cases {
		fc := &fakeWriter{}
		s := sysactor.New(sysactor.Deps{Registry: fakeRegistry{}, Writer: fc, Lookup: fakeLookup{}})
		if err := s.Receive(context.Background(), env); err != nil {
			t.Fatalf("Receive(%s/%s): %v", env.Kind, env.Type, err)
		}
		if len(fc.written) != 0 {
			t.Fatalf("Receive(%s/%s) wrote %d responses, want 0 (not synthesized)", env.Kind, env.Type, len(fc.written))
		}
	}
}

// TestRespondList_RegistryError proves a substrate read failure (ListActive) is
// propagated out of respondList, never swallowed into an empty directory.
func TestRespondList_RegistryError(t *testing.T) {
	want := errors.New("registry down")
	listReq := &message.Envelope{
		ID: "q1", Kind: message.KindRequest, Type: introspect.QueryList,
		Audience: message.Audience{actor.SystemActorID},
	}
	s := sysactor.New(sysactor.Deps{
		Registry: errRegistry{err: want}, Writer: &fakeWriter{}, Lookup: fakeLookup{req: listReq},
	})
	err := s.Receive(context.Background(), listReq)
	if !errors.Is(err, want) {
		t.Fatalf("respondList err=%v, want %v", err, want)
	}
}

// TestRespondReserved_LookupError proves a lookup read failure is propagated
// out of respondReserved (the serve-side truth read is authoritative).
func TestRespondReserved_LookupError(t *testing.T) {
	want := errors.New("lookup down")
	req := newDescribeReq()
	s := sysactor.New(sysactor.Deps{
		Registry: fakeRegistry{}, Writer: &fakeWriter{}, Lookup: errLookup{err: want},
	})
	err := s.Receive(context.Background(), req)
	if !errors.Is(err, want) {
		t.Fatalf("respondReserved err=%v, want %v", err, want)
	}
}

// TestRespondReserved_NotFound proves the defensive branch: when the original
// request cannot be recovered (ok=false), respondReserved errors rather than
// authoring a response anchored to nothing.
func TestRespondReserved_NotFound(t *testing.T) {
	req := newDescribeReq()
	fc := &fakeWriter{}
	s := sysactor.New(sysactor.Deps{
		Registry: fakeRegistry{}, Writer: fc, Lookup: missLookup{},
	})
	err := s.Receive(context.Background(), req)
	if err == nil {
		t.Fatalf("respondReserved with missing request returned nil, want error")
	}
	if len(fc.written) != 0 {
		t.Fatalf("wrote %d responses despite missing request, want 0", len(fc.written))
	}
}

// TestObs_NilStat proves the nil-seam contract: with no liveness seam wired,
// actor.list composes everyone absent with zero uptime (advisory, never a gate).
func TestObs_NilStat(t *testing.T) {
	reg := fakeRegistry{rows: []storespec.Record{
		{ID: "actor:a", Kind: actor.KindAgent, Binding: actor.BindingEmbedded},
	}}
	fc := &fakeWriter{}
	listReq := &message.Envelope{
		ID: "q1", Kind: message.KindRequest, Type: introspect.QueryList,
		Audience: message.Audience{actor.SystemActorID},
	}
	// Stat left nil → everyone absent.
	s := sysactor.New(sysactor.Deps{
		Registry: reg, Writer: fc, Lookup: fakeLookup{req: listReq},
	})
	if err := s.Receive(context.Background(), listReq); err != nil {
		t.Fatalf("actor.list: %v", err)
	}
	var cat introspect.Catalog
	if err := json.Unmarshal(fc.written[0].Payload, &cat); err != nil {
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
	fc := &fakeWriter{}
	listReq := &message.Envelope{
		ID: "q1", Kind: message.KindRequest, Type: introspect.QueryList,
		Audience: message.Audience{actor.SystemActorID},
	}
	// present=true but started=zero time → uptime guarded to 0.
	s := sysactor.New(sysactor.Deps{
		Registry: reg, Writer: fc, Lookup: fakeLookup{req: listReq},
		Stat:  fakeStat{present: map[actor.ActorID]bool{"actor:a": true}, started: time.Time{}},
		Clock: func() time.Time { return time.Unix(1000, 0) },
	})
	if err := s.Receive(context.Background(), listReq); err != nil {
		t.Fatalf("actor.list: %v", err)
	}
	var cat introspect.Catalog
	if err := json.Unmarshal(fc.written[0].Payload, &cat); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !cat.Actors[0].Present {
		t.Fatalf("present=false, want true")
	}
	if cat.Actors[0].UptimeMs != 0 {
		t.Fatalf("uptime=%d, want 0 (zero StartedAt guarded)", cat.Actors[0].UptimeMs)
	}
}
