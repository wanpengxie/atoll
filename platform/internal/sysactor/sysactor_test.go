package sysactor

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/internal/presence"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// fakeRegistry serves a fixed membership set (the durable axis).
type fakeRegistry struct{ rows []storespec.Record }

func (f fakeRegistry) LookupActive(_ context.Context, id actor.ActorID) (storespec.ActorControlRow, bool, error) {
	for _, row := range f.rows {
		if row.ID == id && row.IsActive() {
			return controlRow(row), true, nil
		}
	}
	return storespec.ActorControlRow{}, false, nil
}
func (f fakeRegistry) ListActive(context.Context) ([]storespec.ActorControlRow, error) {
	rows := make([]storespec.ActorControlRow, 0, len(f.rows))
	for _, row := range f.rows {
		if row.IsActive() {
			rows = append(rows, controlRow(row))
		}
	}
	return rows, nil
}
func (f fakeRegistry) CheckAuthor(ctx context.Context, stamp storespec.AuthorStamp) (storespec.AuthorVerdict, error) {
	_, ok, err := f.LookupActive(ctx, stamp.ID)
	if !ok {
		return storespec.AuthorNotMember, err
	}
	return storespec.AuthorOK, err
}
func controlRow(row storespec.Record) storespec.ActorControlRow {
	return storespec.ActorControlRow{ID: row.ID, Kind: row.Kind, Principal: row.Principal, Binding: row.Binding, CreatedAt: row.CreatedAt, CurrentDeclVersion: 1}
}

// fakeStat is the injected obs-read seam (substrate pull-stat stand-in); it reports the
// ids in its set as present with a fixed bind-instant. Stands in for the
// substrate's authoritative liveness/uptime view.
type fakeStat struct {
	present map[actor.ActorID]bool
	started time.Time
}

type fixedPresence struct{ snapshot presence.Snapshot }

func (p fixedPresence) Snapshot(context.Context, actor.ActorID) (presence.Snapshot, error) {
	return p.snapshot, nil
}

func (p fakeStat) Snapshot(_ context.Context, id actor.ActorID) (presence.Snapshot, error) {
	if !p.present[id] {
		return presence.Snapshot{Member: true}, nil
	}
	return presence.Snapshot{Member: true, L1Present: true, L1StartedAt: p.started}, nil
}

func TestActorStatusProjectsPresence(t *testing.T) {
	started := time.Unix(90, 0)
	s := New(Deps{
		Clock: func() time.Time { return time.Unix(100, 0) },
		Presence: fixedPresence{snapshot: presence.Snapshot{
			Member: true, L1Present: true, L1StartedAt: started,
			L3: map[actorrt.ObsKind]presence.Testimony{
				actorrt.ObsKind(introspect.ObsDevicePresence): {Val: introspect.MarshalDevicePresence(true), ReceivedAt: 7},
				"load": {Val: []byte{1, 2}, ReceivedAt: 8, StaleFromPriorLife: true},
			},
		}},
	})
	sys := &fakeSys{}
	s.handle(sys, requestMsg("status", introspect.QueryStatus, []byte(`{"actor_id":"agent:a"}`)))
	if len(sys.replies) != 1 {
		t.Fatalf("replies=%d", len(sys.replies))
	}
	answer := sys.replies[0].v.(introspect.Status)
	if !answer.Member || !answer.Present || answer.UptimeMs != 10_000 || answer.L3[introspect.ObsDevicePresence].Device == nil {
		t.Fatalf("answer=%+v", answer)
	}
	if answer.L3["load"].ValueBase64 != "AQI=" || !answer.L3["load"].StaleFromPriorLife {
		t.Fatalf("unknown-kind testimony=%+v", answer.L3["load"])
	}
	wire, err := json.Marshal(answer)
	if err != nil {
		t.Fatalf("marshal actor.status answer: %v", err)
	}
	var shape struct {
		L3 map[string]map[string]json.RawMessage `json:"l3"`
	}
	if err := json.Unmarshal(wire, &shape); err != nil {
		t.Fatalf("decode actor.status wire shape: %v", err)
	}
	assertKeys := func(kind string, want ...string) {
		t.Helper()
		got := shape.L3[kind]
		if len(got) != len(want) {
			t.Fatalf("actor.status l3[%q] keys=%v, want exactly %v", kind, got, want)
		}
		for _, key := range want {
			if _, ok := got[key]; !ok {
				t.Fatalf("actor.status l3[%q] missing key %q: %v", kind, key, got)
			}
		}
	}
	assertKeys(introspect.ObsDevicePresence, "received_at", "device")
	assertKeys("load", "received_at", "stale_from_prior_life", "value_b64")
}

func TestActorStatusMalformedDoesNotSynthesize(t *testing.T) {
	s := New(Deps{Presence: fixedPresence{}})
	sys := &fakeSys{}
	s.handle(sys, requestMsg("status", introspect.QueryStatus, []byte(`{}`)))
	if len(sys.replies) != 0 {
		t.Fatalf("malformed status produced %d replies", len(sys.replies))
	}
}

// fakeSys is a minimal actorbase.Sys double: it embeds the (nil) interface so
// every verb this actor never touches stays unimplemented (a call would nil-
// panic, failing the test loudly), and overrides only Reply — the sole verb
// the system actor ever calls.
type fakeSys struct {
	actorbase.Sys

	replies []replyRec
}

type replyRec struct {
	msg actorbase.Msg
	v   any
}

func (f *fakeSys) Reply(msg actorbase.Msg, v any) (message.ID, error) {
	f.replies = append(f.replies, replyRec{msg: msg, v: v})
	return msg.ID, nil
}

var _ actorbase.Sys = (*fakeSys)(nil)

func requestMsg(id message.ID, typ string, payload []byte) actorbase.Msg {
	return actorbase.NewMsg(context.Background(), message.Envelope{
		ID: id, ChannelID: "ch", Kind: message.KindRequest, Type: typ,
		Sender:   message.Sender{Kind: actor.KindAgent, ID: "caller"},
		Audience: message.Audience{actor.SystemActorID},
		Payload:  payload,
	})
}

// TestActorList_TwoAxisNoReadiness proves the composed actor.list directory is
// membership (registry) ∧ liveness (lease) and carries NO readiness column —
// readiness is not a substrate axis; whether an actor can service a request is
// the OUTCOME of send→terminal, never projected into this channel-wide view.
func TestActorList_TwoAxisNoReadiness(t *testing.T) {
	reg := fakeRegistry{rows: []storespec.Record{
		{ID: "actor:a", Kind: actor.KindAgent, Binding: actor.BindingEmbedded},
		{ID: "actor:b", Kind: actor.KindAgent, Binding: actor.BindingEmbedded},
	}}
	sys := &fakeSys{}
	listReq := requestMsg("q1", introspect.QueryList, nil)

	// Liveness authority reports actor:a present, actor:b absent — read via the
	// injected seam when composing actor.list (never a message, never truth).
	s := New(Deps{
		Authority: reg,
		Presence:  fakeStat{present: map[actor.ActorID]bool{"actor:a": true}, started: time.Now()},
	})

	s.handle(sys, listReq)
	if len(sys.replies) != 1 {
		t.Fatalf("expected 1 reply, got %d", len(sys.replies))
	}
	raw, err := json.Marshal(sys.replies[0].v)
	if err != nil {
		t.Fatalf("marshal reply: %v", err)
	}
	var body struct {
		Actors []map[string]any `json:"actors"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("unmarshal reply: %v", err)
	}
	if len(body.Actors) != 2 {
		t.Fatalf("catalog has %d actors, want 2 (membership)", len(body.Actors))
	}
	byID := map[string]map[string]any{}
	for _, a := range body.Actors {
		byID[a["id"].(string)] = a
		if _, hasReadiness := a["readiness"]; hasReadiness {
			t.Fatalf("actor.list row carries a readiness field — readiness is not a substrate axis: %+v", a)
		}
	}
	// actor:a has a fresh liveness lease → present; actor:b never reported → absent.
	if byID["actor:a"]["present"] != true {
		t.Fatalf("actor:a present=%v, want true (fresh lease)", byID["actor:a"]["present"])
	}
	if byID["actor:b"]["present"] != false {
		t.Fatalf("actor:b present=%v, want false (no lease)", byID["actor:b"]["present"])
	}
}

type sponsorAuthority struct{ rows []storespec.ActorControlRow }

func (a sponsorAuthority) LookupActive(_ context.Context, id actor.ActorID) (storespec.ActorControlRow, bool, error) {
	for _, row := range a.rows {
		if row.ID == id {
			return row, true, nil
		}
	}
	return storespec.ActorControlRow{}, false, nil
}
func (a sponsorAuthority) ListActive(context.Context) ([]storespec.ActorControlRow, error) {
	return append([]storespec.ActorControlRow(nil), a.rows...), nil
}
func (a sponsorAuthority) CheckAuthor(context.Context, storespec.AuthorStamp) (storespec.AuthorVerdict, error) {
	return storespec.AuthorOK, nil
}

func TestActorListProjectsSponsorForParentRecovery(t *testing.T) {
	parent := actor.ActorID("agent:master")
	child := actor.ActorID("agent:master/worker-1")
	s := New(Deps{Authority: sponsorAuthority{rows: []storespec.ActorControlRow{
		{ID: parent, Kind: actor.KindAgent, CurrentDeclVersion: 1},
		{ID: child, Kind: actor.KindAgent, Sponsor: parent, CurrentDeclVersion: 1},
	}}})
	sys := &fakeSys{}
	s.handle(sys, requestMsg("sponsors", introspect.QueryList, nil))
	catalog := sys.replies[0].v.(introspect.Catalog)
	for _, row := range catalog.Actors {
		if row.ID == string(child) {
			if row.Sponsor != string(parent) {
				t.Fatalf("child sponsor=%q", row.Sponsor)
			}
			return
		}
	}
	t.Fatalf("child absent from catalog: %+v", catalog)
}
