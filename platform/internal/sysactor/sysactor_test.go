package sysactor

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// fakeRegistry serves a fixed membership set (the durable axis).
type fakeRegistry struct{ rows []storespec.Record }

func (f fakeRegistry) Lookup(context.Context, actor.ActorID) (storespec.Record, bool, error) {
	return storespec.Record{}, false, nil
}
func (f fakeRegistry) Exists(context.Context, actor.ActorID) (bool, error) { return false, nil }
func (f fakeRegistry) ListActive(context.Context) ([]storespec.Record, error) {
	return f.rows, nil
}

// fakeStat is the injected obs-read seam (substrate pull-stat stand-in); it reports the
// ids in its set as present with a fixed bind-instant. Stands in for the
// substrate's authoritative liveness/uptime view.
type fakeStat struct {
	present map[actor.ActorID]bool
	started time.Time
}

func (p fakeStat) Stat(id actor.ActorID) (time.Time, bool) {
	if !p.present[id] {
		return time.Time{}, false
	}
	return p.started, true
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
		Registry: reg,
		Stat:     fakeStat{present: map[actor.ActorID]bool{"actor:a": true}, started: time.Now()},
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
