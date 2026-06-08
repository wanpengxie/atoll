package sysactor_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/lib/sysactor"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
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

// fakeWriter records the system actor's written response.
type fakeWriter struct{ written []*message.Envelope }

func (w *fakeWriter) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	w.written = append(w.written, env)
	return harness.WriteResult{MessageID: env.ID}, nil
}

// fakeLookup returns the original request so Respond can anchor the response to it.
type fakeLookup struct{ req *message.Envelope }

func (f fakeLookup) FindByID(_ context.Context, _ message.ID) (*message.Envelope, bool, error) {
	return f.req, true, nil
}

// fakeStat is the injected obs-read seam (substrate pull-stat stand-in); it reports the
// ids in its set as present with a fixed bind-instant. Stands in for the
// substrate's authoritative presence/uptime view.
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

// TestActorList_TwoAxisNoReadiness proves the composed actor.list directory is
// membership (registry) ∧ presence (lease) and carries NO readiness column —
// readiness is not a substrate axis; whether an actor can service a request is
// the OUTCOME of send→terminal, never projected into this channel-wide view.
func TestActorList_TwoAxisNoReadiness(t *testing.T) {
	reg := fakeRegistry{rows: []storespec.Record{
		{ID: "actor:a", Kind: actor.KindAgent, Binding: actor.BindingEmbedded},
		{ID: "actor:b", Kind: actor.KindAgent, Binding: actor.BindingEmbedded},
	}}
	fc := &fakeWriter{}
	listReq := &message.Envelope{
		ID: "q1", ChannelID: "ch", Kind: message.KindRequest, Type: "actor.list",
		Sender: message.Sender{Kind: actor.KindAgent, ID: "caller"}, Audience: message.Audience{actor.SystemActorID},
	}
	// Presence authority reports actor:a present, actor:b absent — read via the
	// injected seam when composing actor.list (never a message, never truth).
	s := sysactor.New(sysactor.Deps{
		Registry: reg, Writer: fc, Lookup: fakeLookup{req: listReq},
		Stat: fakeStat{present: map[actor.ActorID]bool{"actor:a": true}, started: time.Now()},
	})

	if err := s.Receive(context.Background(), listReq); err != nil {
		t.Fatalf("actor.list: %v", err)
	}
	if len(fc.written) != 1 {
		t.Fatalf("expected 1 response written, got %d", len(fc.written))
	}
	var body struct {
		Actors []map[string]any `json:"actors"`
	}
	if err := json.Unmarshal(fc.written[0].Payload, &body); err != nil {
		t.Fatalf("unmarshal response: %v", err)
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
	// actor:a has a fresh presence lease → present; actor:b never reported → absent.
	if byID["actor:a"]["present"] != true {
		t.Fatalf("actor:a present=%v, want true (fresh lease)", byID["actor:a"]["present"])
	}
	if byID["actor:b"]["present"] != false {
		t.Fatalf("actor:b present=%v, want false (no lease)", byID["actor:b"]["present"])
	}
}
