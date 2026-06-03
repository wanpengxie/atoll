package sysactor_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/sysactor"
	rtharness "github.com/wanpengxie/ActOS/runtime/harness"
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

// fakeChain records the system actor's written response.
type fakeChain struct{ written []*message.Envelope }

func (c *fakeChain) Write(_ context.Context, env *message.Envelope) (rtharness.WriteResult, error) {
	c.written = append(c.written, env)
	return rtharness.WriteResult{MessageID: env.ID}, nil
}

// fakeLookup returns the original request so BuildResponseEnvelope can anchor.
type fakeLookup struct{ req *message.Envelope }

func (f fakeLookup) FindByID(_ context.Context, _ message.ID) (*message.Envelope, bool, error) {
	return f.req, true, nil
}

// TestActorList_TwoAxisNoReadiness proves the composed actor.list directory is
// membership (registry) ∧ presence (lease) and carries NO readiness column —
// readiness is not a substrate axis; whether an actor can service a request is
// the OUTCOME of send→terminal, never projected into this channel-wide view.
func TestActorList_TwoAxisNoReadiness(t *testing.T) {
	reg := fakeRegistry{rows: []storespec.Record{
		{ID: "tool:a", Kind: actor.KindTool, Binding: actor.BindingEmbedded},
		{ID: "tool:b", Kind: actor.KindTool, Binding: actor.BindingEmbedded},
	}}
	fc := &fakeChain{}
	listReq := &message.Envelope{
		ID: "q1", ChannelID: "ch", Kind: message.KindRequest, Type: "actor.list",
		Sender: message.Sender{Kind: actor.KindAgent, ID: "caller"}, Audience: message.Audience{actor.SystemActorID},
	}
	s := sysactor.New(sysactor.Deps{
		ChannelID: "ch", Registry: reg, Chain: fc, Lookup: fakeLookup{req: listReq},
	})

	// Feed a presence lease report for tool:a only (Delivered to the cell, never
	// through the truth log).
	if err := s.Receive(context.Background(), sysactor.NewPresenceSignal(sysactor.PresenceReport{
		Actor: "tool:a", Present: true, LeaseTTLMs: 60_000,
	})); err != nil {
		t.Fatalf("presence signal: %v", err)
	}

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
	// tool:a has a fresh presence lease → present; tool:b never reported → absent.
	if byID["tool:a"]["present"] != true {
		t.Fatalf("tool:a present=%v, want true (fresh lease)", byID["tool:a"]["present"])
	}
	if byID["tool:b"]["present"] != false {
		t.Fatalf("tool:b present=%v, want false (no lease)", byID["tool:b"]["present"])
	}
}
