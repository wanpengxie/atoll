package sysactor

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type createFacts map[actor.ActorID]storespec.ActorFacts

func (f createFacts) ActorFacts(_ context.Context, id actor.ActorID) (storespec.ActorFacts, bool, error) {
	facts, ok := f[id]
	return facts, ok, nil
}

func TestResolveChannelCreateDistinguishesMalformedAndNonMemberActorIDs(t *testing.T) {
	s := New(Deps{ActorFacts: createFacts{}})
	for _, tc := range []struct {
		name string
		raw  string
		code string
	}{
		{name: "missing explicit list", raw: `{"name":"child","recipe":{}}`, code: "invalid_args"},
		{name: "bare name", raw: `{"name":"child","recipe":{},"initial_actor_ids":["root"]}`, code: "actor_id_invalid"},
		{name: "well formed foreign or absent id", raw: `{"name":"child","recipe":{},"initial_actor_ids":["human:root:1"]}`, code: "actor_not_in_channel"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, got := s.resolveChannelCreate(context.Background(), json.RawMessage(tc.raw))
			if got == nil || got.code != tc.code {
				t.Fatalf("error=%+v, want code %q", got, tc.code)
			}
		})
	}
}

func TestResolveChannelCreateProducesTypedTrustedSeats(t *testing.T) {
	s := New(Deps{ActorFacts: createFacts{
		"human:root:1":    {Kind: actor.KindHuman, Principal: "root"},
		"agent:steward:2": {Kind: actor.KindAgent, Principal: "steward", SourceDeclID: "decl-steward"},
	}})
	raw, createErr := s.resolveChannelCreate(context.Background(), json.RawMessage(`{
		"name":"child",
		"recipe":{},
		"initial_actor_ids":["human:root:1","agent:steward:2"]
	}`))
	if createErr != nil {
		t.Fatal(createErr.detail)
	}
	var got lagoon.ResolvedChannelCreate
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.InitialSeats) != 2 {
		t.Fatalf("initial seats=%+v", got.InitialSeats)
	}
	if got.InitialSeats[0].Kind != actor.KindHuman || got.InitialSeats[0].Principal != "root" {
		t.Fatalf("human seat=%+v", got.InitialSeats[0])
	}
	if got.InitialSeats[1].Kind != actor.KindAgent || got.InitialSeats[1].Principal != "steward" || got.InitialSeats[1].DeclID != "decl-steward" {
		t.Fatalf("agent seat=%+v", got.InitialSeats[1])
	}
}

// An ordinary seated agent (the member.create birth) carries no explicit
// principal: its seat intent travels by declaration alone, exactly like a
// recipe seat. Only a trusted identity carry (the steward case above) brings
// a principal across.
func TestResolveChannelCreateCarriesDeclarationOnlyForOrdinaryAgents(t *testing.T) {
	s := New(Deps{ActorFacts: createFacts{
		"human:root:1":  {Kind: actor.KindHuman, Principal: "root"},
		"agent:codex:3": {Kind: actor.KindAgent, SourceDeclID: "codex"},
	}})
	raw, createErr := s.resolveChannelCreate(context.Background(), json.RawMessage(`{
		"name":"child",
		"recipe":{},
		"initial_actor_ids":["human:root:1","agent:codex:3"]
	}`))
	if createErr != nil {
		t.Fatal(createErr.detail)
	}
	var got lagoon.ResolvedChannelCreate
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.InitialSeats) != 2 {
		t.Fatalf("initial seats=%+v", got.InitialSeats)
	}
	if seat := got.InitialSeats[1]; seat.Kind != actor.KindAgent || seat.Principal != "" || seat.DeclID != "codex" || seat.SourceActorID != "agent:codex:3" {
		t.Fatalf("agent seat=%+v", seat)
	}
}

func TestResolveChannelCreateRejectsPlatformManagedKindsAndDuplicates(t *testing.T) {
	s := New(Deps{ActorFacts: createFacts{
		"peer:child:1": {Kind: actor.KindPeer, SourceDeclID: "child"},
		"human:root:1": {Kind: actor.KindHuman, Principal: "root"},
	}})
	for _, tc := range []struct {
		raw  string
		code string
	}{
		{raw: `{"name":"child","recipe":{},"initial_actor_ids":["peer:child:1"]}`, code: "actor_kind_not_importable"},
		{raw: `{"name":"child","recipe":{},"initial_actor_ids":["human:root:1","human:root:1"]}`, code: "duplicate_actor_id"},
	} {
		_, got := s.resolveChannelCreate(context.Background(), json.RawMessage(tc.raw))
		if got == nil || got.code != tc.code {
			t.Fatalf("error=%+v, want code %q", got, tc.code)
		}
	}
}
