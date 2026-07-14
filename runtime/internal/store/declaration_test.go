package store_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func TestApplyComputeDeclaration_FifteenRowTable(t *testing.T) {
	type tc struct {
		row         int
		variant     string
		composition string // dead, self, other
		registry    string // self, other, inactive
		declared    bool
		indexed     bool
		wantAllow   bool
		wantReject  bool
		wantPort    storespec.DeclarationPortAction
		wantHost    string
		wantActive  bool
	}
	cases := []tc{
		{1, "declared", "dead", "self", true, false, false, true, storespec.DeclarationPortTakeAny, "d", false},
		{1, "missing", "dead", "self", false, false, false, false, storespec.DeclarationPortTakeAny, "d", false},
		{2, "", "dead", "other", true, false, false, true, storespec.DeclarationPortNone, "", true},
		{3, "", "dead", "other", false, true, false, false, storespec.DeclarationPortTakeLink, "", true},
		{4, "", "dead", "inactive", true, false, false, true, storespec.DeclarationPortNone, "", false},
		{5, "", "dead", "inactive", false, true, false, false, storespec.DeclarationPortTakeLink, "", false},
		{6, "", "self", "self", true, false, true, false, storespec.DeclarationPortNone, "d", true},
		{7, "", "self", "self", false, false, false, false, storespec.DeclarationPortTakeLink, "d", true},
		{8, "local", "self", "other", true, false, true, false, storespec.DeclarationPortTakeCurrent, "d", true},
		{8, "remote", "self", "remote", true, false, true, false, storespec.DeclarationPortTakeAny, "d", true},
		{9, "", "self", "other", false, false, false, false, storespec.DeclarationPortNone, "", true},
		{10, "", "self", "inactive", true, false, false, true, storespec.DeclarationPortNone, "", false},
		{11, "", "self", "inactive", false, false, false, false, storespec.DeclarationPortNone, "", false},
		{12, "declared", "other", "self", true, false, false, true, storespec.DeclarationPortTakeAny, "", true},
		{12, "missing", "other", "self", false, false, false, false, storespec.DeclarationPortTakeAny, "", true},
		{13, "", "other", "other", true, false, false, true, storespec.DeclarationPortNone, "", true},
		{14, "", "other", "other", false, false, false, false, storespec.DeclarationPortNone, "", true},
		{15, "declared", "other", "inactive", true, false, false, true, storespec.DeclarationPortNone, "", false},
		{15, "missing", "other", "inactive", false, false, false, false, storespec.DeclarationPortNone, "", false},
	}
	for _, tt := range cases {
		name := fmt.Sprintf("row_%02d", tt.row)
		if tt.variant != "" {
			name += "_" + tt.variant
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			cs := openTestChannel(t)
			var id actor.ActorID
			if tt.composition == "dead" {
				var err error
				id, err = cs.Membership.Admit(ctx, actor.KindTool, fmt.Sprintf("row-%d", tt.row), int64(100+tt.row))
				if err != nil {
					t.Fatal(err)
				}
			} else {
				placement, desired := storespec.PlacementServer, ""
				if tt.composition == "self" {
					placement, desired = storespec.PlacementDaemon, "d"
				}
				r, _, _, err := cs.Composition.IntroduceComposition(ctx, storespec.CompositionIntroduce{
					DeclID: fmt.Sprintf("decl-%d", tt.row), Principal: fmt.Sprintf("row-%d", tt.row), Class: "tool.test",
					Placement: placement, DesiredHost: desired, Kind: actor.KindTool, At: int64(100 + tt.row),
				})
				if err != nil {
					t.Fatal(err)
				}
				id = r.InstanceID
			}
			switch tt.registry {
			case "self":
				if err := cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{{ID: id, Kind: actor.KindTool, Binding: actor.BindingRuntimeInboundViaRelay, Host: "d", At: 500}}, nil); err != nil {
					t.Fatal(err)
				}
			case "remote":
				if err := cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{{ID: id, Kind: actor.KindTool, Binding: actor.BindingRuntimeInboundViaRelay, Host: "other", At: 500}}, nil); err != nil {
					t.Fatal(err)
				}
			case "inactive":
				if err := cs.Membership.Deregister(ctx, id, 500); err != nil {
					t.Fatal(err)
				}
			}
			in := storespec.ComputeDeclarationInput{DaemonID: "d", At: 900}
			if tt.declared {
				in.Declared = []storespec.ComputeDeclaration{{ActorID: id, Kind: actor.KindTool, Binding: actor.BindingRuntimeInboundViaRelay}}
			}
			if tt.indexed {
				in.IndexedIDs = []actor.ActorID{id}
			}
			var callback []storespec.DeclarationDecision
			result, err := cs.Composition.ApplyComputeDeclaration(ctx, in, func(ds []storespec.DeclarationDecision) error {
				callback = append(callback, ds...)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			find := func(ds []storespec.DeclarationDecision) storespec.DeclarationDecision {
				for _, d := range ds {
					if d.ActorID == id {
						return d
					}
				}
				t.Fatalf("target decision missing: %+v", ds)
				return storespec.DeclarationDecision{}
			}
			got, before := find(result.Decisions), find(callback)
			if got != before {
				t.Fatalf("callback/result differ: callback=%+v result=%+v", before, got)
			}
			if got.Allow != tt.wantAllow || got.Rejected != tt.wantReject || got.PortAction != tt.wantPort {
				t.Fatalf("decision=%+v want allow=%v reject=%v port=%v", got, tt.wantAllow, tt.wantReject, tt.wantPort)
			}
			rec, ok, err := cs.Registry.Lookup(ctx, id)
			if err != nil || !ok {
				t.Fatalf("registry lookup ok=%v err=%v", ok, err)
			}
			if rec.IsActive() != tt.wantActive || rec.Host != tt.wantHost {
				t.Fatalf("registry=%+v want active=%v host=%q", rec, tt.wantActive, tt.wantHost)
			}
		})
	}
}

func TestApplyComputeDeclaration_MetadataMismatchDoesNotCancelDeathAction(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	id, err := cs.Membership.Admit(ctx, actor.KindTool, "dead-priority", 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{{ID: id, Kind: actor.KindTool, Binding: actor.BindingRuntimeInboundViaRelay, Host: "d", At: 101}}, nil); err != nil {
		t.Fatal(err)
	}
	result, err := cs.Composition.ApplyComputeDeclaration(ctx, storespec.ComputeDeclarationInput{
		DaemonID: "d", At: 200,
		Declared: []storespec.ComputeDeclaration{{ActorID: id, Kind: actor.KindAgent, Binding: actor.BindingEmbedded, Epoch: 99}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Decisions) != 1 || !result.Decisions[0].Rejected || result.Decisions[0].PortAction != storespec.DeclarationPortTakeAny {
		t.Fatalf("decision=%+v", result.Decisions)
	}
	rec, _, _ := cs.Registry.Lookup(ctx, id)
	if rec.IsActive() {
		t.Fatal("metadata mismatch incorrectly cancelled composition-death deregistration")
	}
}

func TestApplyComputeDeclaration_Row6MetadataMismatchesAreIndependentRejects(t *testing.T) {
	mutations := map[string]func(*storespec.ComputeDeclaration){
		"kind":    func(d *storespec.ComputeDeclaration) { d.Kind = actor.KindAgent },
		"binding": func(d *storespec.ComputeDeclaration) { d.Binding = actor.BindingEmbedded },
		"epoch":   func(d *storespec.ComputeDeclaration) { d.Epoch = 1 },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			cs := openTestChannel(t)
			r, _, _, err := cs.Composition.IntroduceComposition(ctx, storespec.CompositionIntroduce{
				DeclID: name, Principal: name, Class: "tool.test", Placement: storespec.PlacementDaemon,
				DesiredHost: "d", Kind: actor.KindTool, At: 100,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{{ID: r.InstanceID, Kind: actor.KindTool, Binding: actor.BindingRuntimeInboundViaRelay, Host: "d", At: 101}}, nil); err != nil {
				t.Fatal(err)
			}
			decl := storespec.ComputeDeclaration{ActorID: r.InstanceID, Kind: actor.KindTool, Binding: actor.BindingRuntimeInboundViaRelay}
			mutate(&decl)
			result, err := cs.Composition.ApplyComputeDeclaration(ctx, storespec.ComputeDeclarationInput{DaemonID: "d", Declared: []storespec.ComputeDeclaration{decl}, At: 200}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Decisions) != 1 || result.Decisions[0].Allow || !result.Decisions[0].Rejected || result.Decisions[0].PortAction != storespec.DeclarationPortTakeAny {
				t.Fatalf("decision=%+v", result.Decisions)
			}
			rec, _, _ := cs.Registry.Lookup(ctx, r.InstanceID)
			if !rec.IsActive() || rec.Host != "d" {
				t.Fatalf("row6 metadata mismatch changed DB action: %+v", rec)
			}
		})
	}
}
