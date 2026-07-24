package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/compute"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
	"github.com/wanpengxie/atoll/runtime/actorhost"
)

func TestStorageRootCloseDecisionTransfersOnForwarderLeak(t *testing.T) {
	if shouldCloseStorageRoot(errors.Join(errors.New("other"), compute.ErrForwardersLeaked)) {
		t.Fatal("forwarder leak must transfer Root ownership to process exit")
	}
	if !shouldCloseStorageRoot(errors.New("ordinary failure")) {
		t.Fatal("ordinary failure must still close Root")
	}
}

// Test-only classes: one that builds, one whose constructor always errors. They
// let TestPlanSource_BuildFailureDoesNotCullDesired drive a per-row Build failure
// deterministically (real classes need creds/config to fail).
func init() {
	registry.Register("test-ok-daemon", registry.ClassDecl{
		Kind: actor.KindAgent,
		New: func(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
			return platform.ActorDecl{ID: spec.ID, Kind: actor.KindAgent}, nil
		},
	})
	registry.Register("test-fail-daemon", registry.ClassDecl{
		Kind: actor.KindAgent,
		New: func(registry.InstanceSpec, registry.Deps) (platform.ActorDecl, error) {
			return platform.ActorDecl{}, fmt.Errorf("forced build failure")
		},
	})
	// A constructor that REWRITES the id (like device deriving its id from the
	// device identity, ignoring spec.ID) — used to prove the builder is keyed on the
	// plan InstanceID, not the built decl.ID, so a drift is caught as no_builder
	// rather than filed under an unreachable derived id.
	registry.Register("test-rewrite-id-daemon", registry.ClassDecl{
		Kind: actor.KindAgent,
		New: func(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
			return platform.ActorDecl{ID: spec.ID + ":derived", Kind: actor.KindAgent}, nil
		},
	})
}

func TestPlanSource_InvalidCandidatePreservesLastKnownGood(t *testing.T) {
	cases := []struct {
		name string
		bad  platform.PlanActor
	}{
		{name: "unknown class", bad: platform.PlanActor{ActorID: "agent:bad", Class: "not-registered"}},
		{name: "build failure", bad: platform.PlanActor{ActorID: "agent:bad", Class: "test-fail-daemon"}},
		{name: "id rewrite", bad: platform.PlanActor{ActorID: "agent:bad", Class: "test-rewrite-id-daemon"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newPlanSource("c", "", "dev", slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err := p.ApplyPlan([]platform.PlanActor{{ActorID: "agent:stable", Class: "test-ok-daemon"}}); err != nil {
				t.Fatalf("seed LKG: %v", err)
			}
			if err := p.ApplyPlan([]platform.PlanActor{
				{ActorID: "agent:new", Class: "test-ok-daemon"}, tc.bad,
			}); err == nil {
				t.Fatal("invalid candidate plan unexpectedly published")
			}

			desired, err := p.Members(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(desired) != 1 || desired[0].ActorID != "agent:stable" {
				t.Fatalf("LKG desired changed after rejected plan: %+v", desired)
			}
			stable := desired[0]
			if _, ok := p.LookupExact(stable.ActorID, stable.AttemptKey, stable.ExecutionSpec); !ok {
				t.Fatal("LKG builder disappeared after rejected plan")
			}
			if _, ok := p.LookupExact(
				"agent:new",
				actorhost.AttemptKey("00000000-0000-7000-8000-000000000003"),
				actorhost.ExecutionSpec{Kind: actor.KindAgent, Class: "test-ok-daemon"},
			); ok {
				t.Fatal("partial candidate builder leaked into LKG")
			}
		})
	}
}

func TestPlanSourceLookupExactRejectsSuccessorFactoryForStaleBuild(t *testing.T) {
	p := newPlanSource("c", "", "dev", slog.New(slog.NewTextHandler(io.Discard, nil)))
	g1 := actorhost.AttemptKey("00000000-0000-7000-8000-000000000001")
	g2 := actorhost.AttemptKey("00000000-0000-7000-8000-000000000002")
	if err := p.ApplyPlan([]platform.PlanActor{{
		ActorID: "agent:a", AttemptKey: g1, Class: "test-ok-daemon",
		Config: []byte(`{"generation":1}`),
	}}); err != nil {
		t.Fatal(err)
	}
	first, err := p.Members(context.Background())
	if err != nil || len(first) != 1 {
		t.Fatalf("first desired = %#v, %v", first, err)
	}
	g1Spec := first[0].ExecutionSpec

	if err := p.ApplyPlan([]platform.PlanActor{{
		ActorID: "agent:a", AttemptKey: g2, Class: "test-ok-daemon",
		Config: []byte(`{"generation":2}`),
	}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.LookupExact("agent:a", g1, g1Spec); ok {
		t.Fatal("stale G1 build acquired the current G2 factory")
	}
	current, err := p.Members(context.Background())
	if err != nil || len(current) != 1 {
		t.Fatalf("current desired = %#v, %v", current, err)
	}
	if _, ok := p.LookupExact(
		current[0].ActorID,
		current[0].AttemptKey,
		current[0].ExecutionSpec,
	); !ok {
		t.Fatal("current G2 build could not acquire its exact factory")
	}
	if _, ok := p.LookupExact("agent:a", g2, g1Spec); ok {
		t.Fatal("current G2 attempt acquired a factory for mismatched G1 execution spec")
	}
}
