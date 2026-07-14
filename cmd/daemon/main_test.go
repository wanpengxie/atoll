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
		{name: "unknown class", bad: platform.PlanActor{InstanceID: "agent:bad", Class: "not-registered"}},
		{name: "build failure", bad: platform.PlanActor{InstanceID: "agent:bad", Class: "test-fail-daemon"}},
		{name: "id rewrite", bad: platform.PlanActor{InstanceID: "agent:bad", Class: "test-rewrite-id-daemon"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newPlanSource("c", "", "dev", slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err := p.ApplyPlan([]platform.PlanActor{{InstanceID: "agent:stable", Class: "test-ok-daemon"}}); err != nil {
				t.Fatalf("seed LKG: %v", err)
			}
			if err := p.ApplyPlan([]platform.PlanActor{
				{InstanceID: "agent:new", Class: "test-ok-daemon"}, tc.bad,
			}); err == nil {
				t.Fatal("invalid candidate plan unexpectedly published")
			}

			desired, err := p.Members(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(desired) != 1 || desired[0].ID != "agent:stable" {
				t.Fatalf("LKG desired changed after rejected plan: %+v", desired)
			}
			if _, ok := p.Lookup("agent:stable"); !ok {
				t.Fatal("LKG builder disappeared after rejected plan")
			}
			if _, ok := p.Lookup("agent:new"); ok {
				t.Fatal("partial candidate builder leaked into LKG")
			}
		})
	}
}
