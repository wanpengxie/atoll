package main

import (
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

// okRow is a plan row the way home actually sends one: the Kind travels on the
// wire, carried out of the Controller's desired projection (platform/home/plan.go).
// The daemon adopts it rather than deriving its own from the class registry —
// a second derivation would meet the first at LookupExact's Equal with nothing
// to reconcile them.
func okRow(id actor.ActorID, key actorhost.AttemptKey, config []byte) platform.PlanActor {
	return platform.PlanActor{
		ActorID: id, AttemptKey: key,
		Kind: actor.KindAgent, Class: "test-ok-daemon", Config: config,
	}
}

// specOf rebuilds the ExecutionSpec a plan row produces. The daemon publishes no
// desired read face — the Host holds the only one — so a test that wants to ask
// LookupExact about a row states the row's own spec, exactly as the Host does.
func specOf(row platform.PlanActor) actorhost.ExecutionSpec {
	return actorhost.ExecutionSpec{Kind: row.Kind, Class: row.Class, Config: row.Config}
}

func TestPlanSource_InvalidCandidatePreservesLastKnownGood(t *testing.T) {
	cases := []struct {
		name string
		bad  platform.PlanActor
	}{
		{name: "unknown class", bad: platform.PlanActor{
			ActorID: "agent:bad", Kind: actor.KindAgent, Class: "not-registered"}},
		{name: "build failure", bad: platform.PlanActor{
			ActorID: "agent:bad", Kind: actor.KindAgent, Class: "test-fail-daemon"}},
		{name: "id rewrite", bad: platform.PlanActor{
			ActorID: "agent:bad", Kind: actor.KindAgent, Class: "test-rewrite-id-daemon"}},
		// A row whose Kind the wire never filled in. Adopting the row's Kind means
		// an unparseable one has to be refused HERE: ExecutionSpec canonicalization
		// happens inside Equal, which reports failure as "not a match", so letting
		// it through would publish a builder that can never be looked up — the
		// silent no_builder loop, reached by a different door.
		{name: "invalid kind", bad: platform.PlanActor{
			ActorID: "agent:bad", Kind: actor.Kind("not-a-kind"), Class: "test-ok-daemon"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newPlanSource("c", "", "dev", slog.New(slog.NewTextHandler(io.Discard, nil)))
			stable := okRow("agent:stable", "", nil)
			if err := p.ApplyPlan([]platform.PlanActor{stable}); err != nil {
				t.Fatalf("seed LKG: %v", err)
			}
			if err := p.ApplyPlan([]platform.PlanActor{
				okRow("agent:new", "", nil), tc.bad,
			}); err == nil {
				t.Fatal("invalid candidate plan unexpectedly published")
			}

			if _, ok := p.LookupExact(stable.ActorID, stable.AttemptKey, specOf(stable)); !ok {
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

// The Kind the daemon files a builder under is the row's, not one it looked up
// for itself. Pinning this is the whole point of A14: the Host resolves a build
// claim by handing LookupExact the spec IT holds, which came off the same wire
// row, and the two only agree unconditionally while there is one derivation.
func TestPlanSourceFilesTheBuilderUnderTheRowsKind(t *testing.T) {
	p := newPlanSource("c", "", "dev", slog.New(slog.NewTextHandler(io.Discard, nil)))
	key := actorhost.AttemptKey("00000000-0000-7000-8000-000000000001")
	// test-ok-daemon is registered as KindAgent. A row that says KindTool is what
	// a version skew looks like — the class was reclassified on one side. The
	// builder must be reachable by what the row said, since that is also what the
	// Host will carry into its claim.
	row := platform.PlanActor{
		ActorID: "agent:a", AttemptKey: key,
		Kind: actor.KindTool, Class: "test-ok-daemon",
	}
	if err := p.ApplyPlan([]platform.PlanActor{row}); err != nil {
		t.Fatalf("ApplyPlan: %v", err)
	}
	if _, ok := p.LookupExact(row.ActorID, key, specOf(row)); !ok {
		t.Fatal("the builder is not reachable by the Kind the plan row carried")
	}
	if _, ok := p.LookupExact(row.ActorID, key, actorhost.ExecutionSpec{
		Kind: actor.KindAgent, Class: "test-ok-daemon",
	}); ok {
		t.Fatal("the registry's own Kind still resolves a builder — a second derivation survives")
	}
}

func TestPlanSourceLookupExactRejectsSuccessorFactoryForStaleBuild(t *testing.T) {
	p := newPlanSource("c", "", "dev", slog.New(slog.NewTextHandler(io.Discard, nil)))
	g1 := actorhost.AttemptKey("00000000-0000-7000-8000-000000000001")
	g2 := actorhost.AttemptKey("00000000-0000-7000-8000-000000000002")
	g1Row := okRow("agent:a", g1, []byte(`{"generation":1}`))
	if err := p.ApplyPlan([]platform.PlanActor{g1Row}); err != nil {
		t.Fatal(err)
	}
	g1Spec := specOf(g1Row)

	g2Row := okRow("agent:a", g2, []byte(`{"generation":2}`))
	if err := p.ApplyPlan([]platform.PlanActor{g2Row}); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.LookupExact("agent:a", g1, g1Spec); ok {
		t.Fatal("stale G1 build acquired the current G2 factory")
	}
	if _, ok := p.LookupExact(g2Row.ActorID, g2, specOf(g2Row)); !ok {
		t.Fatal("current G2 build could not acquire its exact factory")
	}
	if _, ok := p.LookupExact("agent:a", g2, g1Spec); ok {
		t.Fatal("current G2 attempt acquired a factory for mismatched G1 execution spec")
	}
}
