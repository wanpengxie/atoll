package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
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

// TestPlanSource_IdRewriteRowSkipped pins the daemon builder-keying fix: a
// constructor that rewrites the id yields a decl the ring could never Lookup (the
// ring keys on the plan InstanceID). Rather than file the factory under the derived
// id (permanent no_builder while Build reported success), the drift row is skipped
// loud — no builder under EITHER id — while a normal row keeps its builder.
func TestPlanSource_IdRewriteRowSkipped(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"assignments":[
			{"instance_id":"agent:ok","class":"test-ok-daemon"},
			{"instance_id":"agent:drift","class":"test-rewrite-id-daemon"}]}`)
	}))
	defer srv.Close()

	serverWS := "ws://" + strings.TrimPrefix(srv.URL, "http://") + "/compute"
	p := newPlanSource(serverWS, "k", "c", "", "dev", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := p.ApplyPlan([]platform.PlanActor{
		{InstanceID: "agent:ok", Class: "test-ok-daemon"},
		{InstanceID: "agent:drift", Class: "test-rewrite-id-daemon"},
	}); err != nil {
		t.Fatalf("ApplyPlan: %v", err)
	}

	desired, err := p.Members(context.Background())
	if err != nil {
		t.Fatalf("Members: %v", err)
	}
	got := map[actor.ActorID]bool{}
	for _, d := range desired {
		got[d.ID] = true
	}
	// Both rows stay in desired (drift is treated as a build failure — no cull).
	if !got["agent:ok"] || !got["agent:drift"] {
		t.Fatalf("desired must contain both rows, got %v", got)
	}
	if _, ok := p.Lookup("agent:ok"); !ok {
		t.Fatal("normal row must have a builder")
	}
	// The drift row has NO builder under its PLAN id (would-be no_builder, retried)...
	if _, ok := p.Lookup("agent:drift"); ok {
		t.Fatal("id-rewrite row must have NO builder under the plan InstanceID")
	}
	// ...and none under the DERIVED id either (never silently filed there).
	if _, ok := p.Lookup("agent:drift:derived"); ok {
		t.Fatal("id-rewrite row must NOT be filed under its derived id (silently-dead entry)")
	}
}

// TestPlanSource_BuildFailureDoesNotCullDesired pins the削臂 fix: a per-row Build
// failure must NOT drop the row from the desired set (which would let computeRing
// cull a still-assigned live cell). Desired is generated from the plan row itself;
// only the BUILDER is absent for the failing row → the ring logs and retries,
// while the buildable row and every other plan member stay desired.
func TestPlanSource_BuildFailureDoesNotCullDesired(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"assignments":[
			{"instance_id":"agent:ok","class":"test-ok-daemon"},
			{"instance_id":"agent:bad","class":"test-fail-daemon"}]}`)
	}))
	defer srv.Close()

	serverWS := "ws://" + strings.TrimPrefix(srv.URL, "http://") + "/compute"
	p := newPlanSource(serverWS, "k", "c", "", "dev", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := p.ApplyPlan([]platform.PlanActor{
		{InstanceID: "agent:ok", Class: "test-ok-daemon"},
		{InstanceID: "agent:bad", Class: "test-fail-daemon"},
	}); err != nil {
		t.Fatalf("ApplyPlan: %v", err)
	}

	desired, err := p.Members(context.Background())
	if err != nil {
		t.Fatalf("Members: %v", err)
	}
	got := map[actor.ActorID]bool{}
	for _, d := range desired {
		got[d.ID] = true
	}
	if !got["agent:ok"] || !got["agent:bad"] {
		t.Fatalf("desired must contain BOTH the buildable and the build-failing row (no cull), got %v", got)
	}
	if _, ok := p.Lookup("agent:ok"); !ok {
		t.Fatal("buildable row must have a builder")
	}
	if _, ok := p.Lookup("agent:bad"); ok {
		t.Fatal("build-failing row must have NO builder (absent from live/missing this round, retried next reconcile), yet stay in desired")
	}
}
