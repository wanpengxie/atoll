package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

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
}

// TestPlanURLFromWS pins the ws→http(s) + /compute/plan derivation the daemon
// uses to pull its assignment.
func TestPlanURLFromWS(t *testing.T) {
	got, err := planURLFromWS("ws://localhost:8080/compute", "k1", "c1")
	if err != nil {
		t.Fatal(err)
	}
	// url.Values.Encode sorts keys: channel, key.
	if want := "http://localhost:8080/compute/plan?channel=c1&key=k1"; got != want {
		t.Fatalf("planURLFromWS = %s, want %s", got, want)
	}
	got, err = planURLFromWS("wss://host/compute", "k", "c")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, "https://host/compute/plan") {
		t.Fatalf("wss must map to https: %s", got)
	}
}

// TestFetchPlan: the daemon pulls its assignment and decodes it (the set it will
// build — no blind-build).
func TestFetchPlan(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/compute/plan" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.URL.Query().Get("key") != "k1" || r.URL.Query().Get("channel") != "c1" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"assignments":[{"instance_id":"agent:rev","class":"claude","config":{"model":"x"}}]}`)
	}))
	defer srv.Close()

	serverWS := "ws://" + strings.TrimPrefix(srv.URL, "http://") + "/compute"
	plan, err := fetchPlan(context.Background(), serverWS, "k1", "c1")
	if err != nil {
		t.Fatalf("fetchPlan: %v", err)
	}
	if len(plan) != 1 || plan[0].InstanceID != "agent:rev" || plan[0].Class != "claude" {
		t.Fatalf("plan = %+v", plan)
	}
	if string(plan[0].Config) == "" {
		t.Fatalf("plan config should carry the resolved config blob")
	}
}

// TestFetchPlan_Non200 surfaces an auth/server error instead of silently running
// an empty plan.
func TestFetchPlan_Non200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()
	serverWS := "ws://" + strings.TrimPrefix(srv.URL, "http://") + "/compute"
	if _, err := fetchPlan(context.Background(), serverWS, "bad", "c1"); err == nil {
		t.Fatal("non-200 plan fetch should error")
	}
}

// TestPlanSource_BuildFailureDoesNotCullDesired pins the削臂 fix: a per-row Build
// failure must NOT drop the row from the desired set (which would let computeRing
// cull a still-assigned live cell). Desired is generated from the plan row itself;
// only the BUILDER is absent for the failing row → the ring records it infeasible
// and retries, while the buildable row and every other plan member stay desired.
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
		t.Fatal("build-failing row must have NO builder (infeasible → retried), yet stay in desired")
	}
}
