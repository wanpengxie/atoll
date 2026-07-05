package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
