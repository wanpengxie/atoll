package main

// auth_middleware_test.go covers the requireBearer middleware shape
// AND the two routes it now guards (T107 R2-FIX-1):
//
//   - /api/channel/create  /  /api/channel/list  — bootstrap routes
//     must reject anonymous/wrong-token callers BEFORE invoking the
//     saga (which writes DDL + bootstrap_registry events).
//
//   - /api/rpc/message.send — must reject the same callers BEFORE
//     reading the body or peeking params.channel_id, so an attacker
//     cannot distinguish "channel exists (was: 401)" from "channel
//     missing (was: 404)".

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coagent-ai/daemon-go/internal/bootstrap"
	"github.com/coagent-ai/daemon-go/internal/store"
	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
)

// ---------------------------------------------------------------------------
// requireBearer unit tests
// ---------------------------------------------------------------------------

// TestRequireBearer_MissingHeader_401 asserts a request without any
// Authorization header is rejected with 401 token_required and the
// wrapped handler is never invoked.
func TestRequireBearer_MissingHeader_401(t *testing.T) {
	called := false
	wrapped := requireBearer("daemon-token")(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
	}))

	req := httptest.NewRequest(http.MethodGet, "/anything", nil)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if called {
		t.Fatal("downstream handler was invoked despite missing token")
	}
	if !strings.Contains(rec.Body.String(), "token_required") {
		t.Errorf("body %q lacks token_required reason", rec.Body.String())
	}
}

// TestRequireBearer_MalformedHeader_401 covers Authorization headers
// that are not Bearer-shaped ("Basic ...", empty bearer, etc.).
func TestRequireBearer_MalformedHeader_401(t *testing.T) {
	cases := []struct {
		name   string
		header string
	}{
		{"non_bearer_prefix", "Basic Zm9vOmJhcg=="},
		{"empty_bearer", "Bearer "},
		{"whitespace_bearer", "Bearer    "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			wrapped := requireBearer("daemon-token")(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				called = true
			}))
			req := httptest.NewRequest(http.MethodGet, "/anything", nil)
			req.Header.Set("Authorization", tc.header)
			rec := httptest.NewRecorder()
			wrapped.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", rec.Code)
			}
			if called {
				t.Fatal("downstream handler invoked despite malformed bearer")
			}
		})
	}
}

// TestRequireBearer_WrongToken_401 asserts a Bearer with the wrong
// value is rejected with 401 token_invalid (constant-time compare path).
func TestRequireBearer_WrongToken_401(t *testing.T) {
	called := false
	wrapped := requireBearer("daemon-token")(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
	}))
	req := httptest.NewRequest(http.MethodGet, "/anything", nil)
	req.Header.Set("Authorization", "Bearer attacker-token")
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if called {
		t.Fatal("downstream handler invoked despite wrong token")
	}
	if !strings.Contains(rec.Body.String(), "token_invalid") {
		t.Errorf("body %q lacks token_invalid reason", rec.Body.String())
	}
}

// TestRequireBearer_ValidToken_Passes asserts the wrapped handler runs
// when the bearer matches the configured token. Verifies that
// constant-time compare returns 1 on equal bytes.
func TestRequireBearer_ValidToken_Passes(t *testing.T) {
	called := false
	wrapped := requireBearer("daemon-token")(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusTeapot)
	}))
	req := httptest.NewRequest(http.MethodGet, "/anything", nil)
	req.Header.Set("Authorization", "Bearer daemon-token")
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if !called {
		t.Fatal("downstream handler not invoked despite valid token")
	}
	if rec.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want 418 (downstream)", rec.Code)
	}
}

// TestRequireBearer_EmptyConstructorPanics enforces the composition-root
// safety net — a misconfigured daemon should fail fast, not silently
// accept anonymous callers.
func TestRequireBearer_EmptyConstructorPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("requireBearer(\"\") did not panic")
		}
	}()
	_ = requireBearer("")
}

// ---------------------------------------------------------------------------
// Integration: bootstrap routes are now bearer-gated.
// ---------------------------------------------------------------------------

// boostrapAuthFixture mirrors the server.go wiring shape: a sub-mux
// holds the bootstrap routes, requireBearer wraps the sub-mux, and the
// wrapped handler is mounted on the test mux at both bootstrap paths.
func bootstrapAuthFixture(t *testing.T, token string) (*httptest.Server, string) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	daemonDB, err := store.OpenDaemon(ctx, dir+"/daemon.sqlite", store.OpenOptions{})
	if err != nil {
		t.Fatalf("open daemon sqlite: %v", err)
	}
	t.Cleanup(func() { _ = daemonDB.Close() })

	channelRoot := dir + "/channels"
	saga := bootstrap.New(daemonDB, bootstrap.WithChannelRoot(channelRoot))

	subMux := http.NewServeMux()
	bootstrap.RegisterRoutes(subMux, saga)

	mux := http.NewServeMux()
	guarded := requireBearer(token)(subMux)
	mux.Handle(bootstrap.CreateChannelPath, guarded)
	mux.Handle(bootstrap.ListChannelsPath, guarded)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, channelRoot
}

// TestBootstrapRoutes_AnonGetList_401 — anonymous GET on the list
// endpoint must NOT leak the channel list.
func TestBootstrapRoutes_AnonGetList_401(t *testing.T) {
	srv, _ := bootstrapAuthFixture(t, "daemon-token")

	resp, err := http.Get(srv.URL + bootstrap.ListChannelsPath)
	if err != nil {
		t.Fatalf("anon GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

// TestBootstrapRoutes_AnonPostCreate_401 — anonymous POST on create
// must NOT reach the saga (no DDL, no bootstrap_registry row).
func TestBootstrapRoutes_AnonPostCreate_401(t *testing.T) {
	srv, _ := bootstrapAuthFixture(t, "daemon-token")
	body, _ := json.Marshal(map[string]any{
		"create_request_id": "anon-req",
		"channel_id":        "anon-ch",
	})
	resp, err := http.Post(srv.URL+bootstrap.CreateChannelPath, "application/json",
		bytes.NewReader(body))
	if err != nil {
		t.Fatalf("anon POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

// TestBootstrapRoutes_WrongTokenGetList_401 covers the constant-time
// compare path on the list endpoint.
func TestBootstrapRoutes_WrongTokenGetList_401(t *testing.T) {
	srv, _ := bootstrapAuthFixture(t, "daemon-token")

	req, err := http.NewRequest(http.MethodGet, srv.URL+bootstrap.ListChannelsPath, nil)
	if err != nil {
		t.Fatalf("build req: %v", err)
	}
	req.Header.Set("Authorization", "Bearer attacker")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

// TestBootstrapRoutes_ValidTokenGetList_200 covers the happy path —
// with the bearer, the saga is reachable and returns its empty list.
func TestBootstrapRoutes_ValidTokenGetList_200(t *testing.T) {
	srv, _ := bootstrapAuthFixture(t, "daemon-token")

	req, err := http.NewRequest(http.MethodGet, srv.URL+bootstrap.ListChannelsPath, nil)
	if err != nil {
		t.Fatalf("build req: %v", err)
	}
	req.Header.Set("Authorization", "Bearer daemon-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if strings.TrimSpace(string(body)) != "[]" {
		t.Errorf("body = %q, want []", string(body))
	}
}

// ---------------------------------------------------------------------------
// Integration: message.send no-token requests do not fingerprint
// channel ids. Both existing AND missing channel ids must return 401.
// ---------------------------------------------------------------------------

// messageSendAuthFixture wires the same router shape as server.go: the
// per-channel router built by newMessageSendRouter is wrapped by
// requireBearer. The supplied channel ids populate the router's lookup
// table; zero-valued Deps are fine because the middleware short-circuits
// before the per-channel handler runs.
func messageSendAuthFixture(t *testing.T, token string, channelIDs ...string) *httptest.Server {
	t.Helper()
	runtimes := make([]*channelRuntime, 0, len(channelIDs))
	for _, id := range channelIDs {
		runtimes = append(runtimes, &channelRuntime{
			channelID: id,
			deps:      pkgharness.Deps{},
		})
	}
	inner := newMessageSendRouter(token, runtimes)
	guarded := requireBearer(token)(inner)

	mux := http.NewServeMux()
	mux.Handle("/api/rpc/message.send", guarded)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// TestMessageSend_AnonExistingChannel_401_NoFingerprint asserts an
// anonymous POST targeting an EXISTING channel id returns 401 — same
// status as the missing-channel case below, so a no-token attacker
// cannot distinguish channel existence.
func TestMessageSend_AnonExistingChannel_401_NoFingerprint(t *testing.T) {
	srv := messageSendAuthFixture(t, "daemon-token", "ch-real")
	body, _ := json.Marshal(map[string]any{
		"params": map[string]any{"channel_id": "ch-real"},
	})
	resp, err := http.Post(srv.URL+"/api/rpc/message.send", "application/json",
		bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("existing-channel anon status = %d, want 401", resp.StatusCode)
	}
}

// TestMessageSend_AnonMissingChannel_401_NoFingerprint asserts an
// anonymous POST targeting a NON-EXISTENT channel id returns 401 —
// NOT 404. Pre-fix this leaked channel existence via 404 vs 401.
func TestMessageSend_AnonMissingChannel_401_NoFingerprint(t *testing.T) {
	srv := messageSendAuthFixture(t, "daemon-token", "ch-real")
	body, _ := json.Marshal(map[string]any{
		"params": map[string]any{"channel_id": "ch-ghost"},
	})
	resp, err := http.Post(srv.URL+"/api/rpc/message.send", "application/json",
		bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("missing-channel anon status = %d, want 401 (no 404 leak)", resp.StatusCode)
	}
}

// TestMessageSend_WrongTokenAnyChannel_401_NoFingerprint covers the
// wrong-bearer leg of the same fingerprinting threat model.
func TestMessageSend_WrongTokenAnyChannel_401_NoFingerprint(t *testing.T) {
	srv := messageSendAuthFixture(t, "daemon-token", "ch-real")

	cases := []string{"ch-real", "ch-ghost"}
	for _, chID := range cases {
		t.Run(chID, func(t *testing.T) {
			body, _ := json.Marshal(map[string]any{
				"params": map[string]any{"channel_id": chID},
			})
			req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/rpc/message.send",
				bytes.NewReader(body))
			if err != nil {
				t.Fatalf("build req: %v", err)
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer attacker-token")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("do: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", resp.StatusCode)
			}
		})
	}
}
