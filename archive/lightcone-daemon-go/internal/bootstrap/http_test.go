package bootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// httpEnv wires a Saga + httptest.Server with both routes mounted.
func httpEnv(t *testing.T, opts ...Option) (*httptest.Server, *Saga, string) {
	t.Helper()
	saga, _, workRoot := newSaga(t, opts...)
	mux := http.NewServeMux()
	RegisterRoutes(mux, saga)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, saga, workRoot
}

// postJSON is a small helper that POSTs raw JSON to the test server and
// returns the parsed response body + status code.
func postJSON(t *testing.T, srv *httptest.Server, path string, body any) (int, map[string]any) {
	t.Helper()
	buf, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp, err := http.Post(srv.URL+path, "application/json", bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	var parsed map[string]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &parsed); err != nil {
			t.Fatalf("response not JSON (status=%d, body=%q): %v",
				resp.StatusCode, raw, err)
		}
	}
	return resp.StatusCode, parsed
}

// ---------------------------------------------------------------------------
// HTTP happy path — 200 + Result body.
// ---------------------------------------------------------------------------

func TestHTTP_CreateChannel_HappyPath(t *testing.T) {
	srv, _, workRoot := httpEnv(t)
	p := happyParams(t, workRoot, "req-http", "ch-http")

	status, body := postJSON(t, srv, CreateChannelPath, p)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%v", status, body)
	}
	if body["channel_id"] != "ch-http" {
		t.Errorf("channel_id = %v, want ch-http", body["channel_id"])
	}
	if body["status"] != StatusCompleted {
		t.Errorf("status field = %v, want completed", body["status"])
	}
}

// ---------------------------------------------------------------------------
// HTTP idempotency — second call returns 200 (saga sees completed).
// ---------------------------------------------------------------------------

func TestHTTP_CreateChannel_Idempotent200(t *testing.T) {
	srv, _, workRoot := httpEnv(t)
	p := happyParams(t, workRoot, "req-idem", "ch-idem")

	for i := 0; i < 3; i++ {
		status, body := postJSON(t, srv, CreateChannelPath, p)
		if status != http.StatusOK {
			t.Fatalf("iteration %d status = %d body=%v", i, status, body)
		}
		if body["status"] != StatusCompleted {
			t.Errorf("iteration %d status field = %v", i, body["status"])
		}
	}
}

// ---------------------------------------------------------------------------
// HTTP 409 for in_progress — synthesise the registry state and POST.
// ---------------------------------------------------------------------------

func TestHTTP_CreateChannel_InProgress_409(t *testing.T) {
	srv, saga, _ := httpEnv(t)
	if _, err := saga.daemonDB.ExecContext(context.Background(),
		`INSERT INTO bootstrap_registry (create_request_id, channel_id, status, workdir_path, started_at)
		 VALUES ('req-ip', 'ch-ip', 'in_progress', '/tmp/ip', 1)`,
	); err != nil {
		t.Fatalf("seed: %v", err)
	}

	status, body := postJSON(t, srv, CreateChannelPath, CreateParams{
		CreateRequestID: "req-ip",
		ChannelID:       "ch-ip",
	})
	if status != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%v", status, body)
	}
	if body["reason"] != "bootstrap_in_progress" {
		t.Errorf("reason = %v, want bootstrap_in_progress", body["reason"])
	}
	if body["channel_id"] != "ch-ip" {
		t.Errorf("channel_id = %v, want ch-ip", body["channel_id"])
	}
}

// ---------------------------------------------------------------------------
// HTTP 409 for rolled_back — caller MUST switch id (spec L2 §1.4.7).
// ---------------------------------------------------------------------------

func TestHTTP_CreateChannel_RolledBack_409(t *testing.T) {
	srv, saga, _ := httpEnv(t)
	if _, err := saga.daemonDB.ExecContext(context.Background(),
		`INSERT INTO bootstrap_registry (create_request_id, channel_id, status, workdir_path, started_at, rollback_reason)
		 VALUES ('req-rb', 'ch-rb', 'rolled_back', '/tmp/rb', 1, 'mock')`,
	); err != nil {
		t.Fatalf("seed: %v", err)
	}

	status, body := postJSON(t, srv, CreateChannelPath, CreateParams{
		CreateRequestID: "req-rb",
		ChannelID:       "ch-rb",
	})
	if status != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%v", status, body)
	}
	if body["reason"] != "bootstrap_rolled_back" {
		t.Errorf("reason = %v, want bootstrap_rolled_back", body["reason"])
	}
}

// ---------------------------------------------------------------------------
// HTTP 400 for invalid params — missing channel_id.
// ---------------------------------------------------------------------------

func TestHTTP_CreateChannel_ParamsInvalid_400(t *testing.T) {
	srv, _, _ := httpEnv(t)
	status, body := postJSON(t, srv, CreateChannelPath, map[string]any{
		"create_request_id": "req-bad",
		"workdir_path":      "/tmp/bad",
	})
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%v", status, body)
	}
	if body["reason"] != "params_invalid" {
		t.Errorf("reason = %v, want params_invalid", body["reason"])
	}
}

// ---------------------------------------------------------------------------
// HTTP 405 for non-POST.
// ---------------------------------------------------------------------------

func TestHTTP_CreateChannel_MethodNotAllowed_405(t *testing.T) {
	srv, _, _ := httpEnv(t)
	resp, err := http.Get(srv.URL + CreateChannelPath)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// HTTP 415 for non-JSON Content-Type.
// ---------------------------------------------------------------------------

func TestHTTP_CreateChannel_UnsupportedMediaType_415(t *testing.T) {
	srv, _, _ := httpEnv(t)
	resp, err := http.Post(srv.URL+CreateChannelPath, "text/plain",
		strings.NewReader("hello"))
	if err != nil {
		t.Fatalf("POST text/plain: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Errorf("status = %d, want 415", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// HTTP 400 for malformed JSON.
// ---------------------------------------------------------------------------

func TestHTTP_CreateChannel_MalformedJSON_400(t *testing.T) {
	srv, _, _ := httpEnv(t)
	resp, err := http.Post(srv.URL+CreateChannelPath, "application/json",
		strings.NewReader("{not json"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// HTTP list endpoint — completed rows visible as JSON array.
// ---------------------------------------------------------------------------

func TestHTTP_ListChannels_HappyPath(t *testing.T) {
	srv, _, workRoot := httpEnv(t)
	// Create two channels then call list.
	for _, ch := range []string{"ch-l1", "ch-l2"} {
		p := happyParams(t, workRoot, "req-"+ch, ch)
		if status, _ := postJSON(t, srv, CreateChannelPath, p); status != 200 {
			t.Fatalf("seed %s: status %d", ch, status)
		}
	}

	resp, err := http.Get(srv.URL + ListChannelsPath)
	if err != nil {
		t.Fatalf("GET list: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	var list []ChannelInfo
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatalf("decode list: %v body=%s", err, body)
	}
	if len(list) != 2 {
		t.Errorf("len = %d, want 2; body=%s", len(list), body)
	}
}

// ---------------------------------------------------------------------------
// HTTP list endpoint — empty result returns `[]` not `null`.
// ---------------------------------------------------------------------------

func TestHTTP_ListChannels_EmptyArray(t *testing.T) {
	srv, _, _ := httpEnv(t)
	resp, err := http.Get(srv.URL + ListChannelsPath)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if strings.TrimSpace(string(body)) != "[]" {
		t.Errorf("body = %q, want []", body)
	}
}

// ---------------------------------------------------------------------------
// HTTP list endpoint — method not allowed.
// ---------------------------------------------------------------------------

func TestHTTP_ListChannels_PostRejected(t *testing.T) {
	srv, _, _ := httpEnv(t)
	resp, err := http.Post(srv.URL+ListChannelsPath, "application/json",
		bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// HTTP bearer-gate composition (T107 R2-FIX-1).
//
// The bootstrap package handlers themselves do not enforce auth — that
// is the daemon composition root's job (cmd/daemon wraps these routes
// with requireBearer). These tests pin the contract that, once wrapped,
// the routes reject missing/wrong tokens with 401 and accept valid
// ones unchanged.
//
// The wrapper is reimplemented locally to keep this test file from
// depending on cmd/daemon. The shape mirrors cmd/daemon/auth_middleware.go.
// ---------------------------------------------------------------------------

// testBearerGate is a minimal bearer middleware kept local to this test
// file so internal/bootstrap does not gain a reverse dependency on
// cmd/daemon. The shape mirrors requireBearer in cmd/daemon/auth_middleware.go.
func testBearerGate(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(auth, prefix) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"reason":"token_required"}}`))
			return
		}
		tok := strings.TrimSpace(strings.TrimPrefix(auth, prefix))
		if tok != token {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"reason":"token_invalid"}}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// httpEnvBearer wires the same Saga + RegisterRoutes layout as
// httpEnv, then composes the testBearerGate in front of the sub-mux,
// matching the cmd/daemon composition root's shape.
func httpEnvBearer(t *testing.T, token string) (*httptest.Server, *Saga, string) {
	t.Helper()
	saga, _, workRoot := newSaga(t)
	sub := http.NewServeMux()
	RegisterRoutes(sub, saga)

	mux := http.NewServeMux()
	gated := testBearerGate(token, sub)
	mux.Handle(CreateChannelPath, gated)
	mux.Handle(ListChannelsPath, gated)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, saga, workRoot
}

// TestRegisterRoutes_RequiresAuth_MissingToken_401 — anonymous callers
// MUST be rejected before the saga is invoked (no DDL, no
// bootstrap_registry write).
func TestRegisterRoutes_RequiresAuth_MissingToken_401(t *testing.T) {
	srv, _, _ := httpEnvBearer(t, "daemon-token")

	resp, err := http.Get(srv.URL + ListChannelsPath)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anon list status = %d, want 401", resp.StatusCode)
	}

	resp2, err := http.Post(srv.URL+CreateChannelPath, "application/json",
		bytes.NewReader([]byte(`{"create_request_id":"x","channel_id":"y"}`)))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anon create status = %d, want 401", resp2.StatusCode)
	}
}

// TestRegisterRoutes_RequiresAuth_WrongToken_401 — a Bearer that does
// not match the daemon token is treated identically to no token.
func TestRegisterRoutes_RequiresAuth_WrongToken_401(t *testing.T) {
	srv, _, _ := httpEnvBearer(t, "daemon-token")

	req, _ := http.NewRequest(http.MethodGet, srv.URL+ListChannelsPath, nil)
	req.Header.Set("Authorization", "Bearer attacker-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong-token status = %d, want 401", resp.StatusCode)
	}
}

// TestRegisterRoutes_RequiresAuth_ValidToken_200 — the gated routes
// still serve their pre-fix happy path when the bearer matches. List
// returns the empty array literal; create returns the saga Result.
func TestRegisterRoutes_RequiresAuth_ValidToken_200(t *testing.T) {
	srv, _, workRoot := httpEnvBearer(t, "daemon-token")

	// GET /api/channel/list with valid bearer → 200 + "[]".
	req, _ := http.NewRequest(http.MethodGet, srv.URL+ListChannelsPath, nil)
	req.Header.Set("Authorization", "Bearer daemon-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("list do: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d, want 200; body=%s", resp.StatusCode, body)
	}
	if strings.TrimSpace(string(body)) != "[]" {
		t.Errorf("list body = %q, want []", string(body))
	}

	// POST /api/channel/create with valid bearer → saga completes.
	p := happyParams(t, workRoot, "req-auth-ok", "ch-auth-ok")
	buf, _ := json.Marshal(p)
	req2, _ := http.NewRequest(http.MethodPost, srv.URL+CreateChannelPath,
		bytes.NewReader(buf))
	req2.Header.Set("Authorization", "Bearer daemon-token")
	req2.Header.Set("Content-Type", "application/json")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("create do: %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusOK {
		out, _ := io.ReadAll(resp2.Body)
		t.Fatalf("create status = %d, want 200; body=%s", resp2.StatusCode, out)
	}
	var parsed map[string]any
	if err := json.NewDecoder(resp2.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	if parsed["channel_id"] != "ch-auth-ok" {
		t.Errorf("channel_id = %v, want ch-auth-ok", parsed["channel_id"])
	}
}

// _ assignment keeps errors imported even if the helpers above shift.
var _ = errors.New
