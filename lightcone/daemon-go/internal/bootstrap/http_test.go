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

// _ assignment keeps errors imported even if the helpers above shift.
var _ = errors.New
