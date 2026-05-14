package xhs

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// recordingCallbackManager captures every OnExternalCallback call so
// tests can assert the adapter name + payload shape.
type recordingCallbackManager struct {
	mu     sync.Mutex
	calls  []recordedCallback
	retErr error
}

type recordedCallback struct {
	AdapterName string
	Payload     []byte
}

func (r *recordingCallbackManager) OnExternalCallback(_ context.Context, name string, payload []byte) error {
	r.mu.Lock()
	r.calls = append(r.calls, recordedCallback{
		AdapterName: name,
		Payload:     append([]byte(nil), payload...),
	})
	err := r.retErr
	r.mu.Unlock()
	return err
}

func (r *recordingCallbackManager) snapshot() []recordedCallback {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recordedCallback, len(r.calls))
	copy(out, r.calls)
	return out
}

// testToken is the shared bearer token wired into every test server. It
// is intentionally a non-empty deterministic string so the auth tests can
// flip between "matching" and "mismatched" by changing the request header.
const testToken = "test-machine-token"

func newCallbackTestServer(t *testing.T, rec *recordingCallbackManager) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("/api/device/", NewCallbackHandler(rec, testToken))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func postJSON(t *testing.T, url, body string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request %s: %v", url, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	return resp, string(raw)
}

// TestCallbackHandler_HappyPath verifies the handler forwards a
// well-formed callback to the manager + injects device_id from the
// URL when the body omits it.
func TestCallbackHandler_HappyPath(t *testing.T) {
	rec := &recordingCallbackManager{}
	srv := newCallbackTestServer(t, rec)

	body := `{"correlation_id":"req-1","status":"ok","result":{"note_id":"n42"}}`
	resp, raw := postJSON(t, srv.URL+"/api/device/dev-pri-001/callback", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d (%s); want 200", resp.StatusCode, raw)
	}
	calls := rec.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 manager call, got %d", len(calls))
	}
	if calls[0].AdapterName != AdapterName {
		t.Fatalf("adapter name = %q; want %q", calls[0].AdapterName, AdapterName)
	}
	var forwarded map[string]any
	if err := json.Unmarshal(calls[0].Payload, &forwarded); err != nil {
		t.Fatalf("decode forwarded payload: %v", err)
	}
	if forwarded["device_id"] != "dev-pri-001" {
		t.Fatalf("device_id not folded from URL; got %v", forwarded["device_id"])
	}
	if forwarded["correlation_id"] != "req-1" {
		t.Fatalf("correlation_id missing/wrong: %v", forwarded["correlation_id"])
	}
}

// TestCallbackHandler_PathDeviceIDPreservesBodyValue makes sure the
// handler doesn't clobber an explicit device_id from the JSON body.
func TestCallbackHandler_PathDeviceIDPreservesBodyValue(t *testing.T) {
	rec := &recordingCallbackManager{}
	srv := newCallbackTestServer(t, rec)
	body := `{"correlation_id":"req-2","status":"ok","device_id":"different-001"}`
	resp, raw := postJSON(t, srv.URL+"/api/device/dev-pri-001/callback", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d (%s); want 200", resp.StatusCode, raw)
	}
	var forwarded map[string]any
	_ = json.Unmarshal(rec.snapshot()[0].Payload, &forwarded)
	if forwarded["device_id"] != "different-001" {
		t.Fatalf("body device_id should win; got %v", forwarded["device_id"])
	}
}

// TestCallbackHandler_Rejects ensure 4xx surfaces for predictable bad
// inputs.
func TestCallbackHandler_Rejects(t *testing.T) {
	rec := &recordingCallbackManager{}
	srv := newCallbackTestServer(t, rec)

	// Bad method
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/device/x/callback", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d; want 405", resp.StatusCode)
	}

	// Unknown path
	resp2, _ := postJSON(t, srv.URL+"/api/device/x/other", `{}`)
	if resp2.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown path status = %d; want 404", resp2.StatusCode)
	}

	// Bad JSON
	resp3, _ := postJSON(t, srv.URL+"/api/device/x/callback", "not json")
	if resp3.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad json status = %d; want 400", resp3.StatusCode)
	}

	// Missing correlation_id
	resp4, _ := postJSON(t, srv.URL+"/api/device/x/callback", `{"status":"ok"}`)
	if resp4.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing correlation_id status = %d; want 400", resp4.StatusCode)
	}

	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("manager should not be called for any rejected request; got %d calls", len(got))
	}
}

// TestCallbackHandler_ManagerError_Maps500 verifies infrastructure-side
// errors from the framework surface as 500.
func TestCallbackHandler_ManagerError_Maps500(t *testing.T) {
	rec := &recordingCallbackManager{retErr: io.ErrUnexpectedEOF}
	srv := newCallbackTestServer(t, rec)
	resp, raw := postJSON(t, srv.URL+"/api/device/dev-pri-001/callback",
		`{"correlation_id":"req-3","status":"ok"}`)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d (%s); want 500", resp.StatusCode, raw)
	}
	if !strings.Contains(raw, "adapter_callback_failed") {
		t.Fatalf("error label missing in body: %s", raw)
	}
}

// TestCallbackHandler_BodyEcho confirms the success response echoes
// the device_id + correlation_id (useful for the extension to verify
// the daemon understood the request).
func TestCallbackHandler_BodyEcho(t *testing.T) {
	rec := &recordingCallbackManager{}
	srv := newCallbackTestServer(t, rec)
	resp, raw := postJSON(t, srv.URL+"/api/device/dev-pri-001/callback",
		`{"correlation_id":"req-4","status":"ok"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d; want 200", resp.StatusCode)
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(raw), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["device_id"] != "dev-pri-001" || body["correlation_id"] != "req-4" {
		t.Fatalf("echo body unexpected: %s", raw)
	}
}

// TestCallbackHandler_NilManagerPanics asserts the constructor refuses
// to build a handler without a manager.
func TestCallbackHandler_NilManagerPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil manager")
		}
	}()
	_ = NewCallbackHandler(nil, testToken)
}

// TestCallbackHandler_EmptyTokenPanics asserts the constructor refuses
// to build a handler without an auth token — T102 FIX-2 mandates that
// the daemon never silently accepts unauthenticated callbacks.
func TestCallbackHandler_EmptyTokenPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on empty token")
		}
	}()
	_ = NewCallbackHandler(&recordingCallbackManager{}, "")
}

// TestCallbackHandler_AuthMissingHeader covers the "no Authorization
// header at all" path: the handler must refuse with 401 token_required
// and MUST NOT invoke the manager (otherwise an attacker could probe
// adapter state without credentials).
func TestCallbackHandler_AuthMissingHeader(t *testing.T) {
	rec := &recordingCallbackManager{}
	srv := newCallbackTestServer(t, rec)

	req, _ := http.NewRequest(http.MethodPost,
		srv.URL+"/api/device/dev-pri-001/callback",
		strings.NewReader(`{"correlation_id":"req-x","status":"ok"}`))
	req.Header.Set("Content-Type", "application/json")
	// intentionally no Authorization header
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d (%s); want 401", resp.StatusCode, raw)
	}
	if !strings.Contains(string(raw), "token_required") {
		t.Fatalf("body missing token_required marker: %s", raw)
	}
	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("manager invoked despite missing token (%d calls)", len(got))
	}
}

// TestCallbackHandler_AuthMalformedHeader covers Authorization values
// that are present but not a Bearer token (e.g. Basic, plain string).
func TestCallbackHandler_AuthMalformedHeader(t *testing.T) {
	rec := &recordingCallbackManager{}
	srv := newCallbackTestServer(t, rec)

	for _, bad := range []string{
		"Basic Zm9vOmJhcg==",
		"Bearer ", // empty token after prefix
		"token-without-bearer-prefix",
	} {
		t.Run(bad, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodPost,
				srv.URL+"/api/device/dev-pri-001/callback",
				strings.NewReader(`{"correlation_id":"req-x","status":"ok"}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", bad)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("POST: %v", err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d; want 401", resp.StatusCode)
			}
		})
	}
	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("manager invoked despite malformed token (%d calls)", len(got))
	}
}

// TestCallbackHandler_AuthWrongToken covers the wrong-bearer-token
// case: header is well-formed Bearer <X> but X != configured machine
// token. Must surface 401 token_invalid (distinct from token_required
// so ops can distinguish "client forgot the header" from "client used
// the wrong creds").
func TestCallbackHandler_AuthWrongToken(t *testing.T) {
	rec := &recordingCallbackManager{}
	srv := newCallbackTestServer(t, rec)

	req, _ := http.NewRequest(http.MethodPost,
		srv.URL+"/api/device/dev-pri-001/callback",
		strings.NewReader(`{"correlation_id":"req-x","status":"ok"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer not-the-right-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d (%s); want 401", resp.StatusCode, raw)
	}
	if !strings.Contains(string(raw), "token_invalid") {
		t.Fatalf("body missing token_invalid marker: %s", raw)
	}
	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("manager invoked despite wrong token (%d calls)", len(got))
	}
}

// TestCallbackHandler_AuthHappyPath confirms the existing happy-path
// flow keeps working once auth is enforced (the postJSON helper sets
// the right header so we just need a positive assertion that the
// manager IS invoked).
func TestCallbackHandler_AuthHappyPath(t *testing.T) {
	rec := &recordingCallbackManager{}
	srv := newCallbackTestServer(t, rec)

	resp, raw := postJSON(t, srv.URL+"/api/device/dev-pri-001/callback",
		`{"correlation_id":"req-auth-ok","status":"ok"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d (%s); want 200", resp.StatusCode, raw)
	}
	if got := rec.snapshot(); len(got) != 1 {
		t.Fatalf("manager call count = %d, want 1", len(got))
	}
}
