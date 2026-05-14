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

func newCallbackTestServer(t *testing.T, rec *recordingCallbackManager) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("/api/device/", NewCallbackHandler(rec))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func postJSON(t *testing.T, url, body string) (*http.Response, string) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
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
	_ = NewCallbackHandler(nil)
}
