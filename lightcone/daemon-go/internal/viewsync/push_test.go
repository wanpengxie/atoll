package viewsync

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// newTestEnvelope is a viewsync-local minimal envelope. We don't seed
// the harness store from push tests; the envelope only needs to round-
// trip JSON + carry the fields the failure event consumes.
func newTestEnvelope(id string) *v4types.Envelope {
	return &v4types.Envelope{
		ID:         id,
		TS:         1700000000000,
		ChannelID:  "ch-1",
		Sender:     v4types.Sender{Kind: v4types.SenderAgent, ID: "alice"},
		Kind:       v4types.KindEvent,
		Type:       "agent.text",
		Payload:    json.RawMessage(`{"text":"hi"}`),
		Visibility: v4types.VisibilityPublic,
		Audience:   []string{"*"},
	}
}

// recordingSink captures every EmitViewSyncFailed call so push tests
// can assert the failure was routed without standing up a real harness.
type recordingSink struct {
	mu    sync.Mutex
	calls []FailureParams
	err   error
}

func (r *recordingSink) EmitViewSyncFailed(_ context.Context, p FailureParams) error {
	r.mu.Lock()
	r.calls = append(r.calls, p)
	r.mu.Unlock()
	return r.err
}

func (r *recordingSink) snapshot() []FailureParams {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]FailureParams, len(r.calls))
	copy(out, r.calls)
	return out
}

func TestNewHTTPPusher_RequiresBaseURL(t *testing.T) {
	t.Parallel()
	if _, err := NewHTTPPusher(HTTPPusherOptions{}); err == nil {
		t.Fatalf("expected error when BaseURL empty, got nil")
	}
	if _, err := NewHTTPPusher(HTTPPusherOptions{BaseURL: "   "}); err == nil {
		t.Fatalf("expected error when BaseURL is whitespace, got nil")
	}
}

func TestHTTPPusher_Success_DecodesAck(t *testing.T) {
	t.Parallel()
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("server: method=%s", r.Method)
		}
		if r.URL.Path != DefaultPushPath {
			t.Errorf("server: path=%s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer push-token" {
			t.Errorf("server: auth=%q", got)
		}
		buf := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(buf)
		gotBody = buf
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"message_id": "ev-1",
			"dedupe":     false,
		})
	}))
	t.Cleanup(srv.Close)

	sink := &recordingSink{}
	pusher, err := NewHTTPPusher(HTTPPusherOptions{
		BaseURL:    srv.URL,
		AuthToken:  "push-token",
		HTTPClient: srv.Client(),
		Failure:    sink,
		Clock:      func() int64 { return 1700000001000 },
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	ack, err := pusher.PushToServer(context.Background(), newTestEnvelope("ev-1"))
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	if ack == nil || ack.MessageID != "ev-1" {
		t.Fatalf("ack = %+v, want MessageID=ev-1", ack)
	}
	if ack.Dedupe {
		t.Fatalf("ack.Dedupe = true, want false")
	}
	// Failure sink must NOT be invoked on success.
	if got := sink.snapshot(); len(got) != 0 {
		t.Fatalf("failure sink received %d calls on success", len(got))
	}
	// Verify the server saw the envelope body.
	if !strings.Contains(string(gotBody), `"id":"ev-1"`) {
		t.Fatalf("server body missing envelope id: %q", string(gotBody))
	}
}

func TestHTTPPusher_Success_EmptyBodyFallbackToEnvelopeID(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		// no body
	}))
	t.Cleanup(srv.Close)
	pusher, err := NewHTTPPusher(HTTPPusherOptions{
		BaseURL:    srv.URL,
		HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ack, err := pusher.PushToServer(context.Background(), newTestEnvelope("ev-bare"))
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	if ack.MessageID != "ev-bare" {
		t.Fatalf("ack.MessageID = %q, want ev-bare (fallback)", ack.MessageID)
	}
}

func TestHTTPPusher_HTTP500_ReturnsPushErrorAndEmitsFailure(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"server boom"}`))
	}))
	t.Cleanup(srv.Close)

	sink := &recordingSink{}
	pusher, err := NewHTTPPusher(HTTPPusherOptions{
		BaseURL:    srv.URL,
		HTTPClient: srv.Client(),
		Failure:    sink,
		Clock:      func() int64 { return 1700000001000 },
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	ack, err := pusher.PushToServer(context.Background(), newTestEnvelope("ev-2"))
	if ack != nil {
		t.Fatalf("ack = %+v, want nil on 500", ack)
	}
	pe, ok := err.(*PushError)
	if !ok {
		t.Fatalf("err type = %T, want *PushError", err)
	}
	if pe.Kind != "http_status" {
		t.Fatalf("PushError.Kind = %q, want http_status", pe.Kind)
	}
	if pe.HTTPStatus != http.StatusInternalServerError {
		t.Fatalf("PushError.HTTPStatus = %d, want 500", pe.HTTPStatus)
	}
	if !strings.Contains(pe.Body, "server boom") {
		t.Fatalf("PushError.Body = %q, want substring server boom", pe.Body)
	}

	calls := sink.snapshot()
	if len(calls) != 1 {
		t.Fatalf("failure sink got %d calls, want 1", len(calls))
	}
	got := calls[0]
	if got.MessageID != "ev-2" || got.ChannelID != "ch-1" {
		t.Fatalf("failure params id/channel = %s/%s", got.MessageID, got.ChannelID)
	}
	if got.Kind != "http_status" || got.HTTPStatus != 500 {
		t.Fatalf("failure params kind/status = %s/%d", got.Kind, got.HTTPStatus)
	}
	if !strings.Contains(got.Detail, "server boom") {
		t.Fatalf("failure params detail = %q", got.Detail)
	}
	if !strings.HasSuffix(got.TargetURL, DefaultPushPath) {
		t.Fatalf("failure params url = %q, want suffix %s", got.TargetURL, DefaultPushPath)
	}
	if got.OccurredAt != 1700000001000 {
		t.Fatalf("failure params occurred_at = %d, want 1700000001000", got.OccurredAt)
	}
}

func TestHTTPPusher_TransportError_EmitsFailure(t *testing.T) {
	t.Parallel()
	// Build a server then close it so the connect attempt errors out.
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	srv.Close()

	sink := &recordingSink{}
	pusher, err := NewHTTPPusher(HTTPPusherOptions{
		BaseURL: srv.URL,
		Failure: sink,
		Clock:   func() int64 { return 1700000002000 },
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	_, err = pusher.PushToServer(context.Background(), newTestEnvelope("ev-3"))
	pe, ok := err.(*PushError)
	if !ok {
		t.Fatalf("err type = %T, want *PushError", err)
	}
	if pe.Kind != "transport_error" {
		t.Fatalf("PushError.Kind = %q, want transport_error", pe.Kind)
	}
	if pe.HTTPStatus != 0 {
		t.Fatalf("PushError.HTTPStatus = %d, want 0 on transport error", pe.HTTPStatus)
	}
	if pe.Cause == nil {
		t.Fatalf("PushError.Cause = nil on transport error")
	}
	calls := sink.snapshot()
	if len(calls) != 1 {
		t.Fatalf("failure sink got %d calls", len(calls))
	}
	if calls[0].Kind != "transport_error" {
		t.Fatalf("failure kind = %q", calls[0].Kind)
	}
}

func TestHTTPPusher_NoSink_FailureStillReturned(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("bad"))
	}))
	t.Cleanup(srv.Close)

	pusher, err := NewHTTPPusher(HTTPPusherOptions{
		BaseURL:    srv.URL,
		HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	_, err = pusher.PushToServer(context.Background(), newTestEnvelope("ev-4"))
	if err == nil {
		t.Fatalf("expected error on 400")
	}
	pe, ok := err.(*PushError)
	if !ok || pe.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("err = %v, want PushError 400", err)
	}
}

func TestHTTPPusher_NilEnvelope(t *testing.T) {
	t.Parallel()
	pusher, err := NewHTTPPusher(HTTPPusherOptions{BaseURL: "http://localhost:1"})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if _, err := pusher.PushToServer(context.Background(), nil); err == nil {
		t.Fatalf("expected error on nil envelope")
	}
}

func TestBuildFailureEvent_ShapeAndDeterministicID(t *testing.T) {
	t.Parallel()
	params := FailureParams{
		ChannelID:  "ch-7",
		MessageID:  "ev-9",
		TargetURL:  "http://srv/api/view/sync",
		Kind:       "http_status",
		HTTPStatus: 503,
		Detail:     "upstream timeout",
		OccurredAt: 1700000005000,
	}
	env, err := buildFailureEvent(params, "system")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	wantID := "view_sync_failed:ch-7:ev-9:http_status"
	if env.ID != wantID {
		t.Fatalf("env.ID = %q, want %q", env.ID, wantID)
	}
	if env.Type != "system.event" {
		t.Fatalf("env.Type = %q", env.Type)
	}
	if env.Kind != v4types.KindEvent {
		t.Fatalf("env.Kind = %q", env.Kind)
	}
	if env.Visibility != v4types.VisibilitySystem {
		t.Fatalf("env.Visibility = %q", env.Visibility)
	}
	if env.Sender.Kind != v4types.SenderSystem || env.Sender.ID != "system" {
		t.Fatalf("sender = %+v", env.Sender)
	}
	// Payload must encode kind/severity per L1 §8.1.1 监控建议.
	var pl map[string]any
	if err := json.Unmarshal(env.Payload, &pl); err != nil {
		t.Fatalf("payload unmarshal: %v", err)
	}
	if pl["kind"] != "view_sync_failed" {
		t.Fatalf("payload.kind = %v", pl["kind"])
	}
	if pl["severity"] != "warn" {
		t.Fatalf("payload.severity = %v", pl["severity"])
	}
	if pl["channel_id"] != "ch-7" || pl["message_id"] != "ev-9" {
		t.Fatalf("payload ids = %v / %v", pl["channel_id"], pl["message_id"])
	}
	if pl["failure"] != "http_status" {
		t.Fatalf("payload.failure = %v", pl["failure"])
	}
	if int(pl["http_status"].(float64)) != 503 {
		t.Fatalf("payload.http_status = %v", pl["http_status"])
	}
	// Determinism: same params → same id.
	env2, err := buildFailureEvent(params, "system")
	if err != nil {
		t.Fatalf("build2: %v", err)
	}
	if env2.ID != env.ID {
		t.Fatalf("non-deterministic id: %q vs %q", env.ID, env2.ID)
	}
}

func TestHarnessFailureSink_NilWriter_Errors(t *testing.T) {
	t.Parallel()
	sink := &HarnessFailureSink{}
	err := sink.EmitViewSyncFailed(context.Background(), FailureParams{})
	if err == nil {
		t.Fatalf("expected error when Writer nil")
	}
}
