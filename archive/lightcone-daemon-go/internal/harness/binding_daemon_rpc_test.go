package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// alwaysAlice is a test AuthFunc that accepts the literal token
// "alice-token" and returns CallerCtx{ActorID: "alice"}. Any other
// token yields auth_failed.
func alwaysAlice(_ context.Context, token string, _ *MessageSendRequest) (pkgharness.CallerCtx, error) {
	if token != "alice-token" {
		return pkgharness.CallerCtx{}, nil
	}
	return pkgharness.CallerCtx{Authenticated: true, ActorID: "alice"}, nil
}

func httpFixture(t *testing.T) (*httptest.Server, *http.Client) {
	t.Helper()
	db := openTestDB(t)
	deps := buildSqliteDeps(t, db)
	handler := NewHTTPHandler(HTTPHandlerOptions{
		Deps: deps,
		Auth: alwaysAlice,
	})
	mux := http.NewServeMux()
	mux.Handle(RPCPath, handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, srv.Client()
}

func postRPC(t *testing.T, srv *httptest.Server, client *http.Client, token string, body any) *http.Response {
	t.Helper()
	buf, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, srv.URL+RPCPath, bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("new req: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	return resp
}

func decode(t *testing.T, resp *http.Response, out any) {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatalf("decode: %v", err)
	}
}

func TestHTTP_HappyPath_200(t *testing.T) {
	srv, client := httpFixture(t)
	body := MessageSendRequest{
		Params: *newSqliteEnv("http-1"),
	}
	resp := postRPC(t, srv, client, "alice-token", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var success MessageSendSuccess
	decode(t, resp, &success)
	if success.ID != "http-1" || success.Kind != v4types.KindEvent {
		t.Fatalf("unexpected success body: %+v", success)
	}
}

func TestHTTP_AuthFailed_401(t *testing.T) {
	srv, client := httpFixture(t)
	body := MessageSendRequest{
		Params: *newSqliteEnv("http-1"),
	}
	resp := postRPC(t, srv, client, "wrong-token", body)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
	var e MessageSendError
	decode(t, resp, &e)
	if e.Error.Reason != v4types.HarnessAuthFailed {
		t.Fatalf("expected auth_failed, got %q", e.Error.Reason)
	}
}

func TestHTTP_MissingAuthHeader_401(t *testing.T) {
	srv, client := httpFixture(t)
	body := MessageSendRequest{Params: *newSqliteEnv("http-1")}
	resp := postRPC(t, srv, client, "", body)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestHTTP_SenderMismatch_403(t *testing.T) {
	srv, client := httpFixture(t)
	env := newSqliteEnv("http-1")
	env.Sender.ID = "bob"
	body := MessageSendRequest{Params: *env}
	resp := postRPC(t, srv, client, "alice-token", body)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}
	var e MessageSendError
	decode(t, resp, &e)
	if e.Error.Reason != v4types.HarnessSenderMismatch {
		t.Fatalf("expected sender_mismatch, got %q", e.Error.Reason)
	}
}

// TestHTTP_ChannelMismatch_400 covers the FIX-3 R1 daemon_rpc surface
// (T103 / codex t91 critical end-to-end check): a binding bound to
// "ch-1" MUST reject an envelope addressed to a different channel with
// HTTP 400 + `channel_mismatch`, NOT route it through.
func TestHTTP_ChannelMismatch_400(t *testing.T) {
	srv, client := httpFixture(t)
	env := newSqliteEnv("http-cm-1")
	env.ChannelID = "ch-other"
	body := MessageSendRequest{Params: *env}
	resp := postRPC(t, srv, client, "alice-token", body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
	var e MessageSendError
	decode(t, resp, &e)
	if e.Error.Reason != v4types.HarnessChannelMismatch {
		t.Fatalf("expected channel_mismatch, got %q", e.Error.Reason)
	}
}

func TestHTTP_MissingRequiredField_400(t *testing.T) {
	srv, client := httpFixture(t)
	env := newSqliteEnv("")
	body := MessageSendRequest{Params: *env}
	resp := postRPC(t, srv, client, "alice-token", body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
	var e MessageSendError
	decode(t, resp, &e)
	if e.Error.Reason != v4types.HarnessMissingRequiredField {
		t.Fatalf("expected missing_required_field, got %q", e.Error.Reason)
	}
}

func TestHTTP_MessageIDConflict_409(t *testing.T) {
	srv, client := httpFixture(t)
	body := MessageSendRequest{Params: *newSqliteEnv("conf-1")}
	if resp := postRPC(t, srv, client, "alice-token", body); resp.StatusCode != 200 {
		t.Fatalf("first write expected 200, got %d", resp.StatusCode)
	}
	// Different content, same id.
	env := newSqliteEnv("conf-1")
	env.Payload = json.RawMessage(`{"text":"different"}`)
	resp := postRPC(t, srv, client, "alice-token", MessageSendRequest{Params: *env})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409, got %d", resp.StatusCode)
	}
	var e MessageSendError
	decode(t, resp, &e)
	if e.Error.Reason != v4types.HarnessMessageIDConflict {
		t.Fatalf("expected message_id_conflict, got %q", e.Error.Reason)
	}
}

func TestHTTP_TerminalDuplicate_409(t *testing.T) {
	// Need to share state with the server, so build a dedicated server
	// against a fresh db that has biz.foo installed + a request seeded.
	db2 := openTestDB(t)
	mustInstallBizType(t, db2)
	if _, err := db2.ExecContext(context.Background(),
		`INSERT INTO messages (id, ts, ts_received, channel_id, sender_kind, sender_id,
		 kind, type, payload, parent_id, correlation_id, visibility, audience, is_terminal)
		 VALUES ('req-h', 1700000000000, 1700000000000, 'ch-1', 'agent', 'alice',
		         'request', 'biz.foo', '{}', NULL, 'req-h', 'public', '["bob"]', 0)`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	deps := buildSqliteDeps(t, db2)
	handler := NewHTTPHandler(HTTPHandlerOptions{Deps: deps, Auth: alwaysAlice})
	srv2 := httptest.NewServer(http.HandlerFunc(handler.ServeHTTP))
	defer srv2.Close()
	client := srv2.Client()

	mkResp := func(id string) MessageSendRequest {
		env := newSqliteEnv(id)
		env.Type = "biz.foo"
		env.Kind = v4types.KindResponse
		env.ParentID = "req-h"
		env.Audience = []string{"alice"}
		env.Payload = json.RawMessage(`{"ok":true}`)
		return MessageSendRequest{Params: *env}
	}

	// First terminal
	req1, _ := http.NewRequest(http.MethodPost, srv2.URL, bytes.NewReader(mustJSON(t, mkResp("term-1"))))
	req1.Header.Set("Authorization", "Bearer alice-token")
	req1.Header.Set("Content-Type", "application/json")
	r1, err := client.Do(req1)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if r1.StatusCode != 200 {
		t.Fatalf("first terminal expected 200, got %d", r1.StatusCode)
	}
	_ = r1.Body.Close()

	// Second terminal — should 409 terminal_duplicate
	req2, _ := http.NewRequest(http.MethodPost, srv2.URL, bytes.NewReader(mustJSON(t, mkResp("term-2"))))
	req2.Header.Set("Authorization", "Bearer alice-token")
	req2.Header.Set("Content-Type", "application/json")
	r2, err := client.Do(req2)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if r2.StatusCode != http.StatusConflict {
		t.Fatalf("second terminal expected 409, got %d", r2.StatusCode)
	}
	var e MessageSendError
	decode(t, r2, &e)
	if e.Error.Reason != v4types.HarnessTerminalDuplicate {
		t.Fatalf("expected terminal_duplicate, got %q", e.Error.Reason)
	}
	if e.Error.DedupeResponseID != "term-1" {
		t.Fatalf("expected dedupe_response_id=term-1, got %q", e.Error.DedupeResponseID)
	}
}

func TestHTTP_MethodNotAllowed(t *testing.T) {
	srv, client := httpFixture(t)
	req, _ := http.NewRequest(http.MethodGet, srv.URL+RPCPath, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// TestWriteReject_AllReasons_OneToOneMapping is the M1.3-T15 acceptance
// gate for "所有 reason → HTTP status 一一对应（L2 §3.6 表 1:1）". It walks
// every v4types.HarnessRejectReason in AllHarnessRejectReasons, feeds it
// through writeReject (the binding's reject serializer), and asserts:
//
//   - the HTTP status equals v4types.HarnessRejectReason.HTTPStatus()
//   - the body decodes as MessageSendError with the same reason
//   - Detail / MessageIDIfPartial / DedupeResponseID round-trip
//   - Content-Type is application/json
//
// A new harness reject reason added to the closed set MUST trip this
// test (cardinalities asserted in v4types/reasons_test.go) — preventing
// drift between the data layer (.HTTPStatus()) and the binding wiring.
func TestWriteReject_AllReasons_OneToOneMapping(t *testing.T) {
	t.Parallel()

	for _, reason := range v4types.AllHarnessRejectReasons {
		reason := reason
		t.Run(string(reason), func(t *testing.T) {
			t.Parallel()
			rec := httptest.NewRecorder()
			rerr := &pkgharness.RejectError{
				Reason: reason,
				Detail: "synthetic detail",
			}
			// terminal_duplicate carries dedupe_response_id; message_id_conflict
			// historically rides with message_id_if_partial so exercise both
			// optional fields via the per-reason matrix.
			switch reason {
			case v4types.HarnessTerminalDuplicate:
				rerr.DedupeResponseID = "winner-id"
			case v4types.HarnessMessageIDConflict:
				rerr.MessageIDIfPartial = "conflicting-id"
			}

			writeReject(rec, rerr)

			wantStatus := reason.HTTPStatus()
			if wantStatus == 0 {
				t.Fatalf("reason %q has no HTTPStatus mapping (data-layer drift)", reason)
			}
			if rec.Code != wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, wantStatus)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("Content-Type = %q, want application/json", ct)
			}
			var body MessageSendError
			if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			if body.Error.Reason != reason {
				t.Fatalf("body.error.reason = %q, want %q", body.Error.Reason, reason)
			}
			if body.Error.Detail != "synthetic detail" {
				t.Fatalf("body.error.detail = %q, want synthetic detail", body.Error.Detail)
			}
			switch reason {
			case v4types.HarnessTerminalDuplicate:
				if body.Error.DedupeResponseID != "winner-id" {
					t.Fatalf("dedupe_response_id = %q, want winner-id", body.Error.DedupeResponseID)
				}
			case v4types.HarnessMessageIDConflict:
				if body.Error.MessageIDIfPartial != "conflicting-id" {
					t.Fatalf("message_id_if_partial = %q, want conflicting-id", body.Error.MessageIDIfPartial)
				}
			}
		})
	}
}

// TestWriteReject_UnknownReason_FallsBackTo400 is the defensive branch
// in writeReject — an out-of-set reason (data drift / future reason
// added but not yet mapped) MUST still produce a 4xx body with the
// reason string intact rather than swallowing it as 500.
func TestWriteReject_UnknownReason_FallsBackTo400(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	rerr := &pkgharness.RejectError{
		Reason: v4types.HarnessRejectReason("not_a_real_reason"),
		Detail: "synthetic",
	}
	writeReject(rec, rerr)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 fallback", rec.Code)
	}
	var body MessageSendError
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(body.Error.Reason) != "not_a_real_reason" {
		t.Fatalf("reason = %q, want preserved", body.Error.Reason)
	}
}
