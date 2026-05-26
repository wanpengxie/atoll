package gateway_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	proxyfacade "github.com/wanpengxie/ActOS/adapters/framework/proxy_facade"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	kerneldaemonbus "github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
	"github.com/wanpengxie/ActOS/server/daemonbus"
	"github.com/wanpengxie/ActOS/server/devicebus"
	"github.com/wanpengxie/ActOS/server/gateway"
	"github.com/wanpengxie/ActOS/server/identity"
	"github.com/wanpengxie/ActOS/server/pushhub"
	"github.com/wanpengxie/ActOS/server/store"
)

// newTestApp boots a real App with an in-tempdir sqlite + fast
// reconcile so tests don't have to wait minutes.
func newTestApp(t *testing.T) *gateway.App {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "g.db"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	app, err := gateway.New(ctx, gateway.Config{
		DB:                        db,
		SessionSecret:             "test-session",
		DaemonSharedSecret:        "test-daemon",
		DeviceAllowedOrigins:      []string{"http://gateway.test"},
		PushhubAllowedOrigins:     []string{"http://gateway.test"},
		DaemonbusAllowedOrigins:   []string{"http://gateway.test"},
		HumanCallerSecret:         "test-human",
		BcryptCost:                4,
		AllowDevSecrets:           true,
		ReconcileGracePeriod:      50 * time.Millisecond,
		ReconcileCreateTimeout:    100 * time.Millisecond,
		ReconcileHeartbeatTimeout: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("gateway.New: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })
	return app
}

// TestEndToEndRegisterLoginCreateChannel exercises the full
// identity → catalog flow over real HTTP.
func TestEndToEndRegisterLoginCreateChannel(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	client := &http.Client{Jar: nil}
	ctx := context.Background()

	// IssueCode → capture the code from notify hook by reading the
	// row through the service directly (HTTP only returns "issued").
	post(t, client, srv.URL+"/api/identity/verification/issue", `{"email":"alice@example.com"}`, http.StatusAccepted)

	// Read the code straight from the database (test-only short-cut).
	var code string
	if err := app.DB().QueryRowContext(ctx,
		`SELECT code FROM verification_codes WHERE email = ? ORDER BY created_at DESC LIMIT 1`,
		"alice@example.com",
	).Scan(&code); err != nil {
		t.Fatalf("read code: %v", err)
	}

	// Register — per impl-layer3 §4.3.2 (R4-9 / R5-13), always returns
	// 202 with an opaque body to prevent user-enumeration.
	post(t, client, srv.URL+"/api/identity/register",
		`{"email":"alice@example.com","password":"topsecret123","code":"`+code+`"}`,
		http.StatusAccepted)

	// Login — collect cookie for downstream calls.
	loginResp := postRaw(t, client, srv.URL+"/api/identity/login",
		`{"email":"alice@example.com","password":"topsecret123"}`,
		http.StatusOK)
	defer func() { _ = loginResp.Body.Close() }()
	var loginBody struct {
		User struct{ ID string } `json:"user"`
	}
	if err := json.NewDecoder(loginResp.Body).Decode(&loginBody); err != nil {
		t.Fatalf("decode login: %v", err)
	}
	cookie := extractSessionCookie(t, loginResp)
	if cookie == "" {
		t.Fatal("session cookie missing")
	}

	// /me — authenticated.
	meReq, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/identity/me", nil)
	meReq.AddCookie(&http.Cookie{Name: identity.CookieName, Value: cookie})
	meResp, err := client.Do(meReq)
	if err != nil || meResp.StatusCode != 200 {
		t.Fatalf("/me err=%v status=%d", err, meResp.StatusCode)
	}
	_ = meResp.Body.Close()

	// Create workspace.
	wsReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/workspaces",
		strings.NewReader(`{"name":"Demo"}`))
	wsReq.Header.Set("Content-Type", "application/json")
	wsReq.AddCookie(&http.Cookie{Name: identity.CookieName, Value: cookie})
	wsResp, err := client.Do(wsReq)
	if err != nil || wsResp.StatusCode != http.StatusCreated {
		t.Fatalf("workspace err=%v status=%d", err, wsResp.StatusCode)
	}
	var ws struct{ ID string }
	_ = json.NewDecoder(wsResp.Body).Decode(&ws)
	_ = wsResp.Body.Close()

	// Create channel.
	chReq, _ := http.NewRequest(http.MethodPost,
		srv.URL+"/api/workspaces/"+ws.ID+"/channels",
		strings.NewReader(`{"name":"general"}`))
	chReq.Header.Set("Content-Type", "application/json")
	chReq.AddCookie(&http.Cookie{Name: identity.CookieName, Value: cookie})
	chResp, err := client.Do(chReq)
	if err != nil || chResp.StatusCode != http.StatusCreated {
		t.Fatalf("channel err=%v status=%d", err, chResp.StatusCode)
	}
	var chBody struct {
		Channel struct{ ID string } `json:"channel"`
	}
	_ = json.NewDecoder(chResp.Body).Decode(&chBody)
	_ = chResp.Body.Close()

	if chBody.Channel.ID == "" {
		t.Fatal("channel id empty")
	}
}

// TestHandleWriteMessage_RequestAudienceRejected covers FIX-T8 phase-1:
// the handler must early-reject a kind=request envelope whose audience
// is not exactly one concrete receiver. The check fires before
// daemonbus / placements are consulted, so this test only needs a
// signed-in user + a channel they're a member of.
func TestHandleWriteMessage_RequestAudienceRejected(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	client := &http.Client{}
	sess := registerLoginAndCreateChannel(t, client, srv.URL, app, "alice-aud@example.com")

	cases := []struct {
		name     string
		audience []string
	}{
		{"wildcard", []string{"*"}},
		{"empty", []string{}},
		{"multi", []string{"agent:a", "agent:b"}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			payload := writeBody{
				ID:       "msg-aud-" + tc.name,
				Type:     "human.text",
				Kind:     "request",
				Payload:  json.RawMessage(`{"text":"hi"}`),
				Audience: tc.audience,
			}
			raw, _ := json.Marshal(payload)
			req, _ := http.NewRequest(http.MethodPost,
				srv.URL+"/api/channels/"+sess.channelID+"/messages",
				strings.NewReader(string(raw)))
			req.Header.Set("Content-Type", "application/json")
			req.AddCookie(&http.Cookie{Name: identity.CookieName, Value: sess.session})
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("POST: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status=%d want 400", resp.StatusCode)
			}
			var errBody struct {
				Error string `json:"error"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&errBody)
			if !strings.Contains(errBody.Error, "harness_request_audience_invalid") {
				t.Errorf("error=%q want harness_request_audience_invalid", errBody.Error)
			}
		})
	}
}

func TestHandleWriteMessage_AudienceTooLarge(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	client := &http.Client{}
	sess := registerLoginAndCreateChannel(t, client, srv.URL, app, "aud-cap@example.com")
	aud := make([]string, 257)
	for i := range aud {
		aud[i] = "agent:" + itoa(int64(i))
	}
	body, _ := json.Marshal(writeBody{
		ID:       "msg-aud-cap",
		Type:     "human.text",
		Kind:     "event",
		Payload:  json.RawMessage(`{"text":"hi"}`),
		Audience: aud,
	})
	req, _ := http.NewRequest(http.MethodPost,
		srv.URL+"/api/channels/"+sess.channelID+"/messages",
		strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: identity.CookieName, Value: sess.session})
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d want 400", resp.StatusCode)
	}
	var out struct {
		Error string `json:"error"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out.Error != "audience_too_large" {
		t.Fatalf("error=%q want audience_too_large", out.Error)
	}
}

func TestIdentityBodyLimit(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	resp := postRaw(t, &http.Client{}, srv.URL+"/api/identity/login",
		`{"email":"`+strings.Repeat("a", 70<<10)+`@example.com","password":"x"}`,
		http.StatusRequestEntityTooLarge)
	defer func() { _ = resp.Body.Close() }()
}

func TestDownloadsRejectsTraversalAndSymlinkEscape(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	db, err := store.Open(ctx, filepath.Join(root, "g.db"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	uiDir := filepath.Join(root, "ui")
	downloads := filepath.Join(uiDir, "downloads")
	if err := os.MkdirAll(downloads, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(uiDir, "index.html"), []byte("index"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(downloads, "ok.txt"), []byte("ok"), 0o644); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(root, "secret.txt")
	if err := os.WriteFile(outside, []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(downloads, "escape.txt")); err != nil {
		t.Fatal(err)
	}

	app, err := gateway.New(ctx, gateway.Config{
		DB:                      db,
		SessionSecret:           "test-session",
		DaemonSharedSecret:      "test-daemon",
		DeviceAllowedOrigins:    []string{"http://gateway.test"},
		PushhubAllowedOrigins:   []string{"http://gateway.test"},
		DaemonbusAllowedOrigins: []string{"http://gateway.test"},
		HumanCallerSecret:       "test-human",
		BcryptCost:              4,
		AllowDevSecrets:         true,
		UIDistDir:               uiDir,
	})
	if err != nil {
		t.Fatalf("gateway.New: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	for _, path := range []string{"/downloads/ok.txt", "/downloads/../secret.txt", "/downloads/escape.txt"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		_ = resp.Body.Close()
		if path == "/downloads/ok.txt" {
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("%s status=%d want 200", path, resp.StatusCode)
			}
			continue
		}
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s status=%d want 404", path, resp.StatusCode)
		}
	}
}

// TestHandleWriteMessage_MissingID covers R4-3: caller MUST supply
// envelope.id per L3 §1.8.1; the gateway's binding:"required" tag
// MUST reject a missing id with HTTP 400 before any daemon work
// happens. Without this guard the daemon would later reject with a
// less specific reason (and earlier versions would silently auto-id
// the envelope, masking the contract).
func TestHandleWriteMessage_MissingID(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	client := &http.Client{}
	sess := registerLoginAndCreateChannel(t, client, srv.URL, app, "missing-id@example.com")

	payload := writeBody{
		// ID intentionally omitted — gateway MUST 400.
		Type:     "human.text",
		Kind:     "event",
		Payload:  json.RawMessage(`{"text":"no-id"}`),
		Audience: []string{"*"},
	}
	raw, _ := json.Marshal(payload)
	req, _ := http.NewRequest(http.MethodPost,
		srv.URL+"/api/channels/"+sess.channelID+"/messages",
		strings.NewReader(string(raw)))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: identity.CookieName, Value: sess.session})

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d want 400 (R4-3 caller MUST supply id)", resp.StatusCode)
	}
	var errBody struct {
		Error string `json:"error"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&errBody)
	// gin's binding error mentions the missing field by name; we just
	// assert it surfaced via the standard error envelope.
	if errBody.Error == "" {
		t.Errorf("error body empty; want gin binding error mentioning id")
	}
}

// TestHandleWriteMessage_UnknownFieldRejected covers R5-16 (impl-layer3
// §1.8.1 normative): the write-message endpoint MUST fail-closed reject
// any unknown top-level field with HTTP 400 + the
// `harness_envelope_unknown_field` reason. The default gin decoder
// silently accepts extra fields; R5-16 swaps in a json.Decoder with
// DisallowUnknownFields so the daemon harness Step 2 invariant is
// observable at the L3 surface.
func TestHandleWriteMessage_UnknownFieldRejected(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	client := &http.Client{}
	sess := registerLoginAndCreateChannel(t, client, srv.URL, app, "unknown-field@example.com")

	// Hand-rolled JSON so we can include a top-level field that
	// writeMessageReq does NOT declare — `not_a_field` is the canary.
	raw := `{
		"id":           "msg-unknown-1",
		"type":         "human.text",
		"kind":         "event",
		"payload":      {"text":"hi"},
		"audience":     ["*"],
		"not_a_field":  "should reject"
	}`
	req, _ := http.NewRequest(http.MethodPost,
		srv.URL+"/api/channels/"+sess.channelID+"/messages",
		strings.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: identity.CookieName, Value: sess.session})

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d want 400 (impl-layer3 §1.8.1 unknown field fail-closed)", resp.StatusCode)
	}
	var errBody struct {
		Error        string `json:"error"`
		RejectReason string `json:"reject_reason"`
		RejectDetail string `json:"reject_detail"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&errBody); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if errBody.RejectReason != "harness_envelope_unknown_field" {
		t.Errorf("reject_reason=%q want harness_envelope_unknown_field", errBody.RejectReason)
	}
	if errBody.Error != "harness_envelope_unknown_field" {
		t.Errorf("error=%q want harness_envelope_unknown_field", errBody.Error)
	}
	if !strings.Contains(errBody.RejectDetail, "not_a_field") {
		t.Errorf("reject_detail=%q should mention the offending field name", errBody.RejectDetail)
	}
}

// TestHandleWriteMessage_KnownFieldsStillAccepted is the positive
// companion to UnknownFieldRejected — a request whose top-level field
// set matches writeMessageReq exactly must still pass through the
// decoder; the R5-16 swap to DisallowUnknownFields MUST NOT regress the
// happy path.
func TestHandleWriteMessage_KnownFieldsStillAccepted(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	client := &http.Client{}
	sess := registerLoginAndCreateChannel(t, client, srv.URL, app, "known-fields@example.com")

	payload := writeBody{
		ID:       "msg-known-1",
		Type:     "human.text",
		Kind:     "event",
		Payload:  json.RawMessage(`{"text":"hi"}`),
		Audience: []string{"*"},
	}
	raw, _ := json.Marshal(payload)
	req, _ := http.NewRequest(http.MethodPost,
		srv.URL+"/api/channels/"+sess.channelID+"/messages",
		strings.NewReader(string(raw)))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: identity.CookieName, Value: sess.session})

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	// We don't assert 200/202 here — there is no daemon attached so
	// the gateway will surface a 503 once it reaches
	// ConnectionForChannel. The bar for this test is "the decode
	// step accepted the body" — which is true for anything that is
	// NOT 400-with-harness_envelope_unknown_field.
	if resp.StatusCode == http.StatusBadRequest {
		var errBody struct {
			Error        string `json:"error"`
			RejectReason string `json:"reject_reason"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errBody)
		if errBody.RejectReason == "harness_envelope_unknown_field" || errBody.Error == "harness_envelope_unknown_field" {
			t.Fatalf("known-field request was wrongly rejected as unknown_field: %+v", errBody)
		}
	}
}

// TestHandleWriteMessage_CallerIDForwardedToDaemon covers R4-3: the
// gateway MUST forward the caller-supplied envelope.id unchanged into
// the daemon control.write_message frame. This is what makes L1 §2.3
// Step 3 dedupe observable end-to-end (the daemon now rejects empty
// ids at its edge — see TestWriteMessageHandler_EmptyEnvelopeID — so
// any gateway-side regression would surface as a daemon reject).
func TestHandleWriteMessage_CallerIDForwardedToDaemon(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	app := newTestApp(t)
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	client := &http.Client{}
	sess := registerLoginAndCreateChannel(t, client, srv.URL, app, "caller-id-fwd@example.com")

	// Mount a stub daemon connection — same pattern as the existing
	// correlation-inherit test above.
	if err := app.Daemonbus().RegisterDaemon(ctx,
		placement.DaemonID("d-callerid"), "h", "v", 0, "test-daemon"); err != nil {
		t.Fatalf("RegisterDaemon: %v", err)
	}
	epoch, err := app.Daemonbus().IssueConnectionEpoch(ctx, placement.DaemonID("d-callerid"))
	if err != nil {
		t.Fatalf("IssueConnectionEpoch: %v", err)
	}
	svrTx, dmnTx := newPipePair()
	conn := daemonbus.NewConnection(placement.DaemonID("d-callerid"), epoch, svrTx)
	app.Daemonbus().Register(conn)
	defer app.Daemonbus().Unregister(placement.DaemonID("d-callerid"))
	go func() { _ = conn.Run(ctx, app.DaemonbusHandlers()) }()

	p, createReq, err := app.Placements().Reserve(ctx,
		channel.ID(sess.channelID),
		placement.DaemonID("d-callerid"),
		placement.ConnectionEpoch(epoch), nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	// Phase 2 ack carries daemon-generated fencing tuple (proto-foundation
	// §3.3.3 + impl-layer2 §3.2.2).
	ack := placement.CreateChannelAck{
		ChannelID:       p.ChannelID,
		CreateRequestID: createReq.CreateRequestID,
		OwnerEpoch:      1,
		FencingToken:    "daemon-tok-callerid",
		DaemonID:        placement.DaemonID("d-callerid"),
		Result:          placement.CreateChannelAccepted,
	}
	if ok, err := app.Placements().Activate(ctx, ack, placement.ConnectionEpoch(epoch)); err != nil || !ok {
		t.Fatalf("Activate: ok=%v err=%v", ok, err)
	}

	const callerSuppliedID = "msg-caller-fwd-12345"

	respCh := make(chan *http.Response, 1)
	errCh := make(chan error, 1)
	go func() {
		body, _ := json.Marshal(writeBody{
			ID:       callerSuppliedID,
			Type:     "human.text",
			Kind:     string(message.KindEvent),
			Payload:  json.RawMessage(`{"text":"hi"}`),
			Audience: []string{"*"},
		})
		req, _ := http.NewRequest(http.MethodPost,
			srv.URL+"/api/channels/"+sess.channelID+"/messages",
			strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		req.AddCookie(&http.Cookie{Name: identity.CookieName, Value: sess.session})
		resp, err := client.Do(req)
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp
	}()

	frame, err := dmnTx.ReadFrame(ctx)
	if err != nil {
		t.Fatalf("read write_message: %v", err)
	}
	var wmbody kerneldaemonbus.WriteMessageBody
	if err := json.Unmarshal(frame.Payload, &wmbody); err != nil {
		t.Fatalf("decode write body: %v", err)
	}
	if got := string(wmbody.EnvelopePartial.ID); got != callerSuppliedID {
		t.Fatalf("gateway forwarded envelope.id=%q want caller-supplied %q (R4-3)",
			got, callerSuppliedID)
	}

	// Ack with the same caller id so the gateway's response surfaces it.
	ackBody := kerneldaemonbus.WriteMessageAckBody{
		FrameID:   wmbody.FrameID,
		Accepted:  true,
		MessageID: message.ID(callerSuppliedID),
		Seq:       1,
	}
	raw, _ := json.Marshal(ackBody)
	if err := dmnTx.WriteFrame(ctx, kerneldaemonbus.Frame{
		FrameID:               frame.FrameID,
		FrameKind:             kerneldaemonbus.FrameTypeControlWriteMessageAck,
		DaemonID:              "d-callerid",
		DaemonConnectionEpoch: epoch,
		Payload:               raw,
	}); err != nil {
		t.Fatalf("write ack: %v", err)
	}

	select {
	case err := <-errCh:
		t.Fatalf("POST: %v", err)
	case resp := <-respCh:
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status=%d want 200", resp.StatusCode)
		}
		var out map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if out["message_id"] != callerSuppliedID {
			t.Fatalf("response message_id=%v want %q (R4-3 echo)", out["message_id"], callerSuppliedID)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestHandleWriteMessageRejectUsesReasonHTTPStatus(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	app := newTestApp(t)
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	client := &http.Client{}
	sess := registerLoginAndCreateChannel(t, client, srv.URL, app, "reject-status@example.com")

	if err := app.Daemonbus().RegisterDaemon(ctx,
		placement.DaemonID("d-reject-status"), "h", "v", 0, "test-daemon"); err != nil {
		t.Fatalf("RegisterDaemon: %v", err)
	}
	epoch, err := app.Daemonbus().IssueConnectionEpoch(ctx, placement.DaemonID("d-reject-status"))
	if err != nil {
		t.Fatalf("IssueConnectionEpoch: %v", err)
	}
	svrTx, dmnTx := newPipePair()
	conn := daemonbus.NewConnection(placement.DaemonID("d-reject-status"), epoch, svrTx)
	app.Daemonbus().Register(conn)
	defer app.Daemonbus().Unregister(placement.DaemonID("d-reject-status"))
	go func() { _ = conn.Run(ctx, app.DaemonbusHandlers()) }()

	p, createReq, err := app.Placements().Reserve(ctx,
		channel.ID(sess.channelID),
		placement.DaemonID("d-reject-status"),
		placement.ConnectionEpoch(epoch), nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	ack := placement.CreateChannelAck{
		ChannelID:       p.ChannelID,
		CreateRequestID: createReq.CreateRequestID,
		OwnerEpoch:      1,
		FencingToken:    "daemon-tok-reject-status",
		DaemonID:        placement.DaemonID("d-reject-status"),
		Result:          placement.CreateChannelAccepted,
	}
	if ok, err := app.Placements().Activate(ctx, ack, placement.ConnectionEpoch(epoch)); err != nil || !ok {
		t.Fatalf("Activate: ok=%v err=%v", ok, err)
	}

	cases := []struct {
		reason string
		want   int
	}{
		{string(message.HarnessSenderKindMismatch), http.StatusBadRequest},
		{string(message.HarnessAudienceMemberNotActive), http.StatusForbidden},
		{string(message.HarnessSenderDeregistered), http.StatusGone},
		{string(message.HarnessWorkerFencingStale), http.StatusGone},
		{string(message.HarnessEngineACLDenied), http.StatusInternalServerError},
		{"replay_nonce_seen", http.StatusConflict},
	}

	for i, tc := range cases {
		tc := tc
		t.Run(tc.reason, func(t *testing.T) {
			msgID := fmt.Sprintf("msg-reject-status-%d", i)
			respCh := make(chan *http.Response, 1)
			errCh := make(chan error, 1)
			go func() {
				body, _ := json.Marshal(writeBody{
					ID:       msgID,
					Type:     "human.text",
					Kind:     string(message.KindEvent),
					Payload:  json.RawMessage(`{"text":"hi"}`),
					Audience: []string{"*"},
				})
				req, _ := http.NewRequest(http.MethodPost,
					srv.URL+"/api/channels/"+sess.channelID+"/messages",
					strings.NewReader(string(body)))
				req.Header.Set("Content-Type", "application/json")
				req.AddCookie(&http.Cookie{Name: identity.CookieName, Value: sess.session})
				resp, err := client.Do(req)
				if err != nil {
					errCh <- err
					return
				}
				respCh <- resp
			}()

			frame, err := dmnTx.ReadFrame(ctx)
			if err != nil {
				t.Fatalf("read write_message: %v", err)
			}
			var wmbody kerneldaemonbus.WriteMessageBody
			if err := json.Unmarshal(frame.Payload, &wmbody); err != nil {
				t.Fatalf("decode write body: %v", err)
			}
			ackBody := kerneldaemonbus.WriteMessageAckBody{
				FrameID:      wmbody.FrameID,
				Accepted:     false,
				RejectReason: tc.reason,
				RejectDetail: "test reject",
			}
			raw, _ := json.Marshal(ackBody)
			if err := dmnTx.WriteFrame(ctx, kerneldaemonbus.Frame{
				FrameID:               frame.FrameID,
				FrameKind:             kerneldaemonbus.FrameTypeControlWriteMessageAck,
				DaemonID:              "d-reject-status",
				DaemonConnectionEpoch: epoch,
				Payload:               raw,
			}); err != nil {
				t.Fatalf("write ack: %v", err)
			}

			select {
			case err := <-errCh:
				t.Fatalf("POST: %v", err)
			case resp := <-respCh:
				defer func() { _ = resp.Body.Close() }()
				if resp.StatusCode != tc.want {
					t.Fatalf("status=%d want=%d for %s", resp.StatusCode, tc.want, tc.reason)
				}
				var out struct {
					RejectReason string `json:"reject_reason"`
					Error        string `json:"error"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
					t.Fatalf("decode response: %v", err)
				}
				if out.RejectReason != tc.reason {
					t.Fatalf("reject_reason=%q want %q", out.RejectReason, tc.reason)
				}
				if out.Error != "" {
					t.Fatalf("error=%q want absent", out.Error)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

func TestDevicebusLegacyActorEndpointsGone(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	client := &http.Client{}
	alice := registerLoginAndCreateChannel(t, client, srv.URL, app, "alice-devbus-gone@example.com")

	cases := []struct {
		method string
		path   string
		body   string
	}{
		{
			method: http.MethodPost,
			path:   "/api/channels/" + alice.channelID + "/device-actor",
			body:   `{"device_id":"dev","device_type":"xhs","daemon_id":"d1"}`,
		},
		{method: http.MethodGet, path: "/api/channels/" + alice.channelID + "/device-actor/tool%3Axhs-adapter"},
		{method: http.MethodDelete, path: "/api/channels/" + alice.channelID + "/device-actor/tool%3Axhs-adapter"},
	}
	for _, tc := range cases {
		var body io.Reader
		if tc.body != "" {
			body = strings.NewReader(tc.body)
		}
		req, _ := http.NewRequest(tc.method, srv.URL+tc.path, body)
		if tc.body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		req.AddCookie(&http.Cookie{Name: identity.CookieName, Value: alice.session})
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s device-actor: %v", tc.method, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusGone {
			t.Fatalf("%s status=%d want 410", tc.method, resp.StatusCode)
		}
	}

	resp, err := client.Get(srv.URL + "/devicebus")
	if err != nil {
		t.Fatalf("GET /devicebus: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusGone {
		t.Fatalf("GET /devicebus status=%d want 410", resp.StatusCode)
	}
}

func TestViewcacheRoutesRequireChannelMembership(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	client := &http.Client{}
	alice := registerLoginAndCreateChannel(t, client, srv.URL, app, "alice-viewcache@example.com")
	bob := registerLoginAndCreateChannel(t, client, srv.URL, app, "bob-viewcache@example.com")
	ctx := context.Background()

	env := message.Envelope{
		ID:         "m-private",
		ChannelID:  channel.ID(alice.channelID),
		Type:       "agent.text",
		Kind:       message.KindEvent,
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
		Payload:    json.RawMessage(`{"text":"secret"}`),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{actor.SystemActorID},
	}
	if _, err := app.Viewcache().Apply(ctx, viewsync.PushFrame{
		ChannelID: channel.ID(alice.channelID),
		Seq:       1,
		MessageID: env.ID,
		Envelope:  env,
	}); err != nil {
		t.Fatalf("seed viewcache: %v", err)
	}

	cases := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodGet, "/api/channels/" + alice.channelID + "/messages", ""},
		{http.MethodGet, "/api/channels/" + alice.channelID + "/cursor", ""},
		{http.MethodPost, "/api/channels/" + alice.channelID + "/resync", `{"since_seq":1,"until_seq":1}`},
	}
	for _, tc := range cases {
		req, _ := http.NewRequest(tc.method, srv.URL+tc.path, strings.NewReader(tc.body))
		if tc.body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		req.AddCookie(&http.Cookie{Name: identity.CookieName, Value: bob.session})
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", tc.method, tc.path, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("%s %s status=%d want 403", tc.method, tc.path, resp.StatusCode)
		}
	}
}

func TestViewcacheMessagesApplyCallerVisibility(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	client := &http.Client{}
	alice := registerLoginAndCreateChannel(t, client, srv.URL, app, "alice-view-visible@example.com")
	_ = registerLoginAndCreateChannel(t, client, srv.URL, app, "bob-view-visible@example.com")
	aliceID := userIDByEmail(t, app, "alice-view-visible@example.com")
	bobID := userIDByEmail(t, app, "bob-view-visible@example.com")

	addReq, _ := http.NewRequest(http.MethodPost,
		srv.URL+"/api/channels/"+alice.channelID+"/members",
		strings.NewReader(`{"user_id":"`+bobID+`"}`))
	addReq.Header.Set("Content-Type", "application/json")
	addReq.AddCookie(&http.Cookie{Name: identity.CookieName, Value: alice.session})
	addResp, err := client.Do(addReq)
	if err != nil {
		t.Fatalf("add member: %v", err)
	}
	_ = addResp.Body.Close()
	if addResp.StatusCode != http.StatusCreated {
		t.Fatalf("add member status=%d want 201", addResp.StatusCode)
	}

	ctx := context.Background()
	seed := []message.Envelope{
		{
			ID:         "m-public",
			ChannelID:  channel.ID(alice.channelID),
			Type:       "agent.text",
			Kind:       message.KindEvent,
			Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
			Payload:    json.RawMessage(`{"text":"public"}`),
			Visibility: message.VisibilityPublic,
			Audience:   message.Audience{actor.SystemActorID},
		},
		{
			ID:         "m-private-alice",
			ChannelID:  channel.ID(alice.channelID),
			Type:       "agent.text",
			Kind:       message.KindEvent,
			Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
			Payload:    json.RawMessage(`{"text":"alice"}`),
			Visibility: message.VisibilityPrivate,
			Audience:   message.Audience{actor.ActorID("user:" + aliceID)},
		},
		{
			ID:         "m-private-bob",
			ChannelID:  channel.ID(alice.channelID),
			Type:       "agent.text",
			Kind:       message.KindEvent,
			Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
			Payload:    json.RawMessage(`{"text":"bob"}`),
			Visibility: message.VisibilityPrivate,
			Audience:   message.Audience{actor.ActorID("user:" + bobID)},
		},
		{
			ID:         "m-system",
			ChannelID:  channel.ID(alice.channelID),
			Type:       "agent.text",
			Kind:       message.KindEvent,
			Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
			Payload:    json.RawMessage(`{"text":"system"}`),
			Visibility: message.VisibilitySystem,
			Audience:   message.Audience{actor.SystemActorID},
		},
	}
	for i, env := range seed {
		if _, err := app.Viewcache().Apply(ctx, viewsync.PushFrame{
			ChannelID: channel.ID(alice.channelID),
			Seq:       viewsync.Seq(i + 1),
			MessageID: env.ID,
			Envelope:  env,
		}); err != nil {
			t.Fatalf("seed viewcache seq=%d: %v", i+1, err)
		}
	}

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/channels/"+alice.channelID+"/messages?limit=50", nil)
	req.AddCookie(&http.Cookie{Name: identity.CookieName, Value: alice.session})
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET messages: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET messages status=%d want 200", resp.StatusCode)
	}
	var body struct {
		Messages []message.Envelope `json:"messages"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode messages: %v", err)
	}
	got := make([]string, 0, len(body.Messages))
	for _, env := range body.Messages {
		got = append(got, env.ID.String())
	}
	want := []string{"m-public", "m-private-alice"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("visible messages=%v want %v", got, want)
	}
}

func TestViewcacheLimitCapAndResyncRangeValidation(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	client := &http.Client{}
	alice := registerLoginAndCreateChannel(t, client, srv.URL, app, "alice-view-limits@example.com")
	ctx := context.Background()
	for i := 1; i <= 501; i++ {
		env := message.Envelope{
			ID:         message.ID(fmt.Sprintf("m-%03d", i)),
			ChannelID:  channel.ID(alice.channelID),
			Type:       "agent.text",
			Kind:       message.KindEvent,
			Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
			Payload:    json.RawMessage(`{}`),
			Visibility: message.VisibilityPublic,
			Audience:   message.Audience{actor.SystemActorID},
		}
		if _, err := app.Viewcache().Apply(ctx, viewsync.PushFrame{
			ChannelID: channel.ID(alice.channelID),
			Seq:       viewsync.Seq(i),
			MessageID: env.ID,
			Envelope:  env,
		}); err != nil {
			t.Fatalf("seed viewcache seq=%d: %v", i, err)
		}
	}

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/channels/"+alice.channelID+"/messages?limit=999", nil)
	req.AddCookie(&http.Cookie{Name: identity.CookieName, Value: alice.session})
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET messages: %v", err)
	}
	var body struct {
		Messages []message.Envelope `json:"messages"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode messages: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET messages status=%d want 200", resp.StatusCode)
	}
	if len(body.Messages) != 500 {
		t.Fatalf("messages len=%d want capped 500", len(body.Messages))
	}

	badBodies := []string{
		`{"since_seq":2,"until_seq":1}`,
		`{"since_seq":-1,"until_seq":1}`,
		`{"since_seq":1,"until_seq":501}`,
	}
	for _, bad := range badBodies {
		req, _ := http.NewRequest(http.MethodPost,
			srv.URL+"/api/channels/"+alice.channelID+"/resync",
			strings.NewReader(bad))
		req.Header.Set("Content-Type", "application/json")
		req.AddCookie(&http.Cookie{Name: identity.CookieName, Value: alice.session})
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("POST resync %s: %v", bad, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("POST resync %s status=%d want 400", bad, resp.StatusCode)
		}
	}
}

func TestPlacementsRoutesEnforceChannelMembership(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	client := &http.Client{}
	alice := registerLoginAndCreateChannel(t, client, srv.URL, app, "alice-placement-acl@example.com")
	bob := registerLoginAndCreateChannel(t, client, srv.URL, app, "bob-placement-acl@example.com")
	ctx := context.Background()

	if _, _, err := app.Placements().Reserve(ctx, channel.ID(alice.channelID), placement.DaemonID("d-alice"), 1, nil); err != nil {
		t.Fatalf("reserve alice placement: %v", err)
	}
	if _, _, err := app.Placements().Reserve(ctx, channel.ID(bob.channelID), placement.DaemonID("d-bob"), 1, nil); err != nil {
		t.Fatalf("reserve bob placement: %v", err)
	}

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/placements/"+alice.channelID, nil)
	req.AddCookie(&http.Cookie{Name: identity.CookieName, Value: bob.session})
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET placement as non-member: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("GET placement status=%d want 403", resp.StatusCode)
	}

	listReq, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/placements?state=creating", nil)
	listReq.AddCookie(&http.Cookie{Name: identity.CookieName, Value: bob.session})
	listResp, err := client.Do(listReq)
	if err != nil {
		t.Fatalf("GET placements list: %v", err)
	}
	defer func() { _ = listResp.Body.Close() }()
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("GET placements list status=%d want 200", listResp.StatusCode)
	}
	var body struct {
		Placements []struct {
			ChannelID string `json:"channel_id"`
			DaemonID  string `json:"daemon_id"`
		} `json:"placements"`
	}
	if err := json.NewDecoder(listResp.Body).Decode(&body); err != nil {
		t.Fatalf("decode placements list: %v", err)
	}
	got := make(map[string]string, len(body.Placements))
	for _, p := range body.Placements {
		got[p.ChannelID] = p.DaemonID
	}
	if _, ok := got[alice.channelID]; ok {
		t.Fatalf("list leaked alice channel placement: %+v", body.Placements)
	}
	if got[bob.channelID] != "d-bob" {
		t.Fatalf("bob placement daemon=%q want d-bob; placements=%+v", got[bob.channelID], body.Placements)
	}
}

func TestPushhubSubscribeRejectsNonMemberAndBlocksFanout(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	client := &http.Client{}
	alice := registerLoginAndCreateChannel(t, client, srv.URL, app, "alice-pushhub@example.com")
	bob := registerLoginAndCreateChannel(t, client, srv.URL, app, "bob-pushhub@example.com")

	header := http.Header{}
	header.Set("Cookie", (&http.Cookie{Name: identity.CookieName, Value: bob.session}).String())
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	ws, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("dial pushhub: status=%d err=%v", status, err)
	}
	defer func() { _ = ws.Close() }()

	if err := ws.WriteJSON(map[string]string{
		"type":       "subscribe",
		"channel_id": alice.channelID,
	}); err != nil {
		t.Fatalf("subscribe write: %v", err)
	}

	if err := ws.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	var reject struct {
		Type      string `json:"type"`
		ChannelID string `json:"channel_id"`
		Error     string `json:"error"`
	}
	if err := ws.ReadJSON(&reject); err != nil {
		t.Fatalf("read subscribe reject: %v", err)
	}
	if reject.Type != "subscribe_rejected" || reject.ChannelID != alice.channelID {
		t.Fatalf("reject frame=%+v want subscribe_rejected for %s", reject, alice.channelID)
	}
	if got := app.Pushhub().SubscriberCount(channel.ID(alice.channelID)); got != 0 {
		t.Fatalf("subscriber count=%d want 0", got)
	}

	app.Pushhub().PushMessage(channel.ID(alice.channelID), 1, message.Envelope{
		ID:         "m-private-push",
		ChannelID:  channel.ID(alice.channelID),
		Type:       "agent.text",
		Kind:       message.KindEvent,
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
		Payload:    json.RawMessage(`{"text":"secret"}`),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{actor.SystemActorID},
	})
	if err := ws.SetReadDeadline(time.Now().Add(150 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if _, raw, err := ws.ReadMessage(); err == nil {
		t.Fatalf("unexpected push for non-member: %s", string(raw))
	}
}

func TestPushhubFanoutAppliesSubscriberVisibility(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	client := &http.Client{}
	alice := registerLoginAndCreateChannel(t, client, srv.URL, app, "alice-push-visible@example.com")
	bob := registerLoginAndCreateChannel(t, client, srv.URL, app, "bob-push-visible@example.com")
	aliceID := userIDByEmail(t, app, "alice-push-visible@example.com")
	bobID := userIDByEmail(t, app, "bob-push-visible@example.com")

	addReq, _ := http.NewRequest(http.MethodPost,
		srv.URL+"/api/channels/"+alice.channelID+"/members",
		strings.NewReader(`{"user_id":"`+bobID+`"}`))
	addReq.Header.Set("Content-Type", "application/json")
	addReq.AddCookie(&http.Cookie{Name: identity.CookieName, Value: alice.session})
	addResp, err := client.Do(addReq)
	if err != nil {
		t.Fatalf("add member: %v", err)
	}
	_ = addResp.Body.Close()
	if addResp.StatusCode != http.StatusCreated {
		t.Fatalf("add member status=%d want 201", addResp.StatusCode)
	}

	dial := func(cookie string) *websocket.Conn {
		t.Helper()
		header := http.Header{}
		header.Set("Cookie", (&http.Cookie{Name: identity.CookieName, Value: cookie}).String())
		wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
		ws, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
		if err != nil {
			status := 0
			if resp != nil {
				status = resp.StatusCode
			}
			t.Fatalf("dial pushhub: status=%d err=%v", status, err)
		}
		if err := ws.WriteJSON(map[string]string{"type": "subscribe", "channel_id": alice.channelID}); err != nil {
			t.Fatalf("subscribe write: %v", err)
		}
		return ws
	}
	aliceWS := dial(alice.session)
	defer func() { _ = aliceWS.Close() }()
	bobWS := dial(bob.session)
	defer func() { _ = bobWS.Close() }()
	pollUntil(t, time.Second, func() bool {
		return app.Pushhub().SubscriberCount(channel.ID(alice.channelID)) == 2
	})

	app.Pushhub().PushMessage(channel.ID(alice.channelID), 1, message.Envelope{
		ID:         "m-public-push",
		ChannelID:  channel.ID(alice.channelID),
		Type:       "agent.text",
		Kind:       message.KindEvent,
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
		Payload:    json.RawMessage(`{"text":"public"}`),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{actor.SystemActorID},
	})
	for name, ws := range map[string]*websocket.Conn{"alice": aliceWS, "bob": bobWS} {
		var frame struct {
			Type     string           `json:"type"`
			Envelope message.Envelope `json:"envelope"`
		}
		if err := ws.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatalf("%s set deadline: %v", name, err)
		}
		if err := ws.ReadJSON(&frame); err != nil {
			t.Fatalf("%s read public: %v", name, err)
		}
		if frame.Type != "message" || frame.Envelope.ID != "m-public-push" {
			t.Fatalf("%s public frame=%+v want m-public-push", name, frame)
		}
	}

	app.Pushhub().PushMessage(channel.ID(alice.channelID), 2, message.Envelope{
		ID:         "m-private-alice-push",
		ChannelID:  channel.ID(alice.channelID),
		Type:       "agent.text",
		Kind:       message.KindEvent,
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
		Payload:    json.RawMessage(`{"text":"alice"}`),
		Visibility: message.VisibilityPrivate,
		Audience:   message.Audience{actor.ActorID("user:" + aliceID)},
	})
	var privateFrame struct {
		Type     string           `json:"type"`
		Envelope message.Envelope `json:"envelope"`
	}
	if err := aliceWS.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("alice set deadline: %v", err)
	}
	if err := aliceWS.ReadJSON(&privateFrame); err != nil {
		t.Fatalf("alice read private: %v", err)
	}
	if privateFrame.Type != "message" || privateFrame.Envelope.ID != "m-private-alice-push" {
		t.Fatalf("alice private frame=%+v want m-private-alice-push", privateFrame)
	}
	if err := bobWS.SetReadDeadline(time.Now().Add(150 * time.Millisecond)); err != nil {
		t.Fatalf("bob set deadline: %v", err)
	}
	if _, raw, err := bobWS.ReadMessage(); err == nil {
		t.Fatalf("bob received private message for alice: %s", string(raw))
	}
}

func TestPushhubRevokesSubscriptionAfterMemberRemoval(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	client := &http.Client{}
	alice := registerLoginAndCreateChannel(t, client, srv.URL, app, "alice-pushhub-revoke@example.com")
	bob := registerLoginAndCreateChannel(t, client, srv.URL, app, "bob-pushhub-revoke@example.com")
	bobID := userIDByEmail(t, app, "bob-pushhub-revoke@example.com")

	addReq, _ := http.NewRequest(http.MethodPost,
		srv.URL+"/api/channels/"+alice.channelID+"/members",
		strings.NewReader(`{"user_id":"`+bobID+`"}`))
	addReq.Header.Set("Content-Type", "application/json")
	addReq.AddCookie(&http.Cookie{Name: identity.CookieName, Value: alice.session})
	addResp, err := client.Do(addReq)
	if err != nil {
		t.Fatalf("add member: %v", err)
	}
	_ = addResp.Body.Close()
	if addResp.StatusCode != http.StatusCreated {
		t.Fatalf("add member status=%d want 201", addResp.StatusCode)
	}

	header := http.Header{}
	header.Set("Cookie", (&http.Cookie{Name: identity.CookieName, Value: bob.session}).String())
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	ws, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("dial pushhub: status=%d err=%v", status, err)
	}
	defer func() { _ = ws.Close() }()

	if err := ws.WriteJSON(map[string]string{
		"type":       "subscribe",
		"channel_id": alice.channelID,
	}); err != nil {
		t.Fatalf("subscribe write: %v", err)
	}
	pollUntil(t, time.Second, func() bool {
		return app.Pushhub().SubscriberCount(channel.ID(alice.channelID)) == 1
	})

	delReq, _ := http.NewRequest(http.MethodDelete,
		srv.URL+"/api/channels/"+alice.channelID+"/members/"+bobID, nil)
	delReq.AddCookie(&http.Cookie{Name: identity.CookieName, Value: alice.session})
	delResp, err := client.Do(delReq)
	if err != nil {
		t.Fatalf("delete member: %v", err)
	}
	_ = delResp.Body.Close()
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("delete member status=%d want 200", delResp.StatusCode)
	}
	if _, err := app.Catalog().ProcessDueMemberTransitions(context.Background(), 10); err != nil {
		t.Fatalf("process member transition: %v", err)
	}
	pollUntil(t, time.Second, func() bool {
		return app.Pushhub().SubscriberCount(channel.ID(alice.channelID)) == 0
	})

	var revoked struct {
		Type      string `json:"type"`
		ChannelID string `json:"channel_id"`
		Error     string `json:"error"`
	}
	if err := ws.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if err := ws.ReadJSON(&revoked); err != nil {
		t.Fatalf("read revoke frame: %v", err)
	}
	if revoked.Type != "subscribe_revoked" || revoked.ChannelID != alice.channelID || revoked.Error != "membership_revoked" {
		t.Fatalf("revoke frame=%+v want subscribe_revoked membership_revoked", revoked)
	}

	app.Pushhub().PushMessage(channel.ID(alice.channelID), 1, message.Envelope{
		ID:         "m-after-revoke",
		ChannelID:  channel.ID(alice.channelID),
		Type:       "agent.text",
		Kind:       message.KindEvent,
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
		Payload:    json.RawMessage(`{"text":"secret"}`),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{actor.SystemActorID},
	})
	if err := ws.SetReadDeadline(time.Now().Add(150 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if _, raw, err := ws.ReadMessage(); err == nil {
		t.Fatalf("unexpected message after membership removal: %s", string(raw))
	}
}

func TestHandleWriteMessageResponseInheritsParentCorrelation(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	client := &http.Client{}
	sess := registerLoginAndCreateChannel(t, client, srv.URL, app, "alice-response-corr@example.com")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	parent := message.Envelope{
		ID:            "parent-req",
		ChannelID:     channel.ID(sess.channelID),
		Type:          "human.text",
		Kind:          message.KindRequest,
		CorrelationID: "corr-parent",
		Sender:        message.Sender{Kind: actor.KindAgent, ID: "agent:requester"},
		Visibility:    message.VisibilityPublic,
		Audience:      message.Audience{"user:alice"},
		Payload:       json.RawMessage(`{"text":"question"}`),
	}
	if _, err := app.Viewcache().Apply(ctx, viewsync.PushFrame{
		ChannelID: channel.ID(sess.channelID),
		Seq:       1,
		MessageID: parent.ID,
		Envelope:  parent,
	}); err != nil {
		t.Fatalf("seed parent viewcache: %v", err)
	}

	if err := app.Daemonbus().RegisterDaemon(ctx, placement.DaemonID("d-corr"), "h", "v", 0, "test-daemon"); err != nil {
		t.Fatalf("RegisterDaemon: %v", err)
	}
	epoch, err := app.Daemonbus().IssueConnectionEpoch(ctx, placement.DaemonID("d-corr"))
	if err != nil {
		t.Fatalf("IssueConnectionEpoch: %v", err)
	}
	svrTx, dmnTx := newPipePair()
	conn := daemonbus.NewConnection(placement.DaemonID("d-corr"), epoch, svrTx)
	app.Daemonbus().Register(conn)
	defer app.Daemonbus().Unregister(placement.DaemonID("d-corr"))
	go func() { _ = conn.Run(ctx, app.DaemonbusHandlers()) }()

	p, createReq, err := app.Placements().Reserve(ctx, channel.ID(sess.channelID), placement.DaemonID("d-corr"), placement.ConnectionEpoch(epoch), nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	// Phase 2 ack carries daemon-generated fencing tuple.
	ack := placement.CreateChannelAck{
		ChannelID:       p.ChannelID,
		CreateRequestID: createReq.CreateRequestID,
		OwnerEpoch:      1,
		FencingToken:    "daemon-tok-corr",
		DaemonID:        placement.DaemonID("d-corr"),
		Result:          placement.CreateChannelAccepted,
	}
	if ok, err := app.Placements().Activate(ctx, ack, placement.ConnectionEpoch(epoch)); err != nil || !ok {
		t.Fatalf("Activate: ok=%v err=%v", ok, err)
	}

	respCh := make(chan *http.Response, 1)
	errCh := make(chan error, 1)
	go func() {
		body, _ := json.Marshal(writeBody{
			ID:       "msg-response-corr",
			Type:     "human.text",
			Kind:     string(message.KindResponse),
			ParentID: parent.ID.String(),
			Payload:  json.RawMessage(`{"text":"answer"}`),
			Audience: []string{"agent:requester"},
		})
		req, _ := http.NewRequest(http.MethodPost,
			srv.URL+"/api/channels/"+sess.channelID+"/messages",
			strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		req.AddCookie(&http.Cookie{Name: identity.CookieName, Value: sess.session})
		resp, err := client.Do(req)
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp
	}()

	frame, err := dmnTx.ReadFrame(ctx)
	if err != nil {
		t.Fatalf("read write_message: %v", err)
	}
	var body kerneldaemonbus.WriteMessageBody
	if err := json.Unmarshal(frame.Payload, &body); err != nil {
		t.Fatalf("decode write body: %v", err)
	}
	if got := body.EnvelopePartial.CorrelationID; got != "corr-parent" {
		t.Fatalf("envelope_partial.correlation_id=%q want corr-parent", got)
	}
	ackBody := kerneldaemonbus.WriteMessageAckBody{
		FrameID:   body.FrameID,
		Accepted:  true,
		MessageID: "response-1",
		Seq:       2,
	}
	raw, _ := json.Marshal(ackBody)
	if err := dmnTx.WriteFrame(ctx, kerneldaemonbus.Frame{
		FrameID:               frame.FrameID,
		FrameKind:             kerneldaemonbus.FrameTypeControlWriteMessageAck,
		DaemonID:              "d-corr",
		DaemonConnectionEpoch: epoch,
		Payload:               raw,
	}); err != nil {
		t.Fatalf("write ack: %v", err)
	}

	select {
	case err := <-errCh:
		t.Fatalf("POST response: %v", err)
	case resp := <-respCh:
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status=%d want 200", resp.StatusCode)
		}
		var out map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if out["correlation_id"] != "corr-parent" {
			t.Fatalf("response correlation_id=%v want corr-parent", out["correlation_id"])
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

// writeBody mirrors writeMessageReq (unexported in gateway). Used by
// TestHandleWriteMessage_* to build wire-shape JSON. R4-3: `id` is
// caller-supplied per L3 §1.8.1; tests fill a fresh uuid by default.
type writeBody struct {
	ID            string          `json:"id,omitempty"`
	Type          string          `json:"type"`
	Kind          string          `json:"kind,omitempty"`
	Payload       json.RawMessage `json:"payload"`
	ParentID      string          `json:"parent_id,omitempty"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	Audience      []string        `json:"audience"`
}

// registerLoginAndCreateChannel is a small fixture that returns a
// session cookie + the channel id the freshly-registered user owns.
// The user is auto-added as the channel's first member by the catalog
// service, satisfying the gateway handler's membership check.
type sessionAndChannel struct {
	session   string
	channelID string
}

func registerLoginAndCreateChannel(t *testing.T, c *http.Client, baseURL string, app *gateway.App, email string) sessionAndChannel {
	t.Helper()
	ctx := context.Background()

	post(t, c, baseURL+"/api/identity/verification/issue",
		`{"email":"`+email+`"}`, http.StatusAccepted)

	var code string
	if err := app.DB().QueryRowContext(ctx,
		`SELECT code FROM verification_codes WHERE email = ? ORDER BY created_at DESC LIMIT 1`,
		email,
	).Scan(&code); err != nil {
		t.Fatalf("read code: %v", err)
	}

	post(t, c, baseURL+"/api/identity/register",
		`{"email":"`+email+`","password":"topsecret123","code":"`+code+`"}`,
		http.StatusAccepted)

	loginResp := postRaw(t, c, baseURL+"/api/identity/login",
		`{"email":"`+email+`","password":"topsecret123"}`, http.StatusOK)
	defer func() { _ = loginResp.Body.Close() }()
	cookie := extractSessionCookie(t, loginResp)
	if cookie == "" {
		t.Fatal("session cookie missing")
	}

	wsReq, _ := http.NewRequest(http.MethodPost, baseURL+"/api/workspaces",
		strings.NewReader(`{"name":"Demo"}`))
	wsReq.Header.Set("Content-Type", "application/json")
	wsReq.AddCookie(&http.Cookie{Name: identity.CookieName, Value: cookie})
	wsResp, err := c.Do(wsReq)
	if err != nil || wsResp.StatusCode != http.StatusCreated {
		t.Fatalf("workspace err=%v status=%d", err, wsResp.StatusCode)
	}
	var ws struct{ ID string }
	_ = json.NewDecoder(wsResp.Body).Decode(&ws)
	_ = wsResp.Body.Close()

	chReq, _ := http.NewRequest(http.MethodPost,
		baseURL+"/api/workspaces/"+ws.ID+"/channels",
		strings.NewReader(`{"name":"general"}`))
	chReq.Header.Set("Content-Type", "application/json")
	chReq.AddCookie(&http.Cookie{Name: identity.CookieName, Value: cookie})
	chResp, err := c.Do(chReq)
	if err != nil || chResp.StatusCode != http.StatusCreated {
		t.Fatalf("channel err=%v status=%d", err, chResp.StatusCode)
	}
	var chBody struct {
		Channel struct{ ID string } `json:"channel"`
	}
	_ = json.NewDecoder(chResp.Body).Decode(&chBody)
	_ = chResp.Body.Close()

	return sessionAndChannel{session: cookie, channelID: chBody.Channel.ID}
}

func pollUntil(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !fn() {
		t.Fatalf("condition not met within %s", timeout)
	}
}

func TestDeviceTransitNoRouteProxyFacadePayloadSynthesizesTerminalEnvelope(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	const daemonID placement.DaemonID = "d-proxy-no-route"
	const chID channel.ID = "ch-proxy-no-route"
	const adapterActor actor.ActorID = "tool:kimi"

	if err := app.Daemonbus().RegisterDaemon(ctx, daemonID, "h", "v", 0, "test-daemon"); err != nil {
		t.Fatalf("RegisterDaemon: %v", err)
	}
	epoch, err := app.Daemonbus().IssueConnectionEpoch(ctx, daemonID)
	if err != nil {
		t.Fatalf("IssueConnectionEpoch: %v", err)
	}
	svr, dmn := newPipePair()
	conn := daemonbus.NewConnection(daemonID, epoch, svr)
	app.Daemonbus().Register(conn)
	defer app.Daemonbus().Unregister(daemonID)

	p, createReq, err := app.Placements().Reserve(ctx, chID, daemonID, placement.ConnectionEpoch(epoch), nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	ack := placement.CreateChannelAck{
		FrameID:         "ack-proxy-no-route",
		ChannelID:       p.ChannelID,
		CreateRequestID: createReq.CreateRequestID,
		OwnerEpoch:      1,
		FencingToken:    "daemon-tok-proxy-no-route",
		DaemonID:        daemonID,
		Result:          placement.CreateChannelAccepted,
	}
	if ok, err := app.Placements().Activate(ctx, ack, placement.ConnectionEpoch(epoch)); err != nil || !ok {
		t.Fatalf("Activate: ok=%v err=%v", ok, err)
	}

	req := message.Envelope{
		ID:            "req-proxy-no-route",
		TS:            1_700_000_000_000,
		ChannelID:     chID,
		Sender:        message.Sender{Kind: actor.KindAgent, ID: "agent:author"},
		Kind:          message.KindRequest,
		Type:          "kimi.ask",
		Payload:       json.RawMessage(`{"prompt":"hi"}`),
		CorrelationID: "trace-proxy-no-route",
		Visibility:    message.VisibilityPublic,
		Audience:      message.Audience{adapterActor},
	}
	reqRaw, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request envelope: %v", err)
	}
	bodyRaw, err := json.Marshal(struct {
		Direction     string          `json:"direction"`
		RequestID     string          `json:"request_id,omitempty"`
		CorrelationID string          `json:"correlation_id,omitempty"`
		Payload       json.RawMessage `json:"payload,omitempty"`
	}{
		Direction:     "to_device",
		RequestID:     req.ID.String(),
		CorrelationID: req.CorrelationID.String(),
		Payload:       reqRaw,
	})
	if err != nil {
		t.Fatalf("marshal transit body: %v", err)
	}
	sf := devicetransit.SendFrame{
		AdapterActorID: adapterActor,
		ChannelID:      chID,
		Body:           bodyRaw,
	}
	framePayload, err := json.Marshal(sf)
	if err != nil {
		t.Fatalf("marshal device_transit.recv payload: %v", err)
	}
	if err := app.DaemonbusHandlers().OnDeviceTransitRecv(ctx, conn, kerneldaemonbus.Frame{
		FrameKind:             kerneldaemonbus.FrameTypeDeviceTransitRecv,
		DaemonID:              daemonID,
		DaemonConnectionEpoch: epoch,
		Payload:               framePayload,
	}); err != nil {
		t.Fatalf("OnDeviceTransitRecv: %v", err)
	}

	outFrame, err := dmn.ReadFrame(ctx)
	if err != nil {
		t.Fatalf("read synthetic callback frame: %v", err)
	}
	if outFrame.FrameKind != kerneldaemonbus.FrameTypeDeviceTransitSend {
		t.Fatalf("frame kind=%q want %q", outFrame.FrameKind, kerneldaemonbus.FrameTypeDeviceTransitSend)
	}
	var outSF devicetransit.SendFrame
	if err := json.Unmarshal(outFrame.Payload, &outSF); err != nil {
		t.Fatalf("decode synthetic send frame: %v", err)
	}
	if outSF.AdapterActorID != adapterActor || outSF.ChannelID != chID {
		t.Fatalf("send frame=%+v want actor=%s channel=%s", outSF, adapterActor, chID)
	}
	var outBody struct {
		Direction     string          `json:"direction"`
		RequestID     string          `json:"request_id,omitempty"`
		CorrelationID string          `json:"correlation_id,omitempty"`
		Payload       json.RawMessage `json:"payload,omitempty"`
	}
	if err := json.Unmarshal(outSF.Body, &outBody); err != nil {
		t.Fatalf("decode synthetic transit body: %v", err)
	}
	if outBody.RequestID != req.ID.String() || outBody.CorrelationID != req.CorrelationID.String() {
		t.Fatalf("transit body request/correlation=%s/%s", outBody.RequestID, outBody.CorrelationID)
	}

	var resp message.Envelope
	if err := json.Unmarshal(outBody.Payload, &resp); err != nil {
		t.Fatalf("synthetic payload is not an envelope: %v", err)
	}
	if resp.Kind != message.KindResponse || resp.ParentID != req.ID || resp.Type != req.Type {
		t.Fatalf("response identity=%+v", resp)
	}
	if resp.CorrelationID != req.CorrelationID {
		t.Fatalf("correlation_id=%q want %q", resp.CorrelationID, req.CorrelationID)
	}
	if resp.Sender.ID != actor.SystemActorID || resp.Sender.Kind != actor.KindSystem {
		t.Fatalf("sender=%+v want system fallback", resp.Sender)
	}
	if len(resp.Audience) != 1 || resp.Audience[0] != req.Sender.ID {
		t.Fatalf("audience=%v want %s", resp.Audience, req.Sender.ID)
	}
	var payload struct {
		Status    string `json:"status"`
		Reason    string `json:"reason"`
		ErrorCode string `json:"error_code"`
	}
	if err := json.Unmarshal(resp.Payload, &payload); err != nil {
		t.Fatalf("decode response payload: %v", err)
	}
	if payload.Status != "failed" ||
		payload.Reason != string(message.TerminalReceiverUnavailable) ||
		payload.ErrorCode != "device_not_bound" {
		t.Fatalf("response payload=%+v", payload)
	}

	mod, err := proxyfacade.New(adapter.Declaration{
		Name:         "kimi",
		ActorID:      adapterActor,
		Types:        []string{req.Type},
		Binding:      actor.BindingRuntimeInboundViaRelay,
		MaxPendingMs: 30_000,
	})
	if err != nil {
		t.Fatalf("proxy facade New: %v", err)
	}
	chain := &proxyFacadeCaptureChain{}
	if err := mod.Init(ctx, &adapter.ModuleContext{
		AdapterName:    "kimi",
		AdapterActorID: adapterActor,
		ChannelID:      chID,
		DeviceTransit:  proxyFacadeNoopTransit{},
		HarnessChain:   chain,
	}); err != nil {
		t.Fatalf("proxy facade Init: %v", err)
	}
	if err := mod.OnExternalCallback(ctx, outBody.Payload); err != nil {
		t.Fatalf("proxy facade OnExternalCallback rejected synthetic envelope: %v", err)
	}
	if chain.env == nil || chain.env.ID != resp.ID || chain.env.ParentID != req.ID {
		t.Fatalf("proxy facade wrote env=%+v want response=%s parent=%s", chain.env, resp.ID, req.ID)
	}
}

// TestMockDaemonViewSyncRoundTrip drives a fake daemonbus connection
// through the App's dispatch loop:
//
//  1. fake daemon pushes viewsync.push seq=1 → server.viewcache.Apply
//     → fan-out (no subscriber yet, but pushhub Service call must succeed)
//  2. ack frame must arrive with last_received_seq=1
//  3. push seq=3 — gap — ack still 1
//  4. push seq=2 — drains; ack 3
//
// This is the §T6 mock validation: viewcache + daemonbus end-to-end.
func TestMockDaemonViewSyncRoundTrip(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Register a daemon row so IssueConnectionEpoch works.
	if err := app.Daemonbus().RegisterDaemon(ctx, placement.DaemonID("mock-d1"), "localhost", "v0", 32, "test-daemon"); err != nil {
		t.Fatalf("RegisterDaemon: %v", err)
	}
	epoch, err := app.Daemonbus().IssueConnectionEpoch(ctx, placement.DaemonID("mock-d1"))
	if err != nil {
		t.Fatalf("IssueConnectionEpoch: %v", err)
	}

	// Build in-memory pipe and wire a Connection.
	svr, dmn := newPipePair()
	conn := daemonbus.NewConnection(placement.DaemonID("mock-d1"), epoch, svr)
	app.Daemonbus().Register(conn)
	defer app.Daemonbus().Unregister(placement.DaemonID("mock-d1"))

	go func() { _ = conn.Run(ctx, app.DaemonbusHandlers()) }()

	const pushOwnerEpoch placement.OwnerEpoch = 1
	const pushFencingToken placement.FencingToken = "daemon-tok-ch-A"
	_, req, err := app.Placements().Reserve(ctx, channel.ID("ch-A"), placement.DaemonID("mock-d1"), placement.ConnectionEpoch(epoch), nil)
	if err != nil {
		t.Fatalf("Reserve ch-A: %v", err)
	}
	ack := placement.CreateChannelAck{
		FrameID:         "ack-ch-A",
		ChannelID:       req.ChannelID,
		CreateRequestID: req.CreateRequestID,
		OwnerEpoch:      pushOwnerEpoch,
		FencingToken:    pushFencingToken,
		DaemonID:        placement.DaemonID("mock-d1"),
		Result:          placement.CreateChannelAccepted,
	}
	if ok, err := app.Placements().Activate(ctx, ack, placement.ConnectionEpoch(epoch)); err != nil || !ok {
		t.Fatalf("Activate ch-A ok=%v err=%v", ok, err)
	}

	send := func(ft kerneldaemonbus.FrameType, payload any) {
		raw, _ := json.Marshal(payload)
		if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
			FrameID: kerneldaemonbus.FrameID("f-" + ft.String()), FrameKind: ft,
			DaemonID: "mock-d1", DaemonConnectionEpoch: epoch, Payload: raw,
		}); err != nil {
			t.Fatalf("write %s: %v", ft, err)
		}
	}

	mkPush := func(seq viewsync.Seq) viewsync.PushFrame {
		id := message.ID("m-" + itoa(int64(seq)))
		return viewsync.PushFrame{
			ChannelID:    channel.ID("ch-A"),
			Seq:          seq,
			MessageID:    id,
			OwnerEpoch:   pushOwnerEpoch,
			FencingToken: pushFencingToken,
			Envelope: message.Envelope{
				ID: id, TS: int64(seq) * 1000, ChannelID: "ch-A",
				Sender: message.Sender{Kind: actor.KindAgent, ID: "a"},
				Kind:   message.KindEvent, Type: "agent.text",
				Payload:    json.RawMessage(`{}`),
				Visibility: message.VisibilityPublic, Audience: message.Audience{actor.SystemActorID},
			},
		}
	}

	expect := func(want viewsync.Seq) {
		t.Helper()
		for {
			f, err := dmn.ReadFrame(ctx)
			if err != nil {
				t.Fatalf("read ack: %v", err)
			}
			if f.FrameKind != kerneldaemonbus.FrameTypeViewsyncAck {
				continue
			}
			var ack viewsync.AckFrame
			if err := json.Unmarshal(f.Payload, &ack); err != nil {
				t.Fatalf("decode ack: %v", err)
			}
			if int64(ack.LastReceivedSeq) != int64(want) {
				t.Fatalf("ack=%d want %d", ack.LastReceivedSeq, want)
			}
			if !ack.Accepted {
				t.Fatalf("ack rejected: %+v", ack)
			}
			return
		}
	}

	send(kerneldaemonbus.FrameTypeViewsyncPush, mkPush(1))
	expect(1)

	send(kerneldaemonbus.FrameTypeViewsyncPush, mkPush(3))
	expect(1) // gap; cursor stays at 1

	send(kerneldaemonbus.FrameTypeViewsyncPush, mkPush(2))
	expect(3) // drain 3 from buffer; cursor jumps to 3

	cur, _ := app.Viewcache().Cursor(ctx, channel.ID("ch-A"))
	if int64(cur) != 3 {
		t.Errorf("final cursor=%d want 3", cur)
	}

	_ = conn.Close()
}

// TestMockDaemonCreateChannelACK exercises the placement path:
//
//  1. App.placements.Reserve → row state=creating
//  2. fake daemon sends control.create_channel_ack with matching
//     fields → OnCreateChannelAck → CASActivate
//  3. row state advances to active
func TestMockDaemonCreateChannelACK(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := app.Daemonbus().RegisterDaemon(ctx, placement.DaemonID("d1"), "h", "v", 0, "test-daemon"); err != nil {
		t.Fatalf("RegisterDaemon: %v", err)
	}
	epoch, _ := app.Daemonbus().IssueConnectionEpoch(ctx, placement.DaemonID("d1"))

	svr, dmn := newPipePair()
	conn := daemonbus.NewConnection(placement.DaemonID("d1"), epoch, svr)
	app.Daemonbus().Register(conn)
	defer app.Daemonbus().Unregister(placement.DaemonID("d1"))

	go func() { _ = conn.Run(ctx, app.DaemonbusHandlers()) }()

	_, req, err := app.Placements().Reserve(ctx, channel.ID("ch-Y"), placement.DaemonID("d1"), placement.ConnectionEpoch(epoch), nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	// Phase 2 ack carries daemon-generated fencing tuple (proto-foundation
	// §3.3.3 + impl-layer2 §3.2.2). The Phase 3 CAS will write these
	// values into the placement row.
	ack := placement.CreateChannelAck{
		FrameID: "ack-1", ChannelID: req.ChannelID, CreateRequestID: req.CreateRequestID,
		OwnerEpoch: 1, FencingToken: "daemon-tok-gateway",
		DaemonID: placement.DaemonID("d1"), DaemonEpoch: 1, Result: placement.CreateChannelAccepted,
	}
	raw, _ := json.Marshal(ack)
	if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
		FrameID: "f-ack", FrameKind: kerneldaemonbus.FrameTypeControlCreateChannelAck,
		DaemonID: "d1", DaemonConnectionEpoch: epoch, Payload: raw,
	}); err != nil {
		t.Fatalf("write ack: %v", err)
	}

	// Poll for state=active.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		p, ok, _ := app.Placements().Get(ctx, req.ChannelID)
		if ok && p.State == placement.StateActive {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("placement never advanced to active")
}

func TestCreateChannelAckCASFalseOrphansAndUnbinds(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := app.Daemonbus().RegisterDaemon(ctx, placement.DaemonID("d-cas-false"), "h", "v", 0, "test-daemon"); err != nil {
		t.Fatalf("RegisterDaemon: %v", err)
	}
	epoch, _ := app.Daemonbus().IssueConnectionEpoch(ctx, placement.DaemonID("d-cas-false"))

	svr, dmn := newPipePair()
	conn := daemonbus.NewConnection(placement.DaemonID("d-cas-false"), epoch, svr)
	app.Daemonbus().Register(conn)
	defer app.Daemonbus().Unregister(placement.DaemonID("d-cas-false"))

	runDone := make(chan error, 1)
	go func() { runDone <- conn.Run(ctx, app.DaemonbusHandlers()) }()

	_, req, err := app.Placements().Reserve(ctx, channel.ID("ch-cas-false"), placement.DaemonID("d-cas-false"), placement.ConnectionEpoch(epoch), nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	ack := placement.CreateChannelAck{
		FrameID: "ack-cas-false", ChannelID: req.ChannelID, CreateRequestID: req.CreateRequestID,
		OwnerEpoch: 1, FencingToken: "", DaemonID: placement.DaemonID("d-cas-false"),
		Result: placement.CreateChannelAccepted,
	}
	raw, _ := json.Marshal(ack)
	if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
		FrameID: "f-cas-false", FrameKind: kerneldaemonbus.FrameTypeControlCreateChannelAck,
		DaemonID: "d-cas-false", DaemonConnectionEpoch: epoch, Payload: raw,
	}); err != nil {
		t.Fatalf("write ack: %v", err)
	}

	frame, err := dmn.ReadFrame(ctx)
	if err != nil {
		t.Fatalf("read unbind: %v", err)
	}
	if frame.FrameKind != kerneldaemonbus.FrameTypeControlUnbindChannel {
		t.Fatalf("frame=%s want unbind_channel", frame.FrameKind)
	}
	var unbind kerneldaemonbus.UnbindChannelBody
	if err := json.Unmarshal(frame.Payload, &unbind); err != nil {
		t.Fatalf("decode unbind: %v", err)
	}
	if unbind.ChannelID != req.ChannelID || unbind.OwnerEpoch != 1 || unbind.Reason != kerneldaemonbus.UnbindChannelReasonAbandon {
		t.Fatalf("unbind=%+v", unbind)
	}
	got, ok, err := app.Placements().Get(ctx, req.ChannelID)
	if err != nil || !ok {
		t.Fatalf("Get ok=%v err=%v", ok, err)
	}
	if got.State != placement.StateOrphan {
		t.Fatalf("placement state=%s want orphan", got.State)
	}

	select {
	case err := <-runDone:
		if err == nil || !strings.Contains(err.Error(), "CAS rejected") {
			t.Fatalf("Run err=%v want CAS rejected", err)
		}
	case <-ctx.Done():
		t.Fatal("connection did not surface CAS rejected error")
	}
}

// TestViewSyncGapDrainFanOut covers FIX-T5: when the daemon pushes a
// gap-then-fill sequence (1, 3, 2), the gateway must:
//
//  1. fan-out seq 1 to subscribers (contiguous)
//  2. NOT fan-out seq 3 alone (gap — client must not see it before 2)
//  3. fan-out seq 2 AND seq 3, in that order, when 2 closes the gap
//
// Ack frames still carry only the contiguous cursor (1, 1, 3).
func TestViewSyncGapDrainFanOut(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Suppress the auto-trigger goroutine — no daemon-side resync
	// server is wired in this test; we feed the recovery frame
	// manually via dmn.WriteFrame.
	app.Viewcache().SetFireResyncForTest(func(channel.ID, viewsync.Seq, viewsync.Seq) {})

	// Capture fan-out from pushhub.
	var (
		obsMu sync.Mutex
		obs   []pushhub.PushedFrame
	)
	cancelObs := app.Pushhub().RegisterPushObserverForTest(channel.ID("ch-fanout"), func(f pushhub.PushedFrame) {
		obsMu.Lock()
		obs = append(obs, f)
		obsMu.Unlock()
	})
	defer cancelObs()

	if err := app.Daemonbus().RegisterDaemon(ctx, placement.DaemonID("d-fanout"), "h", "v", 0, "test-daemon"); err != nil {
		t.Fatalf("RegisterDaemon: %v", err)
	}
	epoch, _ := app.Daemonbus().IssueConnectionEpoch(ctx, placement.DaemonID("d-fanout"))

	svr, dmn := newPipePair()
	conn := daemonbus.NewConnection(placement.DaemonID("d-fanout"), epoch, svr)
	app.Daemonbus().Register(conn)
	defer app.Daemonbus().Unregister(placement.DaemonID("d-fanout"))
	go func() { _ = conn.Run(ctx, app.DaemonbusHandlers()) }()

	const pushOwnerEpoch placement.OwnerEpoch = 1
	const pushFencingToken placement.FencingToken = "daemon-tok-fanout"
	_, req, err := app.Placements().Reserve(ctx, channel.ID("ch-fanout"), placement.DaemonID("d-fanout"), placement.ConnectionEpoch(epoch), nil)
	if err != nil {
		t.Fatalf("Reserve ch-fanout: %v", err)
	}
	ack := placement.CreateChannelAck{
		FrameID:         "ack-ch-fanout",
		ChannelID:       req.ChannelID,
		CreateRequestID: req.CreateRequestID,
		OwnerEpoch:      pushOwnerEpoch,
		FencingToken:    pushFencingToken,
		DaemonID:        placement.DaemonID("d-fanout"),
		Result:          placement.CreateChannelAccepted,
	}
	if ok, err := app.Placements().Activate(ctx, ack, placement.ConnectionEpoch(epoch)); err != nil || !ok {
		t.Fatalf("Activate ch-fanout ok=%v err=%v", ok, err)
	}

	mkPush := func(seq viewsync.Seq) viewsync.PushFrame {
		id := message.ID("m-" + itoa(int64(seq)))
		return viewsync.PushFrame{
			ChannelID:    channel.ID("ch-fanout"),
			Seq:          seq,
			MessageID:    id,
			OwnerEpoch:   pushOwnerEpoch,
			FencingToken: pushFencingToken,
			Envelope: message.Envelope{
				ID: id, TS: int64(seq) * 1000, ChannelID: "ch-fanout",
				Sender: message.Sender{Kind: actor.KindAgent, ID: "a"},
				Kind:   message.KindEvent, Type: "agent.text",
				Payload:    json.RawMessage(`{}`),
				Visibility: message.VisibilityPublic, Audience: message.Audience{actor.SystemActorID},
			},
		}
	}
	send := func(payload viewsync.PushFrame) {
		raw, _ := json.Marshal(payload)
		if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
			FrameID:   kerneldaemonbus.FrameID("f-" + itoa(int64(payload.Seq))),
			FrameKind: kerneldaemonbus.FrameTypeViewsyncPush,
			DaemonID:  "d-fanout", DaemonConnectionEpoch: epoch, Payload: raw,
		}); err != nil {
			t.Fatalf("write push %d: %v", payload.Seq, err)
		}
		// Drain matching ack so the gateway dispatch loop is unblocked
		// before the next push goes in.
		if _, err := dmn.ReadFrame(ctx); err != nil {
			t.Fatalf("read ack %d: %v", payload.Seq, err)
		}
	}

	// 1. push 1 — contiguous → fan-out [1]
	send(mkPush(1))
	// 2. push 3 — gap → fan-out unchanged
	send(mkPush(3))
	// 3. push 2 — drains buffer → fan-out [2, 3]
	send(mkPush(2))

	// Allow pushhub goroutines (synchronous in-process) to settle.
	deadline := time.Now().Add(1 * time.Second)
	for {
		obsMu.Lock()
		n := len(obs)
		obsMu.Unlock()
		if n >= 3 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	obsMu.Lock()
	defer obsMu.Unlock()
	if len(obs) != 3 {
		t.Fatalf("fan-out len=%d want 3 (seqs=%v)", len(obs), seqList(obs))
	}
	want := []viewsync.Seq{1, 2, 3}
	for i, w := range want {
		if obs[i].Seq != w {
			t.Errorf("fan-out[%d].seq=%d want %d (full=%v)", i, obs[i].Seq, w, seqList(obs))
		}
	}
	_ = conn.Close()
}

func TestViewSyncRejectsStaleFencingBeforeApply(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := app.Daemonbus().RegisterDaemon(ctx, placement.DaemonID("d-stale-push"), "h", "v", 0, "test-daemon"); err != nil {
		t.Fatalf("RegisterDaemon: %v", err)
	}
	epoch, _ := app.Daemonbus().IssueConnectionEpoch(ctx, placement.DaemonID("d-stale-push"))
	svr, dmn := newPipePair()
	conn := daemonbus.NewConnection(placement.DaemonID("d-stale-push"), epoch, svr)
	app.Daemonbus().Register(conn)
	defer app.Daemonbus().Unregister(placement.DaemonID("d-stale-push"))
	go func() { _ = conn.Run(ctx, app.DaemonbusHandlers()) }()

	_, req, err := app.Placements().Reserve(ctx, channel.ID("ch-stale-push"), placement.DaemonID("d-stale-push"), placement.ConnectionEpoch(epoch), nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if ok, err := app.Placements().Activate(ctx, placement.CreateChannelAck{
		ChannelID:       req.ChannelID,
		CreateRequestID: req.CreateRequestID,
		OwnerEpoch:      2,
		FencingToken:    "server-token",
		DaemonID:        placement.DaemonID("d-stale-push"),
		Result:          placement.CreateChannelAccepted,
	}, placement.ConnectionEpoch(epoch)); err != nil || !ok {
		t.Fatalf("Activate ok=%v err=%v", ok, err)
	}

	push := viewsync.PushFrame{
		ChannelID:    "ch-stale-push",
		Seq:          1,
		MessageID:    "m-stale-push",
		OwnerEpoch:   1,
		FencingToken: "old-token",
		Envelope: message.Envelope{
			ID:         "m-stale-push",
			TS:         1,
			ChannelID:  "ch-stale-push",
			Sender:     message.Sender{Kind: actor.KindAgent, ID: "a"},
			Kind:       message.KindEvent,
			Type:       "agent.text",
			Payload:    json.RawMessage(`{}`),
			Visibility: message.VisibilityPublic,
			Audience:   message.Audience{actor.SystemActorID},
		},
	}
	raw, _ := json.Marshal(push)
	if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
		FrameID:   "stale-push",
		FrameKind: kerneldaemonbus.FrameTypeViewsyncPush,
		DaemonID:  "d-stale-push", DaemonConnectionEpoch: epoch, Payload: raw,
	}); err != nil {
		t.Fatalf("write push: %v", err)
	}
	f, err := dmn.ReadFrame(ctx)
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}
	var ack viewsync.AckFrame
	if err := json.Unmarshal(f.Payload, &ack); err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	if ack.Accepted || ack.RejectReason != viewsync.RejectReasonMuxOwnerEpochStale {
		t.Fatalf("ack=%+v want mux_owner_epoch_stale reject", ack)
	}
	cur, _ := app.Viewcache().Cursor(ctx, channel.ID("ch-stale-push"))
	if cur != 0 {
		t.Fatalf("cursor=%d want 0; stale push should not apply", cur)
	}
	_ = conn.Close()
}

func TestServerInitiatedReclaimRoundTripActivatesPlacement(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	const daemonID placement.DaemonID = "d-reclaim-saga"
	if err := app.Daemonbus().RegisterDaemon(ctx, daemonID, "h", "v", 0, "test-daemon"); err != nil {
		t.Fatalf("RegisterDaemon: %v", err)
	}
	epoch, _ := app.Daemonbus().IssueConnectionEpoch(ctx, daemonID)
	svr, dmn := newPipePair()
	conn := daemonbus.NewConnection(daemonID, epoch, svr)
	app.Daemonbus().Register(conn)
	defer app.Daemonbus().Unregister(daemonID)
	go func() { _ = conn.Run(ctx, app.DaemonbusHandlers()) }()

	_, createReq, err := app.Placements().Reserve(ctx, channel.ID("ch-reclaim-saga"), daemonID, placement.ConnectionEpoch(epoch), nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if ok, err := app.Placements().Activate(ctx, placement.CreateChannelAck{
		ChannelID:       createReq.ChannelID,
		CreateRequestID: createReq.CreateRequestID,
		OwnerEpoch:      1,
		FencingToken:    "tok-before-reclaim",
		DaemonID:        daemonID,
		Result:          placement.CreateChannelAccepted,
	}, placement.ConnectionEpoch(epoch)); err != nil || !ok {
		t.Fatalf("Activate ok=%v err=%v", ok, err)
	}
	if err := app.Placements().Store().MarkStale(ctx, createReq.ChannelID, time.Now().Add(-time.Second).UnixMilli()); err != nil {
		t.Fatalf("MarkStale: %v", err)
	}

	daemonDone := make(chan error, 1)
	go func() {
		frame, err := dmn.ReadFrame(ctx)
		if err != nil {
			daemonDone <- err
			return
		}
		if frame.FrameKind != kerneldaemonbus.FrameTypeControlDaemonReclaim {
			daemonDone <- fmt.Errorf("frame=%s want daemon_reclaim", frame.FrameKind)
			return
		}
		var req placement.DaemonReclaimRequest
		if err := json.Unmarshal(frame.Payload, &req); err != nil {
			daemonDone <- err
			return
		}
		if req.ChannelID != createReq.ChannelID || req.NewOwnerEpoch != 2 || req.PreviousState != placement.ReclaimOriginStale {
			daemonDone <- fmt.Errorf("bad reclaim req: %+v", req)
			return
		}
		raw, _ := json.Marshal(placement.ReclaimAccepted{
			ChannelID:       req.ChannelID,
			CreateRequestID: req.CreateRequestID,
			NewOwnerEpoch:   req.NewOwnerEpoch,
			FencingToken:    "tok-after-reclaim",
		})
		daemonDone <- dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
			FrameID:               frame.FrameID,
			FrameKind:             kerneldaemonbus.FrameTypeControlReclaimAccepted,
			DaemonID:              daemonID,
			DaemonConnectionEpoch: epoch,
			Payload:               raw,
		})
	}()

	if err := app.Placements().ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}
	if err := <-daemonDone; err != nil {
		t.Fatalf("daemon side: %v", err)
	}
	got, _, err := app.Placements().Get(ctx, createReq.ChannelID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != placement.StateActive || got.OwnerEpoch != 2 || got.FencingToken != "tok-after-reclaim" {
		t.Fatalf("placement after reclaim=%+v", got)
	}
	_ = conn.Close()
}

func TestServerInitiatedReclaimUsesOtherOnlineDaemonWhenPreviousOffline(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	const previous placement.DaemonID = "d-reclaim-prev-offline"
	const candidate placement.DaemonID = "d-reclaim-candidate-online"
	createReq, candidateEpoch, candidateDaemon := prepareStaleReclaimCandidate(t, app, ctx, "ch-reclaim-prev-offline", previous, candidate, false)

	daemonDone := acceptReclaimOnDaemon(t, app, ctx, candidateDaemon, candidate, candidateEpoch, createReq.ChannelID, func(req placement.DaemonReclaimRequest) error {
		if req.PreviousOwnerDaemon == nil || *req.PreviousOwnerDaemon != previous {
			return fmt.Errorf("previous_owner_daemon=%v want %s", req.PreviousOwnerDaemon, previous)
		}
		return nil
	})

	if err := app.Placements().ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}
	if err := <-daemonDone; err != nil {
		t.Fatalf("daemon side: %v", err)
	}
	got, _, err := app.Placements().Get(ctx, createReq.ChannelID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != placement.StateActive || got.DaemonID != candidate || got.DaemonConnectionEpoch != placement.ConnectionEpoch(candidateEpoch) {
		t.Fatalf("placement after reclaim=%+v want active on %s", got, candidate)
	}
}

func TestServerInitiatedReclaimPrefersOtherOnlineDaemon(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	const previous placement.DaemonID = "d-reclaim-prev-online"
	const candidate placement.DaemonID = "d-reclaim-other-online"
	createReq, candidateEpoch, candidateDaemon := prepareStaleReclaimCandidate(t, app, ctx, "ch-reclaim-other-online", previous, candidate, true)

	daemonDone := acceptReclaimOnDaemon(t, app, ctx, candidateDaemon, candidate, candidateEpoch, createReq.ChannelID, nil)
	if err := app.Placements().ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}
	if err := <-daemonDone; err != nil {
		t.Fatalf("daemon side: %v", err)
	}
	got, _, err := app.Placements().Get(ctx, createReq.ChannelID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.DaemonID != candidate {
		t.Fatalf("reclaim daemon_id=%s want other online daemon %s", got.DaemonID, candidate)
	}
}

func TestServerInitiatedReclaimFallsBackToPreviousOwnerWhenOnlyDaemonOnline(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	const previous placement.DaemonID = "d-reclaim-single"
	if err := app.Daemonbus().RegisterDaemon(ctx, previous, "h", "v", 0, "test-daemon"); err != nil {
		t.Fatalf("RegisterDaemon previous: %v", err)
	}
	previousEpoch, _ := app.Daemonbus().IssueConnectionEpoch(ctx, previous)
	svr, dmn := newPipePair()
	conn := daemonbus.NewConnection(previous, previousEpoch, svr)
	app.Daemonbus().Register(conn)
	defer app.Daemonbus().Unregister(previous)
	go func() { _ = conn.Run(ctx, app.DaemonbusHandlers()) }()

	createReq := createStalePlacement(t, app, ctx, "ch-reclaim-single", previous, placement.ConnectionEpoch(previousEpoch))
	daemonDone := acceptReclaimOnDaemon(t, app, ctx, dmn, previous, previousEpoch, createReq.ChannelID, nil)
	if err := app.Placements().ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}
	if err := <-daemonDone; err != nil {
		t.Fatalf("daemon side: %v", err)
	}
	got, _, err := app.Placements().Get(ctx, createReq.ChannelID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.DaemonID != previous || got.State != placement.StateActive {
		t.Fatalf("placement after fallback reclaim=%+v", got)
	}
}

func TestServerInitiatedReclaimNoCandidateReturnsErrorAndLeavesStale(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	const previous placement.DaemonID = "d-reclaim-none"
	if err := app.Daemonbus().RegisterDaemon(ctx, previous, "h", "v", 0, "test-daemon"); err != nil {
		t.Fatalf("RegisterDaemon previous: %v", err)
	}
	previousEpoch, _ := app.Daemonbus().IssueConnectionEpoch(ctx, previous)
	createReq := createStalePlacement(t, app, ctx, "ch-reclaim-none", previous, placement.ConnectionEpoch(previousEpoch))

	err := app.Placements().ReconcileOnce(ctx)
	if err == nil || !strings.Contains(err.Error(), "reclaim no connected daemon candidate") {
		t.Fatalf("ReconcileOnce err=%v want no connected daemon candidate", err)
	}
	got, _, err := app.Placements().Get(ctx, createReq.ChannelID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != placement.StateStale || got.DaemonID != previous {
		t.Fatalf("placement after failed reclaim=%+v want stale previous owner", got)
	}
}

func TestServerInitiatedReclaimCASFalseOrphansAndUnbinds(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	const daemonID placement.DaemonID = "d-reclaim-cas-false"
	if err := app.Daemonbus().RegisterDaemon(ctx, daemonID, "h", "v", 0, "test-daemon"); err != nil {
		t.Fatalf("RegisterDaemon: %v", err)
	}
	epoch, _ := app.Daemonbus().IssueConnectionEpoch(ctx, daemonID)
	svr, dmn := newPipePair()
	conn := daemonbus.NewConnection(daemonID, epoch, svr)
	app.Daemonbus().Register(conn)
	defer app.Daemonbus().Unregister(daemonID)
	go func() { _ = conn.Run(ctx, app.DaemonbusHandlers()) }()

	_, createReq, err := app.Placements().Reserve(ctx, channel.ID("ch-reclaim-cas-false"), daemonID, placement.ConnectionEpoch(epoch), nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if ok, err := app.Placements().Activate(ctx, placement.CreateChannelAck{
		ChannelID:       createReq.ChannelID,
		CreateRequestID: createReq.CreateRequestID,
		OwnerEpoch:      1,
		FencingToken:    "tok-before-reclaim",
		DaemonID:        daemonID,
		Result:          placement.CreateChannelAccepted,
	}, placement.ConnectionEpoch(epoch)); err != nil || !ok {
		t.Fatalf("Activate ok=%v err=%v", ok, err)
	}
	if err := app.Placements().Store().MarkStale(ctx, createReq.ChannelID, time.Now().Add(-time.Second).UnixMilli()); err != nil {
		t.Fatalf("MarkStale: %v", err)
	}

	daemonDone := make(chan error, 1)
	go func() {
		frame, err := dmn.ReadFrame(ctx)
		if err != nil {
			daemonDone <- err
			return
		}
		var req placement.DaemonReclaimRequest
		if err := json.Unmarshal(frame.Payload, &req); err != nil {
			daemonDone <- err
			return
		}
		raw, _ := json.Marshal(placement.ReclaimAccepted{
			ChannelID:       req.ChannelID,
			CreateRequestID: req.CreateRequestID,
			NewOwnerEpoch:   req.NewOwnerEpoch,
			FencingToken:    "",
		})
		if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
			FrameID:               frame.FrameID,
			FrameKind:             kerneldaemonbus.FrameTypeControlReclaimAccepted,
			DaemonID:              daemonID,
			DaemonConnectionEpoch: epoch,
			Payload:               raw,
		}); err != nil {
			daemonDone <- err
			return
		}
		unbindFrame, err := dmn.ReadFrame(ctx)
		if err != nil {
			daemonDone <- err
			return
		}
		if unbindFrame.FrameKind != kerneldaemonbus.FrameTypeControlUnbindChannel {
			daemonDone <- fmt.Errorf("frame=%s want unbind_channel", unbindFrame.FrameKind)
			return
		}
		var unbind kerneldaemonbus.UnbindChannelBody
		if err := json.Unmarshal(unbindFrame.Payload, &unbind); err != nil {
			daemonDone <- err
			return
		}
		if unbind.ChannelID != req.ChannelID || unbind.OwnerEpoch != req.NewOwnerEpoch || unbind.Reason != kerneldaemonbus.UnbindChannelReasonAbandon {
			daemonDone <- fmt.Errorf("bad unbind: %+v", unbind)
			return
		}
		daemonDone <- nil
	}()

	err = app.Placements().ReconcileOnce(ctx)
	if err == nil || !strings.Contains(err.Error(), "CAS rejected") {
		t.Fatalf("ReconcileOnce err=%v want CAS rejected", err)
	}
	if err := <-daemonDone; err != nil {
		t.Fatalf("daemon side: %v", err)
	}
	got, _, err := app.Placements().Get(ctx, createReq.ChannelID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != placement.StateOrphan {
		t.Fatalf("placement after reclaim=%+v want orphan", got)
	}
	_ = conn.Close()
}

func TestReclaim_TimeoutAfterTakeover_TriggersUnbindRetry(t *testing.T) {
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, dmn, epoch, createReq := prepareRollbackReclaim(t, app, ctx, "ch-reclaim-timeout", "d-reclaim-timeout")
	defer app.Daemonbus().Unregister(conn.DaemonID)

	reclaimSeen := make(chan error, 1)
	go func() {
		frame, err := dmn.ReadFrame(ctx)
		if err != nil {
			reclaimSeen <- err
			return
		}
		if frame.FrameKind != kerneldaemonbus.FrameTypeControlDaemonReclaim {
			reclaimSeen <- fmt.Errorf("frame=%s want daemon_reclaim", frame.FrameKind)
			return
		}
		reclaimSeen <- nil
	}()

	shortCtx, shortCancel := context.WithTimeout(ctx, 80*time.Millisecond)
	err := app.Placements().ReconcileOnce(shortCtx)
	shortCancel()
	if err == nil {
		t.Fatal("ReconcileOnce err=nil want timeout")
	}
	if err := <-reclaimSeen; err != nil {
		t.Fatalf("daemon side: %v", err)
	}
	assertRollbackIntentCount(t, app, ctx, createReq.ChannelID, 1)
	drainRollbackUnbindNoAck(t, ctx, dmn, createReq.ChannelID)
	triggerRollbackHeartbeatRetry(t, app, ctx, dmn, conn.DaemonID, epoch, createReq.ChannelID)
	assertRollbackIntentCount(t, app, ctx, createReq.ChannelID, 0)
}

func TestReclaim_MalformedAcceptedAck_TriggersUnbindRetry(t *testing.T) {
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, dmn, epoch, createReq := prepareRollbackReclaim(t, app, ctx, "ch-reclaim-malformed", "d-reclaim-malformed")
	defer app.Daemonbus().Unregister(conn.DaemonID)

	done := replyToReclaim(t, ctx, dmn, conn.DaemonID, epoch, kerneldaemonbus.FrameTypeControlReclaimAccepted, []byte(`{"bad"`))
	err := app.Placements().ReconcileOnce(ctx)
	if err == nil || !strings.Contains(err.Error(), "reclaim_accepted decode") {
		t.Fatalf("ReconcileOnce err=%v want reclaim_accepted decode", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("daemon side: %v", err)
	}
	assertRollbackIntentCount(t, app, ctx, createReq.ChannelID, 1)
	drainRollbackUnbindNoAck(t, ctx, dmn, createReq.ChannelID)
	triggerRollbackHeartbeatRetry(t, app, ctx, dmn, conn.DaemonID, epoch, createReq.ChannelID)
	assertRollbackIntentCount(t, app, ctx, createReq.ChannelID, 0)
}

func TestReclaim_UnexpectedFrame_TriggersUnbindRetry(t *testing.T) {
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, dmn, epoch, createReq := prepareRollbackReclaim(t, app, ctx, "ch-reclaim-unexpected", "d-reclaim-unexpected")
	defer app.Daemonbus().Unregister(conn.DaemonID)

	done := replyToReclaim(t, ctx, dmn, conn.DaemonID, epoch, kerneldaemonbus.FrameTypeControlHeartbeatAck, []byte(`{}`))
	err := app.Placements().ReconcileOnce(ctx)
	if err == nil || !strings.Contains(err.Error(), "unexpected ack frame") {
		t.Fatalf("ReconcileOnce err=%v want unexpected ack frame", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("daemon side: %v", err)
	}
	assertRollbackIntentCount(t, app, ctx, createReq.ChannelID, 1)
	drainRollbackUnbindNoAck(t, ctx, dmn, createReq.ChannelID)
	triggerRollbackHeartbeatRetry(t, app, ctx, dmn, conn.DaemonID, epoch, createReq.ChannelID)
	assertRollbackIntentCount(t, app, ctx, createReq.ChannelID, 0)
}

func TestReclaim_PostTakeoverInternalError_TriggersUnbindRetry(t *testing.T) {
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, dmn, epoch, createReq := prepareRollbackReclaim(t, app, ctx, "ch-reclaim-internal-error", "d-reclaim-internal-error")
	defer app.Daemonbus().Unregister(conn.DaemonID)

	done := rejectReclaimOnDaemon(t, ctx, dmn, conn.DaemonID, epoch, placement.ReclaimRejectInternalError)
	err := app.Placements().ReconcileOnce(ctx)
	if err == nil || !strings.Contains(err.Error(), "rollback") {
		t.Fatalf("ReconcileOnce err=%v want rollback failure after internal reject", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("daemon side: %v", err)
	}
	assertRollbackIntentCount(t, app, ctx, createReq.ChannelID, 1)
	drainRollbackUnbindNoAck(t, ctx, dmn, createReq.ChannelID)
	triggerRollbackHeartbeatRetry(t, app, ctx, dmn, conn.DaemonID, epoch, createReq.ChannelID)
	assertRollbackIntentCount(t, app, ctx, createReq.ChannelID, 0)
}

func TestReclaim_UnbindSendFailure_PersistsIntentForRetry(t *testing.T) {
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, dmn, epoch, createReq := prepareRollbackReclaim(t, app, ctx, "ch-reclaim-unbind-send-fail", "d-reclaim-unbind-send-fail")

	done := make(chan error, 1)
	go func() {
		frame, err := dmn.ReadFrame(ctx)
		if err != nil {
			done <- err
			return
		}
		var req placement.DaemonReclaimRequest
		if err := json.Unmarshal(frame.Payload, &req); err != nil {
			done <- err
			return
		}
		raw, _ := json.Marshal(placement.ReclaimAccepted{
			ChannelID:       req.ChannelID,
			CreateRequestID: req.CreateRequestID,
			NewOwnerEpoch:   req.NewOwnerEpoch + 100,
			FencingToken:    "tok-wrong-epoch",
		})
		if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
			FrameID:               frame.FrameID,
			FrameKind:             kerneldaemonbus.FrameTypeControlReclaimAccepted,
			DaemonID:              conn.DaemonID,
			DaemonConnectionEpoch: epoch,
			Payload:               raw,
		}); err != nil {
			done <- err
			return
		}
		done <- conn.Close()
	}()

	err := app.Placements().ReconcileOnce(ctx)
	if err == nil || !strings.Contains(err.Error(), "daemon_reclaim send") {
		t.Fatalf("ReconcileOnce err=%v want daemon_reclaim send failure", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("daemon side: %v", err)
	}
	assertRollbackIntentCount(t, app, ctx, createReq.ChannelID, 1)

	app.Daemonbus().Unregister(conn.DaemonID)
	svr2, dmn2 := newPipePair()
	conn2 := daemonbus.NewConnection(conn.DaemonID, epoch+1, svr2)
	retryDone := expectRollbackUnbindAndAck(t, ctx, dmn2, createReq.ChannelID)
	if _, err := app.DB().ExecContext(ctx,
		`UPDATE placement_rollback_intents SET next_attempt_at = 0 WHERE channel_id = ?`,
		string(createReq.ChannelID),
	); err != nil {
		t.Fatalf("mark rollback intent due: %v", err)
	}
	app.Daemonbus().Register(conn2)
	defer app.Daemonbus().Unregister(conn.DaemonID)
	go func() { _ = conn2.Run(ctx, app.DaemonbusHandlers()) }()
	if err := <-retryDone; err != nil {
		t.Fatalf("retry daemon side: %v", err)
	}
	assertRollbackIntentCount(t, app, ctx, createReq.ChannelID, 0)
}

func TestRollbackIntent_OwnerEpochStaleAck_TreatedAsCleanup(t *testing.T) {
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, dmn, epoch, createReq := prepareRollbackReclaim(t, app, ctx, "ch-rollback-stale-ack", "d-rollback-stale-ack")
	defer app.Daemonbus().Unregister(conn.DaemonID)

	done := rejectReclaimOnDaemon(t, ctx, dmn, conn.DaemonID, epoch, placement.ReclaimRejectInternalError)
	err := app.Placements().ReconcileOnce(ctx)
	if err == nil || !strings.Contains(err.Error(), "rollback") {
		t.Fatalf("ReconcileOnce err=%v want rollback failure after internal reject", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("daemon side: %v", err)
	}
	assertRollbackIntentCount(t, app, ctx, createReq.ChannelID, 1)
	drainRollbackUnbindNoAck(t, ctx, dmn, createReq.ChannelID)

	if _, err := app.DB().ExecContext(ctx,
		`UPDATE placement_rollback_intents SET next_attempt_at = 0 WHERE channel_id = ?`,
		string(createReq.ChannelID),
	); err != nil {
		t.Fatalf("mark rollback intent due: %v", err)
	}
	retryDone := expectRollbackUnbindAndAckOwnerEpochStale(t, ctx, dmn, createReq.ChannelID)
	raw, _ := json.Marshal(placement.HeartbeatPayload{
		DaemonID:     conn.DaemonID,
		HeartbeatSeq: 1,
	})
	if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
		FrameID:               "frame-heartbeat-stale-ack",
		FrameKind:             kerneldaemonbus.FrameTypeControlHeartbeat,
		DaemonID:              conn.DaemonID,
		DaemonConnectionEpoch: epoch,
		Payload:               raw,
	}); err != nil {
		t.Fatalf("send heartbeat: %v", err)
	}
	if err := <-retryDone; err != nil {
		t.Fatalf("heartbeat retry: %v", err)
	}
	assertRollbackIntentCount(t, app, ctx, createReq.ChannelID, 0)
}

func TestReclaimRollback_CrashBetweenIntentAndOrphan_RecoveryOK(t *testing.T) {
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, dmn, epoch, createReq := prepareRollbackReclaim(t, app, ctx, "ch-rollback-atomic", "d-rollback-atomic")
	defer app.Daemonbus().Unregister(conn.DaemonID)

	done := rejectReclaimOnDaemon(t, ctx, dmn, conn.DaemonID, epoch, placement.ReclaimRejectInternalError)
	err := app.Placements().ReconcileOnce(ctx)
	if err == nil || !strings.Contains(err.Error(), "rollback") {
		t.Fatalf("ReconcileOnce err=%v want rollback failure after internal reject", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("daemon side: %v", err)
	}
	assertRollbackIntentCount(t, app, ctx, createReq.ChannelID, 1)
	got, _, err := app.Placements().Get(ctx, createReq.ChannelID)
	if err != nil {
		t.Fatalf("Get placement: %v", err)
	}
	if got.State != placement.StateOrphan {
		t.Fatalf("placement state=%s want orphan with rollback intent persisted", got.State)
	}
	drainRollbackUnbindNoAck(t, ctx, dmn, createReq.ChannelID)
	triggerRollbackHeartbeatRetry(t, app, ctx, dmn, conn.DaemonID, epoch, createReq.ChannelID)
	assertRollbackIntentCount(t, app, ctx, createReq.ChannelID, 0)
}

func TestReclaimCandidate_3DaemonsBalancedDistribution(t *testing.T) {
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	counts := map[placement.DaemonID]int{}
	var countsMu sync.Mutex
	for _, id := range []placement.DaemonID{"d-balance-a", "d-balance-b", "d-balance-c"} {
		_, dmn, epoch := registerReclaimDaemon(t, app, ctx, id, 0)
		serveCountingReclaims(t, ctx, dmn, id, epoch, counts, &countsMu)
	}
	for i := 0; i < 100; i++ {
		createStalePlacement(t, app, ctx, fmt.Sprintf("ch-balance-%03d", i), "d-balance-prev", 1)
	}
	if err := app.Placements().ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}

	countsMu.Lock()
	defer countsMu.Unlock()
	for _, id := range []placement.DaemonID{"d-balance-a", "d-balance-b", "d-balance-c"} {
		if counts[id] < 30 || counts[id] > 36 {
			t.Fatalf("balanced count[%s]=%d want roughly 33; all=%v", id, counts[id], counts)
		}
	}
}

func TestReclaimCandidate_CapacityRespected(t *testing.T) {
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	counts := map[placement.DaemonID]int{}
	var countsMu sync.Mutex
	for _, spec := range []struct {
		id       placement.DaemonID
		capacity int
	}{
		{"d-cap-a", 20},
		{"d-cap-b", 1},
		{"d-cap-c", 20},
	} {
		_, dmn, epoch := registerReclaimDaemon(t, app, ctx, spec.id, spec.capacity)
		serveCountingReclaims(t, ctx, dmn, spec.id, epoch, counts, &countsMu)
	}
	_, activeReq, err := app.Placements().Reserve(ctx, "ch-cap-b-existing", "d-cap-b", 1, nil)
	if err != nil {
		t.Fatalf("Reserve active B seed: %v", err)
	}
	if ok, err := app.Placements().Activate(ctx, placement.CreateChannelAck{
		ChannelID:       activeReq.ChannelID,
		CreateRequestID: activeReq.CreateRequestID,
		OwnerEpoch:      1,
		FencingToken:    "tok-active-ch-cap-b-existing",
		DaemonID:        "d-cap-b",
		Result:          placement.CreateChannelAccepted,
	}, 1); err != nil || !ok {
		t.Fatalf("activate B seed ok=%v err=%v", ok, err)
	}
	for i := 0; i < 12; i++ {
		createStalePlacement(t, app, ctx, fmt.Sprintf("ch-cap-%02d", i), "d-cap-prev", 1)
	}
	if err := app.Placements().ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}
	countsMu.Lock()
	defer countsMu.Unlock()
	if counts["d-cap-b"] != 0 {
		t.Fatalf("full daemon B received %d reclaims; counts=%v", counts["d-cap-b"], counts)
	}
	if counts["d-cap-a"] == 0 || counts["d-cap-c"] == 0 {
		t.Fatalf("A/C should receive reclaims; counts=%v", counts)
	}
}

func TestReclaimCandidate_ChosenFailFallback(t *testing.T) {
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	closedConn, _, _ := registerReclaimDaemon(t, app, ctx, "d-fallback-a", 0)
	_ = closedConn.Close()
	_, dmnB, epochB := registerReclaimDaemon(t, app, ctx, "d-fallback-b", 0)
	counts := map[placement.DaemonID]int{}
	var countsMu sync.Mutex
	serveCountingReclaims(t, ctx, dmnB, "d-fallback-b", epochB, counts, &countsMu)
	createStalePlacement(t, app, ctx, "ch-fallback", "d-fallback-prev", 1)
	if err := app.Placements().ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}
	countsMu.Lock()
	defer countsMu.Unlock()
	if counts["d-fallback-b"] != 1 {
		t.Fatalf("fallback daemon count=%d want 1; counts=%v", counts["d-fallback-b"], counts)
	}
}

func TestReclaimCandidate_FailedAssignmentDoesNotBumpRR(t *testing.T) {
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	connA, dmnA, epochA := registerReclaimDaemon(t, app, ctx, "d-rr-a", 0)
	_, _, _ = registerReclaimDaemon(t, app, ctx, "d-rr-b", 0)

	first := createStalePlacement(t, app, ctx, "ch-rr-failed", "d-rr-prev", 1)
	done := rejectReclaimOnDaemon(t, ctx, dmnA, connA.DaemonID, epochA, placement.ReclaimRejectInternalError)
	err := app.Placements().ReconcileOnce(ctx)
	if err == nil || !strings.Contains(err.Error(), "rollback") {
		t.Fatalf("first ReconcileOnce err=%v want rollback failure", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("first daemon side: %v", err)
	}
	drainRollbackUnbindNoAck(t, ctx, dmnA, first.ChannelID)
	metrics, err := app.Daemonbus().ConnectedConnectionMetrics(ctx)
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	for _, m := range metrics {
		if m.DaemonID == connA.DaemonID && m.LastReclaimAt != 0 {
			t.Fatalf("failed reclaim bumped RR last_reclaim_at=%d want 0", m.LastReclaimAt)
		}
	}
}

// TestHeldChannelsReportDaemonIDMismatch is the FIX-T4 regression: a
// daemonbus connection authenticated as "d1" must not be able to report
// placements by setting `HeldChannelsReport.DaemonID` to some other
// daemon id. The dispatch hook must reject the frame (the Run loop
// surfaces the error and terminates the connection).
func TestHeldChannelsReportDaemonIDMismatch(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Register two daemons; "owner" actually holds the placement,
	// "attacker" is the misbehaving connection that tries to reclaim.
	for _, did := range []placement.DaemonID{"owner", "attacker"} {
		if err := app.Daemonbus().RegisterDaemon(ctx, did, "h", "v", 0, "test-daemon"); err != nil {
			t.Fatalf("RegisterDaemon %s: %v", did, err)
		}
	}
	ownerEpoch, _ := app.Daemonbus().IssueConnectionEpoch(ctx, placement.DaemonID("owner"))
	attackerEpoch, _ := app.Daemonbus().IssueConnectionEpoch(ctx, placement.DaemonID("attacker"))

	// Seed an active placement owned by "owner".
	p, _, err := app.Placements().Reserve(ctx, channel.ID("ch-victim"),
		placement.DaemonID("owner"), placement.ConnectionEpoch(ownerEpoch), nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	// Phase 2 ack carries daemon-generated fencing tuple — capture it
	// so the attacker's held-channel report presents the post-activate values
	// persisted in placement.
	const daemonOwnerEpoch placement.OwnerEpoch = 1
	const daemonFencingTok placement.FencingToken = "daemon-tok-victim"
	ack := placement.CreateChannelAck{
		ChannelID: p.ChannelID, CreateRequestID: p.CreateRequestID,
		OwnerEpoch: daemonOwnerEpoch, FencingToken: daemonFencingTok,
		DaemonID: placement.DaemonID("owner"), Result: placement.CreateChannelAccepted,
	}
	if ok, err := app.Placements().Activate(ctx, ack, placement.ConnectionEpoch(ownerEpoch)); err != nil || !ok {
		t.Fatalf("Activate: ok=%v err=%v", ok, err)
	}

	// Wire a daemonbus connection authenticated as "attacker".
	svr, dmn := newPipePair()
	conn := daemonbus.NewConnection(placement.DaemonID("attacker"), attackerEpoch, svr)
	app.Daemonbus().Register(conn)
	defer app.Daemonbus().Unregister(placement.DaemonID("attacker"))

	runDone := make(chan error, 1)
	go func() { runDone <- conn.Run(ctx, app.DaemonbusHandlers()) }()

	// Attacker pushes a held-channel report claiming to be "owner".
	// MUST reject; conn.Run returns the validation error.
	raw, _ := json.Marshal(placement.HeldChannelsReport{
		DaemonID:    placement.DaemonID("owner"),
		DaemonEpoch: 1,
		Channels: []placement.HeldChannel{{
			ChannelID:    p.ChannelID,
			FencingToken: daemonFencingTok,
			OwnerEpoch:   daemonOwnerEpoch,
		}},
	})
	if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
		FrameID:               "f-attack",
		FrameKind:             kerneldaemonbus.FrameTypeControlHeldChannelsReport,
		DaemonID:              "attacker",
		DaemonConnectionEpoch: attackerEpoch,
		Payload:               raw,
	}); err != nil {
		t.Fatalf("write reclaim: %v", err)
	}

	select {
	case err := <-runDone:
		if err == nil {
			t.Fatal("conn.Run returned nil; expected daemon_id mismatch error")
		}
		if !strings.Contains(err.Error(), "does not match authenticated conn") {
			t.Errorf("unexpected error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("conn.Run did not return within 1s — DaemonID mismatch was not enforced")
	}

	// Placement row must still belong to "owner" — the attacker
	// cannot hijack ownership even though the (epoch, token) tuple
	// they presented was valid.
	got, _, err := app.Placements().Get(ctx, p.ChannelID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.DaemonID != "owner" {
		t.Errorf("post-attack daemon_id=%q want owner", got.DaemonID)
	}

	_ = conn.Close()
}

func TestChannelMembersBackfill_DaemonReconnectReplaysFullMemberSet(t *testing.T) {
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	client := srv.Client()
	sess := registerLoginAndCreateChannel(t, client, srv.URL, app, "member-backfill@example.com")
	chID := channel.ID(sess.channelID)
	daemonID := placement.DaemonID("d-member-backfill")
	if err := app.Daemonbus().RegisterDaemon(ctx, daemonID, "localhost", "v0", 32, "test-daemon"); err != nil {
		t.Fatalf("RegisterDaemon: %v", err)
	}
	epoch, err := app.Daemonbus().IssueConnectionEpoch(ctx, daemonID)
	if err != nil {
		t.Fatalf("IssueConnectionEpoch: %v", err)
	}
	svrTx, dmn := newPipePair()
	conn := daemonbus.NewConnection(daemonID, epoch, svrTx)
	app.Daemonbus().Register(conn)
	defer app.Daemonbus().Unregister(daemonID)
	go func() { _ = conn.Run(ctx, app.DaemonbusHandlers()) }()

	_, req, err := app.Placements().Reserve(ctx, chID, daemonID, placement.ConnectionEpoch(epoch), nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if ok, err := app.Placements().Activate(ctx, placement.CreateChannelAck{
		ChannelID:       req.ChannelID,
		CreateRequestID: req.CreateRequestID,
		OwnerEpoch:      1,
		FencingToken:    "tok-member-backfill",
		DaemonID:        daemonID,
		Result:          placement.CreateChannelAccepted,
	}, placement.ConnectionEpoch(epoch)); err != nil || !ok {
		t.Fatalf("Activate ok=%v err=%v", ok, err)
	}

	reportRaw, _ := json.Marshal(placement.HeldChannelsReport{
		DaemonID:    daemonID,
		DaemonEpoch: 1,
		Channels: []placement.HeldChannel{{
			ChannelID:    chID,
			OwnerEpoch:   1,
			FencingToken: "tok-member-backfill",
		}},
	})
	if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
		FrameID:               "held-member-backfill",
		FrameKind:             kerneldaemonbus.FrameTypeControlHeldChannelsReport,
		DaemonID:              daemonID,
		DaemonConnectionEpoch: epoch,
		Payload:               reportRaw,
	}); err != nil {
		t.Fatalf("write held report: %v", err)
	}

	var body kerneldaemonbus.UpdateMembersBody
	for {
		frame, err := dmn.ReadFrame(ctx)
		if err != nil {
			t.Fatalf("read daemon frame: %v", err)
		}
		if frame.FrameKind != kerneldaemonbus.FrameTypeControlUpdateMembers {
			continue
		}
		if err := json.Unmarshal(frame.Payload, &body); err != nil {
			t.Fatalf("decode update_members: %v", err)
		}
		ackRaw, _ := json.Marshal(kerneldaemonbus.UpdateMembersAckBody{
			ChannelID: body.ChannelID,
			Accepted:  true,
		})
		if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
			FrameID:               frame.FrameID,
			FrameKind:             kerneldaemonbus.FrameTypeControlUpdateMembersAck,
			DaemonID:              daemonID,
			DaemonConnectionEpoch: epoch,
			Payload:               ackRaw,
		}); err != nil {
			t.Fatalf("ack update_members: %v", err)
		}
		break
	}
	if body.ChannelID != chID {
		t.Fatalf("update_members channel=%s want %s", body.ChannelID, chID)
	}
	if len(body.Adds) != 1 || len(body.Removes) != 0 {
		t.Fatalf("update_members adds=%+v removes=%+v want full one-member replay", body.Adds, body.Removes)
	}
	if body.Adds[0].MemberActorID == "" || body.Adds[0].UserID == "" {
		t.Fatalf("update_members member missing actor/user: %+v", body.Adds[0])
	}
}

func TestNotifyProxyDaemonReadyCarriesProxyHostMetadata(t *testing.T) {
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	client := &http.Client{}
	sess := registerLoginAndCreateChannel(t, client, srv.URL, app, "proxy-host@example.com")
	chID := channel.ID(sess.channelID)
	cloudDaemonID := placement.DaemonID("d-proxy-host-cloud")
	if err := app.Daemonbus().RegisterDaemon(ctx, cloudDaemonID, "localhost", "v0", 32, "test-daemon"); err != nil {
		t.Fatalf("RegisterDaemon: %v", err)
	}
	epoch, err := app.Daemonbus().IssueConnectionEpoch(ctx, cloudDaemonID)
	if err != nil {
		t.Fatalf("IssueConnectionEpoch: %v", err)
	}
	svrTx, dmn := newPipePair()
	conn := daemonbus.NewConnection(cloudDaemonID, epoch, svrTx)
	app.Daemonbus().Register(conn)
	defer app.Daemonbus().Unregister(cloudDaemonID)
	go func() { _ = conn.Run(ctx, app.DaemonbusHandlers()) }()

	_, req, err := app.Placements().Reserve(ctx, chID, cloudDaemonID, placement.ConnectionEpoch(epoch), nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if ok, err := app.Placements().Activate(ctx, placement.CreateChannelAck{
		ChannelID:       req.ChannelID,
		CreateRequestID: req.CreateRequestID,
		OwnerEpoch:      1,
		FencingToken:    "tok-proxy-host",
		DaemonID:        cloudDaemonID,
		Result:          placement.CreateChannelAccepted,
	}, placement.ConnectionEpoch(epoch)); err != nil || !ok {
		t.Fatalf("Activate ok=%v err=%v", ok, err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- app.NotifyProxyDaemonReady(ctx, devicebus.Daemon{
			ID:        "daemon-proxy-host",
			ChannelID: chID,
			OwnerID:   "user-proxy-host",
			Name:      "Proxy Host",
		}, devicebus.DaemonReadyInput{
			Actors: []devicebus.ReadyActor{{
				ActorID:       "tool:proxy-host",
				CapabilitySet: json.RawMessage(`{"types":["proxy.host"]}`),
			}},
		})
	}()

	frame, err := dmn.ReadFrame(ctx)
	if err != nil {
		t.Fatalf("read daemon frame: %v", err)
	}
	if frame.FrameKind != kerneldaemonbus.FrameTypeControlUpdateMembers {
		t.Fatalf("frame kind=%s", frame.FrameKind)
	}
	var body kerneldaemonbus.UpdateMembersBody
	if err := json.Unmarshal(frame.Payload, &body); err != nil {
		t.Fatalf("decode update_members: %v", err)
	}
	if len(body.Adds) != 1 || body.Adds[0].ProxyHost == nil {
		t.Fatalf("proxy update_members missing proxy_host: %+v", body.Adds)
	}
	if got := body.Adds[0].ProxyHost; got.DaemonID != "daemon-proxy-host" || got.DaemonName != "Proxy Host" {
		t.Fatalf("proxy_host=%+v", got)
	}

	ackRaw, _ := json.Marshal(kerneldaemonbus.UpdateMembersAckBody{
		FrameID:   body.FrameID,
		ChannelID: body.ChannelID,
		Accepted:  true,
	})
	if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
		FrameID:               frame.FrameID,
		FrameKind:             kerneldaemonbus.FrameTypeControlUpdateMembersAck,
		DaemonID:              cloudDaemonID,
		DaemonConnectionEpoch: epoch,
		Payload:               ackRaw,
	}); err != nil {
		t.Fatalf("ack update_members: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("NotifyProxyDaemonReady: %v", err)
	}
}

// ----------------------------------------------------------------------
// Test helpers
// ----------------------------------------------------------------------

func post(t *testing.T, c *http.Client, url, body string, wantStatus int) {
	t.Helper()
	resp, err := c.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != wantStatus {
		t.Fatalf("POST %s status=%d want %d", url, resp.StatusCode, wantStatus)
	}
}

func postRaw(t *testing.T, c *http.Client, url, body string, wantStatus int) *http.Response {
	t.Helper()
	resp, err := c.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	if resp.StatusCode != wantStatus {
		_ = resp.Body.Close()
		t.Fatalf("POST %s status=%d want %d", url, resp.StatusCode, wantStatus)
	}
	return resp
}

func extractSessionCookie(t *testing.T, resp *http.Response) string {
	t.Helper()
	for _, c := range resp.Cookies() {
		if c.Name == identity.CookieName {
			return c.Value
		}
	}
	return ""
}

func userIDByEmail(t *testing.T, app *gateway.App, email string) string {
	t.Helper()
	var id string
	if err := app.DB().QueryRowContext(context.Background(),
		`SELECT id FROM users WHERE email = ?`,
		email,
	).Scan(&id); err != nil {
		t.Fatalf("userIDByEmail(%s): %v", email, err)
	}
	return id
}

func prepareRollbackReclaim(
	t *testing.T,
	app *gateway.App,
	ctx context.Context,
	channelID string,
	daemonID placement.DaemonID,
) (*daemonbus.Connection, *pipeTransport, kerneldaemonbus.ConnectionEpoch, placement.CreateChannelRequest) {
	t.Helper()
	if err := app.Daemonbus().RegisterDaemon(ctx, daemonID, "h", "v", 0, "test-daemon"); err != nil {
		t.Fatalf("RegisterDaemon: %v", err)
	}
	epoch, _ := app.Daemonbus().IssueConnectionEpoch(ctx, daemonID)
	svr, dmn := newPipePair()
	conn := daemonbus.NewConnection(daemonID, epoch, svr)
	app.Daemonbus().Register(conn)
	go func() { _ = conn.Run(ctx, app.DaemonbusHandlers()) }()
	createReq := createStalePlacement(t, app, ctx, channelID, daemonID, placement.ConnectionEpoch(epoch))
	return conn, dmn, epoch, createReq
}

func replyToReclaim(
	t *testing.T,
	ctx context.Context,
	dmn *pipeTransport,
	daemonID placement.DaemonID,
	epoch kerneldaemonbus.ConnectionEpoch,
	kind kerneldaemonbus.FrameType,
	payload []byte,
) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		frame, err := dmn.ReadFrame(ctx)
		if err != nil {
			done <- err
			return
		}
		if frame.FrameKind != kerneldaemonbus.FrameTypeControlDaemonReclaim {
			done <- fmt.Errorf("frame=%s want daemon_reclaim", frame.FrameKind)
			return
		}
		done <- dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
			FrameID:               frame.FrameID,
			FrameKind:             kind,
			DaemonID:              daemonID,
			DaemonConnectionEpoch: epoch,
			Payload:               payload,
		})
	}()
	return done
}

func rejectReclaimOnDaemon(
	t *testing.T,
	ctx context.Context,
	dmn *pipeTransport,
	daemonID placement.DaemonID,
	epoch kerneldaemonbus.ConnectionEpoch,
	reason placement.ReclaimRejectReason,
) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		frame, err := dmn.ReadFrame(ctx)
		if err != nil {
			done <- err
			return
		}
		if frame.FrameKind != kerneldaemonbus.FrameTypeControlDaemonReclaim {
			done <- fmt.Errorf("frame=%s want daemon_reclaim", frame.FrameKind)
			return
		}
		var req placement.DaemonReclaimRequest
		if err := json.Unmarshal(frame.Payload, &req); err != nil {
			done <- err
			return
		}
		raw, _ := json.Marshal(placement.ReclaimRejected{
			ChannelID:       req.ChannelID,
			CreateRequestID: req.CreateRequestID,
			Reason:          reason,
		})
		done <- dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
			FrameID:               frame.FrameID,
			FrameKind:             kerneldaemonbus.FrameTypeControlReclaimRejected,
			DaemonID:              daemonID,
			DaemonConnectionEpoch: epoch,
			Payload:               raw,
		})
	}()
	return done
}

func assertRollbackIntentCount(t *testing.T, app *gateway.App, ctx context.Context, chID channel.ID, want int) {
	t.Helper()
	deadline := time.Now().Add(700 * time.Millisecond)
	var got int
	for {
		if err := app.DB().QueryRowContext(ctx,
			`SELECT COUNT(*) FROM placement_rollback_intents WHERE channel_id = ?`,
			string(chID),
		).Scan(&got); err != nil {
			t.Fatalf("rollback intent count: %v", err)
		}
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("rollback intents for %s = %d want %d", chID, got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func drainRollbackUnbindNoAck(t *testing.T, ctx context.Context, dmn *pipeTransport, chID channel.ID) {
	t.Helper()
	frame, err := dmn.ReadFrame(ctx)
	if err != nil {
		t.Fatalf("read initial rollback unbind: %v", err)
	}
	if frame.FrameKind != kerneldaemonbus.FrameTypeControlUnbindChannel {
		t.Fatalf("initial rollback frame=%s want unbind_channel", frame.FrameKind)
	}
	var body kerneldaemonbus.UnbindChannelBody
	if err := json.Unmarshal(frame.Payload, &body); err != nil {
		t.Fatalf("decode initial unbind: %v", err)
	}
	if body.ChannelID != chID || body.Reason != kerneldaemonbus.UnbindChannelReasonAbandon {
		t.Fatalf("initial unbind=%+v want channel %s abandon", body, chID)
	}
}

func triggerRollbackHeartbeatRetry(
	t *testing.T,
	app *gateway.App,
	ctx context.Context,
	dmn *pipeTransport,
	daemonID placement.DaemonID,
	epoch kerneldaemonbus.ConnectionEpoch,
	chID channel.ID,
) {
	t.Helper()
	if _, err := app.DB().ExecContext(ctx,
		`UPDATE placement_rollback_intents SET next_attempt_at = 0 WHERE channel_id = ?`,
		string(chID),
	); err != nil {
		t.Fatalf("mark rollback intent due: %v", err)
	}
	retryDone := expectRollbackUnbindAndAck(t, ctx, dmn, chID)
	raw, _ := json.Marshal(placement.HeartbeatPayload{
		DaemonID:     daemonID,
		HeartbeatSeq: 1,
	})
	if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
		FrameID:               kerneldaemonbus.FrameID("frame-heartbeat-retry-" + string(chID)),
		FrameKind:             kerneldaemonbus.FrameTypeControlHeartbeat,
		DaemonID:              daemonID,
		DaemonConnectionEpoch: epoch,
		Payload:               raw,
	}); err != nil {
		t.Fatalf("send heartbeat: %v", err)
	}
	if err := <-retryDone; err != nil {
		t.Fatalf("heartbeat retry: %v", err)
	}
}

func expectRollbackUnbindAndAck(
	t *testing.T,
	ctx context.Context,
	dmn *pipeTransport,
	chID channel.ID,
) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		var frame kerneldaemonbus.Frame
		for {
			got, err := dmn.ReadFrame(ctx)
			if err != nil {
				done <- err
				return
			}
			if got.FrameKind != kerneldaemonbus.FrameTypeControlUnbindChannel {
				continue
			}
			frame = got
			break
		}
		var body kerneldaemonbus.UnbindChannelBody
		if err := json.Unmarshal(frame.Payload, &body); err != nil {
			done <- err
			return
		}
		if body.ChannelID != chID || body.Reason != kerneldaemonbus.UnbindChannelReasonAbandon {
			done <- fmt.Errorf("bad retry unbind: %+v", body)
			return
		}
		raw, _ := json.Marshal(kerneldaemonbus.UnbindChannelAckBody{
			ChannelID:  body.ChannelID,
			OwnerEpoch: body.OwnerEpoch,
			Result:     kerneldaemonbus.UnbindChannelReleased,
		})
		done <- dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
			FrameID:               frame.FrameID,
			FrameKind:             kerneldaemonbus.FrameTypeControlUnbindChannelAck,
			DaemonID:              frame.DaemonID,
			DaemonConnectionEpoch: frame.DaemonConnectionEpoch,
			Payload:               raw,
		})
	}()
	return done
}

func expectRollbackUnbindAndAckOwnerEpochStale(
	t *testing.T,
	ctx context.Context,
	dmn *pipeTransport,
	chID channel.ID,
) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		var frame kerneldaemonbus.Frame
		for {
			got, err := dmn.ReadFrame(ctx)
			if err != nil {
				done <- err
				return
			}
			if got.FrameKind != kerneldaemonbus.FrameTypeControlUnbindChannel {
				continue
			}
			frame = got
			break
		}
		var body kerneldaemonbus.UnbindChannelBody
		if err := json.Unmarshal(frame.Payload, &body); err != nil {
			done <- err
			return
		}
		if body.ChannelID != chID || body.Reason != kerneldaemonbus.UnbindChannelReasonAbandon {
			done <- fmt.Errorf("bad retry unbind: %+v", body)
			return
		}
		raw, _ := json.Marshal(kerneldaemonbus.UnbindChannelAckBody{
			ChannelID:  body.ChannelID,
			OwnerEpoch: body.OwnerEpoch + 1,
			Result:     kerneldaemonbus.UnbindChannelRejected,
			Reason:     kerneldaemonbus.UnbindChannelRejectOwnerEpochStale,
		})
		done <- dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
			FrameID:               frame.FrameID,
			FrameKind:             kerneldaemonbus.FrameTypeControlUnbindChannelAck,
			DaemonID:              frame.DaemonID,
			DaemonConnectionEpoch: frame.DaemonConnectionEpoch,
			Payload:               raw,
		})
	}()
	return done
}

func registerReclaimDaemon(
	t *testing.T,
	app *gateway.App,
	ctx context.Context,
	id placement.DaemonID,
	capacity int,
) (*daemonbus.Connection, *pipeTransport, kerneldaemonbus.ConnectionEpoch) {
	t.Helper()
	if err := app.Daemonbus().RegisterDaemon(ctx, id, "h", "v", capacity, "test-daemon"); err != nil {
		t.Fatalf("RegisterDaemon %s: %v", id, err)
	}
	epoch, _ := app.Daemonbus().IssueConnectionEpoch(ctx, id)
	svr, dmn := newPipePair()
	conn := daemonbus.NewConnection(id, epoch, svr)
	app.Daemonbus().Register(conn)
	t.Cleanup(func() { app.Daemonbus().Unregister(id) })
	go func() { _ = conn.Run(ctx, app.DaemonbusHandlers()) }()
	return conn, dmn, epoch
}

func serveCountingReclaims(
	t *testing.T,
	ctx context.Context,
	dmn *pipeTransport,
	daemonID placement.DaemonID,
	epoch kerneldaemonbus.ConnectionEpoch,
	counts map[placement.DaemonID]int,
	countsMu *sync.Mutex,
) {
	t.Helper()
	go func() {
		for {
			frame, err := dmn.ReadFrame(ctx)
			if err != nil {
				return
			}
			if frame.FrameKind != kerneldaemonbus.FrameTypeControlDaemonReclaim {
				continue
			}
			var req placement.DaemonReclaimRequest
			if err := json.Unmarshal(frame.Payload, &req); err != nil {
				return
			}
			countsMu.Lock()
			counts[daemonID]++
			countsMu.Unlock()
			raw, _ := json.Marshal(placement.ReclaimAccepted{
				ChannelID:       req.ChannelID,
				CreateRequestID: req.CreateRequestID,
				NewOwnerEpoch:   req.NewOwnerEpoch,
				FencingToken:    placement.FencingToken("tok-reclaim-" + string(daemonID) + "-" + string(req.ChannelID)),
			})
			_ = dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
				FrameID:               frame.FrameID,
				FrameKind:             kerneldaemonbus.FrameTypeControlReclaimAccepted,
				DaemonID:              daemonID,
				DaemonConnectionEpoch: epoch,
				Payload:               raw,
			})
		}
	}()
}

func prepareStaleReclaimCandidate(
	t *testing.T,
	app *gateway.App,
	ctx context.Context,
	channelID string,
	previous placement.DaemonID,
	candidate placement.DaemonID,
	previousOnline bool,
) (placement.CreateChannelRequest, kerneldaemonbus.ConnectionEpoch, *pipeTransport) {
	t.Helper()
	if err := app.Daemonbus().RegisterDaemon(ctx, previous, "h", "v", 0, "test-daemon"); err != nil {
		t.Fatalf("RegisterDaemon previous: %v", err)
	}
	previousEpoch, _ := app.Daemonbus().IssueConnectionEpoch(ctx, previous)
	if previousOnline {
		svr, _ := newPipePair()
		conn := daemonbus.NewConnection(previous, previousEpoch, svr)
		app.Daemonbus().Register(conn)
		t.Cleanup(func() { app.Daemonbus().Unregister(previous) })
	}
	createReq := createStalePlacement(t, app, ctx, channelID, previous, placement.ConnectionEpoch(previousEpoch))

	if err := app.Daemonbus().RegisterDaemon(ctx, candidate, "h", "v", 0, "test-daemon"); err != nil {
		t.Fatalf("RegisterDaemon candidate: %v", err)
	}
	candidateEpoch, _ := app.Daemonbus().IssueConnectionEpoch(ctx, candidate)
	svr, dmn := newPipePair()
	conn := daemonbus.NewConnection(candidate, candidateEpoch, svr)
	app.Daemonbus().Register(conn)
	t.Cleanup(func() { app.Daemonbus().Unregister(candidate) })
	go func() { _ = conn.Run(ctx, app.DaemonbusHandlers()) }()
	return createReq, candidateEpoch, dmn
}

func createStalePlacement(
	t *testing.T,
	app *gateway.App,
	ctx context.Context,
	channelID string,
	daemonID placement.DaemonID,
	connectionEpoch placement.ConnectionEpoch,
) placement.CreateChannelRequest {
	t.Helper()
	_, createReq, err := app.Placements().Reserve(ctx, channel.ID(channelID), daemonID, connectionEpoch, nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if ok, err := app.Placements().Activate(ctx, placement.CreateChannelAck{
		ChannelID:       createReq.ChannelID,
		CreateRequestID: createReq.CreateRequestID,
		OwnerEpoch:      1,
		FencingToken:    placement.FencingToken("tok-before-reclaim-" + channelID),
		DaemonID:        daemonID,
		Result:          placement.CreateChannelAccepted,
	}, connectionEpoch); err != nil || !ok {
		t.Fatalf("Activate ok=%v err=%v", ok, err)
	}
	if err := app.Placements().Store().MarkStale(ctx, createReq.ChannelID, time.Now().Add(-time.Second).UnixMilli()); err != nil {
		t.Fatalf("MarkStale: %v", err)
	}
	return createReq
}

func acceptReclaimOnDaemon(
	t *testing.T,
	app *gateway.App,
	ctx context.Context,
	dmn *pipeTransport,
	daemonID placement.DaemonID,
	epoch kerneldaemonbus.ConnectionEpoch,
	channelID channel.ID,
	check func(placement.DaemonReclaimRequest) error,
) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		frame, err := dmn.ReadFrame(ctx)
		if err != nil {
			done <- err
			return
		}
		if frame.FrameKind != kerneldaemonbus.FrameTypeControlDaemonReclaim {
			done <- fmt.Errorf("frame=%s want daemon_reclaim", frame.FrameKind)
			return
		}
		if frame.DaemonID != daemonID {
			done <- fmt.Errorf("frame daemon_id=%s want %s", frame.DaemonID, daemonID)
			return
		}
		var req placement.DaemonReclaimRequest
		if err := json.Unmarshal(frame.Payload, &req); err != nil {
			done <- err
			return
		}
		if req.ChannelID != channelID || req.PreviousState != placement.ReclaimOriginStale {
			done <- fmt.Errorf("bad reclaim req: %+v", req)
			return
		}
		if check != nil {
			if err := check(req); err != nil {
				done <- err
				return
			}
		}
		got, _, err := app.Placements().Get(ctx, channelID)
		if err != nil {
			done <- err
			return
		}
		if got.DaemonID != daemonID || got.State != placement.StateCreating {
			done <- fmt.Errorf("reserved placement=%+v want creating on %s", got, daemonID)
			return
		}
		raw, _ := json.Marshal(placement.ReclaimAccepted{
			ChannelID:       req.ChannelID,
			CreateRequestID: req.CreateRequestID,
			NewOwnerEpoch:   req.NewOwnerEpoch,
			FencingToken:    placement.FencingToken("tok-after-reclaim-" + string(daemonID)),
		})
		done <- dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
			FrameID:               frame.FrameID,
			FrameKind:             kerneldaemonbus.FrameTypeControlReclaimAccepted,
			DaemonID:              daemonID,
			DaemonConnectionEpoch: epoch,
			Payload:               raw,
		})
	}()
	return done
}

// pipeTransport satisfies daemonbus.Transport in-memory.
type pipeTransport struct {
	in, out chan kerneldaemonbus.Frame
	done    chan struct{}
}

func newPipePair() (server, daemon *pipeTransport) {
	a := make(chan kerneldaemonbus.Frame, 16)
	b := make(chan kerneldaemonbus.Frame, 16)
	done := make(chan struct{})
	server = &pipeTransport{in: a, out: b, done: done}
	daemon = &pipeTransport{in: b, out: a, done: done}
	return
}

func (p *pipeTransport) ReadFrame(ctx context.Context) (kerneldaemonbus.Frame, error) {
	select {
	case f := <-p.in:
		return f, nil
	case <-p.done:
		return kerneldaemonbus.Frame{}, context.Canceled
	case <-ctx.Done():
		return kerneldaemonbus.Frame{}, ctx.Err()
	}
}
func (p *pipeTransport) WriteFrame(ctx context.Context, f kerneldaemonbus.Frame) error {
	select {
	case p.out <- f:
		return nil
	case <-p.done:
		return context.Canceled
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (p *pipeTransport) Close() error { return nil }

type proxyFacadeNoopTransit struct{}

func (proxyFacadeNoopTransit) Send(context.Context, devicetransit.SendFrame) (devicetransit.FrameID, error) {
	return "noop-frame", nil
}

func (proxyFacadeNoopTransit) Ack(context.Context, devicetransit.AckFrame) error { return nil }

type proxyFacadeCaptureChain struct {
	env *message.Envelope
}

func (c *proxyFacadeCaptureChain) Write(_ context.Context, env *message.Envelope) (khar.WriteResult, error) {
	c.env = env
	return khar.WriteResult{MessageID: env.ID, Seq: 1}, nil
}

// seqList extracts the seq field of every captured PushedFrame; used
// for readable failure messages in fan-out ordering tests.
func seqList(fs []pushhub.PushedFrame) []viewsync.Seq {
	out := make([]viewsync.Seq, len(fs))
	for i, f := range fs {
		out[i] = f.Seq
	}
	return out
}

// itoa is a tiny strconv-free int64 → string used in test message ids
// (kept inline so the test file stays self-contained).
func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
