package gateway_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	kerneldaemonbus "github.com/wanpengxie/ActOS/kernel/daemonbus"
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
		DeviceTokenSecret:         "test-device",
		HumanCallerSecret:         "test-human",
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

	// Register.
	post(t, client, srv.URL+"/api/identity/register",
		`{"email":"alice@example.com","password":"topsecret123","code":"`+code+`"}`,
		http.StatusCreated)

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
			if !strings.Contains(errBody.Error, "request_audience_invalid") {
				t.Errorf("error=%q want request_audience_invalid", errBody.Error)
			}
		})
	}
}

func TestDevicebusIssueRequiresChannelMembership(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	client := &http.Client{}
	alice := registerLoginAndCreateChannel(t, client, srv.URL, app, "alice-devbus@example.com")
	bob := registerLoginAndCreateChannel(t, client, srv.URL, app, "bob-devbus@example.com")

	req, _ := http.NewRequest(http.MethodPost,
		srv.URL+"/api/channels/"+alice.channelID+"/devices",
		strings.NewReader(`{"device_id":"dev-bob","device_type":"xhs","daemon_id":"d1"}`))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: identity.CookieName, Value: bob.session})
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST devices: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status=%d want 403", resp.StatusCode)
	}
}

func TestDevicebusIssueFailsClosedWithoutBindNotifier(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	app.Devicebus().SetBindNotifier(nil)
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	client := &http.Client{}
	alice := registerLoginAndCreateChannel(t, client, srv.URL, app, "alice-devbus-nil-notifier@example.com")

	req, _ := http.NewRequest(http.MethodPost,
		srv.URL+"/api/channels/"+alice.channelID+"/devices",
		strings.NewReader(`{"device_id":"dev-alice","device_type":"xhs","daemon_id":"d1"}`))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: identity.CookieName, Value: alice.session})
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST devices: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503", resp.StatusCode)
	}
}

func TestDevicebusSessionAccessRequiresOwnerOrChannelMembership(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	client := &http.Client{}
	alice := registerLoginAndCreateChannel(t, client, srv.URL, app, "alice-device-owner@example.com")
	bob := registerLoginAndCreateChannel(t, client, srv.URL, app, "bob-device-attacker@example.com")
	aliceID := userIDByEmail(t, app, "alice-device-owner@example.com")

	res, err := app.Devicebus().IssueSession(context.Background(), devicebus.IssueInput{
		DeviceID: "dev-alice", ChannelID: channel.ID(alice.channelID), UserID: aliceID, DaemonID: "d1",
	})
	if err != nil {
		t.Fatalf("IssueSession: %v", err)
	}

	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		req, _ := http.NewRequest(method, srv.URL+"/api/devices/"+res.Session.ID, nil)
		req.AddCookie(&http.Cookie{Name: identity.CookieName, Value: bob.session})
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s devices: %v", method, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("%s status=%d want 403", method, resp.StatusCode)
		}
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
		ChannelID:  alice.channelID,
		Type:       "agent.text",
		Kind:       message.KindEvent,
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
		Payload:    json.RawMessage(`{"text":"secret"}`),
		Visibility: message.VisibilityPublic,
		Audience:   []string{"*"},
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
		ChannelID:  alice.channelID,
		Type:       "agent.text",
		Kind:       message.KindEvent,
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
		Payload:    json.RawMessage(`{"text":"secret"}`),
		Visibility: message.VisibilityPublic,
		Audience:   []string{"*"},
	})
	if err := ws.SetReadDeadline(time.Now().Add(150 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if _, raw, err := ws.ReadMessage(); err == nil {
		t.Fatalf("unexpected push for non-member: %s", string(raw))
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
	pollUntil(t, time.Second, func() bool {
		return app.Pushhub().SubscriberCount(channel.ID(alice.channelID)) == 0
	})

	app.Pushhub().PushMessage(channel.ID(alice.channelID), 1, message.Envelope{
		ID:         "m-after-revoke",
		ChannelID:  alice.channelID,
		Type:       "agent.text",
		Kind:       message.KindEvent,
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
		Payload:    json.RawMessage(`{"text":"secret"}`),
		Visibility: message.VisibilityPublic,
		Audience:   []string{"*"},
	})
	if err := ws.SetReadDeadline(time.Now().Add(150 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if _, raw, err := ws.ReadMessage(); err == nil {
		t.Fatalf("unexpected push after membership removal: %s", string(raw))
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
		ChannelID:     sess.channelID,
		Type:          "human.text",
		Kind:          message.KindRequest,
		CorrelationID: "corr-parent",
		Sender:        message.Sender{Kind: actor.KindAgent, ID: "agent:requester"},
		Visibility:    message.VisibilityPublic,
		Audience:      []string{"user:alice"},
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
	ack := placement.CreateChannelAck{
		ChannelID:       p.ChannelID,
		CreateRequestID: createReq.CreateRequestID,
		OwnerEpoch:      p.OwnerEpoch,
		FencingToken:    p.FencingToken,
		DaemonID:        placement.DaemonID("d-corr"),
		Status:          placement.AckBound,
	}
	if ok, err := app.Placements().Activate(ctx, ack, placement.ConnectionEpoch(epoch)); err != nil || !ok {
		t.Fatalf("Activate: ok=%v err=%v", ok, err)
	}

	respCh := make(chan *http.Response, 1)
	errCh := make(chan error, 1)
	go func() {
		body, _ := json.Marshal(writeBody{
			Type:     "human.text",
			Kind:     string(message.KindResponse),
			ParentID: parent.ID,
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
		FrameType:             kerneldaemonbus.FrameTypeControlWriteMessageAck,
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
// TestHandleWriteMessage_* to build wire-shape JSON.
type writeBody struct {
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
		http.StatusCreated)

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

// TestMockDaemonViewSyncRoundTrip drives a fake daemonbus connection
// through the App's dispatch loop:
//
//  1. fake daemon pushes viewsync.push seq=1 → server.viewcache.Apply
//     → fan-out (no subscriber yet, but Hub call must succeed)
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

	send := func(ft kerneldaemonbus.FrameType, payload any) {
		raw, _ := json.Marshal(payload)
		if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
			FrameID: "f-" + ft.String(), FrameType: ft,
			DaemonID: "mock-d1", DaemonConnectionEpoch: epoch, Payload: raw,
		}); err != nil {
			t.Fatalf("write %s: %v", ft, err)
		}
	}

	mkPush := func(seq viewsync.Seq) viewsync.PushFrame {
		id := "m-" + itoa(int64(seq))
		return viewsync.PushFrame{
			ChannelID: channel.ID("ch-A"), Seq: seq,
			MessageID: id,
			Envelope: message.Envelope{
				ID: id, TS: int64(seq) * 1000, ChannelID: "ch-A",
				Sender: message.Sender{Kind: actor.KindAgent, ID: "a"},
				Kind:   message.KindEvent, Type: "agent.text",
				Payload:    json.RawMessage(`{}`),
				Visibility: message.VisibilityPublic, Audience: []string{"*"},
			},
		}
	}

	expect := func(want viewsync.Seq) {
		t.Helper()
		f, err := dmn.ReadFrame(ctx)
		if err != nil {
			t.Fatalf("read ack: %v", err)
		}
		if f.FrameType != kerneldaemonbus.FrameTypeViewsyncAck {
			t.Fatalf("expected viewsync.ack, got %s", f.FrameType)
		}
		var ack viewsync.AckFrame
		if err := json.Unmarshal(f.Payload, &ack); err != nil {
			t.Fatalf("decode ack: %v", err)
		}
		if int64(ack.LastReceivedSeq) != int64(want) {
			t.Fatalf("ack=%d want %d", ack.LastReceivedSeq, want)
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

	ack := placement.CreateChannelAck{
		FrameID: "ack-1", ChannelID: req.ChannelID, CreateRequestID: req.CreateRequestID,
		OwnerEpoch: req.OwnerEpoch, FencingToken: req.FencingToken,
		DaemonID: placement.DaemonID("d1"), DaemonEpoch: 1, Status: placement.AckBound,
	}
	raw, _ := json.Marshal(ack)
	if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
		FrameID: "f-ack", FrameType: kerneldaemonbus.FrameTypeControlCreateChannelAck,
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

	mkPush := func(seq viewsync.Seq) viewsync.PushFrame {
		id := "m-" + itoa(int64(seq))
		return viewsync.PushFrame{
			ChannelID: channel.ID("ch-fanout"), Seq: seq, MessageID: id,
			Envelope: message.Envelope{
				ID: id, TS: int64(seq) * 1000, ChannelID: "ch-fanout",
				Sender: message.Sender{Kind: actor.KindAgent, ID: "a"},
				Kind:   message.KindEvent, Type: "agent.text",
				Payload:    json.RawMessage(`{}`),
				Visibility: message.VisibilityPublic, Audience: []string{"*"},
			},
		}
	}
	send := func(payload viewsync.PushFrame) {
		raw, _ := json.Marshal(payload)
		if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
			FrameID:   "f-" + itoa(int64(payload.Seq)),
			FrameType: kerneldaemonbus.FrameTypeViewsyncPush,
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

// TestReclaimDaemonIDMismatch is the FIX-T4 regression: a daemonbus
// connection authenticated as "d1" must not be able to reclaim
// placements by setting `ReclaimRequest.DaemonID` to some other
// daemon id. The dispatch hook must reject the frame (the Run loop
// surfaces the error and terminates the connection).
func TestReclaimDaemonIDMismatch(t *testing.T) {
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
	ack := placement.CreateChannelAck{
		ChannelID: p.ChannelID, CreateRequestID: p.CreateRequestID,
		OwnerEpoch: p.OwnerEpoch, FencingToken: p.FencingToken,
		DaemonID: placement.DaemonID("owner"), Status: placement.AckBound,
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

	// Attacker pushes a reclaim claiming to be "owner". OnReclaim
	// MUST reject; conn.Run returns the validation error.
	raw, _ := json.Marshal(placement.ReclaimRequest{
		DaemonID:    placement.DaemonID("owner"),
		DaemonEpoch: 1,
		Channels: []placement.ReclaimChannel{{
			ChannelID:    p.ChannelID,
			FencingToken: p.FencingToken,
			OwnerEpoch:   p.OwnerEpoch,
		}},
	})
	if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
		FrameID:               "f-attack",
		FrameType:             kerneldaemonbus.FrameTypeControlDaemonReclaim,
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
