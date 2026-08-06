package app_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/atoll/app/contract"
	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type activityAcceptanceEngine struct{}

func (activityAcceptanceEngine) Turn(_ context.Context, _ base.Trigger, sink base.Sink) error {
	if err := sink.ToolStarted(base.ToolActivity{CallID: "acceptance-tool-1", Tool: "acceptance_tool"}); err != nil {
		return err
	}
	if err := sink.ToolEnded(base.ToolActivity{CallID: "acceptance-tool-1", Tool: "acceptance_tool", Status: "completed"}); err != nil {
		return err
	}
	return sink.Complete(base.FinalValue{Text: "activity-ok", NextAction: "done"})
}
func (activityAcceptanceEngine) Describe() introspect.Describe {
	return introspect.Describe{Description: "activity acceptance"}
}
func (activityAcceptanceEngine) Checkpoint() []byte { return nil }
func (activityAcceptanceEngine) Close() error       { return nil }

// ---------------------------------------------------------------------------
// wsClient — a black-box gateway ws driver for the standard frame protocol
// (gateway 期 S3: attach opening / submit·resolve·cancel·after·cancel_timer up;
// feed·receipt·error down). The client translates the old {"type":…} test-body maps
// into standard frames and normalises the downstream frames back into the
// {"type":"ack"/"error"/"message"} shape the assertions read.
// ---------------------------------------------------------------------------

type wireFrame struct {
	V       int             `json:"v"`
	Type    string          `json:"frame_type"`
	Ref     string          `json:"ref"`
	Payload json.RawMessage `json:"payload"`
}

type wsClient struct {
	t    *testing.T
	conn *websocket.Conn
	tail chan map[string]any
	acks chan map[string]any

	chID string // the channel this test client drives (stamped as channel_id on every business frame — 连接模型勘误期 v2)

	mu      sync.Mutex
	refType map[string]string // ref → originating frame type (for ack["frame"])
}

// dialWS opens a gateway ws (连接模型勘误期: 连接即人 — /ws is channel-blind, the app
// membrane authenticates session→principal only) and sends the opening attach frame
// (报到 + a 游标表 keyed by chID). chID is retained so every business frame this test
// client sends carries it as the required channel_id (v2).
func dialWS(t *testing.T, srv *httptest.Server, cookies []*http.Cookie, chID string, sinceSeq int64) *wsClient {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	hdr := http.Header{}
	var parts []string
	for _, ck := range cookies {
		parts = append(parts, ck.Name+"="+ck.Value)
	}
	hdr.Set("Cookie", strings.Join(parts, "; "))
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, hdr)
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	c := &wsClient{
		t:       t,
		conn:    conn,
		chID:    chID,
		tail:    make(chan map[string]any, 512),
		acks:    make(chan map[string]any, 64),
		refType: map[string]string{},
	}
	var attach map[string]any
	if sinceSeq > 0 {
		attach = map[string]any{"since": map[string]int64{chID: sinceSeq}}
	}
	c.writeFrame("attach", "attach-1", attach)
	// Read the attach receipt synchronously so a test's first nextAck is its own
	// frame's receipt. This pins attach → receipt(ref, contract_version).
	var wf wireFrame
	if err := conn.ReadJSON(&wf); err != nil {
		t.Fatalf("attach receipt read: %v", err)
	}
	var receipt contract.Meta
	if err := json.Unmarshal(wf.Payload, &receipt); err != nil {
		t.Fatalf("attach receipt payload: %v", err)
	}
	if wf.Type != "receipt" || wf.Ref != "attach-1" || receipt.ContractVersion != contract.Version {
		t.Fatalf("attach receipt=%+v payload=%+v", wf, receipt)
	}
	go c.readLoop()
	return c
}

// writeFrame builds one standard frame (frame_type + ref + payload) and writes it. For
// every business frame (not attach) it injects the required channel_id (连接模型勘误期
// v2: the connection is channel-blind, so each frame names its channel) unless the
// caller已 set one explicitly (a test may set "" to exercise bad_payload).
func (c *wsClient) writeFrame(frameType, ref string, payload map[string]any) {
	c.t.Helper()
	if ref != "" {
		c.mu.Lock()
		c.refType[ref] = frameType
		c.mu.Unlock()
	}
	if frameType != "attach" {
		if payload == nil {
			payload = map[string]any{}
		}
		if _, ok := payload["channel_id"]; !ok {
			payload["channel_id"] = c.chID
		}
	}
	f := map[string]any{"v": 2, "frame_type": frameType}
	if ref != "" {
		f["ref"] = ref
	}
	if payload != nil {
		f["payload"] = payload
	}
	if err := c.conn.WriteJSON(f); err != nil {
		c.t.Fatalf("ws write: %v", err)
	}
}

func (c *wsClient) readLoop() {
	for {
		var wf wireFrame
		if err := c.conn.ReadJSON(&wf); err != nil {
			return
		}
		switch wf.Type {
		case "feed":
			var fp struct {
				ChannelID string          `json:"channel_id"`
				Seq       int64           `json:"seq"`
				Envelope  json.RawMessage `json:"envelope"`
			}
			_ = json.Unmarshal(wf.Payload, &fp)
			var env map[string]any
			_ = json.Unmarshal(fp.Envelope, &env)
			m := map[string]any{"type": "message", "channel_id": fp.ChannelID, "seq": fp.Seq, "envelope": env}
			select {
			case c.tail <- m:
			default:
			}
		case "receipt":
			var pm map[string]any
			_ = json.Unmarshal(wf.Payload, &pm)
			// The opening attach receipt is consumed synchronously in
			// dialWS before this loop starts, so every receipt here is a business ack.
			ack := map[string]any{"type": "ack"}
			for k, v := range pm {
				ack[k] = v
			}
			c.mu.Lock()
			if ft, ok := c.refType[wf.Ref]; ok {
				ack["frame"] = ft
			}
			c.mu.Unlock()
			select {
			case c.acks <- ack:
			default:
			}
		case "error":
			var ep struct {
				Frame  string `json:"frame"`
				Code   string `json:"code"`
				Detail string `json:"detail"`
			}
			_ = json.Unmarshal(wf.Payload, &ep)
			select {
			case c.acks <- map[string]any{"type": "error", "error": ep.Code, "detail": ep.Detail, "frame": ep.Frame}:
			default:
			}
		case "observe_ended":
			var ended struct {
				ChannelID string `json:"channel_id"`
				Reason    string `json:"reason"`
			}
			_ = json.Unmarshal(wf.Payload, &ended)
			select {
			case c.acks <- map[string]any{"type": "observe_ended", "channel_id": ended.ChannelID, "reason": ended.Reason}:
			default:
			}
		}
	}
}

// send translates one old-style {"type":…} test map into a standard frame: "type"
// is the frame kind, "ref" is the correlation id, and the remaining keys are the
// frame payload (their names already match the payload struct json tags).
func (c *wsClient) send(m map[string]any) {
	c.t.Helper()
	frameType, _ := m["type"].(string)
	ref, _ := m["ref"].(string)
	payload := map[string]any{}
	for k, v := range m {
		if k == "type" || k == "ref" {
			continue
		}
		payload[k] = v
	}
	if len(payload) == 0 {
		payload = nil
	}
	c.writeFrame(frameType, ref, payload)
}

// nextAck returns the next ack OR error frame.
func (c *wsClient) nextAck(timeout time.Duration) map[string]any {
	c.t.Helper()
	select {
	case m := <-c.acks:
		return m
	case <-time.After(timeout):
		c.t.Fatal("timed out waiting for ack/error frame")
		return nil
	}
}

// sendMessage sends a submit frame and returns its receipt/error frame.
func (c *wsClient) sendMessage(frame map[string]any) map[string]any {
	c.t.Helper()
	frame["type"] = "submit"
	c.send(frame)
	return c.nextAck(3 * time.Second)
}

// waitTail returns the first tail envelope matching pred.
func (c *wsClient) waitTail(pred func(env map[string]any) bool, timeout time.Duration) map[string]any {
	c.t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case m := <-c.tail:
			env, _ := m["envelope"].(map[string]any)
			if env != nil && pred(env) {
				return env
			}
		case <-deadline:
			c.t.Fatal("timed out waiting for matching tail frame")
			return nil
		}
	}
}

func (c *wsClient) assertNoTail(pred func(env map[string]any) bool, timeout time.Duration) {
	c.t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case m := <-c.tail:
			env, _ := m["envelope"].(map[string]any)
			if env != nil && pred(env) {
				c.t.Fatalf("unexpected tail frame: %v", m)
			}
		case <-deadline:
			return
		}
	}
}

func (c *wsClient) close() { _ = c.conn.Close() }

func TestSubmitIdempotencySurvivesReconnect(t *testing.T) {
	env := setupTestApp(t)
	setup := fullSetup(t, env)
	srv := httptest.NewServer(env.handler)
	defer srv.Close()

	submit := func(client *wsClient, ref string, payload map[string]any) map[string]any {
		return client.sendMessage(map[string]any{
			"ref": ref, "id": "client-stable-id", "msg_type": "human.message",
			"kind": "request", "audience": []string{string(setup.actorID)},
			"payload": payload,
		})
	}

	firstClient := dialWS(t, srv, setup.cookies, setup.chID, 0)
	first := submit(firstClient, "attempt-1", map[string]any{"text": "hello", "intent": "steer"})
	if first["type"] != "ack" || first["message_id"] != "client-stable-id" {
		t.Fatalf("first submit=%v", first)
	}
	firstClient.close()

	secondClient := dialWS(t, srv, setup.cookies, setup.chID, 0)
	defer secondClient.close()
	replay := submit(secondClient, "attempt-2", map[string]any{"intent": "steer", "text": "hello"})
	if replay["type"] != "ack" || replay["message_id"] != "client-stable-id" {
		t.Fatalf("same-content reconnect replay=%v", replay)
	}
	conflict := submit(secondClient, "attempt-3", map[string]any{"text": "different", "intent": "steer"})
	if conflict["type"] != "error" || conflict["error"] != "idempotency_conflict" {
		t.Fatalf("different-content replay=%v", conflict)
	}

	rows, err := env.app.MessagesForTest(channel.ID(setup.chID))
	if err != nil {
		t.Fatal(err)
	}
	var matches int
	for _, row := range rows {
		if row.Envelope.ID == "client-stable-id" {
			matches++
			if string(row.Envelope.Payload) != `{"text":"hello","intent":"steer"}` &&
				string(row.Envelope.Payload) != `{"intent":"steer","text":"hello"}` {
				t.Fatalf("intent payload changed in log: %s", row.Envelope.Payload)
			}
		}
	}
	if matches != 1 {
		t.Fatalf("physical rows for idempotency key=%d want 1", matches)
	}
}

func TestWebSocketObservationStateMachine(t *testing.T) {
	env := setupTestApp(t)
	setup := fullSetup(t, env)
	_, observerCookies := register(t, env, "observer@example.com", "secret123", "Observer")
	srv := httptest.NewServer(env.handler)
	defer srv.Close()

	owner := dialWS(t, srv, setup.cookies, setup.chID, 0)
	defer owner.close()
	observer := dialWS(t, srv, observerCookies, setup.chID, 0)

	observe := func(client *wsClient, ref string) map[string]any {
		client.send(map[string]any{"type": "observe", "ref": ref, "channel_id": setup.chID})
		return client.nextAck(3 * time.Second)
	}
	unobserve := func(client *wsClient, ref string) map[string]any {
		client.send(map[string]any{"type": "unobserve", "ref": ref, "channel_id": setup.chID})
		return client.nextAck(3 * time.Second)
	}
	post := func(id string) {
		ack := owner.sendMessage(map[string]any{
			"ref": "post-" + id, "id": id, "msg_type": "chat.text", "kind": "event",
			"audience": []string{string(setup.actorID)}, "payload": map[string]any{"text": id},
		})
		if ack["type"] != "ack" {
			t.Fatalf("post %s=%v", id, ack)
		}
	}
	matches := func(id string) func(map[string]any) bool {
		return func(envelope map[string]any) bool { return envelope["id"] == id }
	}

	for _, ref := range []string{"observe-1", "observe-duplicate"} {
		if got := observe(observer, ref); got["type"] != "ack" || got["channel_id"] != setup.chID {
			t.Fatalf("%s=%v", ref, got)
		}
	}
	post("observed-message")
	observer.waitTail(matches("observed-message"), 3*time.Second)

	for _, ref := range []string{"unobserve-1", "unobserve-noop"} {
		if got := unobserve(observer, ref); got["type"] != "ack" || got["channel_id"] != setup.chID {
			t.Fatalf("%s=%v", ref, got)
		}
	}
	post("after-unobserve")
	observer.assertNoTail(matches("after-unobserve"), 250*time.Millisecond)

	if got := observe(observer, "observe-before-disconnect"); got["type"] != "ack" {
		t.Fatalf("observe before disconnect=%v", got)
	}
	observer.close()
	observer = dialWS(t, srv, observerCookies, setup.chID, 0)
	defer observer.close()
	post("after-reconnect")
	observer.assertNoTail(matches("after-reconnect"), 250*time.Millisecond)

	if got := observe(observer, "observe-before-join"); got["type"] != "ack" {
		t.Fatalf("observe before join=%v", got)
	}
	joined := env.do(t, http.MethodPost, "/api/channels/"+setup.chID+"/join", nil, observerCookies)
	assertStatus(t, joined, http.StatusCreated)
	ended := observer.nextAck(3 * time.Second)
	if ended["type"] != "observe_ended" || ended["channel_id"] != setup.chID || ended["reason"] != "now_member" {
		t.Fatalf("observation invalidation=%v", ended)
	}
	if got := observe(observer, "member-observe"); got["type"] != "error" || got["error"] != "now_member" {
		t.Fatalf("member observe=%v", got)
	}
}

func TestAgentActivityPersistsAndReplaysThroughMessagePage(t *testing.T) {
	env := setupTestApp(t)
	testAgentBuilder = func(_ channel.ID, _ actor.ActorID) (actorbase.Proc, error) {
		def, err := base.Def("activity acceptance", base.Config{NewEngine: func(actorbase.Sys, []byte) (base.Engine, error) {
			return activityAcceptanceEngine{}, nil
		}})
		if err != nil {
			return nil, err
		}
		return def.New()
	}
	setup := fullSetup(t, env)
	srv := httptest.NewServer(env.handler)
	defer srv.Close()
	client := dialWS(t, srv, setup.cookies, setup.chID, 0)
	defer client.close()

	const requestID = "activity-acceptance-request"
	ack := client.sendMessage(map[string]any{
		"id": requestID, "msg_type": "agent.activity.acceptance", "kind": "request",
		"audience": []string{string(setup.boostID)}, "visibility": "public",
		"payload": map[string]any{"text": "run"},
	})
	if ack["type"] != "ack" {
		t.Fatalf("submit=%v", ack)
	}

	wantTypes := []string{
		"agent.activity.acceptance",
		"activity.turn.started",
		"activity.tool.started",
		"activity.tool.ended",
		"agent.activity.acceptance",
		"activity.turn.ended",
	}
	var matched []storespec.StoredRow
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		rows, err := env.app.MessagesForTest(channel.ID(setup.chID))
		if err != nil {
			t.Fatal(err)
		}
		matched = matched[:0]
		for _, row := range rows {
			if strings.Contains(row.Envelope.Type, "delta") || row.Envelope.Type == "agent.text" {
				t.Fatalf("retired/chunk output reached log: type=%q", row.Envelope.Type)
			}
			if row.Envelope.ID == requestID || row.Envelope.CorrelationID == requestID {
				matched = append(matched, row)
			}
		}
		if len(matched) == len(wantTypes) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(matched) != len(wantTypes) {
		t.Fatalf("correlated log rows=%d want %d", len(matched), len(wantTypes))
	}
	for i, row := range matched {
		if row.Envelope.Type != wantTypes[i] {
			t.Fatalf("row %d type=%q want %q", i, row.Envelope.Type, wantTypes[i])
		}
		if strings.HasPrefix(row.Envelope.Type, "activity.") {
			if row.Envelope.Sender.ID != setup.boostID || row.Envelope.CorrelationID != requestID || row.Envelope.Visibility != message.VisibilityPublic {
				t.Fatalf("activity row %d routing=%+v", i, row.Envelope)
			}
		}
	}
	if matched[4].Envelope.Kind != message.KindResponse {
		t.Fatalf("terminal kind=%q want response", matched[4].Envelope.Kind)
	}

	page := env.do(t, http.MethodGet, "/api/channels/"+setup.chID+"/messages?after_seq="+fmt.Sprint(matched[0].Seq-1)+"&limit=20", nil, setup.cookies)
	assertStatus(t, page, http.StatusOK)
	for _, activityType := range wantTypes[1:4] {
		if !strings.Contains(page.Body.String(), `"type":"`+activityType+`"`) {
			t.Fatalf("paginated replay omitted %q: %s", activityType, page.Body.String())
		}
	}
}

func TestAttachValidationErrorEchoesReadableRef(t *testing.T) {
	env := setupTestApp(t)
	_, cookies := register(t, env, "attach-ref@example.com", "secret123", "Attach Ref")
	srv := httptest.NewServer(env.handler)
	defer srv.Close()

	hdr := http.Header{}
	var parts []string
	for _, cookie := range cookies {
		parts = append(parts, cookie.Name+"="+cookie.Value)
	}
	hdr.Set("Cookie", strings.Join(parts, "; "))
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"/ws", hdr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.WriteJSON(map[string]any{
		"v": 2, "frame_type": "attach", "ref": "bad-attach-ref",
		"payload": map[string]any{"since": map[string]int64{}, "misspelled": true},
	}); err != nil {
		t.Fatal(err)
	}
	var got wireFrame
	if err := conn.ReadJSON(&got); err != nil {
		t.Fatal(err)
	}
	if got.Type != "error" || got.Ref != "bad-attach-ref" {
		t.Fatalf("validation error did not echo ref: %+v", got)
	}
}

// setBoostDefault performs the backend half of the client creation
// orchestration through only public wire surfaces.
func setBoostDefault(t *testing.T, env *testEnv, s setupResult, c *wsClient) actor.ActorID {
	t.Helper()
	ack := c.sendMessage(map[string]any{
		"msg_type":   "channel.set_default_agent",
		"kind":       "request",
		"audience":   []string{string(actor.SystemActorID)},
		"visibility": "public",
		"payload":    map[string]any{"source_decl_id": "sys:boost"},
	})
	if ack["type"] != "ack" {
		t.Fatalf("set default submit: want ack, got %v", ack)
	}
	raw := waitForResponse(t, env, s, ack["message_id"].(string), 3*time.Second)
	var response struct {
		Status       string        `json:"status"`
		DefaultAgent actor.ActorID `json:"default_agent"`
	}
	err := json.Unmarshal(raw, &response)
	if err != nil || response.Status != string(message.StatusCompleted) ||
		response.DefaultAgent == "" {
		t.Fatalf("set default response=%s decoded=%+v err=%v", raw, response, err)
	}
	return response.DefaultAgent
}

// addSecondMember registers a fresh user, admits it to the channel as a human
// member, and waits for its cell to embody — a second live
// subject the cross-sender door assertions need.
func addSecondMember(t *testing.T, env *testEnv, s setupResult, email string) ([]*http.Cookie, actor.ActorID) {
	t.Helper()
	regBody, cookies := register(t, env, email, "secret123", "Second")
	uid := regBody["id"].(string)
	aid := actor.ActorID("user:" + uid)
	aid, err := env.app.AdmitForTest(s.chID, aid, actor.KindHuman)
	if err != nil {
		t.Fatalf("admit second member: %v", err)
	}
	return cookies, aid
}

func queryActorStatus(t *testing.T, env *testEnv, s setupResult, client *wsClient, actorID actor.ActorID) introspect.Status {
	t.Helper()
	ack := client.sendMessage(map[string]any{
		"channel_id": s.chID, "msg_type": introspect.QueryStatus, "kind": "request",
		"audience": []string{string(actor.SystemActorID)}, "payload": map[string]any{"actor_id": actorID},
	})
	if ack["type"] != "ack" {
		t.Fatalf("actor.status submit=%v", ack)
	}
	raw := waitForResponse(t, env, s, ack["message_id"].(string), 3*time.Second)
	var status introspect.Status
	if err := json.Unmarshal(raw, &status); err != nil {
		t.Fatalf("decode actor.status %s: %v", raw, err)
	}
	return status
}

func waitActorStatus(t *testing.T, env *testEnv, s setupResult, client *wsClient, actorID actor.ActorID, timeout time.Duration, accept func(introspect.Status) bool) introspect.Status {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		status := queryActorStatus(t, env, s, client, actorID)
		if accept(status) {
			return status
		}
		if time.Now().After(deadline) {
			t.Fatalf("actor.status %s did not converge: %+v", actorID, status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// pollPresence observes the canonical product read path: a member asks the
// system actor for actor.status, which projects the membrane-internal presence
// fold. No realm-side raw Snapshot capability is involved.
func pollPresence(t *testing.T, env *testEnv, s setupResult, client *wsClient, actorID actor.ActorID, wantKnown, wantOnline bool, timeout time.Duration) introspect.Status {
	t.Helper()
	return waitActorStatus(t, env, s, client, actorID, timeout, func(status introspect.Status) bool {
		testimony, known := status.L3[introspect.ObsDevicePresence]
		if known != wantKnown {
			return false
		}
		if !wantKnown {
			return true
		}
		return testimony.Device != nil && testimony.Device.Online == wantOnline
	})
}

// ---------------------------------------------------------------------------
// DoD 2 (wire): submit / resolve / cancel / after / cancel_timer frames + presence.
// ---------------------------------------------------------------------------

// TestWS_MessageFrameEndToEnd: a member's submit frame commits truth through its own
// cell (POST /messages is废) and comes back on the same ws's feed.
func TestWS_MessageFrameEndToEnd(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)

	c := dialWS(t, srv, s.cookies, s.chID, 0)
	defer c.close()
	setBoostDefault(t, env, s, c)

	ack := c.sendMessage(map[string]any{
		"msg_type": "chat.text",
		"kind":     "event",
		"payload":  map[string]any{"text": "hello via frame"},
	})
	if ack["type"] != "ack" {
		t.Fatalf("submit frame: want ack, got %v", ack)
	}
	msgID, _ := ack["message_id"].(string)
	if msgID == "" {
		t.Fatalf("ack missing message_id: %v", ack)
	}

	env2 := c.waitTail(func(e map[string]any) bool { return e["id"] == msgID }, 3*time.Second)
	if env2["type"] != "chat.text" {
		t.Fatalf("feed envelope type = %v, want chat.text", env2["type"])
	}

	// POST /messages is gone (write path is the frame): the route 404s.
	w := env.do(t, "POST", fmt.Sprintf("/api/channels/%s/messages", s.chID),
		map[string]any{"type": "chat.text", "payload": map[string]any{"text": "x"}}, s.cookies)
	if w.Code != http.StatusNotFound {
		t.Fatalf("POST /messages should be 404 (abolished), got %d", w.Code)
	}
}

// TestWS_NonMemberWriteRejected: a realm principal who is not a channel member
// cannot submit a frame (膜律 看得见≠在里面).
func TestWS_NonMemberWriteRejected(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)

	_, cookies := register(t, env, "outsider@test.com", "secret123", "Outsider")

	c := dialWS(t, srv, cookies, s.chID, 0)
	defer c.close()
	ack := c.sendMessage(map[string]any{
		"msg_type": "chat.text", "kind": "event",
		"payload": map[string]any{"text": "let me in"},
	})
	// not_member retired (连接模型勘误期): a connection has no身份色 — an eligibility
	// refusal is uniformly forbidden (observer may not drive business frames, 表①).
	if ack["type"] != "error" || ack["error"] != "forbidden" {
		t.Fatalf("non-member write: want error forbidden, got %v", ack)
	}
}

// TestWS_PostedRequestGetsTheSubstrateWideDeadline: a person's submitted
// request names no deadline, and the one it ends up with must be the
// SUBSTRATE's day-long fallback — the request is a human approval that may sit
// unanswered overnight.
//
// The two halves of this are each unit-tested against a double (the verb
// leaves an absent expires_at absent; the harness stamps its global TTL on an
// absent one), and both would stay green if the write ever went back through
// the caller-closure path, which resolves an absent deadline against the
// in-process default measured in SECONDS. Only the joined path can tell 24
// hours from 30 seconds, so it is asserted here, once, end to end.
func TestWS_PostedRequestGetsTheSubstrateWideDeadline(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)
	_, bActor := addSecondMember(t, env, s, "ttl-approver@test.com")

	ca := dialWS(t, srv, s.cookies, s.chID, 0)
	defer ca.close()

	before := time.Now().UnixMilli()
	ack := ca.sendMessage(map[string]any{
		"msg_type": "human.approve",
		"kind":     "request",
		"audience": []string{string(bActor)},
		"payload":  map[string]any{"q": "approve?"},
		// no expires_at on purpose — that is the whole point.
	})
	if ack["type"] != "ack" {
		t.Fatalf("request frame: want ack, got %v", ack)
	}
	after := time.Now().UnixMilli()
	reqID, _ := ack["message_id"].(string)

	entry := ca.waitTail(func(e map[string]any) bool { return e["id"] == reqID }, 3*time.Second)
	raw, ok := entry["expires_at"].(float64)
	if !ok {
		t.Fatalf("posted request carries no expires_at at all (%v) — an open request with no deadline is never reaped", entry["expires_at"])
	}
	const dayMs = 24 * 60 * 60 * 1000
	got := int64(raw)
	if got < before+dayMs || got > after+dayMs {
		t.Fatalf("posted request expires_at = %d, want the substrate's 24h fallback (between %d and %d) — a caller-side default would land seconds from now",
			got, before+dayMs, after+dayMs)
	}
}

// TestWS_ResolveFrameEndToEnd: A requests B (human.approve → left open); B resolves
// via a resolve frame; A's feed sees the completed terminal with the decision.
func TestWS_ResolveFrameEndToEnd(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)
	bCookies, bActor := addSecondMember(t, env, s, "approver@test.com")

	ca := dialWS(t, srv, s.cookies, s.chID, 0)
	defer ca.close()
	ack := ca.sendMessage(map[string]any{
		"msg_type": "human.approve",
		"kind":     "request",
		"audience": []string{string(bActor)},
		"payload":  map[string]any{"q": "approve?"},
	})
	if ack["type"] != "ack" {
		t.Fatalf("request frame: want ack, got %v", ack)
	}
	reqID := ack["message_id"].(string)

	cb := dialWS(t, srv, bCookies, s.chID, 0)
	defer cb.close()
	cb.send(map[string]any{"type": "resolve", "req_id": reqID, "decision": "approved", "payload": map[string]any{"note": "ok"}})
	rack := cb.nextAck(3 * time.Second)
	if rack["type"] != "ack" {
		t.Fatalf("resolve frame: want ack, got %v", rack)
	}

	term := ca.waitTail(func(e map[string]any) bool {
		return e["kind"] == "response" && e["parent_id"] == reqID
	}, 3*time.Second)
	pay, _ := term["payload"].(map[string]any)
	if pay == nil || pay["decision"] != "approved" {
		t.Fatalf("resolve terminal payload = %v, want decision=approved", term["payload"])
	}
}

// TestWS_CancelFrameEndToEnd: the cancel frame writes the cancel terminal, a
// non-sender is refused (unauthorized_sender), and a second cancel is已结束.
func TestWS_CancelFrameEndToEnd(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)
	bCookies, bActor := addSecondMember(t, env, s, "receiver@test.com")

	ca := dialWS(t, srv, s.cookies, s.chID, 0)
	defer ca.close()
	ack := ca.sendMessage(map[string]any{
		"msg_type": "human.approve",
		"kind":     "request",
		"audience": []string{string(bActor)},
		"payload":  map[string]any{"q": "ok?"},
	})
	if ack["type"] != "ack" {
		t.Fatalf("request frame: want ack, got %v", ack)
	}
	reqID := ack["message_id"].(string)

	// Non-sender B cancels → 门拒 unauthorized_sender (while still open).
	cb := dialWS(t, srv, bCookies, s.chID, 0)
	defer cb.close()
	cb.send(map[string]any{"type": "cancel", "req_id": reqID})
	be := cb.nextAck(3 * time.Second)
	if be["type"] != "error" || be["error"] != "unauthorized_sender" {
		t.Fatalf("non-sender cancel: want error unauthorized_sender, got %v", be)
	}

	// Sender A cancels → ack, and the cancel terminal lands on the feed.
	ca.send(map[string]any{"type": "cancel", "req_id": reqID})
	cack := ca.nextAck(3 * time.Second)
	if cack["type"] != "ack" {
		t.Fatalf("cancel frame: want ack, got %v", cack)
	}
	term := ca.waitTail(func(e map[string]any) bool {
		return e["kind"] == "response" && e["parent_id"] == reqID
	}, 3*time.Second)
	pay, _ := term["payload"].(map[string]any)
	if pay == nil || pay["cancelled"] != true {
		t.Fatalf("cancel terminal payload = %v, want cancelled=true", term["payload"])
	}

	// Second cancel → 已结束 (already_closed).
	ca.send(map[string]any{"type": "cancel", "req_id": reqID})
	cack2 := ca.nextAck(3 * time.Second)
	if cack2["type"] != "error" || cack2["error"] != "already_closed" {
		t.Fatalf("re-cancel: want error already_closed, got %v", cack2)
	}
}

// TestWS_Presence: connect feeds online, disconnect feeds an explicit offline
// snapshot (NOT decay-to-unknown); across a reconnect the L1 cell is the SAME
// (Stat.startedAt unchanged) and stays live throughout — layer2来去不碰layer1.
func TestWS_Presence(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)
	targetCookies, uid := addSecondMember(t, env, s, "presence-target@example.com")
	control := dialWS(t, srv, s.cookies, s.chID, 0)
	defer control.close()
	initial := waitActorStatus(t, env, s, control, uid, 2*time.Second, func(status introspect.Status) bool { return status.Present })

	c := dialWS(t, srv, targetCookies, s.chID, 0)
	connected := pollPresence(t, env, s, control, uid, true, true, 2*time.Second)
	if !connected.Present || connected.UptimeMs < initial.UptimeMs {
		t.Fatalf("connect restarted L1: before=%+v after=%+v", initial, connected)
	}

	c.close()
	pollPresence(t, env, s, control, uid, true, false, 2*time.Second)

	c2 := dialWS(t, srv, targetCookies, s.chID, 0)
	defer c2.close()
	reconnected := pollPresence(t, env, s, control, uid, true, true, 2*time.Second)
	if !reconnected.Present || reconnected.UptimeMs < initial.UptimeMs {
		t.Fatalf("reconnect restarted L1: before=%+v after=%+v", initial, reconnected)
	}
}

// TestWS_PresenceMultiTab: two ws for one (channel, user) — closing one tab must NOT
// flip the still-connected tab to offline; only the LAST disconnect does.
func TestWS_PresenceMultiTab(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)
	targetCookies, uid := addSecondMember(t, env, s, "multitab-target@example.com")
	control := dialWS(t, srv, s.cookies, s.chID, 0)
	defer control.close()
	waitActorStatus(t, env, s, control, uid, 2*time.Second, func(status introspect.Status) bool { return status.Present })

	tab1 := dialWS(t, srv, targetCookies, s.chID, 0)
	tab2 := dialWS(t, srv, targetCookies, s.chID, 0)
	pollPresence(t, env, s, control, uid, true, true, 2*time.Second)

	tab1.close()
	time.Sleep(200 * time.Millisecond)
	status := queryActorStatus(t, env, s, control, uid)
	testimony, known := status.L3[introspect.ObsDevicePresence]
	if !known || testimony.Device == nil || !testimony.Device.Online {
		t.Fatalf("after one tab closed the other is still open: status=%+v", status)
	}

	tab2.close()
	pollPresence(t, env, s, control, uid, true, false, 2*time.Second)
}

// ---------------------------------------------------------------------------
// after / cancel_timer frames (提醒五路收口物).
// ---------------------------------------------------------------------------

// TestWS_AfterFrameEndToEnd: a member arms a self-reminder over the ws; the
// durable-home timer fires and the reminder message lands on the same feed.
func TestWS_AfterFrameEndToEnd(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)

	c := dialWS(t, srv, s.cookies, s.chID, 0)
	defer c.close()

	c.send(map[string]any{
		"type": "after", "ref": "a1", "duration_ms": 100,
		"msg_type": "reminder.note", "payload": map[string]any{"note": "stand up"},
	})
	ack := c.nextAck(3 * time.Second)
	if ack["type"] != "ack" || ack["frame"] != "after" {
		t.Fatalf("after frame: want ack, got %v", ack)
	}
	timerID, _ := ack["timer_id"].(string)
	if timerID == "" {
		t.Fatalf("after ack missing timer_id: %v", ack)
	}

	fired := c.waitTail(func(e map[string]any) bool { return e["type"] == "reminder.note" }, 5*time.Second)
	sender, _ := fired["sender"].(map[string]any)
	if sender == nil || sender["id"] != string(s.actorID) {
		t.Fatalf("reminder sender = %v, want the subject", fired["sender"])
	}
}

// TestWS_CancelTimerFrame: arming then cancelling → no fire; and the input bounds
// refuse a non-positive duration outright (bad_payload — the driver's error词表).
func TestWS_CancelTimerFrame(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)

	c := dialWS(t, srv, s.cookies, s.chID, 0)
	defer c.close()

	// Bounds: zero / negative duration is refused at the driver (a past FireAt would
	// legally fire immediately — 期12 v0.4 P1-7). Error code is the driver's平面词.
	c.send(map[string]any{"type": "after", "ref": "b0", "duration_ms": 0, "msg_type": "reminder.note"})
	if errFrame := c.nextAck(3 * time.Second); errFrame["error"] != "bad_payload" {
		t.Fatalf("duration_ms=0: want bad_payload, got %v", errFrame)
	}

	c.send(map[string]any{
		"type": "after", "ref": "b1", "duration_ms": 250,
		"msg_type": "reminder.later", "payload": map[string]any{},
	})
	ack := c.nextAck(3 * time.Second)
	timerID, _ := ack["timer_id"].(string)
	if ack["type"] != "ack" || timerID == "" {
		t.Fatalf("after frame: %v", ack)
	}
	c.send(map[string]any{"type": "cancel_timer", "ref": "b2", "timer_id": timerID})
	cAck := c.nextAck(3 * time.Second)
	if cAck["type"] != "ack" || cAck["frame"] != "cancel_timer" {
		t.Fatalf("cancel_timer: want ack, got %v", cAck)
	}
	watchEnd := time.After(1 * time.Second)
	for {
		select {
		case m := <-c.tail:
			env2, _ := m["envelope"].(map[string]any)
			if env2 != nil && env2["type"] == "reminder.later" {
				t.Fatal("cancelled timer fired anyway")
			}
		case <-watchEnd:
			return
		}
	}
}

// TestWS_AfterFrameNonMember: a tail-only visitor may not arm identity timers.
func TestWS_AfterFrameNonMember(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)

	_, cookies2 := register(t, env, "visitor@example.com", "secret123", "Visitor")
	c := dialWS(t, srv, cookies2, s.chID, 0)
	defer c.close()

	c.send(map[string]any{"type": "after", "ref": "n1", "duration_ms": 1000, "msg_type": "reminder.note"})
	// not_member retired → uniformly forbidden (连接模型勘误期 表①).
	if errFrame := c.nextAck(3 * time.Second); errFrame["error"] != "forbidden" {
		t.Fatalf("non-member after: want forbidden, got %v", errFrame)
	}
}

// TestWS_OversizedFrameClosesConn pins the ws read-limit hardening: a write
// frame past wsMaxFrameBytes (a few hundred KB) is a malformed / hostile client, so
// SetReadLimit fails the read and the server closes the conn — the client's next
// read errors within a short window instead of the server allocating unboundedly.
// (The write-deadline + ping/pong keepalive on the same link are pure server-side
// network behaviour, not black-box observable in a fast unit test; asserted by
// construction, see ws.go.)
func TestWS_OversizedFrameClosesConn(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	hdr := http.Header{}
	var parts []string
	for _, ck := range s.cookies {
		parts = append(parts, ck.Name+"="+ck.Value)
	}
	hdr.Set("Cookie", strings.Join(parts, "; "))
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, hdr)
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	defer conn.Close()

	// Opening attach frame (channel-blind, v2 — the connector's read limit is armed
	// before this read).
	if err := conn.WriteJSON(map[string]any{"v": 2, "frame_type": "attach"}); err != nil {
		t.Fatalf("attach: %v", err)
	}

	// ~2 MB frame — well past the 512KB read limit (connector SetReadLimit).
	big := strings.Repeat("x", 2*1024*1024)
	if err := conn.WriteJSON(map[string]any{
		"v": 2, "frame_type": "submit",
		"payload": map[string]any{"channel_id": s.chID, "msg_type": "chat.text", "kind": "event", "payload": map[string]any{"text": big}},
	}); err != nil {
		return // a write error here already means the server closed — acceptable
	}

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return // server closed the conn after the oversized frame — as required
		}
	}
}
