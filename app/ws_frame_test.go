package app_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/atoll/app"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

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

	mu      sync.Mutex
	refType map[string]string // ref → originating frame type (for ack["frame"])
}

// dialWS opens a gateway ws for chID (query param — the app membrane authenticates
// pre-upgrade) and sends the opening attach frame (channel + since cursor).
func dialWS(t *testing.T, srv *httptest.Server, cookies []*http.Cookie, chID string, sinceSeq int64) *wsClient {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws?channel=" + chID
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
		tail:    make(chan map[string]any, 512),
		acks:    make(chan map[string]any, 64),
		refType: map[string]string{},
	}
	attach := map[string]any{"channel_id": chID}
	if sinceSeq > 0 {
		attach["since"] = map[string]int64{chID: sinceSeq}
	}
	c.writeFrame("attach", "", attach)
	go c.readLoop()
	return c
}

// writeFrame builds one standard frame (frame_type + ref + payload) and writes it.
func (c *wsClient) writeFrame(frameType, ref string, payload map[string]any) {
	c.t.Helper()
	if ref != "" {
		c.mu.Lock()
		c.refType[ref] = frameType
		c.mu.Unlock()
	}
	f := map[string]any{"v": 1, "frame_type": frameType}
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
			// The opening attach receipt (binding_gen only) is not a business ack —
			// drop it so a test's first nextAck is its own frame's receipt.
			if _, isAttach := pm["binding_gen"]; isAttach && len(pm) == 1 {
				continue
			}
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

func (c *wsClient) close() { _ = c.conn.Close() }

// addSecondMember registers a fresh user, joins it to A's workspace, admits it to
// the channel as a human member, and waits for its cell to embody — a second live
// subject the cross-sender door assertions need.
func addSecondMember(t *testing.T, env *testEnv, s setupResult, email string) ([]*http.Cookie, actor.ActorID) {
	t.Helper()
	regBody, cookies := register(t, env, email, "secret123", "Second")
	uid := regBody["id"].(string)
	if err := env.app.AddWorkspaceMemberForTest(s.wsID, uid); err != nil {
		t.Fatalf("add workspace member: %v", err)
	}
	aid := actor.ActorID("user:" + uid)
	aid, err := env.app.AdmitForTest(s.chID, aid, actor.KindHuman)
	if err != nil {
		t.Fatalf("admit second member: %v", err)
	}
	if !env.app.WaitLiveForTest(s.chID, aid, 2*time.Second) {
		t.Fatal("second member cell did not embody")
	}
	return cookies, aid
}

// pollPresence polls the actor-status endpoint until (known, online) matches, or
// fatals. When wantKnown is false only known is checked.
func pollPresence(t *testing.T, env *testEnv, s setupResult, actorID string, wantKnown, wantOnline bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for {
		w := env.do(t, "GET", fmt.Sprintf("/api/channels/%s/actors/%s/status", s.chID, actorID), nil, s.cookies)
		last = w.Body.String()
		if w.Code == http.StatusOK {
			m := respJSON(t, w)
			known, _ := m["known"].(bool)
			if known == wantKnown {
				if !wantKnown {
					return
				}
				online, _ := m["online"].(bool)
				if online == wantOnline {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("presence %s: want known=%v online=%v; last: %s", actorID, wantKnown, wantOnline, last)
		}
		time.Sleep(20 * time.Millisecond)
	}
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

// TestWS_NonMemberWriteRejected: a workspace member who is NOT a channel member may
// tail but a submit frame is refused (膜律 看得见≠在里面).
func TestWS_NonMemberWriteRejected(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)

	regBody, cookies := register(t, env, "outsider@test.com", "secret123", "Outsider")
	if err := env.app.AddWorkspaceMemberForTest(s.wsID, regBody["id"].(string)); err != nil {
		t.Fatalf("add workspace member: %v", err)
	}

	c := dialWS(t, srv, cookies, s.chID, 0)
	defer c.close()
	ack := c.sendMessage(map[string]any{
		"msg_type": "chat.text", "kind": "event",
		"payload": map[string]any{"text": "let me in"},
	})
	if ack["type"] != "error" || ack["error"] != "not_member" {
		t.Fatalf("non-member write: want error not_member, got %v", ack)
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
	uid := s.actorID

	startedAt0, live0 := env.app.StatForTest(channel.ID(s.chID), uid)
	if !live0 {
		t.Fatal("member cell should be live before any connection (常驻)")
	}

	c := dialWS(t, srv, s.cookies, s.chID, 0)
	pollPresence(t, env, s, string(uid), true, true, 2*time.Second)

	startedAt1, live1 := env.app.StatForTest(channel.ID(s.chID), uid)
	if !live1 || !startedAt1.Equal(startedAt0) {
		t.Fatalf("Stat moved on connect: live=%v startedAt %v→%v (presence must not touch L1)", live1, startedAt0, startedAt1)
	}

	c.close()
	pollPresence(t, env, s, string(uid), true, false, 2*time.Second)

	c2 := dialWS(t, srv, s.cookies, s.chID, 0)
	defer c2.close()
	pollPresence(t, env, s, string(uid), true, true, 2*time.Second)
	startedAt2, live2 := env.app.StatForTest(channel.ID(s.chID), uid)
	if !live2 || !startedAt2.Equal(startedAt0) {
		t.Fatalf("reconnect changed the cell: live=%v startedAt %v→%v", live2, startedAt0, startedAt2)
	}
}

// TestWS_PresenceMultiTab: two ws for one (channel, user) — closing one tab must NOT
// flip the still-connected tab to offline; only the LAST disconnect does.
func TestWS_PresenceMultiTab(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)
	uid := s.actorID

	tab1 := dialWS(t, srv, s.cookies, s.chID, 0)
	tab2 := dialWS(t, srv, s.cookies, s.chID, 0)
	pollPresence(t, env, s, string(uid), true, true, 2*time.Second)

	tab1.close()
	time.Sleep(200 * time.Millisecond)
	w := env.do(t, "GET", fmt.Sprintf("/api/channels/%s/actors/%s/status", s.chID, string(uid)), nil, s.cookies)
	assertStatus(t, w, http.StatusOK)
	m := respJSON(t, w)
	if m["known"] != true || m["online"] != true {
		t.Fatalf("after one tab closed the other is still open: want online, got %v", m)
	}

	tab2.close()
	pollPresence(t, env, s, string(uid), true, false, 2*time.Second)
}

// ---------------------------------------------------------------------------
// after / cancel_timer frames (提醒五路收口物).
// ---------------------------------------------------------------------------

// TestWS_AfterFrameEndToEnd: a member arms a self-reminder over the ws; the
// identity-bound timer fires and the reminder message lands on the same feed.
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

	regBody, cookies2 := register(t, env, "visitor@example.com", "secret123", "Visitor")
	if err := env.app.AddWorkspaceMemberForTest(s.wsID, regBody["id"].(string)); err != nil {
		t.Fatalf("add workspace member: %v", err)
	}
	c := dialWS(t, srv, cookies2, s.chID, 0)
	defer c.close()

	c.send(map[string]any{"type": "after", "ref": "n1", "duration_ms": 1000, "msg_type": "reminder.note"})
	if errFrame := c.nextAck(3 * time.Second); errFrame["error"] != "not_member" {
		t.Fatalf("non-member after: want not_member, got %v", errFrame)
	}
}

// TestWSSubmitErrCode pins the message-frame error mapping (期12 修复批 P0-2 的直接
// 测试): a killed cell is the retryable "unavailable", a closing home "closed" —
// never "internal"; only a genuinely unknown error logs.
func TestWSSubmitErrCode(t *testing.T) {
	cases := []struct {
		err      error
		code     string
		internal bool
	}{
		{platform.ErrCellUnavailable, "unavailable", false},
		{platform.ErrClosed, "closed", false},
		{platform.ErrNotMember, "not_member", false},
		{&platform.WriteRejectedError{Reason: "write_denied", Detail: "d"}, "write_denied", false},
		{fmt.Errorf("wrapped: %w", platform.ErrCellUnavailable), "unavailable", false},
		{errors.New("boom"), "internal", true},
	}
	for _, tc := range cases {
		code, _, internal := app.WSSubmitErrCodeForTest(tc.err)
		if code != tc.code || internal != tc.internal {
			t.Fatalf("wsSubmitErrCode(%v) = (%q, internal=%v), want (%q, %v)", tc.err, code, internal, tc.code, tc.internal)
		}
	}
}
