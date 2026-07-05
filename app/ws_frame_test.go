package app_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// ---------------------------------------------------------------------------
// wsClient — a black-box gateway ws driver for the S4 frame族 (message / resolve
// / cancel up, tail + ack/error down). One reader goroutine fans the two
// downstream frame families into buffered channels so a test can await either.
// ---------------------------------------------------------------------------

type wsClient struct {
	t    *testing.T
	conn *websocket.Conn
	tail chan map[string]any
	acks chan map[string]any
}

// dialWS opens a gateway ws with the given session cookies and sends the opening
// subscribe frame (channel + cursor).
func dialWS(t *testing.T, srv *httptest.Server, cookies []*http.Cookie, chID string, sinceSeq int64) *wsClient {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	hdr := http.Header{}
	var parts []string
	for _, c := range cookies {
		parts = append(parts, c.Name+"="+c.Value)
	}
	hdr.Set("Cookie", strings.Join(parts, "; "))
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, hdr)
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	if err := conn.WriteJSON(map[string]any{"type": "subscribe", "channel_id": chID, "since_seq": sinceSeq}); err != nil {
		t.Fatalf("subscribe frame: %v", err)
	}
	c := &wsClient{
		t:    t,
		conn: conn,
		tail: make(chan map[string]any, 512),
		acks: make(chan map[string]any, 64),
	}
	go c.readLoop()
	return c
}

func (c *wsClient) readLoop() {
	for {
		var m map[string]any
		if err := c.conn.ReadJSON(&m); err != nil {
			return
		}
		switch m["type"] {
		case "message":
			select {
			case c.tail <- m:
			default:
			}
		case "ack", "error":
			select {
			case c.acks <- m:
			default:
			}
		}
	}
}

func (c *wsClient) send(m map[string]any) {
	c.t.Helper()
	if err := c.conn.WriteJSON(m); err != nil {
		c.t.Fatalf("ws write: %v", err)
	}
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

// sendMessage sends a write message frame and returns its ack/error frame.
func (c *wsClient) sendMessage(frame map[string]any) map[string]any {
	c.t.Helper()
	frame["type"] = "message"
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
	if err := env.app.AdmitForTest(s.chID, aid, actor.KindHuman); err != nil {
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
// DoD 5 (S4 wire): message / resolve / cancel frames + presence.
// ---------------------------------------------------------------------------

// TestWS_MessageFrameEndToEnd: a member's message frame commits truth through the
// door (POST /messages is废) and comes back on the same ws's tail.
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
		t.Fatalf("message frame: want ack, got %v", ack)
	}
	msgID, _ := ack["message_id"].(string)
	if msgID == "" {
		t.Fatalf("ack missing message_id: %v", ack)
	}

	env2 := c.waitTail(func(e map[string]any) bool { return e["id"] == msgID }, 3*time.Second)
	if env2["type"] != "chat.text" {
		t.Fatalf("tail envelope type = %v, want chat.text", env2["type"])
	}

	// POST /messages is gone (write path is the frame): the route 404s.
	w := env.do(t, "POST", fmt.Sprintf("/api/channels/%s/messages", s.chID),
		map[string]any{"type": "chat.text", "payload": map[string]any{"text": "x"}}, s.cookies)
	if w.Code != http.StatusNotFound {
		t.Fatalf("POST /messages should be 404 (abolished), got %d", w.Code)
	}
}

// TestWS_NonMemberWriteRejected: a workspace member who is NOT a channel member may
// tail but a message frame is refused (膜律 看得见≠在里面).
func TestWS_NonMemberWriteRejected(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)

	// A second user in the workspace but NOT admitted to the channel.
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
// via a resolve frame; A's tail sees the completed terminal with the decision.
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

	// Sender A cancels → ack, and the cancel terminal lands on the tail.
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
	uid := actor.ActorID("user:" + s.userID)

	startedAt0, live0 := env.app.StatForTest(channel.ID(s.chID), uid)
	if !live0 {
		t.Fatal("member cell should be live before any connection (常驻)")
	}

	c := dialWS(t, srv, s.cookies, s.chID, 0)
	pollPresence(t, env, s, string(uid), true, true, 2*time.Second)

	// Stat stays live and unchanged while a device is connected.
	startedAt1, live1 := env.app.StatForTest(channel.ID(s.chID), uid)
	if !live1 || !startedAt1.Equal(startedAt0) {
		t.Fatalf("Stat moved on connect: live=%v startedAt %v→%v (presence must not touch L1)", live1, startedAt0, startedAt1)
	}

	// Disconnect → explicit offline snapshot (known stays true, online false).
	c.close()
	pollPresence(t, env, s, string(uid), true, false, 2*time.Second)

	// Reconnect → SAME cell (startedAt unchanged), still live.
	c2 := dialWS(t, srv, s.cookies, s.chID, 0)
	defer c2.close()
	pollPresence(t, env, s, string(uid), true, true, 2*time.Second)
	startedAt2, live2 := env.app.StatForTest(channel.ID(s.chID), uid)
	if !live2 || !startedAt2.Equal(startedAt0) {
		t.Fatalf("reconnect changed the cell: live=%v startedAt %v→%v", live2, startedAt0, startedAt2)
	}
}

// TestWS_PresenceMultiTab: two ws for one (channel, user) — closing one tab must
// NOT flip the still-connected tab to offline; only the LAST disconnect does.
func TestWS_PresenceMultiTab(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)
	uid := actor.ActorID("user:" + s.userID)

	tab1 := dialWS(t, srv, s.cookies, s.chID, 0)
	tab2 := dialWS(t, srv, s.cookies, s.chID, 0)
	pollPresence(t, env, s, string(uid), true, true, 2*time.Second)

	// Close tab1 — tab2 is still open, so presence must remain online.
	tab1.close()
	// Give the server time to process tab1's disconnect, then assert still online.
	time.Sleep(200 * time.Millisecond)
	w := env.do(t, "GET", fmt.Sprintf("/api/channels/%s/actors/%s/status", s.chID, string(uid)), nil, s.cookies)
	assertStatus(t, w, http.StatusOK)
	m := respJSON(t, w)
	if m["known"] != true || m["online"] != true {
		t.Fatalf("after one tab closed the other is still open: want online, got %v", m)
	}

	// Close the last tab — now offline.
	tab2.close()
	pollPresence(t, env, s, string(uid), true, false, 2*time.Second)
}
