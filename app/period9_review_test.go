package app_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// ---------------------------------------------------------------------------
// 期9 三轮修复批 — black-box regression tests.
// ---------------------------------------------------------------------------

// TestWS_OversizedFrameClosesConn pins the ws read-limit hardening (#1): a write
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

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws?channel=" + s.chID
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

	// Opening attach frame (the connector's read limit is armed before this read).
	if err := conn.WriteJSON(map[string]any{"v": 1, "frame_type": "attach", "payload": map[string]any{"channel_id": s.chID}}); err != nil {
		t.Fatalf("attach: %v", err)
	}

	// ~2 MB frame — well past the 512KB read limit (connector SetReadLimit).
	big := strings.Repeat("x", 2*1024*1024)
	if err := conn.WriteJSON(map[string]any{
		"v": 1, "frame_type": "submit",
		"payload": map[string]any{"msg_type": "chat.text", "kind": "event", "payload": map[string]any{"text": big}},
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

// TestHalfBuiltChannel_DeleteSucceeds pins #3 ①: a半成品 channel (app-db row + empty
// channel-db membership, the createChannel crash window) must stay deletable. Delete
// authority is WORLD-LAYER (workspace member), judged from the directory alone — it
// must NOT depend on channel-internal membership or a complete home.
func TestHalfBuiltChannel_DeleteSucceeds(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)

	chID, err := env.app.CreateHalfBuiltChannelForTest(s.wsID, "half-built")
	if err != nil {
		t.Fatalf("CreateHalfBuiltChannelForTest: %v", err)
	}

	w := env.do(t, "DELETE", "/api/channels/"+chID, nil, s.cookies)
	if w.Code != http.StatusOK {
		t.Fatalf("delete half-built channel: want 200, got %d (%s)", w.Code, w.Body.String())
	}
	// Gone from the directory now.
	if g := env.do(t, "GET", "/api/channels/"+chID, nil, s.cookies); g.Code != http.StatusNotFound {
		t.Fatalf("deleted channel GET: want 404, got %d", g.Code)
	}
}

// TestHalfBuiltChannel_OpenClearError pins #3 ②: opening a半成品 channel must give a
// clear, non-panic response. A workspace member who is NOT a channel member (the
// creator whose Admit never landed, or any onlooker) may read directory metadata and
// tail, but a ws WRITE frame is refused with an explicit not_member error — never a
// panic on the empty-membership channel.
func TestHalfBuiltChannel_OpenClearError(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)

	chID, err := env.app.CreateHalfBuiltChannelForTest(s.wsID, "half-built-open")
	if err != nil {
		t.Fatalf("CreateHalfBuiltChannelForTest: %v", err)
	}

	// Directory metadata reads cleanly (200), no panic.
	if g := env.do(t, "GET", "/api/channels/"+chID, nil, s.cookies); g.Code != http.StatusOK {
		t.Fatalf("GET half-built channel: want 200, got %d (%s)", g.Code, g.Body.String())
	}
	// The actor roster reads cleanly too (the intrinsic system actor is always
	// present; no panic on empty user membership).
	if la := env.do(t, "GET", fmt.Sprintf("/api/channels/%s/actors", chID), nil, s.cookies); la.Code != http.StatusOK {
		t.Fatalf("list actors on half-built channel: want 200, got %d (%s)", la.Code, la.Body.String())
	}

	// Opening the ws: a non-channel-member (no Admit ever landed) may tail but a
	// write frame is refused with a clear not_member error — never a panic.
	c := dialWS(t, srv, s.cookies, chID, 0)
	defer c.close()
	ack := c.sendMessage(map[string]any{
		"msg_type": "chat.text", "kind": "event",
		"payload": map[string]any{"text": "anyone home?"},
	})
	if ack["type"] != "error" || ack["error"] != "not_member" {
		t.Fatalf("write to half-built channel: want error not_member, got %v", ack)
	}
}
