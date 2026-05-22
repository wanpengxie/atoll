package pushhub

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
	"github.com/wanpengxie/ActOS/pkg/metrics"
)

// allowAllAuth lets every subscribe through — pushhub keepalive tests
// don't care about membership semantics.
type allowAllAuth struct{}

func (allowAllAuth) AuthorizeChannelAccess(context.Context, string, string) error {
	return nil
}

func (allowAllAuth) MemberActorID(_ context.Context, _ string, userID string) (string, error) {
	return "user:" + userID, nil
}

// fakeIdentityAuthenticator stubs identity.Service.Authenticate by
// hooking the HandleWS path with a direct upgrade — avoids pulling
// in the full identity registration flow. Instead we build a tiny
// HTTP handler that mirrors what HandleWS does but skips auth.
//
// We do this because pushhub.HandleWS depends on *identity.Service
// which requires DB + bcrypt + verification code plumbing — heavy
// for a keepalive test. Instead we exercise subscriber + pumpWrite +
// pumpRead directly via a constructed Service + raw upgrader, which is
// what HandleWS does after auth anyway.

// upgradeAndRun mirrors HandleWS minus identity auth. Returns the
// route path the caller can dial.
func upgradeAndRun(t *testing.T, hub *Service, userID string) (*httptest.Server, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/ws", func(c *gin.Context) {
		up := hub.upgrader()
		ws, err := up.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			return
		}
		cadence, idle, pingWrite := hub.keepaliveCfg()
		sub := newSubscriber(ws, userID, cadence, idle, pingWrite)
		go sub.pumpWrite()
		sub.pumpRead(c.Request.Context(), hub)
	})
	srv := httptest.NewServer(r)
	t.Cleanup(func() { srv.Close() })
	return srv, "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
}

func originPolicyServer(t *testing.T, hub *Service) string {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/ws", func(c *gin.Context) {
		up := hub.upgrader()
		ws, err := up.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			return
		}
		_ = ws.Close()
	})
	srv := httptest.NewServer(r)
	t.Cleanup(func() { srv.Close() })
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
}

func TestPushhubOriginPolicy(t *testing.T) {
	t.Parallel()

	const allowedOrigin = "https://ui.example"

	t.Run("browser origin denied without allowlist", func(t *testing.T) {
		t.Parallel()
		wsURL := originPolicyServer(t, NewService())
		header := http.Header{}
		header.Set("Origin", allowedOrigin)
		ws, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
		if err == nil {
			_ = ws.Close()
			t.Fatal("dial with browser Origin and no allowlist succeeded")
		}
		if resp == nil {
			t.Fatalf("dial response nil: %v", err)
		}
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status=%d want 403", resp.StatusCode)
		}
	})

	t.Run("browser origin allowed by exact allowlist", func(t *testing.T) {
		t.Parallel()
		wsURL := originPolicyServer(t, NewService(Config{AllowedOrigins: []string{allowedOrigin}}))
		header := http.Header{}
		header.Set("Origin", allowedOrigin)
		ws, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
		if err != nil {
			status := 0
			if resp != nil {
				status = resp.StatusCode
			}
			t.Fatalf("dial with allowlisted Origin failed: status=%d err=%v", status, err)
		}
		_ = ws.Close()
	})

	t.Run("missing origin allowed for non-browser client", func(t *testing.T) {
		t.Parallel()
		wsURL := originPolicyServer(t, NewService())
		ws, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			status := 0
			if resp != nil {
				status = resp.StatusCode
			}
			t.Fatalf("dial without Origin failed: status=%d err=%v", status, err)
		}
		_ = ws.Close()
	})
}

// TestPushhub_IdleSubscriberReaped is the regression test for the
// audit ask: a UI subscriber that goes silent (browser tab killed
// without sending a close frame, OS TCP keepalive disabled, etc.)
// must be reaped from h.subs within ~IdleReadTimeout. Without
// ping/pong the dead subscriber would linger until the next push
// attempt failed.
func TestPushhub_IdleSubscriberReaped(t *testing.T) {
	t.Parallel()

	hub := NewService()
	hub.SetAccessAuthorizer(allowAllAuth{})
	hub.SetKeepaliveForTest(
		80*time.Millisecond,  // ping cadence
		400*time.Millisecond, // idle read timeout
		200*time.Millisecond, // ping write timeout
	)
	_, wsURL := upgradeAndRun(t, hub, "u1")

	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = ws.Close() }()

	// Subscribe so the subscriber registers in h.subs.
	if err := ws.WriteJSON(map[string]any{"type": "subscribe", "channel_id": "ch-1"}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Wait for the subscribe to take effect.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if hub.SubscriberCount("ch-1") == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if hub.SubscriberCount("ch-1") != 1 {
		t.Fatalf("subscribe didn't take effect: count=%d", hub.SubscriberCount("ch-1"))
	}

	// Silence the client — no pong replies, no reads.
	ws.SetPingHandler(func(string) error { return nil })

	// Wait for the server to reap the subscriber.
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if hub.SubscriberCount("ch-1") == 0 {
			return // success
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("server did not reap idle subscriber within 3s — pushhub idle read deadline not enforced")
}

// TestPushhub_HealthyClientStaysSubscribed asserts that a normal
// client (gorilla's default PingHandler auto-replies with pong) stays
// in h.subs across multiple ping cadences. Otherwise we'd be killing
// healthy subscribers.
func TestPushhub_HealthyClientStaysSubscribed(t *testing.T) {
	t.Parallel()

	hub := NewService()
	hub.SetAccessAuthorizer(allowAllAuth{})
	hub.SetKeepaliveForTest(
		80*time.Millisecond,
		400*time.Millisecond,
		200*time.Millisecond,
	)
	_, wsURL := upgradeAndRun(t, hub, "u2")

	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = ws.Close() }()

	if err := ws.WriteJSON(map[string]any{"type": "subscribe", "channel_id": "ch-2"}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	// Wait for subscribe to register.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if hub.SubscriberCount("ch-2") == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if hub.SubscriberCount("ch-2") != 1 {
		t.Fatal("subscribe didn't take effect")
	}

	// Reader goroutine drains incoming control + data frames. Without
	// an active reader gorilla won't invoke its default PingHandler.
	readDone := make(chan error, 1)
	go func() {
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				readDone <- err
				return
			}
		}
	}()

	// Hold for ~1.2s, well past 3× idle timeout.
	select {
	case err := <-readDone:
		t.Fatalf("client read errored before 1.2s — server prematurely closed: %v", err)
	case <-time.After(1200 * time.Millisecond):
	}

	if hub.SubscriberCount("ch-2") != 1 {
		t.Fatalf("healthy subscriber reaped: count=%d", hub.SubscriberCount("ch-2"))
	}
}

func TestPushMessageSuppressesDuplicateSeqPerSubscriber(t *testing.T) {
	t.Parallel()

	reg := metrics.NewRegistry()
	hub := NewService(Config{Metrics: reg})
	hub.SetAccessAuthorizer(allowAllAuth{})
	ch := channel.ID("ch-dup")
	sub := newPushhubTestSubscriber("u1", 4)
	registerPushhubTestSubscriber(hub, ch, sub)

	hub.PushMessage(ch, 1, pushhubTestEnvelope(ch, "m-1"))
	hub.PushMessage(ch, 1, pushhubTestEnvelope(ch, "m-1-duplicate"))
	if got := len(sub.send); got != 1 {
		t.Fatalf("queued frames after duplicate = %d, want 1", got)
	}

	hub.PushMessage(ch, 2, pushhubTestEnvelope(ch, "m-2"))
	if got := len(sub.send); got != 2 {
		t.Fatalf("queued frames after next seq = %d, want 2", got)
	}

	raw := <-sub.send
	var frame struct {
		Seq int64 `json:"seq"`
	}
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("unmarshal pushed frame: %v", err)
	}
	if frame.Seq != 1 {
		t.Fatalf("first pushed seq = %d, want 1", frame.Seq)
	}
	if !strings.Contains(reg.RenderPrometheus(), `pushhub_fanout{result="duplicate"} 1`) {
		t.Fatalf("duplicate metric missing:\n%s", reg.RenderPrometheus())
	}
}

func TestPushMessageSlowSubscriberClosedOnFullQueue(t *testing.T) {
	t.Parallel()

	reg := metrics.NewRegistry()
	hub := NewService(Config{Metrics: reg})
	hub.SetAccessAuthorizer(allowAllAuth{})
	ch := channel.ID("ch-slow")
	sub := newPushhubTestSubscriber("u1", 1)
	registerPushhubTestSubscriber(hub, ch, sub)
	sub.send <- []byte(`{"type":"busy"}`)

	hub.PushMessage(ch, 1, pushhubTestEnvelope(ch, "m-1"))

	select {
	case <-sub.done:
	default:
		t.Fatal("slow subscriber was not closed")
	}
	if !strings.Contains(reg.RenderPrometheus(), `pushhub_fanout{result="slow_closed"} 1`) {
		t.Fatalf("slow_closed metric missing:\n%s", reg.RenderPrometheus())
	}
}

func newPushhubTestSubscriber(userID string, sendCap int) *subscriber {
	return &subscriber{
		userID:       userID,
		chans:        map[channel.ID]struct{}{},
		send:         make(chan []byte, sendCap),
		done:         make(chan struct{}),
		lastEnqueued: map[channel.ID]viewsync.Seq{},
	}
}

func registerPushhubTestSubscriber(hub *Service, ch channel.ID, sub *subscriber) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if _, ok := hub.subs[ch]; !ok {
		hub.subs[ch] = map[string]map[*subscriber]struct{}{}
	}
	if _, ok := hub.subs[ch][sub.userID]; !ok {
		hub.subs[ch][sub.userID] = map[*subscriber]struct{}{}
	}
	hub.subs[ch][sub.userID][sub] = struct{}{}
	sub.chans[ch] = struct{}{}
}

func pushhubTestEnvelope(ch channel.ID, id message.ID) message.Envelope {
	return message.Envelope{
		ID:        id,
		TS:        time.Now().UnixMilli(),
		ChannelID: ch,
		Sender: message.Sender{
			Kind: actor.KindHuman,
			ID:   actor.ActorID("user:u1"),
			Name: "User",
		},
		Kind:       message.KindEvent,
		Type:       "test.event",
		Payload:    json.RawMessage(`{}`),
		Visibility: message.VisibilityPublic,
	}
}
