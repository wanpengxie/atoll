package transit_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/runtime/transit"
)

// wsHarness wraps an httptest server speaking the same daemonbus
// upgrade protocol as server/daemonbus/HandleWS (query auth + send
// connection_accepted frame).
type wsHarness struct {
	t          *testing.T
	srv        *httptest.Server
	upgrader   websocket.Upgrader
	epochAlloc atomic.Int64

	mu          sync.Mutex
	current     *websocket.Conn
	expectedKey string
	expectedID  string
	// onConn is invoked once per accepted upgrade, with the freshly
	// allocated epoch + the open connection.
	onConn func(epoch int64, conn *websocket.Conn)
}

func newWSHarness(t *testing.T) *wsHarness {
	h := &wsHarness{
		t:           t,
		upgrader:    websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }},
		expectedKey: "sek-1",
		expectedID:  "daemon-A",
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/daemonbus", h.handle)
	h.srv = httptest.NewServer(mux)
	t.Cleanup(func() { h.srv.Close() })
	return h
}

func (h *wsHarness) handle(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Get("daemon_id") != h.expectedID || q.Get("key") != h.expectedKey {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.t.Errorf("upgrade: %v", err)
		return
	}
	epoch := h.epochAlloc.Add(1)
	// Send connection_accepted frame the client expects.
	frame := daemonbus.Frame{
		FrameID:               "boot-1",
		FrameType:             daemonbus.FrameType("control.connection_accepted"),
		DaemonID:              h.expectedID,
		DaemonConnectionEpoch: daemonbus.ConnectionEpoch(epoch),
		SentAt:                time.Now().UnixMilli(),
		Payload:               []byte(`{"connection_epoch":` + itoaInt64(epoch) + `}`),
	}
	data, _ := json.Marshal(frame)
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		h.t.Errorf("write accepted: %v", err)
		_ = conn.Close()
		return
	}
	h.mu.Lock()
	h.current = conn
	cb := h.onConn
	h.mu.Unlock()
	if cb != nil {
		cb(epoch, conn)
	}
}

func (h *wsHarness) Conn() *websocket.Conn {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.current
}

func (h *wsHarness) WSURL() string {
	return strings.Replace(h.srv.URL, "http://", "ws://", 1) + "/api/daemonbus"
}

func itoaInt64(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func TestWSClient_ConnectAndEpoch(t *testing.T) {
	h := newWSHarness(t)
	client, err := transit.NewWSClient(transit.WSClientConfig{
		URL:      h.WSURL(),
		DaemonID: "daemon-A",
		Key:      "sek-1",
		Version:  "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	epoch, err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if epoch != 1 {
		t.Errorf("epoch=%d want 1", epoch)
	}
}

func TestWSClient_SendRecvRoundTrip(t *testing.T) {
	h := newWSHarness(t)
	serverFrameCh := make(chan daemonbus.Frame, 1)
	h.onConn = func(_ int64, conn *websocket.Conn) {
		go func() {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var f daemonbus.Frame
			_ = json.Unmarshal(data, &f)
			serverFrameCh <- f
			// Echo back a control.heartbeat_ack so client can Recv.
			ack := daemonbus.Frame{
				FrameID:               "ack-1",
				FrameType:             daemonbus.FrameTypeControlHeartbeatAck,
				DaemonConnectionEpoch: f.DaemonConnectionEpoch,
				SentAt:                time.Now().UnixMilli(),
				Payload:               []byte(`{"frame_id":"` + f.FrameID + `"}`),
			}
			data, _ = json.Marshal(ack)
			_ = conn.WriteMessage(websocket.TextMessage, data)
		}()
	}

	client, err := transit.NewWSClient(transit.WSClientConfig{
		URL: h.WSURL(), DaemonID: "daemon-A", Key: "sek-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	epoch, err := client.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	frame := daemonbus.Frame{
		FrameID:               "hb-1",
		FrameType:             daemonbus.FrameTypeControlHeartbeat,
		DaemonID:              "daemon-A",
		DaemonConnectionEpoch: epoch,
		SentAt:                time.Now().UnixMilli(),
		Payload:               []byte(`{"channels":[]}`),
	}
	if err := client.Send(ctx, frame); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case got := <-serverFrameCh:
		if got.FrameType != daemonbus.FrameTypeControlHeartbeat {
			t.Errorf("server got frame_type=%s", got.FrameType)
		}
		if got.FrameID != frame.FrameID {
			t.Errorf("server got frame_id=%q want %q", got.FrameID, frame.FrameID)
		}
	case <-ctx.Done():
		t.Fatal("server never received frame")
	}

	got, err := client.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if got.FrameType != daemonbus.FrameTypeControlHeartbeatAck {
		t.Errorf("got frame_type=%s", got.FrameType)
	}
}

func TestWSClient_Reconnect(t *testing.T) {
	h := newWSHarness(t)
	client, err := transit.NewWSClient(transit.WSClientConfig{
		URL: h.WSURL(), DaemonID: "daemon-A", Key: "sek-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ep1, err := client.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ep1 != 1 {
		t.Fatalf("first epoch=%d", ep1)
	}
	// Server closes first connection.
	if c := h.Conn(); c != nil {
		_ = c.Close()
	}

	// Reconnect — should bump to epoch 2.
	ep2, err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	if ep2 <= ep1 {
		t.Errorf("epoch did not advance: %d → %d", ep1, ep2)
	}
}

func TestWSClient_UnauthorizedRejected(t *testing.T) {
	h := newWSHarness(t)
	client, err := transit.NewWSClient(transit.WSClientConfig{
		URL: h.WSURL(), DaemonID: "daemon-A", Key: "WRONG",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := client.Connect(ctx); err == nil {
		t.Fatal("expected unauthorized")
	}
}
