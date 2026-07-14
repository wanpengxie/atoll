package link

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
)

// yamuxPingCarrier counts yamux's real typePing frames at the raw carrier.
// yamux v0.1.2's fixed 12-byte header encodes MsgType at byte 1 and typePing
// as 2. Keeping this observation in the test assembly lets production retain
// one immutable yamux configuration while still proving that real keepalive
// traffic crossed the same wsByteStream the link uses.
type yamuxPingCarrier struct {
	io.ReadWriteCloser
	pings *atomic.Int64
}

func (c yamuxPingCarrier) Write(p []byte) (int, error) {
	if len(p) == 12 && p[0] == 0 && p[1] == 2 {
		c.pings.Add(1)
	}
	return c.ReadWriteCloser.Write(p)
}

func fastKeepAliveConfig() *yamux.Config {
	cfg := yamux.DefaultConfig()
	cfg.KeepAliveInterval = 15 * time.Millisecond
	cfg.LogOutput = io.Discard
	return cfg
}

func testWebSocketPair(t *testing.T) (client, server *websocket.Conn) {
	t.Helper()
	type upgradeResult struct {
		conn *websocket.Conn
		err  error
	}
	upgraded := make(chan upgradeResult, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		upgraded <- upgradeResult{conn: conn, err: err}
	}))
	t.Cleanup(srv.Close)

	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial websocket pair: %v", err)
	}
	result := <-upgraded
	if result.err != nil {
		_ = client.Close()
		t.Fatalf("upgrade websocket pair: %v", result.err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		_ = result.conn.Close()
	})
	return client, result.conn
}

// TestLease_KeepAliveAliveButAppFrozen_Expires proves the two-heartbeat
// boundary without changing production configuration: real, fast yamux
// keepalives cross the real wsByteStream carrier, but no application substream
// bytes arrive after the initial header, so the Lease still expires.
func TestLease_KeepAliveAliveButAppFrozen_Expires(t *testing.T) {
	clientWS, serverWS := testWebSocketPair(t)
	var pings atomic.Int64

	serverYamux, err := yamux.Server(yamuxPingCarrier{ReadWriteCloser: newWSByteStream(serverWS), pings: &pings}, fastKeepAliveConfig())
	if err != nil {
		t.Fatal(err)
	}
	clientYamux, err := yamux.Client(yamuxPingCarrier{ReadWriteCloser: newWSByteStream(clientWS), pings: &pings}, fastKeepAliveConfig())
	if err != nil {
		_ = serverYamux.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = clientYamux.Close()
		_ = serverYamux.Close()
	})

	lease := NewLease(10*time.Millisecond, 180*time.Millisecond)
	var applicationFrames atomic.Int64
	firstFrame := make(chan struct{})
	serverLink := &linkSession{
		ys: serverYamux,
		onFrame: func() {
			lease.Refresh()
			if applicationFrames.Add(1) == 1 {
				close(firstFrame)
			}
		},
		onActor: func(conn net.Conn) { _ = conn.Close() },
	}
	serverLink.start()

	clientLink := &linkSession{ys: clientYamux}
	actorStream, finish, err := clientLink.openTagged(context.Background(), streamActor)
	if err != nil {
		t.Fatalf("open tagged actor stream: %v", err)
	}
	finish()
	_ = actorStream.Close()

	select {
	case <-firstFrame:
	case <-time.After(time.Second):
		t.Fatal("server did not observe the initial application stream header")
	}
	// Let the stream FIN settle, then take the application-frame baseline. From
	// here onward the only carrier activity is yamux's own keepalive traffic.
	time.Sleep(30 * time.Millisecond)
	frameBaseline := applicationFrames.Load()
	pings.Store(0)

	expired := make(chan struct{})
	go lease.Watch(serverYamux.CloseChan(), func() { close(expired) })

	time.Sleep(100 * time.Millisecond)
	if got := pings.Load(); got < 4 {
		t.Fatalf("real yamux keepalive did not run often enough: ping frames=%d", got)
	}
	if got := applicationFrames.Load(); got != frameBaseline {
		t.Fatalf("yamux keepalive leaked into application liveness: frames %d -> %d", frameBaseline, got)
	}

	select {
	case <-expired:
	case <-time.After(time.Second):
		t.Fatal("application Lease did not expire while yamux keepalive remained active")
	}
}
