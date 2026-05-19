package daemonbus_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	kerneldaemonbus "github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
	"github.com/wanpengxie/ActOS/server/daemonbus"
	"github.com/wanpengxie/ActOS/server/store"
)

func newSvc(t *testing.T) *daemonbus.Service {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "db.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return daemonbus.NewService(db, daemonbus.Config{SharedSecret: "test-secret"})
}

// pipeTransport is an in-memory bidirectional transport: send via one
// channel, receive via the other. Used by both "daemon side" and
// "server side" of the test (each side wires opposite read/write
// channels).
type pipeTransport struct {
	in   chan kerneldaemonbus.Frame
	out  chan kerneldaemonbus.Frame
	once sync.Once
	done chan struct{}
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
	case f, ok := <-p.in:
		if !ok {
			return kerneldaemonbus.Frame{}, errors.New("closed")
		}
		return f, nil
	case <-p.done:
		return kerneldaemonbus.Frame{}, errors.New("closed")
	case <-ctx.Done():
		return kerneldaemonbus.Frame{}, ctx.Err()
	}
}
func (p *pipeTransport) WriteFrame(ctx context.Context, f kerneldaemonbus.Frame) error {
	select {
	case p.out <- f:
		return nil
	case <-p.done:
		return errors.New("closed")
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (p *pipeTransport) Close() error {
	p.once.Do(func() { close(p.done) })
	return nil
}

// TestRegisterAndIssueEpoch covers the daemons table + epoch counter.
func TestRegisterAndIssueEpoch(t *testing.T) {
	t.Parallel()
	svc := newSvc(t)
	ctx := context.Background()

	if err := svc.RegisterDaemon(ctx, placement.DaemonID("d1"), "localhost", "v0", 32, "test-secret"); err != nil {
		t.Fatalf("RegisterDaemon: %v", err)
	}
	if err := svc.RegisterDaemon(ctx, placement.DaemonID("d1"), "localhost", "v0", 32, "wrong"); err != daemonbus.ErrDaemonAuthFailed {
		t.Errorf("auth wrong err=%v want ErrDaemonAuthFailed", err)
	}
	e1, err := svc.IssueConnectionEpoch(ctx, placement.DaemonID("d1"))
	if err != nil || e1 != 1 {
		t.Errorf("epoch1=%d err=%v", e1, err)
	}
	e2, _ := svc.IssueConnectionEpoch(ctx, placement.DaemonID("d1"))
	if e2 != 2 {
		t.Errorf("epoch2=%d want 2", e2)
	}
	if _, err := svc.IssueConnectionEpoch(ctx, placement.DaemonID("ghost")); err != daemonbus.ErrDaemonNotRegistered {
		t.Errorf("ghost err=%v want ErrDaemonNotRegistered", err)
	}
}

// TestDispatchPushAndAck routes a viewsync.push through the dispatch
// loop and confirms the handler is invoked + a viewsync.ack is sent
// back with the correct cursor.
func TestDispatchPushAndAck(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	svr, dmn := newPipePair()
	conn := daemonbus.NewConnection(placement.DaemonID("d1"), 5, svr)

	pushed := make(chan viewsync.PushFrame, 1)
	handlers := daemonbus.Handlers{
		OnPush: func(ctx context.Context, c *daemonbus.Connection, f viewsync.PushFrame) (viewsync.LastReceivedSeq, error) {
			pushed <- f
			return viewsync.LastReceivedSeq(int64(f.Seq)), nil
		},
	}

	runErr := make(chan error, 1)
	go func() { runErr <- conn.Run(ctx, handlers) }()

	// "daemon side" sends viewsync.push frame.
	pushFrame := viewsync.PushFrame{
		ChannelID: channel.ID("ch-A"), Seq: 7, MessageID: "m-7",
		Envelope: message.Envelope{
			ID: "m-7", TS: 1, ChannelID: "ch-A",
			Sender: message.Sender{Kind: actor.KindAgent, ID: "a"},
			Kind:   message.KindEvent, Type: "agent.text",
			Payload:    json.RawMessage(`{}`),
			Visibility: message.VisibilityPublic, Audience: []string{"*"},
		},
	}
	rawPayload, _ := json.Marshal(pushFrame)
	if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
		FrameID: "p-1", FrameType: kerneldaemonbus.FrameTypeViewsyncPush,
		DaemonID: "d1", DaemonConnectionEpoch: 5, Payload: rawPayload,
	}); err != nil {
		t.Fatalf("write push: %v", err)
	}

	select {
	case f := <-pushed:
		if int64(f.Seq) != 7 || f.MessageID != "m-7" {
			t.Errorf("push handler got wrong frame: %+v", f)
		}
	case <-ctx.Done():
		t.Fatal("OnPush never called")
	}

	// "daemon side" reads back the ack from server.
	ack, err := dmn.ReadFrame(ctx)
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}
	if ack.FrameType != kerneldaemonbus.FrameTypeViewsyncAck {
		t.Errorf("ack frame_type=%q", ack.FrameType)
	}

	var ackBody viewsync.AckFrame
	if err := json.Unmarshal(ack.Payload, &ackBody); err != nil {
		t.Fatalf("unmarshal ack: %v", err)
	}
	if int64(ackBody.LastReceivedSeq) != 7 {
		t.Errorf("ack seq=%d want 7", ackBody.LastReceivedSeq)
	}

	_ = conn.Close()
	<-runErr
}

// TestDispatch_DeviceTransitSend_RoutesToHandler covers the T147 §A-S1
// routing fix: a daemon-sent device_transit.send frame must reach the
// OnDeviceTransitSend hook (previously the dispatch case matched
// FrameTypeDeviceTransitRecv — the wrong direction — so daemon pushes
// were silently dropped on the server side).
func TestDispatch_DeviceTransitSend_RoutesToHandler(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	svr, dmn := newPipePair()
	conn := daemonbus.NewConnection(placement.DaemonID("d1"), 5, svr)

	got := make(chan kerneldaemonbus.Frame, 1)
	handlers := daemonbus.Handlers{
		OnDeviceTransitSend: func(_ context.Context, _ *daemonbus.Connection, f kerneldaemonbus.Frame) error {
			got <- f
			return nil
		},
	}
	go func() { _ = conn.Run(ctx, handlers) }()

	body := map[string]any{
		"channel_id":        "ch-A",
		"device_session_id": "sess-1",
		"direction":         "to_device",
		"request_id":        "req-1",
		"payload":           []byte(`{"cmd":"publish"}`),
	}
	raw, _ := json.Marshal(body)
	if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
		FrameID:               "frame-send-1",
		FrameType:             kerneldaemonbus.FrameTypeDeviceTransitSend,
		DaemonID:              "d1",
		DaemonConnectionEpoch: 5,
		Payload:               raw,
	}); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	select {
	case f := <-got:
		if f.FrameType != kerneldaemonbus.FrameTypeDeviceTransitSend {
			t.Errorf("frame_type=%q want %q", f.FrameType, kerneldaemonbus.FrameTypeDeviceTransitSend)
		}
		if f.FrameID != "frame-send-1" {
			t.Errorf("frame_id=%q", f.FrameID)
		}
	case <-ctx.Done():
		t.Fatal("OnDeviceTransitSend never invoked — dispatch routing still broken")
	}
	_ = conn.Close()
}

// TestStaleEpochDropped ensures frames with an older epoch are
// silently dropped (L2 §9.4).
func TestStaleEpochDropped(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	svr, dmn := newPipePair()
	conn := daemonbus.NewConnection(placement.DaemonID("d1"), 5, svr)

	pushed := make(chan struct{}, 1)
	handlers := daemonbus.Handlers{
		OnPush: func(ctx context.Context, c *daemonbus.Connection, f viewsync.PushFrame) (viewsync.LastReceivedSeq, error) {
			pushed <- struct{}{}
			return 0, nil
		},
	}

	go func() { _ = conn.Run(ctx, handlers) }()

	pushFrame := viewsync.PushFrame{ChannelID: "ch-A", Seq: 1, MessageID: "m-1"}
	raw, _ := json.Marshal(pushFrame)
	// Frame epoch = 3, server epoch = 5 → stale → dropped.
	_ = dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
		FrameID: "p-stale", FrameType: kerneldaemonbus.FrameTypeViewsyncPush,
		DaemonID: "d1", DaemonConnectionEpoch: 3, Payload: raw,
	})

	select {
	case <-pushed:
		t.Fatal("stale-epoch push frame was dispatched")
	case <-time.After(150 * time.Millisecond):
	}

	_ = conn.Close()
}

// emptyHandlersProvider is a no-op HandlersProvider used to spin up
// the WS handler when the test cares about transport keepalive
// rather than business dispatch.
type emptyHandlersProvider struct{}

func (emptyHandlersProvider) DaemonbusHandlers() daemonbus.Handlers {
	return daemonbus.Handlers{}
}

// TestHandleWS_IdleClientTrippedByReadDeadline is the regression test
// for audit section A (daemonbus-bugs-audit-20260518.md): a daemon
// that connects then goes completely silent (no reads, no writes,
// no pongs) must be reaped on the server side within ~IdleReadTimeout
// rather than lingering forever. Without ping/pong + read deadline
// the server's Connection.Run wedges on ReadMessage indefinitely.
//
// Strategy: dial in but install a no-op PingHandler on the client so
// it ignores server pings (i.e. never replies with pong). Server
// idleReadTimeout is overridden to 500ms; ping cadence 100ms; the
// client SetReadDeadline(time.Time{}) so its own read never trips.
// We then verify the server-side conn is unregistered within ~2s.
func TestHandleWS_IdleClientTrippedByReadDeadline(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "db.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	svc := daemonbus.NewService(db, daemonbus.Config{
		SharedSecret:     "test-secret",
		PingCadence:      100 * time.Millisecond,
		IdleReadTimeout:  500 * time.Millisecond,
		PingWriteTimeout: 250 * time.Millisecond,
	})
	// Pre-register the daemon so HandleWS doesn't fail.
	if err := svc.RegisterDaemon(ctx, placement.DaemonID("d1"), "h", "v", 0, "test-secret"); err != nil {
		t.Fatalf("register: %v", err)
	}

	r := gin.New()
	r.GET("/daemonbus", svc.HandleWS(emptyHandlersProvider{}))
	srv := httptest.NewServer(r)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/daemonbus?daemon_id=d1&key=test-secret"
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = ws.Close() }()

	// Install a no-op PingHandler so the client never replies with
	// pong. This simulates a half-open peer that silently drops
	// server frames. Without ping/pong + read deadline on the server
	// side, ConnectionFor("d1") would still return non-nil minutes
	// from now.
	ws.SetPingHandler(func(string) error { return nil })

	// Drain the connection_accepted frame so it doesn't block the
	// server's send buffer.
	_ = ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := ws.ReadMessage(); err != nil {
		t.Fatalf("read accepted: %v", err)
	}
	// And stop reading entirely from here on. Server pings pile up
	// in the OS send buffer; pong never comes back; server idle
	// deadline trips at +500ms.
	_ = ws.SetReadDeadline(time.Time{})

	// Wait for server to mark the conn dead. Poll ConnectionFor.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := svc.ConnectionFor(placement.DaemonID("d1")); !ok {
			return // success — server cleaned up
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("server did not unregister the silent daemon connection within 3s — idle read deadline not enforced")
}

// TestHandleWS_PingKeepsConnAlive complements the previous test: a
// well-behaved daemon (gorilla default PingHandler auto-replies with
// pong) MUST keep the conn open across multiple ping cadences. This
// asserts we did not over-eagerly close on healthy peers.
func TestHandleWS_PingKeepsConnAlive(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "db.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	svc := daemonbus.NewService(db, daemonbus.Config{
		SharedSecret:     "test-secret",
		PingCadence:      100 * time.Millisecond,
		IdleReadTimeout:  400 * time.Millisecond,
		PingWriteTimeout: 250 * time.Millisecond,
	})
	if err := svc.RegisterDaemon(ctx, placement.DaemonID("d2"), "h", "v", 0, "test-secret"); err != nil {
		t.Fatalf("register: %v", err)
	}

	r := gin.New()
	r.GET("/daemonbus", svc.HandleWS(emptyHandlersProvider{}))
	srv := httptest.NewServer(r)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/daemonbus?daemon_id=d2&key=test-secret"
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = ws.Close() }()

	// Client-side reader goroutine: drain everything (gorilla's
	// default PingHandler auto-replies with pong, so we just need to
	// keep ReadMessage running for pings to be handled). No-op on
	// data frames.
	readErr := make(chan error, 1)
	go func() {
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				readErr <- err
				return
			}
		}
	}()

	// Hold for ~1.2s — well past 3× the idle timeout (400ms). If
	// pong refresh didn't work, the server would close before now.
	select {
	case err := <-readErr:
		t.Fatalf("client read errored before 1.2s — server prematurely closed: %v", err)
	case <-time.After(1200 * time.Millisecond):
	}

	if _, ok := svc.ConnectionFor(placement.DaemonID("d2")); !ok {
		t.Fatal("server unregistered a healthy (pong-responding) daemon")
	}
}
