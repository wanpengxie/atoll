package devicebus

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	proxycontract "github.com/wanpengxie/ActOS/internal/proxy/contract"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/server/store"
)

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "devicebus.db"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type captureForwarder struct {
	ch chan DeviceFrame
}

func (f *captureForwarder) ForwardDeviceFrame(_ context.Context, frame DeviceFrame, adapterActorID actor.ActorID) error {
	frame.ActorID = string(adapterActorID)
	f.ch <- frame
	return nil
}

type failingNotifier struct{}

func (f failingNotifier) NotifyProxyDaemonReady(context.Context, Daemon, DaemonReadyInput) error {
	return context.Canceled
}

func (f failingNotifier) NotifyProxyDaemonOffline(context.Context, Daemon, []actor.ActorID) error {
	return nil
}

func (f failingNotifier) NotifyDeviceLifecycle(context.Context, channel.ID, actor.ActorID, devicetransit.LifecycleEvent, string, string) {
}

type lifecycleCall struct {
	channelID channel.ID
	actorID   actor.ActorID
	event     devicetransit.LifecycleEvent
}

type recordingNotifier struct {
	mu        sync.Mutex
	lifecycle []lifecycleCall
}

func (n *recordingNotifier) NotifyProxyDaemonReady(context.Context, Daemon, DaemonReadyInput) error {
	return nil
}

func (n *recordingNotifier) NotifyProxyDaemonOffline(context.Context, Daemon, []actor.ActorID) error {
	return nil
}

func (n *recordingNotifier) NotifyDeviceLifecycle(_ context.Context, channelID channel.ID, actorID actor.ActorID, event devicetransit.LifecycleEvent, _ string, _ string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.lifecycle = append(n.lifecycle, lifecycleCall{channelID: channelID, actorID: actorID, event: event})
}

func (n *recordingNotifier) eventsFor(event devicetransit.LifecycleEvent) []lifecycleCall {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]lifecycleCall, 0, len(n.lifecycle))
	for _, c := range n.lifecycle {
		if c.event == event {
			out = append(out, c)
		}
	}
	return out
}

func readNextNonAckFrame(t *testing.T, conn *websocket.Conn) DeviceFrame {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	for {
		var frame DeviceFrame
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read outbound actor frame: %v", err)
		}
		if frame.FrameType == proxycontract.FrameTypeAck {
			continue
		}
		return frame
	}
}

func TestDaemonV2HandshakeReadyHeartbeatAndDisconnect(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	svc := NewService(testDB(t), Config{})
	forwarder := &captureForwarder{ch: make(chan DeviceFrame, 1)}
	daemon, apiKey, err := svc.CreateDaemon(ctx, CreateDaemonInput{
		OwnerID: "user-1",
		Name:    "Laptop",
	})
	if err != nil {
		t.Fatalf("CreateDaemon: %v", err)
	}
	if err := svc.AttachDaemonToChannel(ctx, daemon.ID, "ch-proxy"); err != nil {
		t.Fatalf("AttachDaemonToChannel: %v", err)
	}

	router := gin.New()
	router.GET(proxycontract.WSPathV2, svc.HandleWSV2(forwarder))
	srv := httptest.NewServer(router)
	defer srv.Close()

	conn := dialProxyDaemon(t, srv.URL, apiKey)
	ready := DeviceFrame{
		Direction:    "from_device",
		FrameType:    proxycontract.FrameTypeReady,
		Hostname:     "host-1",
		HostLabel:    "Host One",
		ProxyVersion: "0.1.0",
		Actors: []proxycontract.ReadyActorV2{
			{ActorID: "tool:kimi", CapabilitySet: json.RawMessage(`{"types":["kimi.ask"]}`)},
			{ActorID: "tool:xhs", CapabilitySet: json.RawMessage(`{"types":["xhs.publish"]}`)},
		},
	}
	if err := conn.WriteJSON(ready); err != nil {
		t.Fatalf("write ready: %v", err)
	}
	eventually(t, "ready projection", time.Second, func() bool {
		var status, hostname, version string
		var n int
		if err := svc.db.QueryRow(`SELECT status, COALESCE(hostname,''), COALESCE(proxy_version,'') FROM daemons WHERE id=?`, string(daemon.ID)).
			Scan(&status, &hostname, &version); err != nil {
			return false
		}
		if err := svc.db.QueryRow(`SELECT COUNT(*) FROM daemon_active_actors WHERE daemon_id=?`, string(daemon.ID)).Scan(&n); err != nil {
			return false
		}
		return status == "online" && hostname == "host-1" && version == "0.1.0" && n == 2
	})

	readinessPayload := json.RawMessage(`{"actor_id":"tool:kimi","current":{"ready":false,"state":"not_ready","reason":"daemon_unreachable","detail":{"endpoint":"127.0.0.1:10086"},"last_ready_at":111,"last_state_change_at":222},"checked_at":444,"changed_at":333}`)
	readinessEnv := message.Envelope{
		ID:         "event:readiness:test",
		TS:         333,
		ChannelID:  "ch-proxy",
		Sender:     message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:       message.KindEvent,
		Type:       "actor.readiness.changed",
		Payload:    readinessPayload,
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{actor.SystemActorID},
	}
	rawReadiness, err := json.Marshal(readinessEnv)
	if err != nil {
		t.Fatalf("marshal readiness envelope: %v", err)
	}
	if err := conn.WriteJSON(DeviceFrame{
		Direction: "from_device",
		ActorID:   "tool:kimi",
		Payload:   rawReadiness,
	}); err != nil {
		t.Fatalf("write readiness event: %v", err)
	}
	eventually(t, "readiness projection", time.Second, func() bool {
		var state, reason, detail string
		var checkedAt, lastReadyAt, lastStateChangeAt int64
		err := svc.db.QueryRow(`
			SELECT ready_state, ready_reason, ready_detail,
			       readiness_checked_at, last_ready_at, last_state_change_at
			  FROM daemon_hosted_actors
			 WHERE daemon_id=? AND actor_id='tool:kimi'`, string(daemon.ID)).
			Scan(&state, &reason, &detail, &checkedAt, &lastReadyAt, &lastStateChangeAt)
		return err == nil &&
			state == "not_ready" &&
			reason == "daemon_unreachable" &&
			strings.Contains(detail, "127.0.0.1:10086") &&
			checkedAt == 444 &&
			lastReadyAt == 111 &&
			lastStateChangeAt == 222
	})
	select {
	case got := <-forwarder.ch:
		if got.ActorID != "tool:kimi" {
			t.Fatalf("readiness forward actor=%+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("readiness event was not forwarded")
	}

	if err := conn.WriteJSON(DeviceFrame{Direction: "from_device", FrameType: proxycontract.FrameTypeHeartbeat}); err != nil {
		t.Fatalf("write heartbeat: %v", err)
	}
	eventually(t, "heartbeat", time.Second, func() bool {
		var hb int64
		if err := svc.db.QueryRow(`SELECT COALESCE(last_heartbeat,0) FROM daemons WHERE id=?`, string(daemon.ID)).Scan(&hb); err != nil {
			return false
		}
		return hb > 0
	})

	if err := conn.WriteJSON(DeviceFrame{
		Direction: "from_device",
		ActorID:   "tool:kimi",
		RequestID: "req-1",
		Payload:   json.RawMessage(`{"ok":true}`),
	}); err != nil {
		t.Fatalf("write business frame: %v", err)
	}
	select {
	case got := <-forwarder.ch:
		if got.ChannelID != "ch-proxy" || got.ActorID != "tool:kimi" || got.RequestID != "req-1" {
			t.Fatalf("forwarded frame=%+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("business frame not forwarded")
	}

	if err := svc.SendFrameToActor(ctx, "ch-proxy", "tool:kimi", DeviceFrame{
		Direction: "to_device",
		RequestID: "out-1",
		Payload:   json.RawMessage(`{"prompt":"hello"}`),
	}); err != nil {
		t.Fatalf("SendFrameToActor: %v", err)
	}
	outbound := readNextNonAckFrame(t, conn)
	if outbound.ChannelID != "ch-proxy" || outbound.ActorID != "tool:kimi" || outbound.RequestID != "out-1" {
		t.Fatalf("outbound frame=%+v", outbound)
	}

	_ = conn.Close()
	eventually(t, "disconnect clears projection", time.Second, func() bool {
		var status string
		var n int
		if err := svc.db.QueryRow(`SELECT status FROM daemons WHERE id=?`, string(daemon.ID)).Scan(&status); err != nil {
			return false
		}
		if err := svc.db.QueryRow(`SELECT COUNT(*) FROM daemon_active_actors WHERE daemon_id=?`, string(daemon.ID)).Scan(&n); err != nil {
			return false
		}
		return status == "offline" && n == 0
	})
}

// TestDaemonV2EmitsDeviceLifecycle pins the producer side of the
// device-state machine's ③实时态 input: the devicebus ws connect must emit
// LifecycleConnected and the ws disconnect must emit LifecycleDisconnected
// for every (attached channel, ready/active actor) pair, so the
// cloud-daemon proxy_facade can project realtime actor.status reachability.
// Without this the facade is stuck on liveUnknown (device_unreachable).
func TestDaemonV2EmitsDeviceLifecycle(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	svc := NewService(testDB(t), Config{})
	notifier := &recordingNotifier{}
	svc.SetProxyDaemonNotifier(notifier)
	forwarder := &captureForwarder{ch: make(chan DeviceFrame, 1)}
	daemon, apiKey, err := svc.CreateDaemon(ctx, CreateDaemonInput{OwnerID: "user-1", Name: "Laptop"})
	if err != nil {
		t.Fatalf("CreateDaemon: %v", err)
	}
	if err := svc.AttachDaemonToChannel(ctx, daemon.ID, "ch-proxy"); err != nil {
		t.Fatalf("AttachDaemonToChannel: %v", err)
	}

	router := gin.New()
	router.GET(proxycontract.WSPathV2, svc.HandleWSV2(forwarder))
	srv := httptest.NewServer(router)
	defer srv.Close()

	conn := dialProxyDaemon(t, srv.URL, apiKey)
	if err := conn.WriteJSON(DeviceFrame{
		Direction: "from_device",
		FrameType: proxycontract.FrameTypeReady,
		Actors: []proxycontract.ReadyActorV2{
			{ActorID: "tool:kimi"},
			{ActorID: "tool:xhs"},
		},
	}); err != nil {
		t.Fatalf("write ready: %v", err)
	}

	eventually(t, "connect lifecycle emitted", time.Second, func() bool {
		return len(notifier.eventsFor(devicetransit.LifecycleConnected)) == 2
	})
	connected := notifier.eventsFor(devicetransit.LifecycleConnected)
	gotConnected := map[actor.ActorID]bool{}
	for _, c := range connected {
		if c.channelID != "ch-proxy" {
			t.Fatalf("connected channel=%s; want ch-proxy", c.channelID)
		}
		gotConnected[c.actorID] = true
	}
	if !gotConnected["tool:kimi"] || !gotConnected["tool:xhs"] {
		t.Fatalf("connected actors=%v; want tool:kimi + tool:xhs", gotConnected)
	}

	_ = conn.Close()
	eventually(t, "disconnect lifecycle emitted", time.Second, func() bool {
		return len(notifier.eventsFor(devicetransit.LifecycleDisconnected)) == 2
	})
	disconnected := notifier.eventsFor(devicetransit.LifecycleDisconnected)
	gotDisconnected := map[actor.ActorID]bool{}
	for _, c := range disconnected {
		if c.channelID != "ch-proxy" {
			t.Fatalf("disconnected channel=%s; want ch-proxy", c.channelID)
		}
		gotDisconnected[c.actorID] = true
	}
	if !gotDisconnected["tool:kimi"] || !gotDisconnected["tool:xhs"] {
		t.Fatalf("disconnected actors=%v; want tool:kimi + tool:xhs", gotDisconnected)
	}
}

func TestDaemonV2KeepsConnectionWhenReadyNotifyFails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	svc := NewService(testDB(t), Config{})
	svc.SetProxyDaemonNotifier(failingNotifier{})
	forwarder := &captureForwarder{ch: make(chan DeviceFrame, 1)}
	daemon, apiKey, err := svc.CreateDaemon(ctx, CreateDaemonInput{
		OwnerID: "user-1",
		Name:    "Laptop",
	})
	if err != nil {
		t.Fatalf("CreateDaemon: %v", err)
	}
	if err := svc.AttachDaemonToChannel(ctx, daemon.ID, "ch-proxy"); err != nil {
		t.Fatalf("AttachDaemonToChannel: %v", err)
	}

	router := gin.New()
	router.GET(proxycontract.WSPathV2, svc.HandleWSV2(forwarder))
	srv := httptest.NewServer(router)
	defer srv.Close()

	conn := dialProxyDaemon(t, srv.URL, apiKey)
	if err := conn.WriteJSON(DeviceFrame{
		Direction: "from_device",
		FrameType: proxycontract.FrameTypeReady,
		Actors:    []proxycontract.ReadyActorV2{{ActorID: "tool:kimi"}},
	}); err != nil {
		t.Fatalf("write ready: %v", err)
	}
	eventually(t, "route after notify failure", time.Second, func() bool {
		return svc.daemonOwnsActor(daemon.ID, "ch-proxy", "tool:kimi")
	})

	if err := svc.SendFrameToActor(ctx, "ch-proxy", "tool:kimi", DeviceFrame{
		Direction: "to_device",
		RequestID: "out-after-notify-failure",
		Payload:   json.RawMessage(`{"ok":true}`),
	}); err != nil {
		t.Fatalf("SendFrameToActor after notify failure: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	var outbound DeviceFrame
	if err := conn.ReadJSON(&outbound); err != nil {
		t.Fatalf("connection closed after notify failure: %v", err)
	}
	if outbound.RequestID != "out-after-notify-failure" {
		t.Fatalf("outbound=%+v", outbound)
	}
	_ = conn.Close()
}

func TestDaemonV2InvalidKeyAndDuplicateActor(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	svc := NewService(testDB(t), Config{})
	forwarder := &captureForwarder{ch: make(chan DeviceFrame, 1)}
	first, firstKey, err := svc.CreateDaemon(ctx, CreateDaemonInput{OwnerID: "user-1", Name: "First"})
	if err != nil {
		t.Fatalf("CreateDaemon first: %v", err)
	}
	if err := svc.AttachDaemonToChannel(ctx, first.ID, "ch-proxy"); err != nil {
		t.Fatalf("AttachDaemonToChannel first: %v", err)
	}
	second, secondKey, err := svc.CreateDaemon(ctx, CreateDaemonInput{OwnerID: "user-1", Name: "Second"})
	if err != nil {
		t.Fatalf("CreateDaemon second: %v", err)
	}
	if err := svc.AttachDaemonToChannel(ctx, second.ID, "ch-proxy"); err != nil {
		t.Fatalf("AttachDaemonToChannel second: %v", err)
	}

	router := gin.New()
	router.GET(proxycontract.WSPathV2, svc.HandleWSV2(forwarder))
	srv := httptest.NewServer(router)
	defer srv.Close()

	dialer := proxyDialer()
	if conn, resp, err := dialer.Dial(proxyWSURL(srv.URL, "bogus"), nil); err == nil {
		_ = conn.Close()
		t.Fatal("invalid key dial succeeded")
	} else if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("invalid key status=%v err=%v", statusCode(resp), err)
	}

	conn1 := dialProxyDaemon(t, srv.URL, firstKey)
	if err := conn1.WriteJSON(DeviceFrame{
		Direction: "from_device",
		FrameType: proxycontract.FrameTypeReady,
		Actors:    []proxycontract.ReadyActorV2{{ActorID: "tool:kimi"}},
	}); err != nil {
		t.Fatalf("write first ready: %v", err)
	}
	eventually(t, "first active", time.Second, func() bool {
		var daemonID string
		err := svc.db.QueryRow(`SELECT daemon_id FROM daemon_active_actors WHERE channel_id='ch-proxy' AND actor_id='tool:kimi'`).Scan(&daemonID)
		return err == nil && daemonID == string(first.ID)
	})

	conn2 := dialProxyDaemon(t, srv.URL, secondKey)
	if err := conn2.WriteJSON(DeviceFrame{
		Direction: "from_device",
		FrameType: proxycontract.FrameTypeReady,
		Actors:    []proxycontract.ReadyActorV2{{ActorID: "tool:kimi"}},
	}); err != nil {
		t.Fatalf("write duplicate ready: %v", err)
	}
	_ = conn2.SetReadDeadline(time.Now().Add(time.Second))
	var frame DeviceFrame
	if err := conn2.ReadJSON(&frame); err == nil {
		t.Fatalf("duplicate ready got frame instead of close: %+v", frame)
	}
	_ = conn2.Close()
	eventually(t, "duplicate keeps first route", time.Second, func() bool {
		var daemonID string
		err := svc.db.QueryRow(`SELECT daemon_id FROM daemon_active_actors WHERE channel_id='ch-proxy' AND actor_id='tool:kimi'`).Scan(&daemonID)
		return err == nil && daemonID == string(first.ID)
	})
	_ = conn1.Close()
	eventually(t, "first disconnects", time.Second, func() bool {
		var n int
		err := svc.db.QueryRow(`SELECT COUNT(*) FROM daemon_active_actors WHERE daemon_id=?`, string(first.ID)).Scan(&n)
		return err == nil && n == 0
	})
}

func dialProxyDaemon(t *testing.T, serverURL, apiKey string) *websocket.Conn {
	t.Helper()
	dialer := proxyDialer()
	conn, resp, err := dialer.Dial(proxyWSURL(serverURL, apiKey), nil)
	if err != nil {
		t.Fatalf("dial proxy daemon status=%v: %v", statusCode(resp), err)
	}
	if got := conn.Subprotocol(); got != proxycontract.WSSubprotocolV2 {
		t.Fatalf("subprotocol=%q want %q", got, proxycontract.WSSubprotocolV2)
	}
	return conn
}

func proxyDialer() websocket.Dialer {
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = time.Second
	dialer.Subprotocols = []string{proxycontract.WSSubprotocolV2}
	return dialer
}

func proxyWSURL(serverURL, apiKey string) string {
	return "ws" + strings.TrimPrefix(serverURL, "http") +
		proxycontract.WSPathV2 + "?" + url.Values{proxycontract.QueryParamApiKey: {apiKey}}.Encode()
}

func statusCode(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}

func eventually(t *testing.T, name string, timeout time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ok() {
		return
	}
	t.Fatalf("%s did not become true within %s", name, timeout)
}
