package devicebus

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	proxycontract "github.com/wanpengxie/ActOS/internal/proxy/contract"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/placement"
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

func TestRegisterActorTokenRoundTrip(t *testing.T) {
	ctx := context.Background()
	svc := NewService(testDB(t), Config{TokenSecret: "secret"})
	svc.WithClock(func() time.Time { return time.UnixMilli(1_000) })

	res, err := svc.RegisterActor(ctx, RegisterInput{
		ActorID:    "tool:xhs-adapter",
		ChannelID:  "ch-1",
		UserID:     "user-1",
		DaemonID:   "daemon-1",
		DeviceID:   "xhs-chrome-default",
		DeviceType: "xhs.chrome_extension",
	})
	if err != nil {
		t.Fatalf("RegisterActor: %v", err)
	}
	if res.Token == "" || res.TokenFingerprint == "" {
		t.Fatalf("missing token material: %+v", res)
	}
	got, err := svc.ValidateToken(ctx, "tool:xhs-adapter", res.Token)
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	if got.ChannelID != "ch-1" || got.ActorID != "tool:xhs-adapter" {
		t.Fatalf("registration mismatch: %+v", got)
	}
}

func TestRegisterActorReplacesRouteToken(t *testing.T) {
	ctx := context.Background()
	svc := NewService(testDB(t), Config{TokenSecret: "secret"})
	in := RegisterInput{
		ActorID:    "tool:xhs-adapter",
		ChannelID:  "ch-1",
		UserID:     "user-1",
		DaemonID:   "daemon-1",
		DeviceID:   "xhs-chrome-default",
		DeviceType: "xhs.chrome_extension",
	}
	first, err := svc.RegisterActor(ctx, in)
	if err != nil {
		t.Fatalf("first register: %v", err)
	}
	second, err := svc.RegisterActor(ctx, in)
	if err != nil {
		t.Fatalf("second register: %v", err)
	}
	if _, err := svc.ValidateToken(ctx, in.ActorID, first.Token); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("old token err=%v want ErrTokenInvalid", err)
	}
	if _, err := svc.ValidateToken(ctx, in.ActorID, second.Token); err != nil {
		t.Fatalf("new token invalid: %v", err)
	}
}

type noopDeviceTransport struct{}

func (noopDeviceTransport) ReadFrame(context.Context) (DeviceFrame, error) {
	return DeviceFrame{}, errors.New("unused")
}

func (noopDeviceTransport) WriteFrame(context.Context, DeviceFrame) error { return nil }
func (noopDeviceTransport) Close() error                                  { return nil }

func TestUnregisterConnectionCompareAndDelete(t *testing.T) {
	svc := NewService(nil, Config{})
	reg := ActorRegistration{
		ActorID:   actor.ActorID("tool:xhs-adapter"),
		ChannelID: channel.ID("ch-1"),
		DaemonID:  placement.DaemonID("daemon-1"),
	}
	oldConn := NewConnection(reg, noopDeviceTransport{})
	newConn := NewConnection(reg, noopDeviceTransport{})

	svc.registerConnection(reg.ChannelID, reg.ActorID, oldConn)
	svc.registerConnection(reg.ChannelID, reg.ActorID, newConn)

	key := routeKey(reg.ChannelID, reg.ActorID)
	if svc.unregisterConnection(reg.ChannelID, reg.ActorID, oldConn) {
		t.Fatal("old connection unregister removed current connection")
	}
	if got := svc.routes[key]; got != newConn {
		t.Fatalf("current connection = %p want %p", got, newConn)
	}
	if !svc.unregisterConnection(reg.ChannelID, reg.ActorID, newConn) {
		t.Fatal("current connection unregister did not remove entry")
	}
	if got := svc.routes[key]; got != nil {
		t.Fatalf("route still registered: %p", got)
	}
}

type captureForwarder struct {
	ch chan DeviceFrame
}

func (f *captureForwarder) ForwardDeviceFrame(_ context.Context, frame DeviceFrame, adapterActorID actor.ActorID) error {
	frame.ActorID = string(adapterActorID)
	f.ch <- frame
	return nil
}

func TestDaemonV2HandshakeReadyHeartbeatAndDisconnect(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	svc := NewService(testDB(t), Config{TokenSecret: "secret"})
	forwarder := &captureForwarder{ch: make(chan DeviceFrame, 1)}
	daemon, apiKey, err := svc.CreateDaemon(ctx, CreateDaemonInput{
		ChannelID: "ch-proxy",
		OwnerID:   "user-1",
		Name:      "Laptop",
	})
	if err != nil {
		t.Fatalf("CreateDaemon: %v", err)
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

func TestDaemonV2InvalidKeyAndDuplicateActor(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	svc := NewService(testDB(t), Config{TokenSecret: "secret"})
	forwarder := &captureForwarder{ch: make(chan DeviceFrame, 1)}
	first, firstKey, err := svc.CreateDaemon(ctx, CreateDaemonInput{ChannelID: "ch-proxy", OwnerID: "user-1", Name: "First"})
	if err != nil {
		t.Fatalf("CreateDaemon first: %v", err)
	}
	_, secondKey, err := svc.CreateDaemon(ctx, CreateDaemonInput{ChannelID: "ch-proxy", OwnerID: "user-1", Name: "Second"})
	if err != nil {
		t.Fatalf("CreateDaemon second: %v", err)
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
