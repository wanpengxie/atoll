package daemon

import (
	"context"
	"encoding/json"
	"net"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	devicexhs "github.com/wanpengxie/ActOS/adapters/device/xhs"
	"github.com/wanpengxie/ActOS/adapters/proxy/actorapi"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

func TestStartLocalListenerBindsLoopback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ll, err := StartLocalListener(ctx, 0, NewRegistry(), noopLogger{})
	if err != nil {
		t.Fatalf("StartLocalListener: %v", err)
	}
	defer func() { _ = ll.Shutdown(context.Background()) }()
	addr, ok := ll.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("addr type = %T", ll.Addr())
	}
	if got := addr.IP.String(); got != LocalListenHost {
		t.Fatalf("listen host = %s want %s", got, LocalListenHost)
	}
}

func TestStartLocalListenerPortInUseHasRemediation(t *testing.T) {
	ln, err := net.Listen("tcp", net.JoinHostPort(LocalListenHost, "0"))
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer func() { _ = ln.Close() }()
	port := ln.Addr().(*net.TCPAddr).Port
	_, err = StartLocalListener(context.Background(), port, NewRegistry(), noopLogger{})
	if err == nil {
		t.Fatal("StartLocalListener succeeded on occupied port")
	}
	if !strings.Contains(err.Error(), "--port <free-port>") {
		t.Fatalf("error missing remediation: %v", err)
	}
}

func TestXHSLocalBridgeRoundTrip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod := NewXHSLocalModule()
	reg := NewRegistry()
	if err := reg.Register(DefaultXHSProxyActorID, func() (actorapi.ActorModule, error) {
		return mod, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := reg.InitEnabled(ctx, Config{
		EnabledActors: []actor.ActorID{DefaultXHSProxyActorID},
	}); err != nil {
		t.Fatalf("InitEnabled: %v", err)
	}
	ll, err := StartLocalListener(ctx, 0, reg, noopLogger{})
	if err != nil {
		t.Fatalf("StartLocalListener: %v", err)
	}
	defer func() { _ = ll.Shutdown(context.Background()) }()

	wsURL := url.URL{Scheme: "ws", Host: ll.Addr().String(), Path: "/"}
	conn, _, err := websocket.DefaultDialer.Dial(wsURL.String(), nil)
	if err != nil {
		t.Fatalf("dial local listener: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.WriteJSON(map[string]any{
		"frame_type": "hello",
		"actor_id":   string(DefaultXHSProxyActorID),
	}); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	eventually(t, time.Second, func() bool {
		ready, _, _ := mod.Readiness(context.Background())
		return ready
	})

	req := message.Envelope{
		ID:         "req-xhs-1",
		TS:         time.Now().UnixMilli(),
		ChannelID:  "ch-1",
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:test"},
		Kind:       message.KindRequest,
		Type:       devicexhs.TypePublish,
		Payload:    json.RawMessage(`{"title":"hi","content":"body"}`),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{DefaultXHSProxyActorID},
	}
	respCh := make(chan message.Envelope, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := mod.Handle(ctx, req)
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp
	}()

	var outbound DeviceFrame
	if err := conn.ReadJSON(&outbound); err != nil {
		t.Fatalf("read outbound frame: %v", err)
	}
	if outbound.Direction != "to_device" || outbound.ActorID != string(DefaultXHSProxyActorID) || outbound.RequestID != req.ID.String() {
		t.Fatalf("outbound frame = %+v", outbound)
	}
	var cmd devicexhs.Command
	if err := json.Unmarshal(outbound.Payload, &cmd); err != nil {
		t.Fatalf("decode command: %v raw=%s", err, string(outbound.Payload))
	}
	if cmd.Type != devicexhs.CommandWireType || cmd.Cmd != "publish" || cmd.CorrelationID != req.ID.String() {
		t.Fatalf("command = %+v", cmd)
	}
	if err := conn.WriteJSON(DeviceFrame{
		Direction:     "from_device",
		ActorID:       string(DefaultXHSProxyActorID),
		ChannelID:     string(req.ChannelID),
		RequestID:     outbound.RequestID,
		CorrelationID: outbound.CorrelationID,
		Payload:       json.RawMessage(`{"correlation_id":"req-xhs-1","status":"ok","result":{"note_id":"n1","url":"https://xhs.example/n1"}}`),
	}); err != nil {
		t.Fatalf("write callback: %v", err)
	}

	select {
	case err := <-errCh:
		t.Fatalf("Handle: %v", err)
	case resp := <-respCh:
		if resp.Kind != message.KindResponse || resp.ParentID != req.ID || resp.Sender.ID != DefaultXHSProxyActorID {
			t.Fatalf("response = %+v", resp)
		}
		var payload struct {
			Status string `json:"status"`
			NoteID string `json:"note_id"`
			URL    string `json:"url"`
		}
		if err := json.Unmarshal(resp.Payload, &payload); err != nil {
			t.Fatalf("decode response payload: %v", err)
		}
		if payload.Status != "completed" || payload.NoteID != "n1" || payload.URL == "" {
			t.Fatalf("payload = %+v raw=%s", payload, string(resp.Payload))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Handle response timeout")
	}
}

func eventually(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !fn() {
		t.Fatal("condition not met before timeout")
	}
}

func TestLocalListenAddrUsesConfiguredPort(t *testing.T) {
	if got := localListenAddr(10387); !strings.HasSuffix(got, ":"+strconv.Itoa(10387)) {
		t.Fatalf("localListenAddr = %q", got)
	}
}
