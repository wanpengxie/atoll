package kimi

import (
	"context"
	"encoding/json"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/wanpengxie/ActOS/adapters/proxy/actorapi"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

func TestModuleHandleCommandSuccess(t *testing.T) {
	mod := newTestModule(t)
	rawCfg, _ := json.Marshal(Config{DefaultSession: "kimi", TimeoutMs: 1_000})
	if err := mod.Init(context.Background(), actorapi.ModuleConfig{Raw: rawCfg}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	conn := dialAndHelloModule(t, mod, "1.9.14")
	defer func() { _ = conn.CloseNow() }()

	req := message.Envelope{
		ID:         "req-kimi",
		TS:         time.Now().UnixMilli(),
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:test"},
		Kind:       message.KindRequest,
		Type:       TypeCommand,
		Payload:    json.RawMessage(`{"action":"snapshot","args":{}}`),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{DefaultAdapterActorID},
	}
	respCh := make(chan message.Envelope, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := mod.Handle(context.Background(), req)
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp
	}()

	frame := readWSFrame(t, conn)
	if frame.Type != "tool_call" || frame.RequestID == "" {
		t.Fatalf("tool_call frame = %+v", frame)
	}
	var payload toolCallPayload
	if err := json.Unmarshal(frame.Payload, &payload); err != nil {
		t.Fatalf("decode tool_call payload: %v raw=%s", err, string(frame.Payload))
	}
	if payload.Name != "snapshot" {
		t.Fatalf("tool name = %q", payload.Name)
	}
	var args map[string]string
	if err := json.Unmarshal(payload.Args, &args); err != nil {
		t.Fatalf("decode tool args: %v raw=%s", err, string(payload.Args))
	}
	if args["_session"] != "kimi" {
		t.Fatalf("tool args = %+v", args)
	}
	writeWSFrame(t, conn, wsFrame{
		Type:                "tool_result",
		ResponseToRequestID: frame.RequestID,
		Payload:             json.RawMessage(`{"data":{"tree":"button \"Send\" @e1"}}`),
	})

	select {
	case err := <-errCh:
		t.Fatalf("Handle: %v", err)
	case resp := <-respCh:
		var payload struct {
			Status string `json:"status"`
			Tree   string `json:"tree"`
		}
		if err := json.Unmarshal(resp.Payload, &payload); err != nil {
			t.Fatalf("payload decode: %v raw=%s", err, string(resp.Payload))
		}
		if payload.Status != "completed" || payload.Tree == "" {
			t.Fatalf("payload = %+v raw=%s", payload, string(resp.Payload))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Handle response timeout")
	}
}

func TestModuleReadinessStates(t *testing.T) {
	mod := newTestModule(t)
	ready, reason, err := mod.Readiness(context.Background())
	if err != nil {
		t.Fatalf("Readiness before init: %v", err)
	}
	if ready || reason != "initializing" {
		t.Fatalf("before init ready=%v reason=%q", ready, reason)
	}
	if err := mod.Init(context.Background(), actorapi.ModuleConfig{}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	ready, reason, err = mod.Readiness(context.Background())
	if err != nil {
		t.Fatalf("Readiness after init: %v", err)
	}
	if ready || reason != "extension_disconnected" {
		t.Fatalf("after init ready=%v reason=%q", ready, reason)
	}
	conn := dialAndHelloModule(t, mod, "1.9.14")
	defer func() { _ = conn.CloseNow() }()
	eventually(t, time.Second, func() bool {
		ready, reason, _ := mod.Readiness(context.Background())
		return ready && reason == "ok"
	})
}

func TestModuleStatusIncludesServerDetail(t *testing.T) {
	mod := newTestModule(t)
	if err := mod.Init(context.Background(), actorapi.ModuleConfig{}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	req := message.Envelope{
		ID:         "req-status",
		TS:         time.Now().UnixMilli(),
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:test"},
		Kind:       message.KindRequest,
		Type:       "actor.status",
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{DefaultAdapterActorID},
	}
	resp, err := mod.Handle(context.Background(), req)
	if err != nil {
		t.Fatalf("Handle status: %v", err)
	}
	var payload struct {
		Status string `json:"status"`
		Detail struct {
			Port int `json:"port"`
		} `json:"detail"`
	}
	if err := json.Unmarshal(resp.Payload, &payload); err != nil {
		t.Fatalf("decode status: %v raw=%s", err, string(resp.Payload))
	}
	if payload.Status != "completed" || payload.Detail.Port != mod.server.Port() {
		t.Fatalf("status payload = %+v raw=%s", payload, string(resp.Payload))
	}
}

func TestModulePortFallback(t *testing.T) {
	start, fallback := freePortPair(t)
	mod1 := New()
	mod1.serverStartPort = start
	mod1.serverFallbackPort = fallback
	mod1.serverPingInterval = time.Hour
	t.Cleanup(func() { _ = mod1.Shutdown(context.Background()) })
	if err := mod1.Init(context.Background(), actorapi.ModuleConfig{}); err != nil {
		t.Fatalf("mod1 Init: %v", err)
	}
	if got := mod1.server.Port(); got != start {
		t.Fatalf("mod1 port = %d want %d", got, start)
	}

	mod2 := New()
	mod2.serverStartPort = start
	mod2.serverFallbackPort = fallback
	mod2.serverPingInterval = time.Hour
	t.Cleanup(func() { _ = mod2.Shutdown(context.Background()) })
	if err := mod2.Init(context.Background(), actorapi.ModuleConfig{}); err != nil {
		t.Fatalf("mod2 Init: %v", err)
	}
	if got := mod2.server.Port(); got != fallback {
		t.Fatalf("mod2 port = %d want fallback %d", got, fallback)
	}
}

func newTestModule(t *testing.T) *Module {
	t.Helper()
	start, fallback := freePortPair(t)
	mod := New()
	mod.serverStartPort = start
	mod.serverFallbackPort = fallback
	mod.serverPingInterval = time.Hour
	t.Cleanup(func() { _ = mod.Shutdown(context.Background()) })
	return mod
}

func dialAndHelloModule(t *testing.T, mod *Module, version string) *websocket.Conn {
	t.Helper()
	conn := dialServer(t, mod.server.Addr())
	writeWSFrame(t, conn, wsFrame{
		Type:    "hello",
		Payload: json.RawMessage(`{"extensionVersion":"` + version + `"}`),
	})
	ack := readWSFrame(t, conn)
	if ack.Type != "hello_ack" {
		t.Fatalf("hello ack = %+v", ack)
	}
	return conn
}

func freePortPair(t *testing.T) (int, int) {
	t.Helper()
	for range 100 {
		ln, err := net.Listen("tcp", net.JoinHostPort(wsListenHost, "0"))
		if err != nil {
			t.Fatalf("listen free port: %v", err)
		}
		port := ln.Addr().(*net.TCPAddr).Port
		_ = ln.Close()
		if port >= 65535 {
			continue
		}
		fallback := port + 1
		ln2, err := net.Listen("tcp", net.JoinHostPort(wsListenHost, strconv.Itoa(fallback)))
		if err != nil {
			continue
		}
		_ = ln2.Close()
		return port, fallback
	}
	t.Fatal("could not find adjacent free ports")
	return 0, 0
}

func writeWSFrame(t *testing.T, conn *websocket.Conn, frame wsFrame) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := wsjson.Write(ctx, conn, frame); err != nil {
		t.Fatalf("write ws frame: %v", err)
	}
}

func readWSFrame(t *testing.T, conn *websocket.Conn) wsFrame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var frame wsFrame
	if err := wsjson.Read(ctx, conn, &frame); err != nil {
		t.Fatalf("read ws frame: %v", err)
	}
	return frame
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
