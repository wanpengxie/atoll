package kimi

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func TestServerHelloAndToolRoundTrip(t *testing.T) {
	server := startTestServer(t, serverOptions{
		RequestID:    func() string { return "req-tool-1" },
		PingInterval: time.Hour,
	})
	conn := dialServer(t, server.Addr())
	defer func() { _ = conn.CloseNow() }()

	writeWSFrame(t, conn, wsFrame{
		Type:    "hello",
		Payload: json.RawMessage(`{"extensionVersion":"1.9.14"}`),
	})
	ack := readWSFrame(t, conn)
	if ack.Type != "hello_ack" {
		t.Fatalf("hello ack = %+v", ack)
	}
	if !server.HasConnectedExtension() || server.ExtensionVersion() != "1.9.14" {
		t.Fatalf("connected=%v version=%q", server.HasConnectedExtension(), server.ExtensionVersion())
	}

	resultCh := make(chan json.RawMessage, 1)
	errCh := make(chan error, 1)
	go func() {
		got, err := server.CallTool(context.Background(), "snapshot", json.RawMessage(`{"_session":"kimi"}`))
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- got
	}()

	call := readWSFrame(t, conn)
	if call.Type != "tool_call" || call.RequestID != "req-tool-1" {
		t.Fatalf("tool_call = %+v", call)
	}
	var payload toolCallPayload
	if err := json.Unmarshal(call.Payload, &payload); err != nil {
		t.Fatalf("decode tool_call payload: %v raw=%s", err, string(call.Payload))
	}
	if payload.Name != "snapshot" || string(payload.Args) != `{"_session":"kimi"}` {
		t.Fatalf("tool payload = %+v args=%s", payload, string(payload.Args))
	}
	writeWSFrame(t, conn, wsFrame{
		Type:                "tool_result",
		ResponseToRequestID: call.RequestID,
		Payload:             json.RawMessage(`{"data":{"tree":"ok"}}`),
	})

	select {
	case err := <-errCh:
		t.Fatalf("CallTool: %v", err)
	case got := <-resultCh:
		if string(got) != `{"tree":"ok"}` {
			t.Fatalf("result = %s", string(got))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CallTool timeout")
	}
}

func TestServerPingPongKeepalive(t *testing.T) {
	server := startTestServer(t, serverOptions{
		PingInterval:    10 * time.Millisecond,
		MissedPongLimit: 3,
	})
	conn := dialServer(t, server.Addr())
	defer func() { _ = conn.CloseNow() }()

	writeWSFrame(t, conn, wsFrame{Type: "hello"})
	ack := readWSFrame(t, conn)
	if ack.Type != "hello_ack" {
		t.Fatalf("hello ack = %+v", ack)
	}
	for range 2 {
		ping := readWSFrame(t, conn)
		if ping.Type != "ping" {
			t.Fatalf("ping frame = %+v", ping)
		}
		writeWSFrame(t, conn, wsFrame{Type: "pong"})
	}
	if !server.HasConnectedExtension() {
		t.Fatal("extension disconnected after pong replies")
	}
}

func TestServerToolResultError(t *testing.T) {
	server := startTestServer(t, serverOptions{
		RequestID:    func() string { return "req-tool-error" },
		PingInterval: time.Hour,
	})
	conn := dialServer(t, server.Addr())
	defer func() { _ = conn.CloseNow() }()
	writeWSFrame(t, conn, wsFrame{Type: "hello"})
	_ = readWSFrame(t, conn)

	errCh := make(chan error, 1)
	go func() {
		_, err := server.CallTool(context.Background(), "evaluate", json.RawMessage(`{"code":"bad()"}`))
		errCh <- err
	}()
	call := readWSFrame(t, conn)
	writeWSFrame(t, conn, wsFrame{
		Type:                "tool_result",
		ResponseToRequestID: call.RequestID,
		Payload:             json.RawMessage(`{"error":"evaluate: boom"}`),
	})
	select {
	case err := <-errCh:
		var toolErr *ToolError
		if !errors.As(err, &toolErr) || !strings.Contains(err.Error(), "boom") {
			t.Fatalf("err = %T %v", err, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CallTool error timeout")
	}
}

func TestServerSecondConnectionReplacesFirst(t *testing.T) {
	server := startTestServer(t, serverOptions{
		RequestID:    func() string { return "req-replaced" },
		PingInterval: time.Hour,
	})
	first := dialServer(t, server.Addr())
	defer func() { _ = first.CloseNow() }()
	writeWSFrame(t, first, wsFrame{Type: "hello"})
	_ = readWSFrame(t, first)

	second := dialServer(t, server.Addr())
	defer func() { _ = second.CloseNow() }()
	writeWSFrame(t, second, wsFrame{Type: "hello"})
	_ = readWSFrame(t, second)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var frame wsFrame
	if err := wsjson.Read(ctx, first, &frame); err == nil {
		t.Fatalf("first connection unexpectedly read frame %+v", frame)
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := server.CallTool(context.Background(), "list_tabs", json.RawMessage(`{}`))
		errCh <- err
	}()
	call := readWSFrame(t, second)
	if call.Type != "tool_call" || call.RequestID != "req-replaced" {
		t.Fatalf("replacement tool_call = %+v", call)
	}
	writeWSFrame(t, second, wsFrame{
		Type:                "tool_result",
		ResponseToRequestID: call.RequestID,
		Payload:             json.RawMessage(`{"data":{"tabs":[]}}`),
	})
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("CallTool after replacement: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CallTool after replacement timeout")
	}
}

func TestServerShutdownFailsInflightCall(t *testing.T) {
	server := startTestServer(t, serverOptions{
		RequestID:    func() string { return "req-inflight" },
		PingInterval: time.Hour,
	})
	conn := dialServer(t, server.Addr())
	defer func() { _ = conn.CloseNow() }()
	writeWSFrame(t, conn, wsFrame{Type: "hello"})
	_ = readWSFrame(t, conn)

	errCh := make(chan error, 1)
	go func() {
		_, err := server.CallTool(context.Background(), "snapshot", json.RawMessage(`{}`))
		errCh <- err
	}()
	call := readWSFrame(t, conn)
	if call.Type != "tool_call" {
		t.Fatalf("tool_call = %+v", call)
	}
	if err := server.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrServerClosed) {
			t.Fatalf("err=%v want ErrServerClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("inflight CallTool did not fail")
	}
}

func startTestServer(t *testing.T, opts serverOptions) *Server {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	opts.Port = 0
	if opts.PingInterval == 0 {
		opts.PingInterval = time.Hour
	}
	server, err := StartServer(ctx, opts)
	if err != nil {
		cancel()
		t.Fatalf("StartServer: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = server.Shutdown(context.Background())
	})
	return server
}

func dialServer(t *testing.T, addr string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws://"+addr+wsEndpointPath, nil)
	if err != nil {
		t.Fatalf("dial server: %v", err)
	}
	return conn
}
