package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"
)

func TestReferenceServerBothTransportsAndSSE(t *testing.T) {
	python := os.Getenv("ATOLL_MCP_TESTSERVER")
	if python == "" {
		t.Skip("ATOLL_MCP_TESTSERVER is unset; set it to mcp-testserver/.venv/bin/python to run the real MCP fixture")
	}
	python, err := filepath.Abs(python)
	if err != nil {
		t.Fatal(err)
	}
	project := filepath.Dir(filepath.Dir(filepath.Dir(python)))
	port := freeTCPPort(t)
	server := exec.Command(python, "-m", "server.main", "--transport", "http", "--port", fmt.Sprint(port))
	server.Dir = project
	server.Stdout = os.Stderr
	server.Stderr = os.Stderr
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = server.Process.Kill()
		_ = server.Wait()
	})
	waitTCP(t, fmt.Sprintf("127.0.0.1:%d", port))

	t.Run("http json and sse", func(t *testing.T) {
		c, err := newClient(Config{Name: "reference-http", Transport: transportHTTP, URL: fmt.Sprintf("http://127.0.0.1:%d/mcp", port)})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		verifyReferenceClient(t, c)

		without, plainInfo, err := c.callTool(context.Background(), "slow_task", json.RawMessage(`{"seconds":0.12}`), false)
		if err != nil {
			t.Fatal(err)
		}
		with, streamInfo, err := c.callTool(context.Background(), "slow_task", json.RawMessage(`{"seconds":0.12}`), true)
		if err != nil {
			t.Fatal(err)
		}
		if plainInfo.ContentType != "application/json" {
			t.Fatalf("plain content-type=%q", plainInfo.ContentType)
		}
		if streamInfo.ContentType != "text/event-stream" || streamInfo.Notifications < 2 {
			t.Fatalf("SSE info=%+v", streamInfo)
		}
		if !reflect.DeepEqual(without, with) {
			t.Fatalf("progress changed final result:\nwithout=%#v\nwith=%#v", without, with)
		}
		t.Logf("plain=%+v stream=%+v final results equal", plainInfo, streamInfo)
	})

	t.Run("stdio and close owns child", func(t *testing.T) {
		c, err := newClient(Config{
			Name: "reference-stdio", Transport: transportStdio, Command: python,
			Args: []string{"-m", "server.main", "--transport", "stdio"}, Cwd: project,
		})
		if err != nil {
			t.Fatal(err)
		}
		stdio := c.transport.(*stdioTransport)
		pid := stdio.cmd.Process.Pid
		verifyReferenceClient(t, c)
		if err := c.Close(); err != nil && !stringsContains(err.Error(), "killed") {
			t.Fatalf("close: %v", err)
		}
		if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
			t.Fatalf("stdio child %d still exists: %v", pid, err)
		}
		t.Logf("stdio child %d exited after client close", pid)
	})
}

func verifyReferenceClient(t *testing.T, c *client) {
	t.Helper()
	discover, err := c.discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var serverInfo struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	}
	if err := json.Unmarshal(discover.Meta.ServerInfo, &serverInfo); err != nil {
		t.Fatalf("decode server info %s: %v", discover.Meta.ServerInfo, err)
	}
	if serverInfo.Name != "mcp-v2-reference-testserver" || serverInfo.Version != "0.1.0" {
		t.Fatalf("server info=%+v", serverInfo)
	}
	if discover.Instructions != "Deterministic MCP 2026-07-28 protocol and tool conformance fixture." {
		t.Fatalf("instructions=%q", discover.Instructions)
	}
	listing, err := c.listTools(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(listing.Tools) != 15 {
		t.Fatalf("tools=%d", len(listing.Tools))
	}
	result, _, err := c.callTool(context.Background(), "echo", json.RawMessage(`{"text":"go-mcp"}`), false)
	if err != nil {
		t.Fatal(err)
	}
	payload, _, err := translateCallResult(result)
	if err != nil || payload["text"] != "go-mcp" {
		t.Fatalf("echo payload=%v err=%v", payload, err)
	}
	failed, _, err := c.callTool(context.Background(), "fail_tool_error", json.RawMessage(`{}`), false)
	if err != nil || !failed.IsError {
		t.Fatalf("tool failure=%+v err=%v", failed, err)
	}
	_, _, err = c.callTool(context.Background(), "fail_protocol_error", json.RawMessage(`{}`), false)
	var protocolErr *rpcError
	if !errors.As(err, &protocolErr) || protocolErr.Code != -32602 {
		t.Fatalf("protocol failure=%T %v", err, err)
	}
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func waitTCP(t *testing.T, address string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server did not listen on %s", address)
}

func stringsContains(value, part string) bool {
	for i := 0; i+len(part) <= len(value); i++ {
		if value[i:i+len(part)] == part {
			return true
		}
	}
	return false
}
