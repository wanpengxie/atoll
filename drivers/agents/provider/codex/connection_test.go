package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestRetiredConnectionRejectsNotificationRPCAndEOF(t *testing.T) {
	release := make(chan struct{})
	p := mockProcess(t, func(method string, _ map[string]any, _ int) (any, *rpcError) {
		switch method {
		case "initialize":
			return map[string]any{}, nil
		case "turn/steer":
			<-release
			return map[string]any{}, nil
		default:
			return map[string]any{}, nil
		}
	})
	events := &recordingEvents{}
	e := &engine{cfg: Config{Logger: slog.New(slog.DiscardHandler), processFactory: func(context.Context, Config) (*childProcess, error) { return p, nil }}, events: events, life: context.Background(), final: map[string]string{}}
	c, err := e.openConnection(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	e.current, e.threadID, e.turnID = c, "thread", "turn"
	done := make(chan struct{})
	go func() {
		e.executeControl(controlCall{kind: base.TypeSteer, op: "old-rpc", item: base.Trigger{Envelope: message.Envelope{Payload: []byte(`{"text":"x"}`)}}})
		close(done)
	}()
	time.Sleep(10 * time.Millisecond)
	if err := e.Terminate(); err != nil {
		t.Fatal(err)
	}
	c.rpc.onNotification("turn/started", []byte(`{"threadId":"thread","turn":{"id":"turn"}}`))
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("old RPC did not retire")
	}
	c.rpc.closeWith(errors.New("late eof"))
	if got := events.snapshot(); len(got) != 0 {
		t.Fatalf("retired connection leaked events: %#v", got)
	}
}

func TestEnsureAliveFailuresCompleteOnceWithoutProviderLost(t *testing.T) {
	for _, tt := range []struct {
		name    string
		seed    string
		factory processFactory
	}{
		{name: "spawn", factory: func(context.Context, Config) (*childProcess, error) { return nil, errors.New("spawn failed") }},
		{name: "initialize", factory: scriptedFactory(t, func(method string, _ map[string]any, _ int) (any, *rpcError) {
			return nil, &rpcError{Code: -32000, Message: "initialize failed: " + method}
		})},
		{name: "resume", seed: "old", factory: scriptedFactory(t, func(method string, _ map[string]any, _ int) (any, *rpcError) {
			if method == "initialize" {
				return map[string]any{}, nil
			}
			return nil, &rpcError{Code: -32000, Message: "permission denied"}
		})},
		{name: "ready-eof", factory: eofBeforeReadyFactory(t)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			events := &recordingEvents{}
			e := &engine{cfg: Config{WorkspaceDir: "/w", Logger: slog.New(slog.DiscardHandler), processFactory: tt.factory}, events: events, life: context.Background(), seed: tt.seed, final: map[string]string{}}
			if err := e.EnsureAlive("ensure"); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				records := events.snapshot()
				if len(records) > 0 {
					controls, lost := 0, 0
					for _, record := range records {
						if record.kind == "control" && record.verdict == base.ControlRPCError {
							controls++
						}
						if record.kind == "lost" {
							lost++
						}
					}
					if controls != 1 || lost != 0 {
						t.Fatalf("records=%#v", records)
					}
					return
				}
				time.Sleep(time.Millisecond)
			}
			t.Fatalf("EnsureAlive did not complete: %#v", events.snapshot())
		})
	}
}

func scriptedFactory(t *testing.T, script scriptedRPC) processFactory {
	return func(context.Context, Config) (*childProcess, error) { return mockProcess(t, script), nil }
}

func eofBeforeReadyFactory(t *testing.T) processFactory {
	return func(context.Context, Config) (*childProcess, error) {
		serverRead, clientWrite := io.Pipe()
		clientRead, serverWrite := io.Pipe()
		p := &childProcess{stdin: clientWrite, stdout: clientRead, done: make(chan error)}
		go func() {
			s := bufio.NewScanner(serverRead)
			for s.Scan() {
				var msg struct {
					ID     json.RawMessage `json:"id"`
					Method string          `json:"method"`
				}
				if json.Unmarshal(s.Bytes(), &msg) != nil || len(msg.ID) == 0 {
					continue
				}
				if msg.Method == "initialize" {
					raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": msg.ID, "result": map[string]any{}})
					_, _ = serverWrite.Write(append(raw, '\n'))
					continue
				}
				_ = serverWrite.Close()
				return
			}
		}()
		return p, nil
	}
}
