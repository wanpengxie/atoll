package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/protocol/message"
)

// The public entry of every lifecycle defect: an in-service connection dies
// mid-turn. Exactly one ProviderLost must be reported, and the NEXT StartTurn
// must lazily respawn — a dead generation's turn account lives on the dead
// connection and therefore cannot block rebirth.
func TestActiveTurnEOFReportsOnceAndNextStartTurnRespawns(t *testing.T) {
	var spawned int32
	script := func(method string, _ map[string]any, _ int) (any, *rpcError) {
		switch method {
		case "initialize":
			return map[string]any{}, nil
		case "thread/start", "thread/resume":
			return map[string]any{"thread": map[string]any{"id": "thread-1"}}, nil
		default:
			return map[string]any{}, nil
		}
	}
	events := &recordingEvents{}
	e := &engine{cfg: Config{WorkspaceDir: "/w", Logger: slog.New(slog.DiscardHandler), processFactory: func(context.Context, Config) (*childProcess, error) {
		atomic.AddInt32(&spawned, 1)
		return mockProcess(t, script), nil
	}}, events: events, life: context.Background()}
	if err := e.Boot(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	trigger := base.Trigger{Envelope: message.Envelope{Payload: []byte(`{"text":"hi"}`)}}
	if err := e.StartTurn("op-1", []base.Trigger{trigger}, nil); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	c1 := e.current
	e.mu.Unlock()
	waitUntil(t, "turn/start submitted", func() bool {
		e.mu.Lock()
		defer e.mu.Unlock()
		return c1.startOp == "op-1"
	})
	e.handleNotification(c1, "turn/started", []byte(`{"threadId":"thread-1","turn":{"id":"turn-1"}}`))
	c1.rpc.closeWith(io.ErrUnexpectedEOF)
	lost := 0
	for _, record := range events.snapshot() {
		if record.kind == "lost" {
			lost++
		}
	}
	if lost != 1 {
		t.Fatalf("EOF of an in-service turn must report exactly one ProviderLost: %#v", events.snapshot())
	}
	if err := e.StartTurn("op-2", []base.Trigger{trigger}, nil); err != nil {
		t.Fatalf("post-crash StartTurn was blocked by the dead generation: %v", err)
	}
	waitUntil(t, "lazy respawn", func() bool { return atomic.LoadInt32(&spawned) == 2 })
	for _, record := range events.snapshot() {
		if record.kind == "rejected" && record.op == "op-2" {
			t.Fatalf("post-crash turn rejected instead of respawned: %#v", record)
		}
	}
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%s did not happen within deadline", what)
}

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
	e := &engine{cfg: Config{Logger: slog.New(slog.DiscardHandler), processFactory: func(context.Context, Config) (*childProcess, error) { return p, nil }}, events: events, life: context.Background()}
	c, err := e.openConnection(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	e.current, e.threadID = c, "thread"
	c.turnID = "turn"
	// Bound while the connection is still in service — this is the in-flight
	// RPC whose late completion must stay silent after retirement.
	call := e.bindControl(controlCall{kind: base.TypeSteer, op: "old-rpc", item: base.Trigger{Envelope: message.Envelope{Payload: []byte(`{"text":"x"}`)}}})
	done := make(chan struct{})
	go func() {
		e.executeControl(call)
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
			e := &engine{cfg: Config{WorkspaceDir: "/w", Logger: slog.New(slog.DiscardHandler), processFactory: tt.factory}, events: events, life: context.Background(), seed: tt.seed}
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
