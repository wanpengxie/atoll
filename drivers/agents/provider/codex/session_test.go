package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/wanpengxie/atoll/drivers/agents/base"
)

type scriptedRPC func(method string, params map[string]any, call int) (any, *rpcError)

func mockProcess(t *testing.T, script scriptedRPC) *childProcess {
	t.Helper()
	serverRead, clientWrite := io.Pipe()
	clientRead, serverWrite := io.Pipe()
	p := &childProcess{stdin: clientWrite, stdout: clientRead, done: make(chan error)}
	go func() {
		defer serverWrite.Close()
		s := bufio.NewScanner(serverRead)
		calls := map[string]int{}
		for s.Scan() {
			var msg struct {
				ID     json.RawMessage `json:"id"`
				Method string          `json:"method"`
				Params map[string]any  `json:"params"`
			}
			if json.Unmarshal(s.Bytes(), &msg) != nil || len(msg.ID) == 0 {
				continue
			}
			calls[msg.Method]++
			result, rpcErr := script(msg.Method, msg.Params, calls[msg.Method])
			response := map[string]any{"jsonrpc": "2.0", "id": msg.ID}
			if rpcErr != nil {
				response["error"] = rpcErr
			} else {
				response["result"] = result
			}
			raw, _ := json.Marshal(response)
			raw = append(raw, '\n')
			_, _ = serverWrite.Write(raw)
		}
	}()
	return p
}

func TestResumeKeepsThreadAndStartPersistsImmediately(t *testing.T) {
	events := &recordingEvents{}
	p := mockProcess(t, func(method string, params map[string]any, _ int) (any, *rpcError) {
		switch method {
		case "initialize":
			return map[string]any{"userAgent": "mock/1"}, nil
		case "thread/resume":
			if params["threadId"] != "thread-old" || params["excludeTurns"] != true {
				t.Fatalf("resume params=%#v", params)
			}
			return map[string]any{"thread": map[string]any{"id": "thread-old"}}, nil
		default:
			t.Fatalf("unexpected call %s", method)
			return nil, nil
		}
	})
	e := &engine{cfg: Config{WorkspaceDir: "/workspace", Logger: slog.New(slog.DiscardHandler), processFactory: func(context.Context, Config) (*childProcess, error) { return p, nil }}, events: events, life: context.Background(), seed: "thread-old", final: map[string]string{}}
	if err := e.Boot(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if string(events.persist[base.ResumeSeedKey]) != "thread-old" || e.threadID != "thread-old" {
		t.Fatalf("persist=%q thread=%q", events.persist[base.ResumeSeedKey], e.threadID)
	}
}

func TestResumeFallbackPatternsAndClosingRetry(t *testing.T) {
	resumeCalls := 0
	p := mockProcess(t, func(method string, params map[string]any, call int) (any, *rpcError) {
		switch method {
		case "initialize":
			return map[string]any{}, nil
		case "thread/resume":
			resumeCalls++
			if call == 1 {
				return nil, &rpcError{Code: -32000, Message: "thread is closing"}
			}
			return nil, &rpcError{Code: -32000, Message: "rollout not found"}
		case "thread/start":
			if params["approvalPolicy"] != "never" || params["sandbox"] != "danger-full-access" || params["cwd"] != "/workspace" {
				t.Fatalf("start params=%#v", params)
			}
			return map[string]any{"thread": map[string]any{"id": "thread-new"}}, nil
		default:
			t.Fatalf("unexpected call %s", method)
			return nil, nil
		}
	})
	events := &recordingEvents{}
	e := &engine{cfg: Config{WorkspaceDir: "/workspace", Logger: slog.New(slog.DiscardHandler), processFactory: func(context.Context, Config) (*childProcess, error) { return p, nil }}, events: events, life: context.Background(), seed: "thread-old", final: map[string]string{}}
	if err := e.Boot(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if resumeCalls != 2 || e.threadID != "thread-new" {
		t.Fatalf("resumeCalls=%d thread=%q", resumeCalls, e.threadID)
	}
}

func TestTerminateFencesConnectionStillBecomingReady(t *testing.T) {
	readyToCommit := make(chan struct{})
	release := make(chan struct{})
	p := mockProcess(t, func(method string, _ map[string]any, _ int) (any, *rpcError) {
		switch method {
		case "initialize":
			return map[string]any{}, nil
		case "thread/start":
			close(readyToCommit)
			<-release
			return map[string]any{"thread": map[string]any{"id": "stale-thread"}}, nil
		default:
			t.Fatalf("unexpected call %s", method)
			return nil, nil
		}
	})
	events := &recordingEvents{}
	e := &engine{cfg: Config{WorkspaceDir: "/workspace", Logger: slog.New(slog.DiscardHandler), processFactory: func(context.Context, Config) (*childProcess, error) { return p, nil }}, events: events, life: context.Background(), final: map[string]string{}}
	result := make(chan error, 1)
	go func() {
		_, _, err := e.ensureService(context.Background())
		result <- err
	}()
	<-readyToCommit
	if err := e.Terminate(); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-result; err == nil || err.Error() != "codex: service retired" {
		t.Fatalf("ensure result=%v", err)
	}
	e.mu.Lock()
	current := e.current
	e.mu.Unlock()
	if current != nil {
		t.Fatal("pre-terminate connection was promoted after the fence")
	}
}
