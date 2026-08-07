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

	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/protocol/message"
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
	e := &engine{cfg: Config{WorkspaceDir: "/workspace", Logger: slog.New(slog.DiscardHandler), processFactory: func(context.Context, Config) (*childProcess, error) { return p, nil }}, events: events, life: context.Background(), seed: "thread-old"}
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
	e := &engine{cfg: Config{WorkspaceDir: "/workspace", Logger: slog.New(slog.DiscardHandler), processFactory: func(context.Context, Config) (*childProcess, error) { return p, nil }}, events: events, life: context.Background(), seed: "thread-old"}
	if err := e.Boot(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if resumeCalls != 2 || e.threadID != "thread-new" {
		t.Fatalf("resumeCalls=%d thread=%q", resumeCalls, e.threadID)
	}
}

func TestResumeInvalidConversationAndArchiveStartFresh(t *testing.T) {
	for _, detail := range []string{"conversation not found", "session thread-old is archived"} {
		t.Run(detail, func(t *testing.T) {
			p := mockProcess(t, func(method string, _ map[string]any, _ int) (any, *rpcError) {
				switch method {
				case "initialize":
					return map[string]any{}, nil
				case "thread/resume":
					return nil, &rpcError{Code: -32000, Message: detail}
				case "thread/start":
					return map[string]any{"thread": map[string]any{"id": "thread-new"}}, nil
				default:
					t.Fatalf("unexpected call %s", method)
					return nil, nil
				}
			})
			events := &recordingEvents{}
			e := &engine{cfg: Config{WorkspaceDir: "/workspace", Logger: slog.New(slog.DiscardHandler), processFactory: func(context.Context, Config) (*childProcess, error) { return p, nil }}, events: events, life: context.Background(), seed: "thread-old"}
			if err := e.Boot(context.Background(), events); err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			if e.threadID != "thread-new" {
				t.Fatalf("thread=%q", e.threadID)
			}
		})
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
	e := &engine{cfg: Config{WorkspaceDir: "/workspace", Logger: slog.New(slog.DiscardHandler), processFactory: func(context.Context, Config) (*childProcess, error) { return p, nil }}, events: events, life: context.Background()}
	result := make(chan error, 1)
	go func() {
		_, _, err := e.ensureService(context.Background(), 0)
		result <- err
	}()
	<-readyToCommit
	if err := e.Terminate(); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-result; !errors.Is(err, errServiceRetired) {
		t.Fatalf("ensure result=%v", err)
	}
	e.mu.Lock()
	current := e.current
	e.mu.Unlock()
	if current != nil {
		t.Fatal("pre-terminate connection was promoted after the fence")
	}
}

// A Terminate that lands after StartTurn was accepted but before it reached a
// provider must void that start outright: no process may be spawned for it, no
// turn submitted, and — critically — the intent must not linger and make the
// NEXT request look like "a turn is already in flight".
func TestTerminateVoidsAcceptedStartBeforeItReachesProvider(t *testing.T) {
	var spawned int32
	events := &recordingEvents{}
	e := &engine{cfg: Config{WorkspaceDir: "/w", Logger: slog.New(slog.DiscardHandler), processFactory: func(context.Context, Config) (*childProcess, error) {
		atomic.AddInt32(&spawned, 1)
		return mockProcess(t, func(string, map[string]any, int) (any, *rpcError) { return map[string]any{}, nil }), nil
	}}, events: events, life: context.Background()}

	// The window this pins: the start was accepted under epoch 0, a fence
	// landed, and only THEN does the start reach establishment. It must be
	// refused before anything is spawned — a cancelled turn may not cost a
	// process, and must not bind to the generation that replaces it.
	if err := e.Terminate(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.ensureService(context.Background(), 0); !errors.Is(err, errServiceRetired) {
		t.Fatalf("stale-epoch establishment was allowed: %v", err)
	}
	if got := atomic.LoadInt32(&spawned); got != 0 {
		t.Fatalf("a voided start spawned %d process(es)", got)
	}
	e.mu.Lock()
	current := e.current
	e.mu.Unlock()
	if current != nil {
		t.Fatal("a cancelled start promoted a generation into service")
	}
}

// A control verb queued against one generation must never land on the next.
// The queue is serial, so a call can sit behind a blocked RPC while a restart
// replaces the world underneath it — and a steer injected into the wrong turn,
// or an interrupt aimed at the wrong turn, is a provider-side side effect that
// no later bookkeeping can undo.
func TestQueuedControlNeverLandsOnALaterGeneration(t *testing.T) {
	var steers, interrupts int32
	script := func(method string, _ map[string]any, _ int) (any, *rpcError) {
		switch method {
		case "turn/steer":
			atomic.AddInt32(&steers, 1)
		case "turn/interrupt":
			atomic.AddInt32(&interrupts, 1)
		case "thread/start", "thread/resume":
			return map[string]any{"thread": map[string]any{"id": "thread-1"}}, nil
		}
		return map[string]any{}, nil
	}
	events := &recordingEvents{}
	e := &engine{cfg: Config{WorkspaceDir: "/w", Logger: slog.New(slog.DiscardHandler), processFactory: func(context.Context, Config) (*childProcess, error) {
		return mockProcess(t, script), nil
	}}, events: events, life: context.Background()}
	if err := e.Boot(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	e.mu.Lock()
	old := e.current
	old.turnID = "turn-old"
	e.mu.Unlock()
	// Bound to the old generation and its turn, then the world moves on before
	// the worker gets to them.
	steer := e.bindControl(controlCall{kind: base.TypeSteer, op: "steer-old", item: base.Trigger{Envelope: message.Envelope{Payload: []byte(`{"text":"old content"}`)}}})
	interrupt := e.bindControl(controlCall{kind: base.TypeInterrupt, op: "interrupt-old"})
	if err := e.Terminate(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.ensureService(context.Background(), e.currentEpoch()); err != nil {
		t.Fatalf("respawn failed: %v", err)
	}
	e.mu.Lock()
	e.current.turnID = "turn-new"
	e.mu.Unlock()

	e.executeControl(steer)
	e.executeControl(interrupt)
	if got := atomic.LoadInt32(&steers); got != 0 {
		t.Fatalf("stale steer injected content into the new turn (%d calls)", got)
	}
	if got := atomic.LoadInt32(&interrupts); got != 0 {
		t.Fatalf("stale interrupt killed the new turn (%d calls)", got)
	}
	for _, record := range events.snapshot() {
		if record.kind == "control" && record.verdict != base.ControlNoActiveTurn {
			t.Fatalf("stale control settled as %v, want noActiveTurn: %#v", record.verdict, record)
		}
	}
}

// Whoever wins the detach owes the loss report — including the caller that
// stumbles on a dead generation before its own observer got there. Otherwise
// an in-service death is reported zero times.
func TestCallerThatReapsADeadGenerationReportsTheLoss(t *testing.T) {
	p := mockProcess(t, func(string, map[string]any, int) (any, *rpcError) { return map[string]any{}, nil })
	c := &connection{id: 1, process: p, rpc: newRPC(p), final: map[string]string{}}
	events := &recordingEvents{}
	e := &engine{cfg: Config{Logger: slog.New(slog.DiscardHandler)}, events: events, life: context.Background(), current: c}
	c.dead.Store(true) // died, but its observer has not detached it yet

	if _, _, err, done := e.serviceFastPath(0); !done || !errors.Is(err, errServiceRetired) {
		t.Fatalf("dead generation was treated as usable: err=%v done=%v", err, done)
	}
	lost := 0
	for _, record := range events.snapshot() {
		if record.kind == "lost" {
			lost++
		}
	}
	if lost != 1 {
		t.Fatalf("death reported %d times, want exactly one: %#v", lost, events.snapshot())
	}
}

// Two callers racing to bring a generation up must yield exactly one process:
// if both promoted, the loser's connection would be overwritten in place and
// its process never reaped.
func TestConcurrentEstablishmentPromotesExactlyOneGeneration(t *testing.T) {
	var spawned int32
	events := &recordingEvents{}
	e := &engine{cfg: Config{WorkspaceDir: "/w", Logger: slog.New(slog.DiscardHandler), processFactory: func(context.Context, Config) (*childProcess, error) {
		atomic.AddInt32(&spawned, 1)
		return mockProcess(t, func(method string, _ map[string]any, _ int) (any, *rpcError) {
			if method == "thread/start" || method == "thread/resume" {
				return map[string]any{"thread": map[string]any{"id": "thread-1"}}, nil
			}
			return map[string]any{}, nil
		}), nil
	}}, events: events, life: context.Background()}

	const racers = 4
	done := make(chan *connection, racers)
	for i := 0; i < racers; i++ {
		go func() {
			c, _, err := e.ensureService(context.Background(), 0)
			if err != nil {
				t.Errorf("establishment failed: %v", err)
			}
			done <- c
		}()
	}
	first := <-done
	for i := 1; i < racers; i++ {
		if c := <-done; c != first {
			t.Fatalf("racers got different generations: %p vs %p", c, first)
		}
	}
	if got := atomic.LoadInt32(&spawned); got != 1 {
		t.Fatalf("concurrent establishment spawned %d processes", got)
	}
}

// The voided intent must also be released, or the NEXT request would be turned
// away with "turn already in flight" — a cancelled turn blocking live ones.
func TestVoidedStartIntentDoesNotBlockTheNextRequest(t *testing.T) {
	events := &recordingEvents{}
	e := &engine{cfg: Config{WorkspaceDir: "/w", Logger: slog.New(slog.DiscardHandler), processFactory: func(context.Context, Config) (*childProcess, error) {
		return mockProcess(t, func(string, map[string]any, int) (any, *rpcError) { return map[string]any{}, nil }), nil
	}}, events: events, life: context.Background()}
	trigger := base.Trigger{Envelope: message.Envelope{Payload: []byte(`{"text":"hi"}`)}}
	// Hold establishment so op-1 is guaranteed to remain pending across the
	// Terminate return. The admission assertion below therefore exercises the
	// exact stale-intent window instead of waiting for it to disappear first.
	e.establishMu.Lock()
	if err := e.StartTurn("op-1", []base.Trigger{trigger}, nil); err != nil {
		e.establishMu.Unlock()
		t.Fatal(err)
	}
	if err := e.Terminate(); err != nil {
		e.establishMu.Unlock()
		t.Fatal(err)
	}
	if err := e.StartTurn("op-2", []base.Trigger{trigger}, nil); err != nil {
		e.establishMu.Unlock()
		t.Fatalf("next request rejected after a cancelled start: %v", err)
	}
	e.mu.Lock()
	if e.pending == nil || e.pending.op != "op-2" || e.pending.epoch != e.serviceEpoch {
		got := e.pending
		e.mu.Unlock()
		e.establishMu.Unlock()
		t.Fatalf("new intent was not installed immediately: %#v", got)
	}
	e.mu.Unlock()
	// Void op-2 as cleanup, then release both goroutines. Neither epoch may
	// spawn, and op-1's late cleanup must not erase op-2 before its own cleanup.
	if err := e.Terminate(); err != nil {
		e.establishMu.Unlock()
		t.Fatal(err)
	}
	e.establishMu.Unlock()
	waitUntil(t, "voided intents released", func() bool {
		e.mu.Lock()
		defer e.mu.Unlock()
		return e.pending == nil
	})
}

func TestFailedStartTransportFencesConnectionBeforeOutcome(t *testing.T) {
	for _, started := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-turn-started", true: "after-turn-started"}[started], func(t *testing.T) {
			p := mockProcess(t, func(string, map[string]any, int) (any, *rpcError) { return map[string]any{}, nil })
			c := &connection{id: 1, process: p, rpc: newRPC(p), final: map[string]string{}}
			events := &recordingEvents{}
			e := &engine{cfg: Config{Logger: slog.New(slog.DiscardHandler)}, events: events, life: context.Background(), current: c}
			c.startOp = "start"
			if started {
				c.startOp, c.turnOp, c.turnID = "", "start", "ghost-turn"
				c.final["ghost-turn"] = "partial"
			}
			e.fenceFailedStart(c, "start", errors.New("transport down"))
			if !c.retired.Load() || e.current != nil {
				t.Fatalf("connection not fenced: retired=%v current=%#v", c.retired.Load(), e.current)
			}
			records := events.snapshot()
			if len(records) != 1 {
				t.Fatalf("fence must report exactly once: %#v", records)
			}
			if started && records[0].kind != "lost" {
				t.Fatalf("started turn must report ProviderLost: %#v", records[0])
			}
			if !started && records[0].kind != "rejected" {
				t.Fatalf("unstarted turn must report TurnRejected: %#v", records[0])
			}
		})
	}
}

// An EOF observer that wins the detach CAS first owns the loss report; the
// late transport-failure fence must stay silent — exactly-once is carried by
// the single detach transition, not by each observer checking flags.
func TestFailedStartFenceStaysSilentWhenEOFWonDetach(t *testing.T) {
	p := mockProcess(t, func(string, map[string]any, int) (any, *rpcError) { return map[string]any{}, nil })
	c := &connection{id: 1, process: p, rpc: newRPC(p), final: map[string]string{}}
	events := &recordingEvents{}
	e := &engine{cfg: Config{Logger: slog.New(slog.DiscardHandler)}, events: events, life: context.Background(), current: c}
	c.turnOp, c.turnID = "start", "ghost-turn"
	if !e.detach(c) {
		t.Fatal("EOF observer failed to win the detach")
	}
	e.fenceFailedStart(c, "start", errors.New("transport down"))
	if got := events.snapshot(); len(got) != 0 {
		t.Fatalf("losing fence reported anyway: %#v", got)
	}
}
