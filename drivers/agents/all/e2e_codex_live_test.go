//go:build unix

package all

// Real-machine E2E: drives the production Runtime against a live codex
// app-server (real process, real wire protocol, real model turns).
// Gated behind CODEX_E2E=1 because it needs the codex binary, auth in
// ~/.codex, network, and several minutes of wall clock.
//
// Lines covered: cold start → turn cycle ×2 (worker reuse) → resume with the
// persisted seed → interrupt of a running turn → worker crash (kill -9) →
// automatic respawn on next demand.

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/provider/codex"
	"github.com/wanpengxie/atoll/drivers/agents/runtime"
	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
)

type liveEvent struct {
	kind    string
	op      runtimeproto.OpID
	turn    runtimeproto.TurnID
	text    string
	detail  string
	verdict runtimeproto.ControlVerdict
	status  runtimeproto.TurnStatus
	usage   runtimeproto.TurnUsage
}

type liveCollector struct {
	t  *testing.T
	ch chan liveEvent

	mu   sync.Mutex
	seed []byte
}

func newLiveCollector(t *testing.T) *liveCollector {
	return &liveCollector{t: t, ch: make(chan liveEvent, 256)}
}

func (c *liveCollector) push(e liveEvent) {
	c.t.Logf("event %s op=%d turn=%s status=%v verdict=%v text=%.120q detail=%.200q",
		e.kind, e.op, e.turn, e.status, e.verdict, e.text, e.detail)
	c.ch <- e
}

func (c *liveCollector) TurnStarted(op runtimeproto.OpID, id runtimeproto.TurnID) {
	c.push(liveEvent{kind: "started", op: op, turn: id})
}
func (c *liveCollector) TurnRejected(op runtimeproto.OpID, code, detail string) {
	c.push(liveEvent{kind: "rejected", op: op, text: code, detail: detail})
}
func (c *liveCollector) Tool(id runtimeproto.TurnID, e runtimeproto.ToolEvent) {
	c.push(liveEvent{kind: "tool", turn: id, text: e.Name, detail: e.Phase + " " + e.Detail})
}
func (c *liveCollector) Progress(id runtimeproto.TurnID, stage string) {
	c.push(liveEvent{kind: "progress", turn: id, text: stage})
}
func (c *liveCollector) TurnEnded(id runtimeproto.TurnID, status runtimeproto.TurnStatus, text, detail string, usage runtimeproto.TurnUsage) {
	c.push(liveEvent{kind: "ended", turn: id, status: status, text: text, detail: detail, usage: usage})
}
func (c *liveCollector) ControlDone(op runtimeproto.OpID, id runtimeproto.TurnID, verdict runtimeproto.ControlVerdict, detail string) {
	c.push(liveEvent{kind: "control", op: op, turn: id, verdict: verdict, detail: detail})
}
func (c *liveCollector) ReadyDone(op runtimeproto.OpID, r runtimeproto.ReadyResult) {
	c.push(liveEvent{kind: "ready", op: op, detail: r.Detail})
}
func (c *liveCollector) ProviderLost(id runtimeproto.TurnID, cause runtimeproto.LostCause, detail string) {
	c.push(liveEvent{kind: "lost", turn: id, text: fmt.Sprint(cause), detail: detail})
}
func (c *liveCollector) ResumeSeedUpdated(v []byte) {
	c.mu.Lock()
	c.seed = append([]byte(nil), v...)
	c.mu.Unlock()
	c.t.Logf("event seed %q", v)
}
func (c *liveCollector) RuntimeFault(code, detail string) {
	c.push(liveEvent{kind: "fault", text: code, detail: detail})
}

func (c *liveCollector) currentSeed() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.seed...)
}

// await pumps events until one of the wanted kinds arrives; every other event
// is already logged by push. Fatal on timeout or on fault/rejected unless those
// are the wanted kinds.
func (c *liveCollector) await(t *testing.T, timeout time.Duration, kinds ...string) liveEvent {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case e := <-c.ch:
			for _, k := range kinds {
				if e.kind == k {
					return e
				}
			}
			if e.kind == "fault" || e.kind == "rejected" || e.kind == "lost" {
				t.Fatalf("unexpected %s while waiting for %v: %+v", e.kind, kinds, e)
			}
		case <-deadline:
			t.Fatalf("timed out after %s waiting for %v", timeout, kinds)
		}
	}
}

const turnWait = 4 * time.Minute

func startTurn(t *testing.T, rt runtimeproto.Runtime, op runtimeproto.OpID, text string) {
	t.Helper()
	if err := rt.Start(runtimeproto.StartCommand{Op: op, Messages: []runtimeproto.Input{{Text: text}}}); err != nil {
		t.Fatalf("Start op=%d: %v", op, err)
	}
}

func runTurnControlsLive(t *testing.T, rt runtimeproto.Runtime, events *liveCollector, baseOp runtimeproto.OpID, options runtimeproto.TurnOptions) {
	t.Helper()
	if err := rt.Start(runtimeproto.StartCommand{Op: baseOp, Kind: runtimeproto.TurnSelect, Options: options}); err != nil {
		t.Fatal(err)
	}
	events.await(t, turnWait, "started")
	selected := events.await(t, turnWait, "ended")
	if selected.status != runtimeproto.TurnStatusOK || selected.usage.Model != options.Model || selected.usage.Effort != options.Effort {
		t.Fatalf("select ended=%+v", selected)
	}
	startTurn(t, rt, baseOp+1, "Reply with exactly OK and nothing else. Do not run tools.")
	events.await(t, turnWait, "started")
	chat := events.await(t, turnWait, "ended")
	if chat.status != runtimeproto.TurnStatusOK || chat.usage.Model != options.Model || chat.usage.Effort != options.Effort {
		t.Fatalf("selected chat ended=%+v", chat)
	}
	if err := rt.Start(runtimeproto.StartCommand{Op: baseOp + 2, Kind: runtimeproto.TurnCompact}); err != nil {
		t.Fatal(err)
	}
	events.await(t, turnWait, "started")
	compact := events.await(t, turnWait, "ended")
	if compact.status != runtimeproto.TurnStatusOK {
		t.Fatalf("compact ended=%+v", compact)
	}
	if chat.usage.ContextTokens > 0 && compact.usage.ContextTokens >= chat.usage.ContextTokens {
		t.Fatalf("compact did not reduce context: before=%d after=%d", chat.usage.ContextTokens, compact.usage.ContextTokens)
	}
}

func TestCodexLiveE2E(t *testing.T) {
	if os.Getenv("CODEX_E2E") != "1" {
		t.Skip("set CODEX_E2E=1 to run the live codex E2E")
	}
	workspace := t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	cfg, err := codex.ParseConfig(nil, workspace, logger)
	if err != nil {
		t.Fatal(err)
	}
	factory, spec, err := runtime.Build(codex.NewProvider(cfg), runtime.Policy{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("spec: caps=%+v receipt=%s", spec.Capabilities, spec.Bounds.ReceiptDeadline)

	events := newLiveCollector(t)
	rt, err := factory(runtimeproto.Deps{Parent: context.Background(), Logger: logger}, nil, runtimeproto.TurnOptions{}, events)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			rt.Close()
		}
	}()

	// Line 1: cold start → first turn closes.
	t0 := time.Now()
	startTurn(t, rt, 1, "Reply with exactly the word PONG and nothing else. Do not run any commands.")
	started := events.await(t, turnWait, "started")
	t.Logf("line1: cold start to TurnStarted in %s", time.Since(t0))
	ended := events.await(t, turnWait, "ended")
	if ended.turn != started.turn || ended.status != runtimeproto.TurnStatusOK {
		t.Fatalf("line1: ended=%+v", ended)
	}
	t.Logf("line1 OK in %s, final=%q", time.Since(t0), ended.text)

	// Line 2: second cycle on the same worker (reuse, no respawn).
	t0 = time.Now()
	startTurn(t, rt, 2, "Reply with the same word you replied last time, lowercase. Do not run any commands.")
	events.await(t, turnWait, "started")
	ended = events.await(t, turnWait, "ended")
	if ended.status != runtimeproto.TurnStatusOK {
		t.Fatalf("line2: ended=%+v", ended)
	}
	t.Logf("line2 OK in %s, final=%q", time.Since(t0), ended.text)

	// Line 3: interrupt a running turn.
	t0 = time.Now()
	startTurn(t, rt, 3, "Use your shell tool to run exactly: sleep 90 && echo done. Wait for it to finish, then reply DONE.")
	started = events.await(t, turnWait, "started")
	time.Sleep(5 * time.Second) // let it reach the tool call
	if err := rt.Control(runtimeproto.ControlCommand{Op: 4, Target: started.turn, Kind: runtimeproto.ControlInterrupt}); err != nil {
		t.Fatalf("line3 interrupt: %v", err)
	}
	sawControl, sawEnd := false, false
	for !(sawControl && sawEnd) {
		e := events.await(t, turnWait, "control", "ended")
		switch e.kind {
		case "control":
			sawControl = true
			t.Logf("line3 control verdict=%v detail=%q", e.verdict, e.detail)
		case "ended":
			sawEnd = true
			t.Logf("line3 turn ended status=%v", e.status)
		}
	}
	t.Logf("line3 OK in %s", time.Since(t0))

	// Line 4: resume — close this runtime, open a new one with the seed.
	seed := events.currentSeed()
	if len(seed) == 0 {
		t.Fatal("line4: no resume seed was published")
	}
	rt.Close()
	closed = true
	t.Logf("line4: resuming with seed %q", seed)

	events2 := newLiveCollector(t)
	rt2, err := factory(runtimeproto.Deps{Parent: context.Background(), Logger: logger}, seed, runtimeproto.TurnOptions{}, events2)
	if err != nil {
		t.Fatal(err)
	}
	defer rt2.Close()
	if err := rt2.EnsureReady(1); err != nil {
		t.Fatal(err)
	}
	events2.await(t, turnWait, "ready")
	t0 = time.Now()
	startTurn(t, rt2, 2, "Earlier in this conversation I asked you to reply with one specific uppercase word. Reply with just that word. Do not run any commands.")
	events2.await(t, turnWait, "started")
	ended = events2.await(t, turnWait, "ended")
	if ended.status != runtimeproto.TurnStatusOK {
		t.Fatalf("line4: ended=%+v", ended)
	}
	t.Logf("line4 OK in %s, recalled=%q (expect PONG)", time.Since(t0), ended.text)
	if !strings.Contains(strings.ToUpper(ended.text), "PONG") {
		t.Errorf("line4: resumed session did not recall PONG: %q", ended.text)
	}

	// Line 5: crash the worker mid-turn, expect lost + automatic respawn on
	// the next demand.
	t0 = time.Now()
	startTurn(t, rt2, 3, "Use your shell tool to run exactly: sleep 120 && echo done. Wait for it, then reply DONE.")
	events2.await(t, turnWait, "started")
	time.Sleep(3 * time.Second)
	out, err := exec.Command("pgrep", "-P", strconv.Itoa(os.Getpid()), "-f", "app-server").Output()
	if err != nil {
		t.Fatalf("line5: cannot find live app-server child: %v", err)
	}
	pids := strings.Fields(strings.TrimSpace(string(out)))
	t.Logf("line5: killing app-server pid(s) %v", pids)
	for _, pid := range pids {
		_ = exec.Command("kill", "-9", pid).Run()
	}
	deadline := time.After(turnWait)
	gotLost := false
	for !gotLost {
		select {
		case e := <-events2.ch:
			if e.kind == "lost" {
				t.Logf("line5 lost cause=%s detail=%q", e.text, e.detail)
				gotLost = true
			}
		case <-deadline:
			t.Fatal("line5: no ProviderLost after kill -9")
		}
	}
	// Next demand must spawn a fresh worker and serve normally.
	t0 = time.Now()
	startTurn(t, rt2, 5, "Reply with exactly OK. Do not run any commands.")
	events2.await(t, turnWait, "started")
	ended = events2.await(t, turnWait, "ended")
	if ended.status != runtimeproto.TurnStatusOK {
		t.Fatalf("line5 respawn: ended=%+v", ended)
	}
	t.Logf("line5 OK: respawn turn in %s, final=%q", time.Since(t0), ended.text)

	// Turn controls run last: compact rewrites the session summary, so it must
	// not precede the resume-recall line.
	if model := os.Getenv("CODEX_E2E_MODEL"); model != "" {
		effort := os.Getenv("CODEX_E2E_EFFORT")
		if effort == "" {
			effort = "low"
		}
		runTurnControlsLive(t, rt2, events2, 20, runtimeproto.TurnOptions{Model: model, Effort: effort})
	} else {
		t.Log("turn-controls segment skipped: CODEX_E2E_MODEL is unset")
	}
}
