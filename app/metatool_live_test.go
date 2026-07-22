package app_test

// metatool_live_test is the device-adapter slice acceptance ③: an AGENT, using
// its REAL invocation machinery (metatool.Shell.call_actor — the SAME shell the
// go-kimi Bridge holds), drives a daemon-hosted adapter end to end. Nothing here
// is mocked except the device's canned replies and the LLM (the test drives the
// shell directly instead of letting a model emit the tool call).
//
// Full live path per call:
//   metatool.ExecuteCallActor (the agent's real call entry)
//     → shell builds + emits a kind=request into the live channel home
//       → home routes it over the daemon /compute link to the hosted cell
//         → adapter dispatches a device down-frame
//           → mock device replies a canned up-frame
//         → adapter commits a kind=response into the channel
//       → home routes the response back to the agent cell (audience = caller)
//     → cell.Receive feeds shell.Deliver → call_actor returns the device result.
//
// The agent cell is built by a custom AgentFactory: it HOLDS a metatool.Shell
// exactly as kimiagent.Bridge does (NewShell with the cell's pen as Writer) and
// its Receive forwards responses to shell.Deliver — the kimiagent Receive path
// minus the LLM. The test then drives call_actor on its own goroutine while the
// cell goroutine delivers; the shell's two-goroutine contract (Arm on the caller
// edge, Match on the mailbox) is what production relies on.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/atoll/app"
	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/drivers/gateway"
	"github.com/wanpengxie/atoll/drivers/gateway/connector/web"
	"github.com/wanpengxie/atoll/drivers/tools/kimi"
	"github.com/wanpengxie/atoll/drivers/tools/xhs"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/metatool"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/compute"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

// Fixed loopback device ports for this test's two adapters. Distinct from the
// xhs_live_test ports (18090/18091) so concurrent live tests never collide.
const (
	metatoolXHSDeviceAddr  = "127.0.0.1:18092"
	metatoolKimiDeviceAddr = "127.0.0.1:18093"
)

// --- the agent cell: a JobTable-driven Proc, no LLM ---------------------------

// shellAgent is a minimal channel agent cell (期10 S5): an actorbase.Proc whose
// engine IS the substrate JobTable (the SAME out-station account the production
// agents drive through metatool.Exec). Its run loop idles on sys.Recv() to keep
// the cell live — the engine's own pump matches inbound responses to the ledger,
// no manual Deliver needed. The test drives the meta-tool call_actor directly
// against the engine's Exec face from a separate goroutine (the mind-binding
// caller class — JobTable is cross-goroutine safe by design).
type shellAgent struct {
	self  actor.ActorID
	chID  channel.ID
	mu    sync.Mutex
	exec  *metatool.Exec
	ready chan struct{}
}

func newShellAgent(self actor.ActorID, chID channel.ID) *shellAgent {
	return &shellAgent{self: self, chID: chID, ready: make(chan struct{})}
}

// run is the cell Proc: build the Exec face from Sys (the JobTable + sys.Call),
// publish it, then idle so the cell (and its response-matching pump) stays live.
func (a *shellAgent) run(sys actorbase.Sys) error {
	a.mu.Lock()
	a.exec = base.ExecFace(sys, 10*time.Second)
	a.mu.Unlock()
	close(a.ready)
	for {
		if _, err := sys.Recv(); err != nil {
			return err
		}
	}
}

// callActor drives the REAL call_actor entry with a synthesised turn trigger.
// call_actor Submits through the JobTable and blocks the fast-path window until
// the engine's pump matches the device response, so this runs on its own
// goroutine.
func (a *shellAgent) callActor(ctx context.Context, actorID, envType string, params map[string]any) metatool.ResultValue {
	<-a.ready
	a.mu.Lock()
	x := a.exec
	a.mu.Unlock()
	paramRaw, _ := json.Marshal(params)
	callRaw, _ := json.Marshal(map[string]any{
		"actor_id": actorID,
		"type":     envType,
		"payload":  json.RawMessage(paramRaw),
		// omit wait → bounded fast-path (final inline within the window).
	})
	rc := metatool.RuntimeContext{
		Trigger: metatool.Trigger{
			Envelope: message.Envelope{
				ID:        message.ID(fmt.Sprintf("trigger-%d", time.Now().UnixNano())),
				ChannelID: a.chID,
				Kind:      message.KindRequest,
				Type:      "agent.turn",
				Sender:    message.Sender{Kind: actor.KindHuman, ID: "user:tester"},
			},
		},
	}
	return metatool.ExecuteCallActor(ctx, callRaw, x, rc)
}

func (a *shellAgent) stop() {}

// --- test-local App setup with the shellAgent injected as the channel agent ---

// setupShellAgentApp mirrors setupTestApp but injects an AgentFactory that builds
// the shellAgent and publishes it back through agentSink, so the test can drive
// its call_actor after the channel spawns it.
func setupShellAgentApp(t *testing.T, agentSink func(*shellAgent)) *testEnv {
	t.Helper()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "app.db")
	chDBDir := filepath.Join(tmpDir, "channels")

	db, err := openTestAppDB(t, dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}

	factory := func(chID channel.ID, agentID actor.ActorID) (actorbase.Proc, error) {
		sa := newShellAgent(agentID, chID)
		agentSink(sa)
		return sa.run, nil
	}

	a, err := app.New(app.Config{
		DB: db,
		HostFactory: func(deps channelhost.HomeDeps) (channelhost.LocalHost, error) {
			return channelhost.New(chDBDir, deps)
		},
	})
	if err != nil {
		db.Close()
		t.Fatalf("app.New: %v", err)
	}
	gw, err := gateway.New(gateway.Config{Resolver: testGatewayResolver(a)})
	if err != nil {
		a.Close()
		db.Close()
		t.Fatalf("gateway.New: %v", err)
	}
	gw.Start()
	a.SetGateway(web.New(gw))
	a.SetMembershipPoke(gw.Poke)
	testAgentBuilder = factory
	t.Cleanup(func() {
		testAgentBuilder = nil
		gw.Close()
		a.Close()
		db.Close()
	})

	return &testEnv{handler: a.Handler(), app: a, tmpDir: tmpDir}
}

// startToolDaemon runs a single daemon (compute.Run) hosting BOTH the
// tool:xhs and tool:kimi cells over one real /compute link. Device dialing is
// the readiness proof for each membrane-internal plan consumer.
func startToolDaemon(t *testing.T, env *testEnv, s setupResult, srv *httptest.Server, logger *slog.Logger) (actor.ActorID, actor.ActorID) {
	t.Helper()

	daemonBody := createAndBindDaemon(t, env, s.chID, "tool-daemon", s.cookies)
	apiKey := daemonBody["api_key"].(string)
	daemonID := daemonBody["id"].(string)
	if apiKey == "" {
		t.Fatal("daemon api_key empty")
	}

	ctx, cancel := context.WithCancel(context.Background())
	serverWS := fmt.Sprintf("ws://%s/compute?channel=%s&key=%s", srv.Listener.Addr(), s.chID, apiKey)
	xhsID, err := env.app.ComposeDaemonForTest(s.chID, "xhs", "xhs", daemonID, actor.KindTool)
	if err != nil {
		t.Fatalf("pre-admit tool:xhs: %v", err)
	}
	kimiID, err := env.app.ComposeDaemonForTest(s.chID, "kimi", "kimi", daemonID, actor.KindTool)
	if err != nil {
		t.Fatalf("pre-admit tool:kimi: %v", err)
	}
	runErr := make(chan error, 1)
	plan := authenticatedTestPlan([]platform.ActorDecl{
		{
			ID:   xhsID,
			Kind: actor.KindTool,
			Factory: platform.ActorFactory{Proc: xhs.Def(xhs.Config{
				ListenAddr:     metatoolXHSDeviceAddr,
				ReaperInterval: 20 * time.Millisecond,
				Logger:         logger,
			})},
		},
		{
			ID:   kimiID,
			Kind: actor.KindTool,
			Factory: platform.ActorFactory{Proc: kimi.Def(kimi.Config{
				ListenAddr:     metatoolKimiDeviceAddr,
				ReaperInterval: 20 * time.Millisecond,
				Logger:         logger,
			})},
		},
	})
	go func() {
		runErr <- compute.Run(ctx,
			compute.Config{ServerWS: serverWS, Logger: logger, PlanSource: plan, Poll: 20 * time.Millisecond},
		)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runErr:
		case <-time.After(3 * time.Second):
			t.Log("compute.Run did not return within 3s after cancel")
		}
	})
	return xhsID, kimiID
}

// mockDevice is a canned-reply device serving one adapter's private /device WS.
// reply maps a down-frame to its up-frame body (canned per-cmd). It runs until
// the connection closes.
type mockDevice struct {
	conn *websocket.Conn
	done chan error

	mu       sync.Mutex
	lastDown metatoolDownFrame // most recent down-frame the adapter pushed
}

// startMockDevice dials the adapter's /device endpoint (retrying until the cell
// binds) and serves canned replies built by replyFn. It records the most recent
// down-frame so a test can assert the adapter's type→cmd / payload mapping landed
// on the wire (not just that the response came back).
func startMockDevice(t *testing.T, addr string, replyFn func(down metatoolDownFrame) metatoolUpFrame) *mockDevice {
	t.Helper()
	conn := dialDeviceWithRetry(t, fmt.Sprintf("ws://%s/device", addr), 3*time.Second)
	md := &mockDevice{conn: conn, done: make(chan error, 1)}
	go func() {
		for {
			var down metatoolDownFrame
			if err := conn.ReadJSON(&down); err != nil {
				md.done <- err
				return
			}
			t.Logf("device[%s] down: cid=%s cmd=%s params=%s", addr, down.CorrelationID, down.Cmd, string(down.Params))
			md.mu.Lock()
			md.lastDown = down
			md.mu.Unlock()
			if err := conn.WriteJSON(replyFn(down)); err != nil {
				md.done <- err
				return
			}
		}
	}()
	return md
}

// last returns the most recent down-frame the adapter pushed to this device.
func (m *mockDevice) last() metatoolDownFrame {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastDown
}

func (m *mockDevice) close() { _ = m.conn.Close() }

// metatoolDownFrame / metatoolUpFrame mirror the adapters' PRIVATE device wire
// language (actors/{xhs,kimi}/wire.go). Both adapters share the same frame shape
// (correlation_id + cmd/params down; correlation_id + ok + result up).
type metatoolDownFrame struct {
	CorrelationID string          `json:"correlation_id"`
	Cmd           string          `json:"cmd"`
	Params        json.RawMessage `json:"params"`
}

type metatoolUpFrame struct {
	CorrelationID string          `json:"correlation_id"`
	OK            bool            `json:"ok"`
	Result        json.RawMessage `json:"result,omitempty"`
}

func xhsCannedUp(down metatoolDownFrame) metatoolUpFrame {
	var result map[string]any
	switch down.Cmd {
	case "search":
		result = map[string]any{"results": []map[string]any{{"note_id": "n1", "title": "mock"}}}
	default:
		result = map[string]any{}
	}
	raw, _ := json.Marshal(result)
	return metatoolUpFrame{CorrelationID: down.CorrelationID, OK: true, Result: raw}
}

func kimiCannedUp(down metatoolDownFrame) metatoolUpFrame {
	// kimi.command carries the device verb in down.Cmd (the action). Reply ok with
	// a small navigated result so the channel response is a completed success.
	result := map[string]any{"ok": true, "action": down.Cmd}
	raw, _ := json.Marshal(result)
	return metatoolUpFrame{CorrelationID: down.CorrelationID, OK: true, Result: raw}
}

// TestMetatoolLiveCallActor is the slice-③ green gate: the agent's call_actor
// drives BOTH daemon-hosted adapters end to end, then a device hang-up surfaces
// as a call_actor failure.
func TestMetatoolLiveCallActor(t *testing.T) {
	if testing.Short() {
		t.Skip("short: ~2s live tool round-trip journey — full gate (make test-full) runs it")
	}
	var (
		agentMu sync.Mutex
		agent   *shellAgent
	)
	env := setupShellAgentApp(t, func(sa *shellAgent) {
		agentMu.Lock()
		agent = sa
		agentMu.Unlock()
	})
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)

	s := fullSetup(t, env)
	control := dialWS(t, srv, s.cookies, s.chID, 0)
	defer control.close()

	// The channel agent cell (our shellAgent) is spawned when the channel home is
	// created in fullSetup. Grab the published handle.
	agentMu.Lock()
	sa := agent
	agentMu.Unlock()
	if sa == nil {
		t.Fatal("shellAgent was never built by the channel home (agent factory not invoked)")
	}
	t.Cleanup(sa.stop)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// One daemon hosting both tool cells.
	xhsID, kimiID := startToolDaemon(t, env, s, srv, logger)

	// Connect a mock device to each adapter's private /device WS.
	xhsDev := startMockDevice(t, metatoolXHSDeviceAddr, xhsCannedUp)
	t.Cleanup(xhsDev.close)
	kimiDev := startMockDevice(t, metatoolKimiDeviceAddr, kimiCannedUp)
	t.Cleanup(kimiDev.close)

	ctx := context.Background()

	// --- STAGE 1: xhs via call_actor -------------------------------------------
	t.Run("xhs", func(t *testing.T) {
		rv := callActorAsync(t, sa, ctx, string(xhsID), "xhs.search",
			map[string]any{"keyword": "go"}, 8*time.Second)
		if rv.IsError {
			t.Fatalf("xhs call_actor returned error: %+v", rv.Value)
		}
		assertStatusCompleted(t, rv)
		if _, ok := rv.Value["results"]; !ok {
			t.Fatalf("xhs call_actor result missing results: %+v", rv.Value)
		}
	})

	// --- STAGE 2: kimi via call_actor ------------------------------------------
	t.Run("kimi", func(t *testing.T) {
		rv := callActorAsync(t, sa, ctx, string(kimiID), "kimi.command",
			map[string]any{"action": "navigate", "args": map[string]any{"url": "https://example.com"}}, 8*time.Second)
		if rv.IsError {
			t.Fatalf("kimi call_actor returned error: %+v", rv.Value)
		}
		assertStatusCompleted(t, rv)

		// Assert the down-frame the adapter pushed: kimi.command's `action` must map
		// to down.Cmd and `args` (carrying the url) must pass through as down.Params.
		// Guards against a kimi.command→cmd mapping regression that would otherwise
		// pass vacuously (the canned reply succeeds regardless of what arrived).
		down := kimiDev.last()
		if down.Cmd != "navigate" {
			t.Fatalf("kimi down-frame cmd=%q want navigate (action→cmd mapping); down=%+v", down.Cmd, down)
		}
		if !strings.Contains(string(down.Params), "https://example.com") {
			t.Fatalf("kimi down-frame params missing url; params=%s", down.Params)
		}
	})

	// --- STAGE 3: device hang-up → call_actor surfaces device_offline ----------
	t.Run("device_offline", func(t *testing.T) {
		xhsDev.close()
		// Wait for the adapter to observe the closed socket (its readLoop flips
		// device-absent shortly after the close). Poll via actor.status so the
		// next call_actor is guaranteed to hit the offline path, not a race.
		pollPresence(t, env, s, control, xhsID, true, false, 5*time.Second)

		rv := callActorAsync(t, sa, ctx, string(xhsID), "xhs.search",
			map[string]any{"keyword": "go"}, 8*time.Second)
		if !rv.IsError {
			t.Fatalf("xhs call_actor after device hang-up should fail, got: %+v", rv.Value)
		}
		// The adapter fails with error_code=device_offline; metatool maps the
		// terminal reason into the actor-CLI error set and carries the device
		// error_code in the failure detail/payload.
		blob, _ := json.Marshal(rv.Value)
		if !strings.Contains(string(blob), "device_offline") {
			t.Fatalf("expected device_offline in failure, got: %s", blob)
		}
	})
}

// callActorAsync runs call_actor on its own goroutine (the shell blocks on the
// inline window until the cell's Receive delivers) and returns the result, or
// fails if the call does not complete within timeout (a wedged path).
func callActorAsync(t *testing.T, sa *shellAgent, ctx context.Context, actorID, envType string, params map[string]any, timeout time.Duration) metatool.ResultValue {
	t.Helper()
	resCh := make(chan metatool.ResultValue, 1)
	go func() { resCh <- sa.callActor(ctx, actorID, envType, params) }()
	select {
	case rv := <-resCh:
		return rv
	case <-time.After(timeout):
		t.Fatalf("call_actor(%s, %s) did not return within %s — link/route/delivery wedged", actorID, envType, timeout)
		return metatool.ResultValue{}
	}
}

// assertStatusCompleted asserts the call_actor success value carries the
// completed final status the adapter stamped on its response.
func assertStatusCompleted(t *testing.T, rv metatool.ResultValue) {
	t.Helper()
	status, _ := rv.Value["status"].(string)
	if status != "completed" {
		t.Fatalf("call_actor result status=%q want completed; value=%+v", status, rv.Value)
	}
}
