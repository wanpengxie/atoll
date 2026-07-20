package app_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/atoll/drivers/tools/xhs"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/compute"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

// xhsDeviceAddr is a fixed loopback port for the adapter's private device
// endpoint. The mock device dials this directly, so the test needs a known
// address; port 0 would bind a random port the test couldn't address before the
// cell starts. A fixed high port on loopback is the simplest live path.
const xhsDeviceAddr = "127.0.0.1:18090"

// --- canned device behaviour (mirrors cmd/devtools/mock-device xhsReply) -----
//
// Kept as a few self-contained lines rather than importing the cmd binary
// (package main is not importable). The wire frames below are the adapter's
// PRIVATE device language (actors/xhs/wire.go).

type devDownFrame struct {
	CorrelationID string          `json:"correlation_id"`
	Cmd           string          `json:"cmd"`
	Params        json.RawMessage `json:"params"`
}

type devUpFrame struct {
	CorrelationID string          `json:"correlation_id"`
	OK            bool            `json:"ok"`
	Result        json.RawMessage `json:"result,omitempty"`
}

func cannedUp(down devDownFrame) devUpFrame {
	var result map[string]any
	switch down.Cmd {
	case "search":
		result = map[string]any{"results": []map[string]any{{"note_id": "n1", "title": "mock"}}}
	case "publish":
		result = map[string]any{"status": "completed", "note_id": "mock1", "url": "https://x/mock1"}
	case "get-note":
		result = map[string]any{"note": map[string]any{"note_id": "n1", "title": "mock"}}
	case "get-my-recent":
		result = map[string]any{"notes": []map[string]any{{"note_id": "n1"}}}
	default:
		result = map[string]any{}
	}
	raw, _ := json.Marshal(result)
	return devUpFrame{CorrelationID: down.CorrelationID, OK: true, Result: raw}
}

// TestXHSLiveEndToEnd is the green gate for the xhs adapter under REAL live
// conditions: a real HTTP/WS server, a real daemon (compute.Run) that
// attaches over a real /compute WS and hosts the tool:xhs cell, a real device
// connected over the cell's private /device WS, and a real xhs.search request
// that traverses the whole path and comes back as a completed response.
//
// Stages asserted in order:
//  1. daemon attaches  → tool:xhs appears as a channel member (/actors)
//  2. device connects  → adapter accepts the /device WS
//  3. request routes   → device receives a down-frame (cmd=search)
//  4. reply returns    → channel gets a kind=response, completed, results
func TestXHSLiveEndToEnd(t *testing.T) {
	// --- real server (in-process App, real HTTP listener so ws:// works) -----
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)

	s := fullSetup(t, env)

	// --- create + attach a daemon, grab its one-time api key ----------------
	daemonBody := createAndBindDaemon(t, env, s.chID, "xhs-daemon", s.cookies)
	apiKey := daemonBody["api_key"].(string)
	daemonID := daemonBody["id"].(string)
	if apiKey == "" {
		t.Fatal("daemon api_key empty")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// --- run the daemon: real /compute attach + hosted tool:xhs cell --------
	ctx, cancel := context.WithCancel(context.Background())
	serverWS := fmt.Sprintf("ws://%s/compute?channel=%s&key=%s", srv.Listener.Addr(), s.chID, apiKey)
	xhsID, err := env.app.ComposeDaemonForTest(s.chID, "xhs", "xhs", daemonID, actor.KindTool)
	if err != nil {
		t.Fatalf("pre-admit tool:xhs: %v", err)
	}

	runErr := make(chan error, 1)
	plan := authenticatedTestPlan([]platform.ActorDecl{{
		ID:   xhsID,
		Kind: actor.KindTool,
		Factory: platform.ActorFactory{Proc: xhs.Def(xhs.Config{
			ListenAddr:     xhsDeviceAddr,
			ReaperInterval: 20 * time.Millisecond,
			Logger:         logger,
		})},
	}})
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

	// --- STAGE 1: daemon attach → tool:xhs registers as a channel member ----
	waitForActor(t, env, s, string(xhsID), 5*time.Second)

	// --- STAGE 2: real device connects to the cell's private /device WS -----
	devURL := fmt.Sprintf("ws://%s/device", xhsDeviceAddr)
	conn := dialDeviceWithRetry(t, devURL, 3*time.Second)
	t.Cleanup(func() { _ = conn.Close() })

	// Device serve loop: read down-frames, reply canned ok. Runs until conn close.
	devErr := make(chan error, 1)
	go func() {
		for {
			var down devDownFrame
			if err := conn.ReadJSON(&down); err != nil {
				devErr <- err
				return
			}
			t.Logf("device down: cid=%s cmd=%s params=%s", down.CorrelationID, down.Cmd, string(down.Params))
			if err := conn.WriteJSON(cannedUp(down)); err != nil {
				devErr <- err
				return
			}
		}
	}()

	// --- STAGE 3+4: send xhs.search → completed response with results -------
	wsc := dialWS(t, srv, s.cookies, s.chID, 0)
	defer wsc.close()
	ack := wsc.sendMessage(map[string]any{
		"msg_type": "xhs.search",
		"kind":     "request",
		"payload":  map[string]any{"keyword": "go"},
		"audience": []string{string(xhsID)},
	})
	if ack["type"] != "ack" {
		t.Fatalf("send xhs.search: want ack, got %v", ack)
	}
	reqMsgID := ack["message_id"].(string)
	if reqMsgID == "" {
		t.Fatal("send returned empty message_id")
	}

	resp := waitForResponse(t, env, s, reqMsgID, 5*time.Second)

	// Assert: kind=response, status completed, payload carries results.
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(resp, &payload); err != nil {
		t.Fatalf("decode response payload: %v\nraw: %s", err, resp)
	}
	var status string
	_ = json.Unmarshal(payload["status"], &status)
	if status != "completed" {
		t.Fatalf("response status=%q want completed; payload=%s", status, resp)
	}
	if _, ok := payload["results"]; !ok {
		t.Fatalf("response payload missing results; payload=%s", resp)
	}

	// Surface a device read failure (e.g. never got the down-frame) if any.
	select {
	case err := <-devErr:
		t.Fatalf("device loop error: %v", err)
	default:
	}
}

// waitForActor waits for the actual daemon port embodiment. Identity admission
// precedes attachment, so registry presence alone is not an attachment proof.
func waitForActor(t *testing.T, env *testEnv, s setupResult, id string, timeout time.Duration) {
	t.Helper()
	if !env.app.WaitLiveForTest(s.chID, actor.ActorID(id), timeout) {
		t.Fatalf("actor %q never attached a live embodiment within %s", id, timeout)
	}
}

// waitForResponse polls the channel message log for a kind=response whose
// parent_id is the request message id, returning its raw payload.
func waitForResponse(t *testing.T, env *testEnv, s setupResult, parentID string, timeout time.Duration) json.RawMessage {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		rows, err := env.app.MessagesForTest(channel.ID(s.chID))
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if row.Envelope.Kind == message.KindResponse && string(row.Envelope.ParentID) == parentID {
				return append(json.RawMessage(nil), row.Envelope.Payload...)
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no kind=response with parent_id=%s within %s", parentID, timeout)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// decodeEnvelope normalises the stored envelope (object or JSON string) to a map.
func decodeEnvelope(envelope any) map[string]any {
	switch v := envelope.(type) {
	case map[string]any:
		return v
	case string:
		var m map[string]any
		if json.Unmarshal([]byte(v), &m) == nil {
			return m
		}
	}
	return nil
}

// dialDeviceWithRetry dials the device WS endpoint, retrying until the adapter
// has bound its listener (Start binds the port slightly after the cell spawns).
func dialDeviceWithRetry(t *testing.T, url string, timeout time.Duration) *websocket.Conn {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err == nil {
			return conn
		}
		if time.Now().After(deadline) {
			t.Fatalf("device dial never succeeded within %s: %v", timeout, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// xhsStatusDeviceAddr is a SEPARATE fixed loopback port for the status live
// test's adapter device endpoint (distinct from xhsDeviceAddr so the two live
// tests never collide on the port).
const xhsStatusDeviceAddr = "127.0.0.1:18091"

// TestXHSLiveActorStatus is the green gate for the app status route under REAL
// live conditions: a real server + real daemon hosting the tool:xhs cell + a real
// mock device over the cell's private /device WS.
//
//  1. device connected → GET /actors/tool:xhs/status reports known:true, online:true
//  2. device disconnects → the same read reports known:true, online:false
//
// This proves the FULL L3 obs-PUSH chain end-to-end: the adapter publishes a
// device-presence edge (PublishObs) → the daemon's population forwarder sends it UP
// the link as a KindObs frame → the home port relays it into publishObs → the
// home presence fold materialises the level → View.Snapshot → /status reads
// it OUT-OF-BAND (no probe, no truth-log write — the retired anti-pattern).
func TestXHSLiveActorStatus(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)

	s := fullSetup(t, env)

	daemonBody := createAndBindDaemon(t, env, s.chID, "xhs-status-daemon", s.cookies)
	apiKey := daemonBody["api_key"].(string)
	daemonID := daemonBody["id"].(string)
	if apiKey == "" {
		t.Fatal("daemon api_key empty")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	ctx, cancel := context.WithCancel(context.Background())
	serverWS := fmt.Sprintf("ws://%s/compute?channel=%s&key=%s", srv.Listener.Addr(), s.chID, apiKey)
	xhsID, err := env.app.ComposeDaemonForTest(s.chID, "xhs", "xhs", daemonID, actor.KindTool)
	if err != nil {
		t.Fatalf("pre-admit tool:xhs: %v", err)
	}
	runErr := make(chan error, 1)
	plan := authenticatedTestPlan([]platform.ActorDecl{{
		ID:   xhsID,
		Kind: actor.KindTool,
		Factory: platform.ActorFactory{Proc: xhs.Def(xhs.Config{
			ListenAddr:     xhsStatusDeviceAddr,
			ReaperInterval: 20 * time.Millisecond,
			Logger:         logger,
		})},
	}})
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

	// Daemon attach → tool:xhs registers as a channel member.
	waitForActor(t, env, s, string(xhsID), 5*time.Second)

	// --- STAGE 1: device connected → status reports device_online:true --------
	devURL := fmt.Sprintf("ws://%s/device", xhsStatusDeviceAddr)
	conn := dialDeviceWithRetry(t, devURL, 3*time.Second)

	waitDeviceOnline(t, env, s, string(xhsID), true, 5*time.Second)

	// --- STAGE 2: device disconnects → status reports device_online:false -----
	_ = conn.Close()
	waitDeviceOnline(t, env, s, string(xhsID), false, 5*time.Second)
}

// waitDeviceOnline polls the canonical Home presence fold until the device
// status matches want. It deliberately does NOT accept live:false as proof of
// offline: live:false means the actor was unreachable (it never answered), which
// would let a never-answering actor pass the offline assertion vacuously. The
// offline proof must be a real answer carrying device_online:false — that is what
// shows the status route reflects the dropped device. It tolerates transient
// mismatch (the readLoop's offline flip lags the socket close by a poll or two)
// by retrying until the deadline.
func waitDeviceOnline(t *testing.T, env *testEnv, s setupResult, id string, want bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		_, known, online, err := env.app.PresenceForTest(channel.ID(s.chID), actor.ActorID(id))
		if err != nil {
			t.Fatal(err)
		}
		// Only a KNOWN device presence (the adapter
		// actually published a device-presence edge that the obs chain folded at the
		// home) proves the want — known:false (unknown) must never satisfy either
		// assertion vacuously. This exercises the full L3 obs push chain end-to-end:
		// adapter PublishObs → daemon population forward → KindObs wire → home port →
		// publishObs → fold → View.Snapshot → /status.
		if known && online == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("status online never became %v (via a known device presence) within %s (known=%v online=%v)", want, timeout, known, online)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// NOTE (期7 review 修复 P2b): the former TestXHSLiveDeviceUnknownOnDaemonDeath
// (L3 decays to unknown when the hosting daemon's /compute link drops) lived
// here. Its trigger was a graceful ctx-cancel, which since 期7 S1 tears down as
// a QUIET KindDetach — per the device-fold's "abnormal death only" contract
// that no longer decays, so the scenario is only expressible as a raw
// transport-level link drop. That abnormal-path lifecycle coverage now lives
// at its mechanism layer: platform/internal/link
// TestHardLinkDrop_DownEdgeDecaysDevicePresence. This file keeps only the
// device's own-edge coverage (TestXHSLiveActorStatus) per the "app tests are
// mechanical call-shape migrations only" red line.
