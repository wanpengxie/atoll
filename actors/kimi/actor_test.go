package kimi

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

const testChannelID = channel.ID("ch-test")

// recordingWriter is a concurrency-safe harness.Pen double: the adapter emits
// from the read loop + reaper goroutines as well as the cell goroutine, so the
// writer must be safe.
type recordingWriter struct {
	mu      sync.Mutex
	written []message.Envelope
}

func (w *recordingWriter) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.written = append(w.written, *env)
	return harness.WriteResult{MessageID: env.ID}, nil
}

func (w *recordingWriter) Written() []message.Envelope {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]message.Envelope, len(w.written))
	copy(out, w.written)
	return out
}

// waitResponse polls for a kind=response envelope with the given parent_id.
func (w *recordingWriter) waitResponse(t *testing.T, parentID message.ID, timeout time.Duration) (message.Envelope, bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		for _, env := range w.Written() {
			if env.Kind == message.KindResponse && env.ParentID == parentID {
				return env, true
			}
		}
		if time.Now().After(deadline) {
			return message.Envelope{}, false
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// startActor builds + starts a kimi adapter on a free port. Cleanup stops it. A
// short reaper interval keeps the timeout test prompt without a prod constant.
func startActor(t *testing.T, w *recordingWriter, cfg Config) *Actor {
	t.Helper()
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = "127.0.0.1:0"
	}
	if cfg.ReaperInterval == 0 {
		cfg.ReaperInterval = 20 * time.Millisecond
	}
	a := NewActor(w, cfg)
	if err := a.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = a.Stop(context.Background()) })
	return a
}

// waitOnline blocks until the adapter has registered a live device connection.
// (Dial returns after the HTTP upgrade handshake, slightly before handleAccept
// finishes registering the conn — white-box synchronisation.)
func waitOnline(t *testing.T, a *Actor) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		a.dev.mu.Lock()
		c := a.dev.conn
		a.dev.mu.Unlock()
		if c != nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("device never came online")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// fakeExtension is a gorilla WS client standing in for the browser extension.
type fakeExtension struct {
	conn *websocket.Conn
}

func dialExtension(t *testing.T, a *Actor) *fakeExtension {
	t.Helper()
	url := "ws://" + a.ListenAddr() + "/device"
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial extension: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &fakeExtension{conn: conn}
}

func (f *fakeExtension) read(t *testing.T) downFrame {
	t.Helper()
	_ = f.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var d downFrame
	if err := f.conn.ReadJSON(&d); err != nil {
		t.Fatalf("extension read: %v", err)
	}
	return d
}

func (f *fakeExtension) reply(t *testing.T, up upFrame) {
	t.Helper()
	if err := f.conn.WriteJSON(up); err != nil {
		t.Fatalf("extension reply: %v", err)
	}
}

// command builds a kimi.command request with the given action + args.
func command(id, action string, args map[string]any) *message.Envelope {
	payload := map[string]any{"action": action}
	if args != nil {
		payload["args"] = args
	}
	body, _ := json.Marshal(payload)
	return &message.Envelope{
		ID:         message.ID(id),
		ChannelID:  testChannelID,
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:main"},
		Kind:       message.KindRequest,
		Type:       TypeCommand,
		Payload:    body,
		Visibility: message.VisibilityPublic,
	}
}

func responseStatus(t *testing.T, env message.Envelope) (status, errorCode string, result map[string]any) {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(env.Payload, &m); err != nil {
		t.Fatalf("decode response payload: %v", err)
	}
	_ = json.Unmarshal(m["status"], &status)
	_ = json.Unmarshal(m["error_code"], &errorCode)
	result = map[string]any{}
	_ = json.Unmarshal(env.Payload, &result)
	return status, errorCode, result
}

//  1. Round-trip: a navigate command flows down to the extension as
//     {cmd:navigate, params:{url}} and the reply comes back as a completed
//     response. The device verb is drawn from the payload action; args becomes
//     the frame params.
func TestRoundTrip(t *testing.T) {
	w := &recordingWriter{}
	a := startActor(t, w, Config{})
	ext := dialExtension(t, a)
	waitOnline(t, a)

	req := command("req-1", "navigate", map[string]any{"url": "x"})
	if err := a.Receive(context.Background(), req); err != nil {
		t.Fatalf("Receive: %v", err)
	}

	down := ext.read(t)
	if down.CorrelationID != "req-1" {
		t.Errorf("correlation_id=%q want req-1", down.CorrelationID)
	}
	if down.Cmd != "navigate" {
		t.Errorf("cmd=%q want navigate", down.Cmd)
	}
	var params map[string]any
	_ = json.Unmarshal(down.Params, &params)
	if params["url"] != "x" {
		t.Errorf("params.url=%v want x", params["url"])
	}

	result, _ := json.Marshal(map[string]any{"tabId": 7})
	ext.reply(t, upFrame{CorrelationID: "req-1", OK: true, Result: result})

	resp, ok := w.waitResponse(t, "req-1", 2*time.Second)
	if !ok {
		t.Fatal("no response for req-1")
	}
	status, _, body := responseStatus(t, resp)
	if status != "completed" {
		t.Errorf("status=%q want completed", status)
	}
	if _, has := body["tabId"]; !has {
		t.Errorf("response payload missing tabId: %v", body)
	}
}

//  2. Offline: with no extension connected, a command fails immediately with
//     device_offline.
func TestOffline(t *testing.T) {
	w := &recordingWriter{}
	a := startActor(t, w, Config{})

	req := command("req-off", "snapshot", nil)
	if err := a.Receive(context.Background(), req); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	resp, ok := w.waitResponse(t, "req-off", time.Second)
	if !ok {
		t.Fatal("no response for req-off")
	}
	status, code, _ := responseStatus(t, resp)
	if status != "failed" {
		t.Errorf("status=%q want failed", status)
	}
	if code != "device_offline" {
		t.Errorf("error_code=%q want device_offline", code)
	}
}

//  3. Timeout: a connected extension that never replies; the reaper produces a
//     failed/timeout terminal. NowFn is advanced past the deadline.
func TestTimeout(t *testing.T) {
	w := &recordingWriter{}
	var mu sync.Mutex
	base := time.Now()
	now := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return base
	}
	a := startActor(t, w, Config{NowFn: now})
	ext := dialExtension(t, a)
	waitOnline(t, a)

	req := command("req-to", "snapshot", nil)
	if err := a.Receive(context.Background(), req); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	_ = ext.read(t) // extension receives but never replies

	// Advance the clock past the deadline so the reaper collects it.
	mu.Lock()
	base = base.Add(commandDeadline + time.Second)
	mu.Unlock()

	resp, ok := w.waitResponse(t, "req-to", 2*time.Second)
	if !ok {
		t.Fatal("no timeout response for req-to")
	}
	status, code, _ := responseStatus(t, resp)
	if status != "failed" {
		t.Errorf("status=%q want failed", status)
	}
	if code != "timeout" {
		t.Errorf("error_code=%q want timeout", code)
	}
}

//  4. Describe: actor.describe returns the single kimi.command type with all 13
//     actions visible.
func TestDescribe(t *testing.T) {
	w := &recordingWriter{}
	a := startActor(t, w, Config{})

	req := &message.Envelope{
		ID:         message.ID("req-desc"),
		ChannelID:  testChannelID,
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:main"},
		Kind:       message.KindRequest,
		Type:       "actor.describe",
		Payload:    json.RawMessage(`{}`),
		Visibility: message.VisibilityPublic,
	}
	if err := a.Receive(context.Background(), req); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	resp, ok := w.waitResponse(t, "req-desc", time.Second)
	if !ok {
		t.Fatal("no describe response")
	}
	var payload struct {
		ActorID string `json:"actor_id"`
		Types   map[string]struct {
			Notes string `json:"notes"`
		} `json:"types"`
	}
	if err := json.Unmarshal(resp.Payload, &payload); err != nil {
		t.Fatalf("decode describe: %v", err)
	}
	if payload.ActorID != string(DefaultActorID) {
		t.Errorf("actor_id=%q want %q", payload.ActorID, DefaultActorID)
	}
	if len(payload.Types) != 1 {
		t.Errorf("describe has %d types, want 1", len(payload.Types))
	}
	meta, has := payload.Types[TypeCommand]
	if !has {
		t.Fatalf("describe missing type %s", TypeCommand)
	}
	for action := range actions {
		if !strings.Contains(meta.Notes, action) {
			t.Errorf("describe notes missing action %q", action)
		}
	}
}

//  5. InvalidAction: an action outside the 13-primitive set fails invalid_action
//     and nothing is dispatched to the device.
func TestInvalidAction(t *testing.T) {
	w := &recordingWriter{}
	a := startActor(t, w, Config{})
	ext := dialExtension(t, a)
	waitOnline(t, a)

	req := command("req-bogus", "bogus", map[string]any{"x": 1})
	if err := a.Receive(context.Background(), req); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	resp, ok := w.waitResponse(t, "req-bogus", time.Second)
	if !ok {
		t.Fatal("no response for req-bogus")
	}
	status, code, _ := responseStatus(t, resp)
	if status != "failed" {
		t.Errorf("status=%q want failed", status)
	}
	if code != "invalid_action" {
		t.Errorf("error_code=%q want invalid_action", code)
	}

	// Nothing must have been dispatched: the extension sees no down-frame.
	_ = ext.conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	var d downFrame
	if err := ext.conn.ReadJSON(&d); err == nil {
		t.Fatalf("expected no down-frame for invalid action, got cmd=%q", d.Cmd)
	}
}

//  6. KindGuard: a non-request (event-kind) addressed to kimi.command is dropped
//     — the adapter has no terminal to author, so it emits nothing and dispatches
//     nothing.
func TestKindGuardDropsNonRequest(t *testing.T) {
	w := &recordingWriter{}
	a := startActor(t, w, Config{})

	ev := command("ev-1", "snapshot", nil)
	ev.Kind = message.KindEvent
	if err := a.Receive(context.Background(), ev); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	// Give any erroneous async path a moment, then assert nothing was emitted.
	time.Sleep(30 * time.Millisecond)
	if got := w.Written(); len(got) != 0 {
		t.Fatalf("expected no emit for non-request, got %d", len(got))
	}
}
