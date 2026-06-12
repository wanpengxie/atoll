package xhs

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

const testChannelID = channel.ID("ch-test")

// recordingWriter is a concurrency-safe harness.Writer double (mirrors the
// kimiagent test double — the adapter emits from the read loop + reaper
// goroutines as well as the cell goroutine, so the writer must be safe).
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

// startActor builds + starts an xhs adapter on a free port. Cleanup stops it. A
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
// finishes registering the conn — this is the white-box synchronisation that the
// dropped online event used to provide.)
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

func request(id, typ string, payload map[string]any) *message.Envelope {
	body, _ := json.Marshal(payload)
	return &message.Envelope{
		ID:         message.ID(id),
		ChannelID:  testChannelID,
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:main"},
		Kind:       message.KindRequest,
		Type:       typ,
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

//  1. Round-trip: a search request flows down to the extension and the reply
//     comes back as a completed response carrying the result.
func TestRoundTrip(t *testing.T) {
	w := &recordingWriter{}
	a := startActor(t, w, Config{})
	ext := dialExtension(t, a)
	waitOnline(t, a)

	req := request("req-1", TypeSearch, map[string]any{"keyword": "go", "limit": 5})
	if err := a.Receive(context.Background(), req); err != nil {
		t.Fatalf("Receive: %v", err)
	}

	down := ext.read(t)
	if down.CorrelationID != "req-1" {
		t.Errorf("correlation_id=%q want req-1", down.CorrelationID)
	}
	if down.Cmd != "search" {
		t.Errorf("cmd=%q want search", down.Cmd)
	}
	var params map[string]any
	_ = json.Unmarshal(down.Params, &params)
	if params["keyword"] != "go" {
		t.Errorf("params.keyword=%v want go", params["keyword"])
	}

	result, _ := json.Marshal(map[string]any{"results": []map[string]any{{"id": "n1"}}})
	ext.reply(t, upFrame{CorrelationID: "req-1", OK: true, Result: result})

	resp, ok := w.waitResponse(t, "req-1", 2*time.Second)
	if !ok {
		t.Fatal("no response for req-1")
	}
	status, _, body := responseStatus(t, resp)
	if status != "completed" {
		t.Errorf("status=%q want completed", status)
	}
	if _, has := body["results"]; !has {
		t.Errorf("response payload missing results: %v", body)
	}
}

//  2. Offline: with no extension connected, a request fails immediately with
//     device_offline.
func TestOffline(t *testing.T) {
	w := &recordingWriter{}
	a := startActor(t, w, Config{})

	req := request("req-off", TypeSearch, map[string]any{"keyword": "x"})
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

	req := request("req-to", TypeSearch, map[string]any{"keyword": "x"})
	if err := a.Receive(context.Background(), req); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	_ = ext.read(t) // extension receives but never replies

	// Advance the clock past the short deadline so the reaper collects it.
	mu.Lock()
	base = base.Add(shortDeadline + time.Second)
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

// 4. Describe: actor.describe returns the four-type catalog.
func TestDescribe(t *testing.T) {
	w := &recordingWriter{}
	a := startActor(t, w, Config{})

	req := request("req-desc", "actor.describe", map[string]any{})
	if err := a.Receive(context.Background(), req); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	resp, ok := w.waitResponse(t, "req-desc", time.Second)
	if !ok {
		t.Fatal("no describe response")
	}
	var payload struct {
		ActorID string                     `json:"actor_id"`
		Types   map[string]json.RawMessage `json:"types"`
	}
	if err := json.Unmarshal(resp.Payload, &payload); err != nil {
		t.Fatalf("decode describe: %v", err)
	}
	if payload.ActorID != string(DefaultActorID) {
		t.Errorf("actor_id=%q want %q", payload.ActorID, DefaultActorID)
	}
	for _, want := range []string{TypePublish, TypeSearch, TypeNoteFetch, TypeRecentFetch} {
		if _, has := payload.Types[want]; !has {
			t.Errorf("describe missing type %s", want)
		}
	}
	if len(payload.Types) != 4 {
		t.Errorf("describe has %d types, want 4", len(payload.Types))
	}
}

//  5. ConnReplacement: a new device connection displaces the old one (one
//     adapter, one device). The old socket is closed; the live one serves.
func TestConnReplacement(t *testing.T) {
	w := &recordingWriter{}
	a := startActor(t, w, Config{})
	ext1 := dialExtension(t, a)
	waitOnline(t, a)

	ext2 := dialExtension(t, a)
	waitOnline(t, a)

	// ext1 must be displaced: its read errors out once the adapter closes it.
	_ = ext1.conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := ext1.conn.ReadMessage(); err == nil {
		t.Fatal("expected ext1 to be displaced (read should error)")
	}

	// The live connection (ext2) serves the next request.
	req := request("req-repl", TypeSearch, map[string]any{"keyword": "x"})
	if err := a.Receive(context.Background(), req); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	down := ext2.read(t)
	if down.CorrelationID != "req-repl" {
		t.Errorf("ext2 got correlation_id=%q want req-repl", down.CorrelationID)
	}
}

//  6. Status (online): with a device attached, actor.status answers
//     device_online=true.
func TestStatusOnline(t *testing.T) {
	w := &recordingWriter{}
	a := startActor(t, w, Config{})
	_ = dialExtension(t, a)
	waitOnline(t, a)

	req := request("req-st-on", "actor.status", map[string]any{})
	if err := a.Receive(context.Background(), req); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	resp, ok := w.waitResponse(t, "req-st-on", time.Second)
	if !ok {
		t.Fatal("no status response")
	}
	if online := statusDeviceOnline(t, resp); !online {
		t.Errorf("device_online=false want true (device attached)")
	}
}

//  7. Status (offline): with no device, actor.status answers
//     device_online=false. The request still completes (status is a self-answer,
//     never a device round-trip — unlike a business request which would fail
//     device_offline).
func TestStatusOffline(t *testing.T) {
	w := &recordingWriter{}
	a := startActor(t, w, Config{})

	req := request("req-st-off", "actor.status", map[string]any{})
	if err := a.Receive(context.Background(), req); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	resp, ok := w.waitResponse(t, "req-st-off", time.Second)
	if !ok {
		t.Fatal("no status response")
	}
	status, _, _ := responseStatus(t, resp)
	if status != "completed" {
		t.Errorf("status=%q want completed (self-answer, not a device round-trip)", status)
	}
	if online := statusDeviceOnline(t, resp); online {
		t.Errorf("device_online=true want false (no device attached)")
	}
}

// statusDeviceOnline extracts status_snapshot.device_online from an actor.status
// response envelope.
func statusDeviceOnline(t *testing.T, env message.Envelope) bool {
	t.Helper()
	var payload struct {
		Snapshot struct {
			DeviceOnline bool `json:"device_online"`
		} `json:"status_snapshot"`
	}
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		t.Fatalf("decode status payload: %v\nraw: %s", err, env.Payload)
	}
	return payload.Snapshot.DeviceOnline
}

//  8. KindGuard: a non-request (event-kind) addressed to a served type is
//     dropped — the adapter has no terminal to author, so it emits nothing.
func TestKindGuardDropsNonRequest(t *testing.T) {
	w := &recordingWriter{}
	a := startActor(t, w, Config{})

	ev := request("ev-1", TypeSearch, map[string]any{"keyword": "x"})
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
