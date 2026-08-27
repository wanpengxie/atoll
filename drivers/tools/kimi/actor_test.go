package kimi

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

const testChannelID = channel.ID("ch-test")

// errFakeSysStopped is fakeSys.Recv's loop-termination signal once the test
// closes the delivery channel (the fakeSys.stop() cleanup path) — mirrors the
// engine's ErrRecvDone contract (spec §1.2).
var errFakeSysStopped = errors.New("fakeSys: stopped")

// replyRec / failRec record one recorded sys.Reply/sys.Fail call — the
// concurrency-safe double for asserting what run()/device.go wrote, since
// several goroutines (worker, read loop, reaper) may call these concurrently
// (spec §1.2 fan-out).
type replyRec struct {
	id message.ID
	v  any
}

type failRec struct {
	id           message.ID
	code, detail string
}

// fakeSys is a minimal actorbase.Sys double: it embeds the (nil) interface so
// every verb this actor never touches stays unimplemented (a call would nil-
// panic, failing the test loudly), and overrides only the verbs the kimi actor
// actually calls: Recv, Reply, Fail, PublishObs, Self, and Life. Recv blocks on
// a channel because the actor's device goroutines close requests asynchronously
// while the worker goroutine is parked in Recv.
type fakeSys struct {
	actorbase.Sys

	selfID actor.ActorID
	recvCh chan actorbase.Msg
	quit   chan struct{}
	once   sync.Once
	life   context.Context
	cancel context.CancelFunc
	state  *fakeState

	mu      sync.Mutex
	replies []replyRec
	fails   []failRec
}

func newFakeSys(selfID actor.ActorID) *fakeSys {
	life, cancel := context.WithCancel(context.Background())
	return &fakeSys{
		selfID: selfID,
		recvCh: make(chan actorbase.Msg, 16),
		quit:   make(chan struct{}),
		life:   life,
		cancel: cancel,
		state:  newFakeState(),
	}
}

// fakeState is an in-memory StateHandle. The adapter reads its stored listen
// address at birth and writes it on kimi.listen.set, so the double has to model
// state rather than nil-panic on it.
type fakeState struct {
	mu   sync.Mutex
	kv   map[resource.ResourceID][]byte
	fail bool
}

func newFakeState() *fakeState { return &fakeState{kv: map[resource.ResourceID][]byte{}} }

func (s *fakeState) Get(id resource.ResourceID) (accessdoor.Outcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.kv[id]
	return accessdoor.Outcome{Value: v, Found: ok}, nil
}

func (s *fakeState) Put(id resource.ResourceID, args []byte) (accessdoor.Outcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return accessdoor.Outcome{RejectReason: access.AccessDenied}, nil
	}
	s.kv[id] = append([]byte(nil), args...)
	return accessdoor.Outcome{}, nil
}

func (s *fakeState) Del(id resource.ResourceID) (accessdoor.Outcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.kv, id)
	return accessdoor.Outcome{}, nil
}

func (f *fakeSys) State() actorbase.StateHandle { return f.state }

// push enqueues one delivery for the worker's Recv loop to pick up (dropping it
// if the double has already stopped, so timer goroutines never send on a dead
// path).
func (f *fakeSys) push(msg actorbase.Msg) {
	select {
	case f.recvCh <- msg:
	case <-f.quit:
	}
}

// stop cancels the process life and closes delivery so both actor-owned
// goroutines join.
func (f *fakeSys) stop() {
	f.once.Do(func() {
		f.cancel()
		close(f.quit)
	})
}

func (f *fakeSys) Recv() (actorbase.Msg, error) {
	select {
	case msg := <-f.recvCh:
		return msg, nil
	case <-f.quit:
		return actorbase.Msg{}, errFakeSysStopped
	}
}

func (f *fakeSys) Reply(msg actorbase.Msg, v any) (message.ID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replies = append(f.replies, replyRec{id: msg.ID, v: v})
	return msg.ID, nil
}

func (f *fakeSys) Fail(msg actorbase.Msg, code, detail string, _ ...map[string]any) (message.ID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fails = append(f.fails, failRec{id: msg.ID, code: code, detail: detail})
	return msg.ID, nil
}

func (f *fakeSys) PublishObs(actorrt.ObsKind, actorrt.ObsValue) error { return nil }

func (f *fakeSys) Self() actor.ActorID { return f.selfID }

func (f *fakeSys) Life() context.Context { return f.life }

var _ actorbase.Sys = (*fakeSys)(nil)

func (f *fakeSys) repliesSnapshot() []replyRec {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]replyRec, len(f.replies))
	copy(out, f.replies)
	return out
}

func (f *fakeSys) failsSnapshot() []failRec {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]failRec, len(f.fails))
	copy(out, f.fails)
	return out
}

// waitReply polls for a recorded Reply call for id.
func (f *fakeSys) waitReply(t *testing.T, id message.ID, timeout time.Duration) (replyRec, bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		for _, r := range f.repliesSnapshot() {
			if r.id == id {
				return r, true
			}
		}
		if time.Now().After(deadline) {
			return replyRec{}, false
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// waitFail polls for a recorded Fail call for id.
func (f *fakeSys) waitFail(t *testing.T, id message.ID, timeout time.Duration) (failRec, bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		for _, r := range f.failsSnapshot() {
			if r.id == id {
				return r, true
			}
		}
		if time.Now().After(deadline) {
			return failRec{}, false
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// startActor builds a kimi actor on a free port and runs its Proc body against
// a fakeSys, in a goroutine (run() blocks in sys.Recv()). Cleanup stops the
// fakeSys (which ends run()'s loop) and waits for it to return. A short reaper
// interval keeps the timeout test prompt without a prod constant.
func startActor(t *testing.T, cfg Config) (*Actor, *fakeSys) {
	t.Helper()
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = "127.0.0.1:0"
	}
	if cfg.ReaperInterval == 0 {
		cfg.ReaperInterval = 20 * time.Millisecond
	}
	if cfg.BindRetryInterval == 0 {
		cfg.BindRetryInterval = 20 * time.Millisecond
	}
	a := NewActor(cfg)
	sys := newFakeSys(DefaultActorID)
	done := make(chan error, 1)
	go func() { done <- a.run(sys) }()
	t.Cleanup(func() {
		sys.stop()
		<-done
	})
	waitListening(t, a)
	return a, sys
}

// waitListening blocks until run() has bound the device's WS endpoint (run()
// binds before entering its Recv loop, off the goroutine startActor spawned).
func waitListening(t *testing.T, a *Actor) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		if a.ListenAddr() != "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("device never bound its listen address")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// waitOnline blocks until the adapter has registered a live device connection.
// (Dial returns after the HTTP upgrade handshake, slightly before handleAccept
// finishes registering the conn — white-box synchronisation.)
func waitOnline(t *testing.T, a *Actor) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		if a.Online() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("device never came online")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// fakeExtension is a gorilla WS client standing in for the kimi-webbridge
// Chrome extension. It speaks that extension's OWN protocol — /ws, a hello
// handshake, tool_call/tool_result — because that is what the adapter has to
// satisfy; a double that spoke a frame family of our own choosing would prove
// nothing about the real thing.
type fakeExtension struct {
	conn *websocket.Conn
}

// toolCall is one command the adapter pushes down.
type toolCall struct {
	Type      string `json:"type"`
	RequestID string `json:"requestId"`
	Payload   struct {
		Name string          `json:"name"`
		Args json.RawMessage `json:"args"`
	} `json:"payload"`
}

func dialExtension(t *testing.T, a *Actor) *fakeExtension {
	t.Helper()
	url := "ws://" + a.ListenAddr() + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial extension: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ext := &fakeExtension{conn: conn}
	ext.hello(t)
	return ext
}

// hello performs the handshake the real extension performs. It is not optional:
// the extension does not consider itself ready until the ack comes back, so an
// adapter that skipped it would look connected and answer nothing.
func (f *fakeExtension) hello(t *testing.T) {
	t.Helper()
	if err := f.conn.WriteJSON(map[string]any{
		"type":    "hello",
		"payload": map[string]any{"extensionVersion": "test"},
	}); err != nil {
		t.Fatalf("extension hello: %v", err)
	}
	_ = f.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var ack struct {
		Type string `json:"type"`
	}
	if err := f.conn.ReadJSON(&ack); err != nil {
		t.Fatalf("no answer to hello: %v", err)
	}
	if ack.Type != "hello_ack" {
		t.Fatalf("answer to hello was %q, want hello_ack", ack.Type)
	}
}

// read waits for the next tool_call, stepping over heartbeats.
func (f *fakeExtension) read(t *testing.T) toolCall {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		_ = f.conn.SetReadDeadline(deadline)
		var call toolCall
		if err := f.conn.ReadJSON(&call); err != nil {
			t.Fatalf("extension read: %v", err)
		}
		if call.Type == "tool_call" {
			return call
		}
	}
}

func (f *fakeExtension) reply(t *testing.T, requestID string, data any) {
	t.Helper()
	if err := f.conn.WriteJSON(map[string]any{
		"type":                "tool_result",
		"responseToRequestId": requestID,
		"payload":             map[string]any{"data": data},
	}); err != nil {
		t.Fatalf("extension reply: %v", err)
	}
}

func (f *fakeExtension) replyError(t *testing.T, requestID, message string) {
	t.Helper()
	if err := f.conn.WriteJSON(map[string]any{
		"type":                "tool_result",
		"responseToRequestId": requestID,
		"payload":             map[string]any{"error": message},
	}); err != nil {
		t.Fatalf("extension reply: %v", err)
	}
}

// command builds a kimi.command request Msg with the given action + args.
func command(id, action string, args map[string]any) actorbase.Msg {
	payload := map[string]any{"action": action}
	if args != nil {
		payload["args"] = args
	}
	body, _ := json.Marshal(map[string]any{"body": payload})
	return actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID:         message.ID(id),
		ChannelID:  testChannelID,
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:main"},
		Kind:       message.KindRequest,
		Type:       TypeCommand,
		Payload:    body,
		Visibility: message.VisibilityPublic,
	})
}

//  1. Round-trip: a navigate command flows down to the extension as
//     {cmd:navigate, params:{url}} and the reply comes back as a Reply call
//     carrying the device result. The device verb is drawn from the payload
//     action; args becomes the frame params.
func TestRoundTrip(t *testing.T) {
	a, sys := startActor(t, Config{})
	ext := dialExtension(t, a)
	waitOnline(t, a)

	req := command("req-1", "navigate", map[string]any{"url": "x"})
	sys.push(req)

	down := ext.read(t)
	if down.RequestID != "req-1" {
		t.Errorf("requestId=%q want req-1", down.RequestID)
	}
	if down.Payload.Name != "navigate" {
		t.Errorf("name=%q want navigate", down.Payload.Name)
	}
	var params map[string]any
	_ = json.Unmarshal(down.Payload.Args, &params)
	if params["url"] != "x" {
		t.Errorf("params.url=%v want x", params["url"])
	}

	ext.reply(t, "req-1", map[string]any{"tabId": 7})

	rep, ok := sys.waitReply(t, "req-1", 2*time.Second)
	if !ok {
		t.Fatal("no Reply call for req-1")
	}
	// rep.v is device.go's raw device result (json.RawMessage or {} — see
	// handleUp); re-marshal + decode rather than asserting its static Go type.
	raw, err := json.Marshal(rep.v)
	if err != nil {
		t.Fatalf("marshal reply value: %v", err)
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode reply value: %v", err)
	}
	if _, has := body["tabId"]; !has {
		t.Errorf("reply value missing tabId: %v", rep.v)
	}
}

//  2. Offline: with no extension connected, a command fails immediately with
//     device_offline.
func TestOffline(t *testing.T) {
	_, sys := startActor(t, Config{})

	req := command("req-off", "snapshot", nil)
	sys.push(req)

	fail, ok := sys.waitFail(t, "req-off", time.Second)
	if !ok {
		t.Fatal("no Fail call for req-off")
	}
	if fail.code != "device_offline" {
		t.Errorf("code=%q want device_offline", fail.code)
	}
}

//  3. Timeout: a connected extension that never replies; the reaper produces a
//     Fail call with code=timeout. NowFn is advanced past the deadline.
func TestTimeout(t *testing.T) {
	var mu sync.Mutex
	base := time.Now()
	now := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return base
	}
	a, sys := startActor(t, Config{NowFn: now})
	ext := dialExtension(t, a)
	waitOnline(t, a)

	req := command("req-to", "snapshot", nil)
	sys.push(req)
	_ = ext.read(t) // extension receives but never replies

	// Advance the clock past the deadline so the reaper collects it.
	mu.Lock()
	base = base.Add(commandDeadline + time.Second)
	mu.Unlock()

	fail, ok := sys.waitFail(t, "req-to", 2*time.Second)
	if !ok {
		t.Fatal("no timeout Fail call for req-to")
	}
	if fail.code != "timeout" {
		t.Errorf("code=%q want timeout", fail.code)
	}
}

//  5. InvalidAction: an action outside the 13-primitive set fails invalid_action
//     and nothing is dispatched to the device.
func TestInvalidAction(t *testing.T) {
	a, sys := startActor(t, Config{})
	ext := dialExtension(t, a)
	waitOnline(t, a)

	req := command("req-bogus", "bogus", map[string]any{"x": 1})
	sys.push(req)

	fail, ok := sys.waitFail(t, "req-bogus", time.Second)
	if !ok {
		t.Fatal("no Fail call for req-bogus")
	}
	if fail.code != "invalid_action" {
		t.Errorf("code=%q want invalid_action", fail.code)
	}

	// Nothing must have been dispatched: the extension sees no tool_call.
	_ = ext.conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	var call toolCall
	if err := ext.conn.ReadJSON(&call); err == nil && call.Type == "tool_call" {
		t.Fatalf("expected no tool_call for invalid action, got name=%q", call.Payload.Name)
	}
}

//  6. KindGuard: a non-request (event-kind) addressed to kimi.command is dropped
//     — the adapter has no terminal to author, so it neither replies nor fails
//     nor dispatches.
func TestKindGuardDropsNonRequest(t *testing.T) {
	_, sys := startActor(t, Config{})

	ev := command("ev-1", "snapshot", nil)
	evEnv := message.Envelope{ID: ev.ID, Kind: message.KindEvent, Type: ev.Type, Payload: ev.Payload}
	sys.push(actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), evEnv))

	// Give any erroneous async path a moment, then assert nothing was recorded.
	time.Sleep(30 * time.Millisecond)
	if got := sys.repliesSnapshot(); len(got) != 0 {
		t.Fatalf("expected no replies for non-request, got %d", len(got))
	}
	if got := sys.failsSnapshot(); len(got) != 0 {
		t.Fatalf("expected no fails for non-request, got %d", len(got))
	}
}

//  7. FixedPortReplacement (Q8=B): a successor incarnation on the SAME fixed
//     loopback port cannot bind while the predecessor holds it; its retry loop
//     keeps trying and it binds once the predecessor releases the port — the
//     exclusive-resource contention is resolved by the domain, not the kernel.
func TestFixedPortReplacementRetriesUntilBound(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	predecessor, predSys := startActor(t, Config{ListenAddr: addr})
	if predecessor.ListenAddr() == "" {
		t.Fatal("predecessor never bound")
	}

	successor := NewActor(Config{ListenAddr: addr, BindRetryInterval: 20 * time.Millisecond, ReaperInterval: 20 * time.Millisecond})
	succSys := newFakeSys(DefaultActorID)
	done := make(chan error, 1)
	go func() { done <- successor.run(succSys) }()
	t.Cleanup(func() {
		succSys.stop()
		<-done
	})

	// It must NOT be bound while the predecessor holds the port.
	time.Sleep(60 * time.Millisecond)
	if successor.ListenAddr() != "" {
		t.Fatal("successor bound while predecessor still held the port")
	}

	// Release the port (predecessor dies); the successor's retry loop must bind.
	predSys.stop()

	deadline := time.Now().Add(2 * time.Second)
	for successor.ListenAddr() == "" {
		if time.Now().After(deadline) {
			t.Fatal("successor never bound after predecessor released the port")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestDeviceMaintenanceDoesNotDependOnScheduleCapability(t *testing.T) {
	a, sys := startActor(t, Config{
		ListenAddr:        "127.0.0.1:0",
		ReaperInterval:    20 * time.Millisecond,
		BindRetryInterval: 20 * time.Millisecond,
	})
	if a.ListenAddr() == "" {
		t.Fatal("local device maintenance did not bind without Schedule")
	}
	sys.stop()
}
