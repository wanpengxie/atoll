package kimi

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/schedule"
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
// actually calls: Recv, Reply, Fail, PublishObs, Self, Life, After. Recv blocks
// on a channel (not a pre-built queue) because the actor's device goroutines
// close requests asynchronously while the worker goroutine is parked in Recv.
// After is real — the reaper + bind-retry are now sys.After self-wakes
// (spec §3), so the double must actually schedule delivery of the timer event.
type fakeSys struct {
	actorbase.Sys

	selfID actor.ActorID
	recvCh chan actorbase.Msg
	quit   chan struct{}
	once   sync.Once

	// afterErr, when set, makes After fail without scheduling — the injected
	// scheduler-arm failure the self-wake死-loud path is tested against.
	afterErr error

	mu      sync.Mutex
	replies []replyRec
	fails   []failRec
}

func newFakeSys(selfID actor.ActorID) *fakeSys {
	return &fakeSys{selfID: selfID, recvCh: make(chan actorbase.Msg, 16), quit: make(chan struct{})}
}

// push enqueues one delivery for the worker's Recv loop to pick up (dropping it
// if the double has already stopped, so timer goroutines never send on a dead
// path).
func (f *fakeSys) push(msg actorbase.Msg) {
	select {
	case f.recvCh <- msg:
	case <-f.quit:
	}
}

// stop closes the quit channel — Recv returns errFakeSysStopped, ending run()'s
// loop (its death) so the deferred device teardown runs.
func (f *fakeSys) stop() { f.once.Do(func() { close(f.quit) }) }

func (f *fakeSys) Recv() (actorbase.Msg, error) {
	select {
	case msg := <-f.recvCh:
		return msg, nil
	case <-f.quit:
		return actorbase.Msg{}, errFakeSysStopped
	}
}

// After schedules a self-authored KindEvent delivery after d — the substrate's
// self-wake, modelled with a real timer. It quits cleanly if the test stops.
func (f *fakeSys) After(d time.Duration, msgType string, payload any) (schedule.TimerID, error) {
	if f.afterErr != nil {
		return "", f.afterErr
	}
	raw, _ := json.Marshal(payload)
	go func() {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-timer.C:
			f.push(actorbase.NewMsg(context.Background(), message.Envelope{
				Kind: message.KindEvent, Type: msgType, Payload: raw,
			}))
		case <-f.quit:
		}
	}()
	return schedule.TimerID("fake-timer"), nil
}

func (f *fakeSys) Reply(msg actorbase.Msg, v any) (message.ID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replies = append(f.replies, replyRec{id: msg.ID, v: v})
	return msg.ID, nil
}

func (f *fakeSys) Fail(msg actorbase.Msg, code, detail string) (message.ID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fails = append(f.fails, failRec{id: msg.ID, code: code, detail: detail})
	return msg.ID, nil
}

func (f *fakeSys) PublishObs(actorrt.ObsKind, actorrt.ObsValue) error { return nil }

func (f *fakeSys) Self() actor.ActorID { return f.selfID }

func (f *fakeSys) Life() context.Context { return context.Background() }

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

// command builds a kimi.command request Msg with the given action + args.
func command(id, action string, args map[string]any) actorbase.Msg {
	payload := map[string]any{"action": action}
	if args != nil {
		payload["args"] = args
	}
	body, _ := json.Marshal(payload)
	return actorbase.NewMsg(context.Background(), message.Envelope{
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

//  4. Describe: actor.describe returns the single kimi.command type with all 13
//     actions visible.
func TestDescribe(t *testing.T) {
	_, sys := startActor(t, Config{})

	req := actorbase.NewMsg(context.Background(), message.Envelope{
		ID:         message.ID("req-desc"),
		ChannelID:  testChannelID,
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:main"},
		Kind:       message.KindRequest,
		Type:       "actor.describe",
		Payload:    json.RawMessage(`{}`),
		Visibility: message.VisibilityPublic,
	})
	sys.push(req)

	rep, ok := sys.waitReply(t, "req-desc", time.Second)
	if !ok {
		t.Fatal("no describe Reply call")
	}
	// rep.v is the introspect.Describe answer describeCatalog built; re-marshal
	// + decode to inspect its shape without importing introspect's concrete
	// type here a second time.
	raw, err := json.Marshal(rep.v)
	if err != nil {
		t.Fatalf("marshal describe reply: %v", err)
	}
	var payload struct {
		ActorID string `json:"actor_id"`
		Types   map[string]struct {
			Notes string `json:"notes"`
		} `json:"types"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
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

	// Nothing must have been dispatched: the extension sees no down-frame.
	_ = ext.conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	var d downFrame
	if err := ext.conn.ReadJSON(&d); err == nil {
		t.Fatalf("expected no down-frame for invalid action, got cmd=%q", d.Cmd)
	}
}

//  6. KindGuard: a non-request (event-kind) addressed to kimi.command is dropped
//     — the adapter has no terminal to author, so it neither replies nor fails
//     nor dispatches.
func TestKindGuardDropsNonRequest(t *testing.T) {
	_, sys := startActor(t, Config{})

	ev := command("ev-1", "snapshot", nil)
	evEnv := message.Envelope{ID: ev.ID, Kind: message.KindEvent, Type: ev.Type, Payload: ev.Payload}
	sys.push(actorbase.NewMsg(context.Background(), evEnv))

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

// P2-1 (期10 review): if the reaper self-wake cannot be ARMED (sys.After fails),
// the actor must die loud rather than silently ossify (sweeps would stop, stale
// sessions never reaped — a static half-alive actor is worse than death; the ring
// re-forges a healthy incarnation). (Pre-fix: `_, _ = a.sys.After(...)` swallowed
// the arm error and run() carried on with a dead self-wake chain.)
func TestReaperArmFailureDiesLoud(t *testing.T) {
	a := NewActor(Config{
		ListenAddr:        "127.0.0.1:0",
		ReaperInterval:    20 * time.Millisecond,
		BindRetryInterval: 20 * time.Millisecond,
	})
	sys := newFakeSys(DefaultActorID)
	sys.afterErr = errors.New("scheduler down") // every arm fails
	done := make(chan error, 1)
	go func() { done <- a.run(sys) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected a loud death when the reaper self-wake cannot be armed")
		}
	case <-time.After(2 * time.Second):
		sys.stop()
		t.Fatal("actor did not die on a failed After arm (silent ossification)")
	}
}
