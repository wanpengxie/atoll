package xhs

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/atoll/drivers/tools/plugindevice"
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
// stops it — mirrors the engine's ErrRecvDone contract (spec §1.2).
var errFakeSysStopped = errors.New("fakeSys: stopped")

type replyRec struct {
	id message.ID
	v  any
}

type failRec struct {
	id           message.ID
	code, detail string
}

// fakeSys is a minimal actorbase.Sys double. It embeds the (nil) interface so
// any verb the actor never touches nil-panics loudly, and overrides the ones it
// uses: Recv, Reply, Fail, PublishObs, Self, and Life.
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
// address at birth and writes it on xhs.listen.set, so the double has to model
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

func (f *fakeSys) push(msg actorbase.Msg) {
	select {
	case f.recvCh <- msg:
	case <-f.quit:
	}
}

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

// startActor builds an xhs actor and runs its Proc body against a fakeSys, in a
// goroutine (run() blocks in sys.Recv()). Cleanup stops the fakeSys (ending
// run()'s loop) and joins it. A short reaper interval keeps the timeout test
// prompt without a prod constant.
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

// waitListening blocks until run() has bound the device's WS endpoint.
func waitListening(t *testing.T, a *Actor) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
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

func (f *fakeExtension) read(t *testing.T) plugindevice.DownFrame {
	t.Helper()
	_ = f.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var d plugindevice.DownFrame
	if err := f.conn.ReadJSON(&d); err != nil {
		t.Fatalf("extension read: %v", err)
	}
	return d
}

func (f *fakeExtension) reply(t *testing.T, up plugindevice.UpFrame) {
	t.Helper()
	if err := f.conn.WriteJSON(up); err != nil {
		t.Fatalf("extension reply: %v", err)
	}
}

// request builds an xhs request Msg of the given type + payload.
func request(id, typ string, payload map[string]any) actorbase.Msg {
	body, _ := json.Marshal(map[string]any{"body": payload})
	return actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID:         message.ID(id),
		ChannelID:  testChannelID,
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:main"},
		Kind:       message.KindRequest,
		Type:       typ,
		Payload:    body,
		Visibility: message.VisibilityPublic,
	})
}

//  1. Round-trip: a search request flows down to the extension and the reply
//     comes back as a Reply call carrying the device result.
func TestRoundTrip(t *testing.T) {
	a, sys := startActor(t, Config{})
	ext := dialExtension(t, a)
	waitOnline(t, a)

	sys.push(request("req-1", TypeSearch, map[string]any{"keyword": "go", "limit": 5}))

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
	ext.reply(t, plugindevice.UpFrame{CorrelationID: "req-1", OK: true, Result: result})

	rep, ok := sys.waitReply(t, "req-1", 2*time.Second)
	if !ok {
		t.Fatal("no Reply call for req-1")
	}
	raw, _ := json.Marshal(rep.v)
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode reply value: %v", err)
	}
	if _, has := body["results"]; !has {
		t.Errorf("reply value missing results: %v", rep.v)
	}
}

//  2. Offline: with no extension connected, a request fails immediately with
//     device_offline.
func TestOffline(t *testing.T) {
	_, sys := startActor(t, Config{})

	sys.push(request("req-off", TypeSearch, map[string]any{"keyword": "x"}))

	fail, ok := sys.waitFail(t, "req-off", time.Second)
	if !ok {
		t.Fatal("no Fail call for req-off")
	}
	if fail.code != "device_offline" {
		t.Errorf("code=%q want device_offline", fail.code)
	}
}

//  3. Timeout: a connected extension that never replies; the reaper self-wake
//     produces a Fail call with code=timeout. NowFn is advanced past the deadline.
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

	sys.push(request("req-to", TypeSearch, map[string]any{"keyword": "x"}))
	_ = ext.read(t) // extension receives but never replies

	// Advance the clock past the short deadline so the next reaper sweep collects it.
	mu.Lock()
	base = base.Add(shortDeadline + time.Second)
	mu.Unlock()

	fail, ok := sys.waitFail(t, "req-to", 2*time.Second)
	if !ok {
		t.Fatal("no timeout Fail call for req-to")
	}
	if fail.code != "timeout" {
		t.Errorf("code=%q want timeout", fail.code)
	}
}

// 5. ConnReplacement: a new device connection displaces the old one.
func TestConnReplacement(t *testing.T) {
	a, sys := startActor(t, Config{})
	ext1 := dialExtension(t, a)
	waitOnline(t, a)

	ext2 := dialExtension(t, a)
	waitOnline(t, a)

	_ = ext1.conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := ext1.conn.ReadMessage(); err == nil {
		t.Fatal("expected ext1 to be displaced (read should error)")
	}

	sys.push(request("req-repl", TypeSearch, map[string]any{"keyword": "x"}))
	down := ext2.read(t)
	if down.CorrelationID != "req-repl" {
		t.Errorf("ext2 got correlation_id=%q want req-repl", down.CorrelationID)
	}
}

// 6. KindGuard: a non-request addressed to a served type is dropped.
func TestKindGuardDropsNonRequest(t *testing.T) {
	_, sys := startActor(t, Config{})

	ev := request("ev-1", TypeSearch, map[string]any{"keyword": "x"})
	evEnv := message.Envelope{ID: ev.ID, Kind: message.KindEvent, Type: ev.Type, Payload: ev.Payload}
	sys.push(actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), evEnv))

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
	// Reserve a concrete loopback port, then free it so two incarnations can
	// contend for the exact same addr.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	// Predecessor binds the port.
	predecessor, predSys := startActor(t, Config{ListenAddr: addr})
	if predecessor.ListenAddr() == "" {
		t.Fatal("predecessor never bound")
	}

	// Successor on the same addr: its initial bind fails (port held), so it
	// enters the retry loop rather than dying.
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
