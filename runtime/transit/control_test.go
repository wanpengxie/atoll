package transit_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/transit"
)

// --- test stubs ---------------------------------------------------------

type stubRegistry struct {
	mu        sync.Mutex
	records   map[actor.ActorID]actor.Record
	lookupErr error
}

func newStubRegistry() *stubRegistry {
	return &stubRegistry{records: map[actor.ActorID]actor.Record{}}
}

func (r *stubRegistry) put(rec actor.Record) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records[rec.ID] = rec
}

func (r *stubRegistry) Lookup(_ context.Context, id actor.ActorID) (actor.Record, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lookupErr != nil {
		return actor.Record{}, false, r.lookupErr
	}
	rec, ok := r.records[id]
	return rec, ok, nil
}

func (r *stubRegistry) Exists(_ context.Context, id actor.ActorID) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.records[id]
	return ok, nil
}

func (r *stubRegistry) ListActive(_ context.Context) ([]actor.Record, error) {
	return nil, nil
}

func (r *stubRegistry) Insert(_ context.Context, rec actor.Record) error {
	r.put(rec)
	return nil
}

func (r *stubRegistry) Deregister(_ context.Context, _ actor.ActorID, _ int64) error {
	return nil
}

// stubChain records writes and returns the configured result.
type stubChain struct {
	mu        sync.Mutex
	lastEnv   *message.Envelope
	lastCtxOK bool
	result    transit.HarnessWriteResult
	err       error
}

func (c *stubChain) Write(ctx context.Context, env *message.Envelope) (transit.HarnessWriteResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	clone := *env
	c.lastEnv = &clone
	if v := ctx.Value(testCallerKey{}); v != nil {
		c.lastCtxOK = true
	}
	if c.err != nil {
		return transit.HarnessWriteResult{}, c.err
	}
	res := c.result
	if res.MessageID == "" {
		res.MessageID = env.ID
	}
	return res, nil
}

type testCallerKey struct{}

func stamper(ctx context.Context, actorID actor.ActorID, ch channel.ID) context.Context {
	return context.WithValue(ctx, testCallerKey{}, struct {
		Actor actor.ActorID
		Chan  channel.ID
	}{Actor: actorID, Chan: ch})
}

const (
	testWriteSecret  = "test-human-secret"
	testWriteChannel = "ch-1"
	testWriteActor   = "user:alice"
)

func mustNewWriteHandler(t *testing.T, router transit.WriteMessageRouter, window time.Duration) *transit.WriteMessageHandler {
	t.Helper()
	h, err := transit.NewWriteMessageHandler(transit.WriteMessageHandlerConfig{
		Secret:       []byte(testWriteSecret),
		Router:       router,
		NowMs:        func() int64 { return 10_000 },
		ReplayWindow: window,
	})
	if err != nil {
		t.Fatalf("NewWriteMessageHandler: %v", err)
	}
	return h
}

func newWriteMessageBody(t *testing.T, payload json.RawMessage, ts int64) transit.WriteMessageBody {
	t.Helper()
	hc := transit.HumanCaller{
		UserID:           "user-1",
		ActorIDInChannel: testWriteActor,
		TS:               ts,
		Nonce:            "nonce-1",
	}
	hc.ServerToken = transit.SignHumanCaller(
		[]byte(testWriteSecret),
		testWriteChannel,
		hc.UserID, hc.ActorIDInChannel, hc.TS, hc.Nonce,
	)
	if payload == nil {
		payload = json.RawMessage(`{"text":"hello"}`)
	}
	return transit.WriteMessageBody{
		FrameID:     "frame-1",
		ChannelID:   testWriteChannel,
		HumanCaller: hc,
		EnvelopePartial: message.Envelope{
			Type:       "human.text",
			Kind:       message.KindEvent,
			Payload:    payload,
			Audience:   []string{"*"},
			Visibility: message.VisibilityPublic,
			TS:         ts,
		},
	}
}

func routerFor(chain transit.HarnessChain, registry actor.Registry) transit.WriteMessageRouter {
	return func(_ context.Context, ch channel.ID) (transit.HarnessChain, actor.Registry, transit.CallerStamper, bool) {
		if string(ch) != testWriteChannel {
			return nil, nil, nil, false
		}
		return chain, registry, stamper, true
	}
}

// --- handler-level tests -----------------------------------------------

func TestWriteMessageHandler_Accept(t *testing.T) {
	reg := newStubRegistry()
	reg.put(actor.Record{ID: testWriteActor, Kind: message.SenderHuman, DisplayName: "Alice"})
	chain := &stubChain{result: transit.HarnessWriteResult{Seq: 42}}
	h := mustNewWriteHandler(t, routerFor(chain, reg), 0)

	body := newWriteMessageBody(t, nil, 9_900)
	ack := h.Handle(context.Background(), body)
	if !ack.Accepted {
		t.Fatalf("expected accept; got %+v", ack)
	}
	if ack.Seq != 42 {
		t.Errorf("Seq=%d", ack.Seq)
	}
	if ack.MessageID == "" {
		t.Error("MessageID empty")
	}
	if chain.lastEnv == nil {
		t.Fatal("chain.Write never invoked")
	}
	if chain.lastEnv.Sender.Kind != message.SenderHuman || chain.lastEnv.Sender.ID != testWriteActor {
		t.Errorf("sender stamp wrong: %+v", chain.lastEnv.Sender)
	}
	if chain.lastEnv.ID != ack.MessageID {
		t.Errorf("env.ID=%q ack.MessageID=%q", chain.lastEnv.ID, ack.MessageID)
	}
	if !chain.lastCtxOK {
		t.Error("CallerStamper was never applied to chain ctx")
	}
}

func TestWriteMessageHandler_BadHMAC(t *testing.T) {
	reg := newStubRegistry()
	reg.put(actor.Record{ID: testWriteActor, Kind: message.SenderHuman})
	chain := &stubChain{}
	h := mustNewWriteHandler(t, routerFor(chain, reg), 0)

	body := newWriteMessageBody(t, nil, 9_900)
	body.HumanCaller.ServerToken = "deadbeef"
	ack := h.Handle(context.Background(), body)
	if ack.Accepted {
		t.Fatal("expected reject on bad HMAC")
	}
	if ack.RejectReason != transit.RejectReasonAuthFailed {
		t.Errorf("RejectReason=%q", ack.RejectReason)
	}
	if chain.lastEnv != nil {
		t.Error("chain.Write must NOT run when HMAC fails")
	}
}

func TestWriteMessageHandler_ReplayWindow(t *testing.T) {
	reg := newStubRegistry()
	reg.put(actor.Record{ID: testWriteActor, Kind: message.SenderHuman})
	chain := &stubChain{}
	h := mustNewWriteHandler(t, routerFor(chain, reg), 60*time.Second)

	// NowMs=10_000ms, window=60s → reject anything older than 50_000ms-old.
	body := newWriteMessageBody(t, nil, -60_000)
	ack := h.Handle(context.Background(), body)
	if ack.Accepted {
		t.Fatal("expected reject on stale ts")
	}
	if ack.RejectReason != transit.RejectReasonReplayWindow {
		t.Errorf("RejectReason=%q want replay_window_expired", ack.RejectReason)
	}
	if chain.lastEnv != nil {
		t.Error("chain must not run when replay window fails")
	}
}

func TestWriteMessageHandler_UnknownChannel(t *testing.T) {
	chain := &stubChain{}
	router := func(_ context.Context, _ channel.ID) (transit.HarnessChain, actor.Registry, transit.CallerStamper, bool) {
		return nil, nil, nil, false
	}
	h := mustNewWriteHandler(t, router, 0)

	body := newWriteMessageBody(t, nil, 9_900)
	ack := h.Handle(context.Background(), body)
	if ack.Accepted {
		t.Fatal("expected reject when channel unbound")
	}
	if ack.RejectReason != transit.RejectReasonChannelUnbound {
		t.Errorf("RejectReason=%q", ack.RejectReason)
	}
	if chain.lastEnv != nil {
		t.Error("chain must not run when channel unbound")
	}
}

func TestWriteMessageHandler_UnknownActor(t *testing.T) {
	reg := newStubRegistry() // no record for testWriteActor
	chain := &stubChain{}
	h := mustNewWriteHandler(t, routerFor(chain, reg), 0)

	body := newWriteMessageBody(t, nil, 9_900)
	ack := h.Handle(context.Background(), body)
	if ack.Accepted {
		t.Fatal("expected reject on unknown actor")
	}
	if ack.RejectReason != transit.RejectReasonAuthFailed {
		t.Errorf("RejectReason=%q want auth_failed", ack.RejectReason)
	}
}

func TestWriteMessageHandler_DeregisteredActor(t *testing.T) {
	reg := newStubRegistry()
	reg.put(actor.Record{ID: testWriteActor, Kind: message.SenderHuman, DeregisteredAt: 1})
	chain := &stubChain{}
	h := mustNewWriteHandler(t, routerFor(chain, reg), 0)

	body := newWriteMessageBody(t, nil, 9_900)
	ack := h.Handle(context.Background(), body)
	if ack.Accepted {
		t.Fatal("expected reject on deregistered actor")
	}
	if ack.RejectReason != transit.RejectReasonAuthFailed {
		t.Errorf("RejectReason=%q", ack.RejectReason)
	}
}

func TestWriteMessageHandler_HarnessReject(t *testing.T) {
	reg := newStubRegistry()
	reg.put(actor.Record{ID: testWriteActor, Kind: message.SenderHuman})
	chain := &stubChain{result: transit.HarnessWriteResult{
		RejectReason: "kind_not_allowed",
		RejectDetail: "demo reject",
	}}
	h := mustNewWriteHandler(t, routerFor(chain, reg), 0)

	body := newWriteMessageBody(t, nil, 9_900)
	ack := h.Handle(context.Background(), body)
	if ack.Accepted {
		t.Fatal("expected ack.Accepted=false on harness reject")
	}
	if ack.RejectReason != "kind_not_allowed" {
		t.Errorf("RejectReason=%q", ack.RejectReason)
	}
	if ack.MessageID == "" {
		t.Error("MessageID should still be populated on reject (canonical hash succeeded)")
	}
}

func TestWriteMessageHandler_HarnessError(t *testing.T) {
	reg := newStubRegistry()
	reg.put(actor.Record{ID: testWriteActor, Kind: message.SenderHuman})
	chain := &stubChain{err: errors.New("store down")}
	h := mustNewWriteHandler(t, routerFor(chain, reg), 0)

	body := newWriteMessageBody(t, nil, 9_900)
	ack := h.Handle(context.Background(), body)
	if ack.Accepted {
		t.Fatal("expected reject on harness error")
	}
	if ack.RejectReason != transit.RejectReasonInternal {
		t.Errorf("RejectReason=%q want internal", ack.RejectReason)
	}
}

// --- Dispatcher integration ---------------------------------------------

// TestDispatcher_WriteMessageRoundTrip verifies the dispatcher decodes a
// control.write_message frame, invokes OnWriteMessage, and emits the
// ack frame back on the bus.
func TestDispatcher_WriteMessageRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reg := newStubRegistry()
	reg.put(actor.Record{ID: testWriteActor, Kind: message.SenderHuman})
	chain := &stubChain{result: transit.HarnessWriteResult{Seq: 7}}
	h := mustNewWriteHandler(t, routerFor(chain, reg), 0)

	bus := transit.NewMockBus(64)
	defer func() { _ = bus.Close() }()
	client, err := transit.NewClient(transit.ClientConfig{
		DaemonID: "daemon-A", Transport: bus,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	dispatcher, err := transit.NewDispatcher(transit.DispatcherConfig{
		Client:  client,
		FrameID: atomicFrameID(),
		Handlers: transit.ControlHandlers{
			OnWriteMessage: func(ctx context.Context, _ daemonbus.Frame, body transit.WriteMessageBody) transit.WriteMessageAckBody {
				return h.Handle(ctx, body)
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Run one Recv→Dispatch.
	done := make(chan error, 1)
	go func() {
		frame, recvErr := client.Recv(ctx)
		if recvErr != nil {
			done <- recvErr
			return
		}
		done <- dispatcher.Dispatch(ctx, frame)
	}()

	// Server sends control.write_message. Use the daemon's current
	// connection epoch so the FIX-T8 stale-epoch guard accepts it.
	body := newWriteMessageBody(t, nil, 9_900)
	server := bus.ServerSide()
	reqFrame, _ := transit.Encode("frame-srv-1",
		daemonbus.FrameTypeControlWriteMessage,
		"server", client.Epoch(), 0, body)
	if err := server.SendToDaemon(ctx, reqFrame); err != nil {
		t.Fatal(err)
	}

	select {
	case derr := <-done:
		if derr != nil {
			t.Fatalf("dispatch err: %v", derr)
		}
	case <-ctx.Done():
		t.Fatal("dispatch never returned")
	}

	// Daemon should have emitted control.write_message_ack.
	ackFrame, err := server.RecvFromDaemon(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ackFrame.FrameType != daemonbus.FrameTypeControlWriteMessageAck {
		t.Fatalf("ack frame type = %s", ackFrame.FrameType)
	}
	var ack transit.WriteMessageAckBody
	if err := transit.DecodePayload(ackFrame, &ack); err != nil {
		t.Fatal(err)
	}
	if !ack.Accepted {
		t.Errorf("ack.Accepted=false: %+v", ack)
	}
	if ack.Seq != 7 {
		t.Errorf("ack.Seq=%d", ack.Seq)
	}
	if ack.FrameID != body.FrameID {
		t.Errorf("ack.FrameID=%q want %q", ack.FrameID, body.FrameID)
	}
	if ack.MessageID == "" {
		t.Error("ack.MessageID empty")
	}
}
