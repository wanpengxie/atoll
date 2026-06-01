package transit_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/framework/multiuser/daemonbus"
	"github.com/wanpengxie/ActOS/framework/multiuser/runtime/transit"
	"github.com/wanpengxie/ActOS/framework/multiuser/viewsync"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/pkg/requestctx"
)

// --- test stubs ---------------------------------------------------------

type stubRegistry struct {
	mu        sync.Mutex
	records   map[actor.ActorID]actorreg.Record
	lookupErr error
}

func newStubRegistry() *stubRegistry {
	return &stubRegistry{records: map[actor.ActorID]actorreg.Record{}}
}

func (r *stubRegistry) put(rec actorreg.Record) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records[rec.ID] = rec
}

func (r *stubRegistry) Lookup(_ context.Context, id actor.ActorID) (actorreg.Record, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lookupErr != nil {
		return actorreg.Record{}, false, r.lookupErr
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

func (r *stubRegistry) ListActive(_ context.Context) ([]actorreg.Record, error) {
	return nil, nil
}

func (r *stubRegistry) Insert(_ context.Context, rec actorreg.Record) error {
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
	testWriteSecret          = "test-human-secret"
	testWriteChannel         = "ch-1"
	testWriteActor           = "user:alice"
	defaultWriteReplayWindow = 60 * time.Second
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

func TestSignHumanCallerDisambiguatesPipeFields(t *testing.T) {
	t.Parallel()
	const legacyConcat = "ch|user|id|user:alice|7|nonce"
	aLegacy := "ch|user" + "|" + "id" + "|" + string(actor.ActorID("user:alice")) + "|" + "7" + "|" + "nonce"
	bLegacy := "ch" + "|" + "user|id" + "|" + string(actor.ActorID("user:alice")) + "|" + "7" + "|" + "nonce"
	if aLegacy != legacyConcat || bLegacy != legacyConcat {
		t.Fatalf("test setup no longer creates an old pipe-concat collision: %q %q", aLegacy, bLegacy)
	}

	a := transit.SignHumanCaller([]byte(testWriteSecret), "ch|user", "id", "user:alice", 7, "nonce")
	b := transit.SignHumanCaller([]byte(testWriteSecret), "ch", "user|id", "user:alice", 7, "nonce")
	if a == b {
		t.Fatal("structured HumanCaller signatures collided for different field segmentation")
	}
}

func newWriteMessageBody(t *testing.T, payload json.RawMessage, ts int64) transit.WriteMessageBody {
	t.Helper()
	hc := transit.HumanCaller{
		UserID:        "user-1",
		MemberActorID: testWriteActor,
		TS:            ts,
		Nonce:         "nonce-1",
	}
	hc.ServerToken = transit.SignHumanCaller(
		[]byte(testWriteSecret),
		testWriteChannel,
		hc.UserID, hc.MemberActorID, hc.TS, hc.Nonce,
	)
	if payload == nil {
		payload = json.RawMessage(`{"text":"hello"}`)
	}
	return transit.WriteMessageBody{
		FrameID:     "frame-1",
		ChannelID:   testWriteChannel,
		HumanCaller: hc,
		EnvelopePartial: message.Envelope{
			// R4-3: caller-supplied envelope.id (L0 §1.1 / L3 §1.8.1).
			// The gateway stamps this from the inbound HTTP body; tests
			// fabricate a stable id so retries within a test reuse it.
			ID:         "msg-write-1",
			Type:       "human.text",
			Kind:       message.KindEvent,
			Payload:    payload,
			Audience:   message.Audience{"agent:channel-agent"},
			Visibility: message.VisibilityPublic,
			TS:         ts,
		},
	}
}

func routerFor(chain transit.HarnessChain, registry actorreg.Registry) transit.WriteMessageRouter {
	return func(_ context.Context, ch channel.ID) (transit.HarnessChain, actorreg.Registry, transit.CallerStamper, bool) {
		if string(ch) != testWriteChannel {
			return nil, nil, nil, false
		}
		return chain, registry, stamper, true
	}
}

// --- handler-level tests -----------------------------------------------

func TestWriteMessageHandler_Accept(t *testing.T) {
	reg := newStubRegistry()
	reg.put(actorreg.Record{ID: testWriteActor, Kind: actor.KindHuman, DisplayName: "Alice"})
	chain := &stubChain{result: transit.HarnessWriteResult{Seq: 42}}
	h := mustNewWriteHandler(t, routerFor(chain, reg), defaultWriteReplayWindow)

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
	if chain.lastEnv.Sender.Kind != actor.KindHuman || chain.lastEnv.Sender.ID != actor.ActorID(testWriteActor) {
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
	reg.put(actorreg.Record{ID: testWriteActor, Kind: actor.KindHuman})
	chain := &stubChain{}
	h := mustNewWriteHandler(t, routerFor(chain, reg), defaultWriteReplayWindow)

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
	reg.put(actorreg.Record{ID: testWriteActor, Kind: actor.KindHuman})
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
	router := func(_ context.Context, _ channel.ID) (transit.HarnessChain, actorreg.Registry, transit.CallerStamper, bool) {
		return nil, nil, nil, false
	}
	h := mustNewWriteHandler(t, router, defaultWriteReplayWindow)

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
	h := mustNewWriteHandler(t, routerFor(chain, reg), defaultWriteReplayWindow)

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
	reg.put(actorreg.Record{ID: testWriteActor, Kind: actor.KindHuman, DeregisteredAt: 1})
	chain := &stubChain{}
	h := mustNewWriteHandler(t, routerFor(chain, reg), defaultWriteReplayWindow)

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
	reg.put(actorreg.Record{ID: testWriteActor, Kind: actor.KindHuman})
	chain := &stubChain{result: transit.HarnessWriteResult{
		RejectReason: "harness_kind_not_allowed_for_type",
		RejectDetail: "demo reject",
	}}
	h := mustNewWriteHandler(t, routerFor(chain, reg), defaultWriteReplayWindow)

	body := newWriteMessageBody(t, nil, 9_900)
	ack := h.Handle(context.Background(), body)
	if ack.Accepted {
		t.Fatal("expected ack.Accepted=false on harness reject")
	}
	if ack.RejectReason != "harness_kind_not_allowed_for_type" {
		t.Errorf("RejectReason=%q", ack.RejectReason)
	}
	if ack.MessageID == "" {
		t.Error("MessageID should still be populated on reject (canonical hash succeeded)")
	}
}

func TestWriteMessageHandler_HarnessError(t *testing.T) {
	reg := newStubRegistry()
	reg.put(actorreg.Record{ID: testWriteActor, Kind: actor.KindHuman})
	chain := &stubChain{err: errors.New("store down")}
	h := mustNewWriteHandler(t, routerFor(chain, reg), defaultWriteReplayWindow)

	body := newWriteMessageBody(t, nil, 9_900)
	ack := h.Handle(context.Background(), body)
	if ack.Accepted {
		t.Fatal("expected reject on harness error")
	}
	if ack.RejectReason != transit.RejectReasonInternal {
		t.Errorf("RejectReason=%q want internal", ack.RejectReason)
	}
}

func TestWriteMessageHandler_RejectObservabilityIncludesReasonAndCorrelation(t *testing.T) {
	logger := &recordingTransitLogger{}
	metrics := newRecordingTransitMetrics()
	h, err := transit.NewWriteMessageHandler(transit.WriteMessageHandlerConfig{
		Secret:       []byte(testWriteSecret),
		Router:       routerFor(&stubChain{}, newStubRegistry()),
		NowMs:        func() int64 { return 10_000 },
		ReplayWindow: defaultWriteReplayWindow,
		Logger:       logger,
		Metrics:      metrics,
	})
	if err != nil {
		t.Fatalf("NewWriteMessageHandler: %v", err)
	}
	body := newWriteMessageBody(t, nil, 9_900)
	body.FrameID = "frame-observe"
	body.EnvelopePartial.CorrelationID = "corr-transit-1"
	body.HumanCaller.ServerToken = "bad-token"

	ack := h.Handle(context.Background(), body)
	if ack.Accepted || ack.RejectReason != transit.RejectReasonAuthFailed {
		t.Fatalf("ack=%+v want auth_failed reject", ack)
	}
	if got := metrics.counter("write_message.reject", "reason", transit.RejectReasonAuthFailed); got != 1 {
		t.Fatalf("write_message.reject counter=%d want 1", got)
	}
	if !logger.has("warn", "write_message.reject", "reason", transit.RejectReasonAuthFailed, "correlation_id", "corr-transit-1") {
		t.Fatalf("missing reject log with reason + correlation_id: %+v", logger.lines())
	}
}

type recordingTransitLogger struct {
	mu   sync.Mutex
	logs []recordedTransitLog
}

type recordedTransitLog struct {
	level string
	msg   string
	args  map[string]string
}

func (l *recordingTransitLogger) Debug(msg string, args ...any) { l.record("debug", msg, args...) }
func (l *recordingTransitLogger) Warn(msg string, args ...any)  { l.record("warn", msg, args...) }
func (l *recordingTransitLogger) Error(msg string, args ...any) { l.record("error", msg, args...) }

func (l *recordingTransitLogger) record(level, msg string, args ...any) {
	line := recordedTransitLog{level: level, msg: msg, args: map[string]string{}}
	for i := 0; i < len(args); i += 2 {
		key := fmt.Sprint(args[i])
		value := ""
		if i+1 < len(args) {
			value = fmt.Sprint(args[i+1])
		}
		line.args[key] = value
	}
	l.mu.Lock()
	l.logs = append(l.logs, line)
	l.mu.Unlock()
}

func (l *recordingTransitLogger) has(level, msg string, fields ...string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.logs {
		if line.level != level || line.msg != msg {
			continue
		}
		ok := true
		for i := 0; i < len(fields); i += 2 {
			if line.args[fields[i]] != fields[i+1] {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

func (l *recordingTransitLogger) lines() []recordedTransitLog {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]recordedTransitLog, len(l.logs))
	copy(out, l.logs)
	return out
}

type recordingTransitMetrics struct {
	mu       sync.Mutex
	counters map[string]int64
}

func newRecordingTransitMetrics() *recordingTransitMetrics {
	return &recordingTransitMetrics{counters: map[string]int64{}}
}

func (m *recordingTransitMetrics) IncCounter(name string, tags ...string) {
	m.mu.Lock()
	m.counters[transitMetricKey(name, tags...)]++
	m.mu.Unlock()
}

func (m *recordingTransitMetrics) counter(name string, tags ...string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[transitMetricKey(name, tags...)]
}

func transitMetricKey(name string, tags ...string) string {
	key := name
	for i := 0; i < len(tags); i += 2 {
		value := ""
		if i+1 < len(tags) {
			value = tags[i+1]
		}
		key += "|" + tags[i] + "=" + value
	}
	return key
}

// --- Dispatcher integration ---------------------------------------------

// TestDispatcher_WriteMessageRoundTrip verifies the dispatcher decodes a
// control.write_message frame, invokes OnWriteMessage, and emits the
// ack frame back on the bus.
func TestDispatcher_WriteMessageRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reg := newStubRegistry()
	reg.put(actorreg.Record{ID: testWriteActor, Kind: actor.KindHuman})
	chain := &stubChain{result: transit.HarnessWriteResult{Seq: 7}}
	h := mustNewWriteHandler(t, routerFor(chain, reg), defaultWriteReplayWindow)

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
				if got := requestctx.RequestID(ctx); got != "req-http-1" {
					return transit.WriteMessageAckBody{
						FrameID:      body.FrameID,
						RejectReason: transit.RejectReasonInternal,
						RejectDetail: "request_id not propagated: " + got,
					}
				}
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
	reqFrame.RequestID = "req-http-1"
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
	if ackFrame.FrameKind != daemonbus.FrameTypeControlWriteMessageAck {
		t.Fatalf("ack frame type = %s", ackFrame.FrameKind)
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
	if ackFrame.RequestID != "req-http-1" {
		t.Errorf("ackFrame.RequestID=%q want req-http-1", ackFrame.RequestID)
	}
	if ack.MessageID == "" {
		t.Error("ack.MessageID empty")
	}
	// FIX-2026-05-18 regression: ack envelope frame_id MUST echo the
	// inbound envelope frame_id. Server-side daemonbus.Connection
	// matchAck registers pending callers under the SendAndAwait
	// envelope frame_id; if the daemon emits a fresh id here, the
	// match misses forever and SendAndAwait times out (the production
	// 524 incident root cause).
	if ackFrame.FrameID != reqFrame.FrameID {
		t.Errorf("ack envelope frame_id=%q want %q (must echo inbound for SendAndAwait pairing)",
			ackFrame.FrameID, reqFrame.FrameID)
	}
}

func TestDispatcher_WriteMessagePanicReturnsRejectedAck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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
			OnWriteMessage: func(context.Context, daemonbus.Frame, transit.WriteMessageBody) transit.WriteMessageAckBody {
				panic("write panic")
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	body := newWriteMessageBody(t, nil, 10_100)
	frame, _ := transit.Encode("frame-panic",
		daemonbus.FrameTypeControlWriteMessage,
		"server", client.Epoch(), 0, body)
	if err := dispatcher.Dispatch(ctx, frame); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	ackFrame, err := bus.ServerSide().RecvFromDaemon(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var ack transit.WriteMessageAckBody
	if err := transit.DecodePayload(ackFrame, &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Accepted || ack.RejectReason != transit.RejectReasonInternal {
		t.Fatalf("ack=%+v want internal reject", ack)
	}
	if ack.FrameID != body.FrameID {
		t.Fatalf("ack.FrameID=%q want %q", ack.FrameID, body.FrameID)
	}
}

// TestDispatcher_AckEnvelopeFrameIDEchoesInbound is the explicit
// regression for the 2026-05-18 daemonbus ack-pairing bug: every ack
// path on the daemon side (write_message_ack / update_members_ack /
// viewsync.resync_response / create_channel_ack / unbind_channel_ack)
// MUST emit an envelope
// frame_id that equals the inbound envelope frame_id, otherwise
// server/daemonbus.Connection.SendAndAwait can never pair the ack
// against its pending entry and the gateway HTTP request times out
// (cloudflare 524 in production).
//
// Covers the SendAndAwait-bound paths through Dispatcher; the
// daemon.go-owned create_channel_ack / unbind_channel_ack paths are
// not routed through Dispatcher and are exercised by their own tests.
func TestDispatcher_AckEnvelopeFrameIDEchoesInbound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reg := newStubRegistry()
	reg.put(actorreg.Record{ID: testWriteActor, Kind: actor.KindHuman})
	chain := &stubChain{result: transit.HarnessWriteResult{Seq: 1}}
	h := mustNewWriteHandler(t, routerFor(chain, reg), defaultWriteReplayWindow)

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

	updateCalled := false
	resyncCalled := false
	dispatcher, err := transit.NewDispatcher(transit.DispatcherConfig{
		Client:  client,
		FrameID: atomicFrameID(),
		Handlers: transit.ControlHandlers{
			OnWriteMessage: func(ctx context.Context, _ daemonbus.Frame, body transit.WriteMessageBody) transit.WriteMessageAckBody {
				return h.Handle(ctx, body)
			},
			OnUpdateMembers: func(_ context.Context, _ daemonbus.Frame, body transit.UpdateMembersBody) transit.UpdateMembersAckBody {
				updateCalled = true
				return transit.UpdateMembersAckBody{
					FrameID:   body.FrameID,
					ChannelID: body.ChannelID,
					Accepted:  true,
				}
			},
			OnViewsyncResyncRequest: func(_ context.Context, _ viewsync.ResyncRequest) (viewsync.ResyncResponse, error) {
				resyncCalled = true
				return viewsync.ResyncResponse{ChannelID: "ch-1"}, nil
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	server := bus.ServerSide()

	// Run Recv→Dispatch loop in background.
	loopDone := make(chan error, 1)
	go func() {
		for {
			frame, recvErr := client.Recv(ctx)
			if recvErr != nil {
				loopDone <- recvErr
				return
			}
			if derr := dispatcher.Dispatch(ctx, frame); derr != nil {
				loopDone <- derr
				return
			}
		}
	}()

	// --- Case A: control.write_message ---
	caseAFrameID := "envelope-from-server-A"
	{
		body := newWriteMessageBody(t, nil, 9_900)
		reqFrame, _ := transit.Encode(caseAFrameID,
			daemonbus.FrameTypeControlWriteMessage,
			"server", client.Epoch(), 0, body)
		if err := server.SendToDaemon(ctx, reqFrame); err != nil {
			t.Fatal(err)
		}
		ackFrame, err := server.RecvFromDaemon(ctx)
		if err != nil {
			t.Fatalf("write_message ack recv: %v", err)
		}
		if ackFrame.FrameKind != daemonbus.FrameTypeControlWriteMessageAck {
			t.Fatalf("write_message ack type=%s", ackFrame.FrameKind)
		}
		if ackFrame.FrameID != daemonbus.FrameID(caseAFrameID) {
			t.Errorf("write_message_ack envelope frame_id=%q want %q — SendAndAwait pairing would BREAK",
				ackFrame.FrameID, caseAFrameID)
		}
	}

	// --- Case B: control.update_members ---
	caseDFrameID := "envelope-from-server-B"
	{
		updateBody := transit.UpdateMembersBody{
			FrameID:   "body-frame-D",
			ChannelID: "ch-D",
			Adds: []daemonbus.UpdateMember{{
				UserID:        "user-D",
				MemberActorID: "user:member-D",
				Kind:          actor.KindHuman,
			}},
		}
		reqFrame, _ := transit.Encode(caseDFrameID,
			daemonbus.FrameTypeControlUpdateMembers,
			"server", client.Epoch(), 0, updateBody)
		if err := server.SendToDaemon(ctx, reqFrame); err != nil {
			t.Fatal(err)
		}
		ackFrame, err := server.RecvFromDaemon(ctx)
		if err != nil {
			t.Fatalf("update_members ack recv: %v", err)
		}
		if ackFrame.FrameKind != daemonbus.FrameTypeControlUpdateMembersAck {
			t.Fatalf("update_members ack type=%s", ackFrame.FrameKind)
		}
		if ackFrame.FrameID != daemonbus.FrameID(caseDFrameID) {
			t.Errorf("update_members_ack envelope frame_id=%q want %q",
				ackFrame.FrameID, caseDFrameID)
		}
		if !updateCalled {
			t.Error("OnUpdateMembers not invoked")
		}
	}

	// --- Case C: viewsync.resync_request ---
	caseEFrameID := "envelope-from-server-C"
	{
		req := viewsync.ResyncRequest{ChannelID: "ch-1", SinceSeq: 1, UntilSeq: 5}
		reqFrame, _ := transit.Encode(caseEFrameID,
			daemonbus.FrameTypeViewsyncResyncRequest,
			"server", client.Epoch(), 0, req)
		if err := server.SendToDaemon(ctx, reqFrame); err != nil {
			t.Fatal(err)
		}
		ackFrame, err := server.RecvFromDaemon(ctx)
		if err != nil {
			t.Fatalf("resync resp recv: %v", err)
		}
		if ackFrame.FrameKind != daemonbus.FrameTypeViewsyncResyncResponse {
			t.Fatalf("resync resp type=%s", ackFrame.FrameKind)
		}
		if ackFrame.FrameID != daemonbus.FrameID(caseEFrameID) {
			t.Errorf("viewsync.resync_response envelope frame_id=%q want %q",
				ackFrame.FrameID, caseEFrameID)
		}
		if !resyncCalled {
			t.Error("OnViewsyncResyncRequest not invoked")
		}
	}

	cancel()
	select {
	case <-loopDone:
	case <-time.After(1 * time.Second):
	}
}

// TestDispatcher_StaleEpochRejected covers FIX-T8 phase-3 — when a
// server-emitted frame stamps a connection_epoch that doesn't match
// the daemon's current epoch (e.g. an old session frame races a
// reconnect), Dispatch MUST return ErrStaleEpoch and MUST NOT invoke
// the per-frame-type handler.
func TestDispatcher_StaleEpochRejected(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	bus := transit.NewMockBus(8)
	defer func() { _ = bus.Close() }()
	client, err := transit.NewClient(transit.ClientConfig{
		DaemonID: "daemon-stale", Transport: bus,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	called := false
	dispatcher, err := transit.NewDispatcher(transit.DispatcherConfig{
		Client:  client,
		FrameID: atomicFrameID(),
		Handlers: transit.ControlHandlers{
			OnWriteMessage: func(ctx context.Context, _ daemonbus.Frame, _ transit.WriteMessageBody) transit.WriteMessageAckBody {
				called = true
				return transit.WriteMessageAckBody{}
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Build a frame whose epoch is one less than the client's current
	// epoch — simulates a frame from the previous WS session.
	stale := client.Epoch() - 1
	body := newWriteMessageBody(t, nil, 9_900)
	frame, _ := transit.Encode("frame-stale", daemonbus.FrameTypeControlWriteMessage,
		"server", stale, 0, body)

	derr := dispatcher.Dispatch(ctx, frame)
	if !errors.Is(derr, transit.ErrStaleEpoch) {
		t.Fatalf("Dispatch err=%v want ErrStaleEpoch", derr)
	}
	if called {
		t.Error("OnWriteMessage handler must NOT run for stale-epoch frames")
	}
}

// TestWriteMessageHandler_NonceReplay covers FIX-T8 phase-5 — replaying
// the same (channel_id, nonce) tuple within the configured window
// MUST be rejected with replay_nonce_seen even though the HMAC, ts,
// and registry checks all pass.
func TestWriteMessageHandler_NonceReplay(t *testing.T) {
	t.Parallel()
	reg := newStubRegistry()
	reg.put(actorreg.Record{ID: testWriteActor, Kind: actor.KindHuman, DisplayName: "Alice"})
	chain := &stubChain{result: transit.HarnessWriteResult{Seq: 1}}
	h := mustNewWriteHandler(t, routerFor(chain, reg), 60*time.Second)

	body := newWriteMessageBody(t, nil, 9_900)

	// First write — accepted.
	ack := h.Handle(context.Background(), body)
	if !ack.Accepted {
		t.Fatalf("first write should accept: %+v", ack)
	}

	// Re-send with the SAME nonce + ts → reject with replay_nonce_seen.
	ack2 := h.Handle(context.Background(), body)
	if ack2.Accepted {
		t.Fatal("nonce reuse should reject")
	}
	if ack2.RejectReason != transit.RejectReasonReplayNonce {
		t.Errorf("RejectReason=%q want %q", ack2.RejectReason, transit.RejectReasonReplayNonce)
	}
}

func TestWriteMessageHandler_RejectsDisabledReplayWindowByDefault(t *testing.T) {
	t.Parallel()
	reg := newStubRegistry()
	chain := &stubChain{result: transit.HarnessWriteResult{Seq: 1}}
	_, err := transit.NewWriteMessageHandler(transit.WriteMessageHandlerConfig{
		Secret:       []byte(testWriteSecret),
		Router:       routerFor(chain, reg),
		NowMs:        func() int64 { return 10_000 },
		ReplayWindow: 0,
	})
	if err == nil {
		t.Fatal("ReplayWindow=0 without opt-out should fail")
	}
}

// TestWriteMessageHandler_ReplayWindowOptOutDisablesNonceCache confirms
// the test/dev escape hatch is explicit: only AllowReplayWindowDisabled
// permits ReplayWindow=0, and that path disables nonce replay tracking.
func TestWriteMessageHandler_ReplayWindowOptOutDisablesNonceCache(t *testing.T) {
	t.Parallel()
	reg := newStubRegistry()
	reg.put(actorreg.Record{ID: testWriteActor, Kind: actor.KindHuman})
	chain := &stubChain{result: transit.HarnessWriteResult{Seq: 1}}
	h, err := transit.NewWriteMessageHandler(transit.WriteMessageHandlerConfig{
		Secret:                    []byte(testWriteSecret),
		Router:                    routerFor(chain, reg),
		NowMs:                     func() int64 { return 10_000 },
		ReplayWindow:              0,
		AllowReplayWindowDisabled: true,
	})
	if err != nil {
		t.Fatalf("NewWriteMessageHandler with opt-out: %v", err)
	}

	body := newWriteMessageBody(t, nil, 9_900)
	if ack := h.Handle(context.Background(), body); !ack.Accepted {
		t.Fatalf("first: %+v", ack)
	}
	if ack := h.Handle(context.Background(), body); !ack.Accepted {
		t.Fatalf("replay should be accepted when opt-out disables cache: %+v", ack)
	}
}

// TestWriteMessageHandler_EmptyEnvelopeID covers R4-3: caller MUST
// supply envelope.id per L3 §1.8.1. The daemon edge rejects an empty
// id BEFORE invoking the harness chain so the contract violation
// surfaces with a specific reason rather than a less-specific Step 2
// envelope-shape reject downstream.
func TestWriteMessageHandler_EmptyEnvelopeID(t *testing.T) {
	t.Parallel()
	reg := newStubRegistry()
	reg.put(actorreg.Record{ID: testWriteActor, Kind: actor.KindHuman, DisplayName: "Alice"})
	chain := &stubChain{result: transit.HarnessWriteResult{Seq: 1}}
	h := mustNewWriteHandler(t, routerFor(chain, reg), defaultWriteReplayWindow)

	body := newWriteMessageBody(t, nil, 9_900)
	body.EnvelopePartial.ID = "" // wipe caller-supplied id

	ack := h.Handle(context.Background(), body)
	if ack.Accepted {
		t.Fatalf("empty envelope.id MUST reject; got %+v", ack)
	}
	if ack.RejectReason != transit.RejectReasonAuthFailed {
		t.Errorf("RejectReason=%q want %q (R4-3 caller-id missing)",
			ack.RejectReason, transit.RejectReasonAuthFailed)
	}
	if chain.lastEnv != nil {
		t.Error("harness chain MUST NOT see frames missing envelope.id (R4-3)")
	}
}

// TestWriteMessageHandler_PreservesCallerID covers R4-3: the daemon
// MUST forward the caller-supplied envelope.id unchanged into the
// harness chain (no canonical-hash regeneration). This is what makes
// L1 §2.3 Step 3 dedupe-on-retry observable from the caller's POV.
func TestWriteMessageHandler_PreservesCallerID(t *testing.T) {
	t.Parallel()
	reg := newStubRegistry()
	reg.put(actorreg.Record{ID: testWriteActor, Kind: actor.KindHuman, DisplayName: "Alice"})
	chain := &stubChain{result: transit.HarnessWriteResult{Seq: 7}}
	h := mustNewWriteHandler(t, routerFor(chain, reg), defaultWriteReplayWindow)

	const callerID = "msg-caller-supplied-abc123"
	body := newWriteMessageBody(t, nil, 9_900)
	body.EnvelopePartial.ID = callerID

	ack := h.Handle(context.Background(), body)
	if !ack.Accepted {
		t.Fatalf("Handle rejected: %+v", ack)
	}
	if chain.lastEnv == nil {
		t.Fatal("chain never invoked")
	}
	if string(chain.lastEnv.ID) != callerID {
		t.Errorf("daemon mutated envelope.id: got %q want %q (R4-3 invariant)",
			chain.lastEnv.ID, callerID)
	}
	if string(ack.MessageID) != callerID {
		t.Errorf("ack.MessageID=%q want caller id %q (R4-3 echo)",
			ack.MessageID, callerID)
	}
}
