package fleet_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/server/fleet"
	"github.com/wanpengxie/ActOS/wire/computebus"
	"github.com/wanpengxie/ActOS/wire/placement"
)

const testChannelID = channel.ID("test-channel")

// stubWriter is a minimal harness.Writer for testing.
type stubWriter struct {
	mu      sync.Mutex
	writes  []*message.Envelope
	nextSeq int64
}

func (s *stubWriter) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextSeq++
	s.writes = append(s.writes, env)
	return harness.WriteResult{MessageID: env.ID, Seq: s.nextSeq}, nil
}

func (s *stubWriter) lastWrite() *message.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.writes) == 0 {
		return nil
	}
	return s.writes[len(s.writes)-1]
}

// TestAttach_AcceptAndDispatch tests the compute attach flow and dispatch.
func TestAttach_AcceptAndDispatch(t *testing.T) {
	w := &stubWriter{}
	plc := placement.New()
	var attached []computebus.AttachDeclaration
	var attachMu sync.Mutex

	flt := fleet.New(fleet.Config{
		Writer:    w,
		ChannelID: testChannelID,
		APIKey:    "test-key",
		Placement: plc,
		OnAttach: func(_ context.Context, _ channel.ID, decls []computebus.AttachDeclaration) error {
			attachMu.Lock()
			attached = decls
			attachMu.Unlock()
			return nil
		},
		OnDeath: func(_ context.Context, _ actor.ActorID) {},
	})

	srv := httptest.NewServer(http.HandlerFunc(flt.ServeWS))
	defer srv.Close()

	// Connect as compute.
	wsURL := "ws" + srv.URL[4:] // http -> ws
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = ws.Close() }()

	// Send attach request.
	attachReq := computebus.Frame{
		Type: computebus.FrameAttach,
		Attach: &computebus.AttachRequest{
			APIKey:    "test-key",
			ComputeID: "compute-1",
			Declarations: []computebus.AttachDeclaration{
				{ActorID: "agent:test", Kind: actor.KindAgent, Binding: actor.BindingEmbedded},
			},
		},
	}
	raw, _ := computebus.Encode(attachReq)
	if err := ws.WriteMessage(websocket.TextMessage, raw); err != nil {
		t.Fatalf("write attach: %v", err)
	}

	// Read attach reply.
	_, replyRaw, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	reply, err := computebus.Decode(replyRaw)
	if err != nil {
		t.Fatalf("decode reply: %v", err)
	}
	if reply.Type != computebus.FrameAttachReply || reply.Reply == nil || !reply.Reply.Accepted {
		t.Fatalf("attach not accepted: %+v", reply)
	}

	// Verify placement was registered.
	if cid, ok := plc.Lookup("agent:test"); !ok || cid != "compute-1" {
		t.Errorf("placement Lookup = (%q, %v), want (compute-1, true)", cid, ok)
	}

	// Verify onAttach callback.
	attachMu.Lock()
	if len(attached) != 1 || attached[0].ActorID != "agent:test" {
		t.Errorf("onAttach decls = %+v, want agent:test", attached)
	}
	attachMu.Unlock()

	// Dispatch to the attached compute.
	env := &message.Envelope{
		ID:        "dispatch-001",
		ChannelID: testChannelID,
		Kind:      message.KindRequest,
		Audience:  message.Audience{"agent:test"},
	}
	ok := flt.Dispatch("agent:test", env)
	if !ok {
		t.Fatal("Dispatch returned false for attached actor")
	}

	// Read the dispatched frame.
	_, dispRaw, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("read dispatch: %v", err)
	}
	disp, err := computebus.Decode(dispRaw)
	if err != nil {
		t.Fatalf("decode dispatch: %v", err)
	}
	if disp.Type != computebus.FrameDispatch || disp.Dispatch == nil {
		t.Fatalf("expected dispatch frame, got %+v", disp)
	}
	if disp.Dispatch.Target != "agent:test" {
		t.Errorf("dispatch target = %q, want agent:test", disp.Dispatch.Target)
	}
}

// TestAttach_BadAPIKey tests that a bad api-key is rejected.
func TestAttach_BadAPIKey(t *testing.T) {
	w := &stubWriter{}
	plc := placement.New()
	flt := fleet.New(fleet.Config{
		Writer:    w,
		ChannelID: testChannelID,
		APIKey:    "secret",
		Placement: plc,
	})

	srv := httptest.NewServer(http.HandlerFunc(flt.ServeWS))
	defer srv.Close()

	wsURL := "ws" + srv.URL[4:]
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = ws.Close() }()

	attachReq := computebus.Frame{
		Type: computebus.FrameAttach,
		Attach: &computebus.AttachRequest{
			APIKey:    "wrong-key",
			ComputeID: "compute-bad",
		},
	}
	raw, _ := computebus.Encode(attachReq)
	if err := ws.WriteMessage(websocket.TextMessage, raw); err != nil {
		t.Fatalf("write attach: %v", err)
	}

	_, replyRaw, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	reply, err := computebus.Decode(replyRaw)
	if err != nil {
		t.Fatalf("decode reply: %v", err)
	}
	if reply.Reply == nil || reply.Reply.Accepted {
		t.Fatal("expected rejection, got accepted")
	}
}

// TestEmit_WritesAndAcks tests the emit -> harness write -> ack flow.
func TestEmit_WritesAndAcks(t *testing.T) {
	w := &stubWriter{}
	plc := placement.New()
	flt := fleet.New(fleet.Config{
		Writer:    w,
		ChannelID: testChannelID,
		APIKey:    "",
		Placement: plc,
		OnAttach:  func(_ context.Context, _ channel.ID, _ []computebus.AttachDeclaration) error { return nil },
		OnDeath:   func(_ context.Context, _ actor.ActorID) {},
	})

	srv := httptest.NewServer(http.HandlerFunc(flt.ServeWS))
	defer srv.Close()

	wsURL := "ws" + srv.URL[4:]
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = ws.Close() }()

	// Attach.
	attachReq := computebus.Frame{
		Type: computebus.FrameAttach,
		Attach: &computebus.AttachRequest{
			ComputeID:    "compute-emit",
			Declarations: []computebus.AttachDeclaration{{ActorID: "agent:emitter", Kind: actor.KindAgent}},
		},
	}
	raw, _ := computebus.Encode(attachReq)
	_ = ws.WriteMessage(websocket.TextMessage, raw)
	_, _, _ = ws.ReadMessage() // attach reply

	// Emit.
	emitFrame := computebus.Frame{
		Type:   computebus.FrameEmit,
		EmitID: "emit-001",
		Emit: &computebus.EmitFrame{
			Source: "agent:emitter",
			Envelope: &message.Envelope{
				ID:        "resp-001",
				ChannelID: testChannelID,
				Kind:      message.KindResponse,
				Type:      "test.response",
				Sender:    message.Sender{Kind: actor.KindAgent, ID: "agent:emitter"},
			},
		},
	}
	raw, _ = computebus.Encode(emitFrame)
	if err := ws.WriteMessage(websocket.TextMessage, raw); err != nil {
		t.Fatalf("write emit: %v", err)
	}

	// Read ack.
	_, ackRaw, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}
	ack, err := computebus.Decode(ackRaw)
	if err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	if ack.Type != computebus.FrameEmitAck || ack.Ack == nil {
		t.Fatalf("expected emit_ack, got %+v", ack)
	}
	if ack.Ack.EmitID != "emit-001" {
		t.Errorf("ack EmitID = %q, want emit-001", ack.Ack.EmitID)
	}
	if ack.Ack.MessageID != "resp-001" {
		t.Errorf("ack MessageID = %q, want resp-001", ack.Ack.MessageID)
	}

	// Verify the envelope was written.
	last := w.lastWrite()
	if last == nil || last.ID != "resp-001" {
		t.Errorf("expected write of resp-001, got %v", last)
	}
}

// TestDeath_MaterialisesOnDeathFrame tests that a death frame triggers onDeath.
func TestDeath_MaterialisesOnDeathFrame(t *testing.T) {
	w := &stubWriter{}
	plc := placement.New()
	var deadActors []actor.ActorID
	var mu sync.Mutex

	flt := fleet.New(fleet.Config{
		Writer:    w,
		ChannelID: testChannelID,
		APIKey:    "",
		Placement: plc,
		OnAttach:  func(_ context.Context, _ channel.ID, _ []computebus.AttachDeclaration) error { return nil },
		OnDeath: func(_ context.Context, dead actor.ActorID) {
			mu.Lock()
			deadActors = append(deadActors, dead)
			mu.Unlock()
		},
	})

	srv := httptest.NewServer(http.HandlerFunc(flt.ServeWS))
	defer srv.Close()

	wsURL := "ws" + srv.URL[4:]
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = ws.Close() }()

	// Attach with one actor.
	attachReq := computebus.Frame{
		Type: computebus.FrameAttach,
		Attach: &computebus.AttachRequest{
			ComputeID:    "compute-death",
			Declarations: []computebus.AttachDeclaration{{ActorID: "agent:dying", Kind: actor.KindAgent}},
		},
	}
	raw, _ := computebus.Encode(attachReq)
	_ = ws.WriteMessage(websocket.TextMessage, raw)
	_, _, _ = ws.ReadMessage() // attach reply

	// Send death frame.
	deathFrame := computebus.Frame{
		Type:  computebus.FrameDeath,
		Death: &computebus.DeathFrame{Actor: "agent:dying", Cause: "panic"},
	}
	raw, _ = computebus.Encode(deathFrame)
	if err := ws.WriteMessage(websocket.TextMessage, raw); err != nil {
		t.Fatalf("write death: %v", err)
	}

	// Give the server time to process.
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if len(deadActors) != 1 || deadActors[0] != "agent:dying" {
		t.Errorf("deadActors = %v, want [agent:dying]", deadActors)
	}
	mu.Unlock()

	// Verify placement was cleaned.
	if _, ok := plc.Lookup("agent:dying"); ok {
		t.Error("dead actor still in placement")
	}
}

// TestDisconnect_BatchDeath tests that compute disconnect triggers batch death
// for all its hosted actors.
func TestDisconnect_BatchDeath(t *testing.T) {
	w := &stubWriter{}
	plc := placement.New()
	var deadActors []actor.ActorID
	var mu sync.Mutex

	flt := fleet.New(fleet.Config{
		Writer:    w,
		ChannelID: testChannelID,
		APIKey:    "",
		Placement: plc,
		OnAttach:  func(_ context.Context, _ channel.ID, _ []computebus.AttachDeclaration) error { return nil },
		OnDeath: func(_ context.Context, dead actor.ActorID) {
			mu.Lock()
			deadActors = append(deadActors, dead)
			mu.Unlock()
		},
	})

	srv := httptest.NewServer(http.HandlerFunc(flt.ServeWS))
	defer srv.Close()

	wsURL := "ws" + srv.URL[4:]
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	// Attach with two actors.
	attachReq := computebus.Frame{
		Type: computebus.FrameAttach,
		Attach: &computebus.AttachRequest{
			ComputeID: "compute-batch",
			Declarations: []computebus.AttachDeclaration{
				{ActorID: "agent:a", Kind: actor.KindAgent},
				{ActorID: "agent:b", Kind: actor.KindAgent},
			},
		},
	}
	raw, _ := computebus.Encode(attachReq)
	_ = ws.WriteMessage(websocket.TextMessage, raw)
	_, _, _ = ws.ReadMessage() // attach reply

	// Close the WS connection (simulate disconnect).
	_ = ws.Close()

	// Give time for disconnect processing.
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	if len(deadActors) != 2 {
		t.Errorf("deadActors count = %d, want 2", len(deadActors))
	}
	mu.Unlock()
}

// TestDispatch_UnknownActor tests that dispatch to an unknown actor returns false.
func TestDispatch_UnknownActor(t *testing.T) {
	w := &stubWriter{}
	plc := placement.New()
	flt := fleet.New(fleet.Config{
		Writer:    w,
		ChannelID: testChannelID,
		Placement: plc,
	})

	env := &message.Envelope{ID: "x", ChannelID: testChannelID}
	if flt.Dispatch("nonexistent", env) {
		t.Error("Dispatch returned true for unknown actor")
	}
}

// TestEmit_unmarshalled verifies the Emit ack carries the emit_id.
func TestEmit_Correlation(t *testing.T) {
	// Test that EmitAck.EmitID round-trips correctly through JSON.
	ack := computebus.EmitAck{
		EmitID:    "corr-123",
		MessageID: "msg-456",
	}
	b, err := json.Marshal(ack)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got computebus.EmitAck
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.EmitID != "corr-123" {
		t.Errorf("EmitID = %q, want corr-123", got.EmitID)
	}
}
