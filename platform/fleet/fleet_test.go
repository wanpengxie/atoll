package fleet_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
	"github.com/wanpengxie/ActOS/platform/fleet"
	"github.com/wanpengxie/ActOS/platform/computebus"
)

const testChannelID = channel.ID("test-channel")

// --- stubs ---

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

// stubMembership is a minimal MembershipControlPlane for testing.
type stubMembership struct {
	mu   sync.Mutex
	adds []storespec.MemberActorAdd
}

func (s *stubMembership) Insert(_ context.Context, _ storespec.Record) error { return nil }
func (s *stubMembership) Deregister(_ context.Context, _ actor.ActorID, _ int64) error {
	return nil
}
func (s *stubMembership) ApplyMemberTransitions(_ context.Context, _ channel.ID, adds []storespec.MemberActorAdd, _ []storespec.MemberActorRemove) error {
	s.mu.Lock()
	s.adds = append(s.adds, adds...)
	s.mu.Unlock()
	return nil
}

func (s *stubMembership) getAdds() []storespec.MemberActorAdd {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]storespec.MemberActorAdd, len(s.adds))
	copy(cp, s.adds)
	return cp
}

// --- helpers ---

// newTestFleet creates a fleet with an actorrt.Runtime, stub writer, stub
// membership, and a presence watcher that records OnDown calls.
type testRig struct {
	fleet    *fleet.Fleet
	runtime  *actorrt.Runtime
	deliver  actorrt.Deliverer
	writer   *stubWriter
	membership *stubMembership

	mu         sync.Mutex
	downActors []actor.ActorID
}

func newTestRig(apiKey string) *testRig {
	rt, deliverer, _ := actorrt.New(actorrt.Config{
		Parent: context.Background(),
	})
	w := &stubWriter{}
	m := &stubMembership{}

	rig := &testRig{
		runtime:    rt,
		deliver:    deliverer,
		writer:     w,
		membership: m,
	}

	// Watch presence for OnDown events.
	rt.WatchPresence(presenceWatcherFunc(func(_ context.Context, id actor.ActorID, _ error) {
		rig.mu.Lock()
		rig.downActors = append(rig.downActors, id)
		rig.mu.Unlock()
	}))

	rig.fleet = fleet.New(fleet.Config{
		Writer:     w,
		Runtime:    rt,
		Membership: m,
		ChannelID:  testChannelID,
		APIKey:     apiKey,
	})
	return rig
}

func (r *testRig) getDownActors() []actor.ActorID {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]actor.ActorID, len(r.downActors))
	copy(cp, r.downActors)
	return cp
}

// presenceWatcherFunc adapts a function to actorrt.PresenceWatcher.
type presenceWatcherFunc func(ctx context.Context, id actor.ActorID, cause error)

func (f presenceWatcherFunc) OnDown(ctx context.Context, id actor.ActorID, cause error) {
	f(ctx, id, cause)
}

// dialFleet starts an httptest server and dials it as a WS client.
func dialFleet(t *testing.T, rig *testRig) (*websocket.Conn, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(rig.fleet.ServeWS))
	wsURL := "ws" + srv.URL[4:]
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		srv.Close()
		t.Fatalf("dial: %v", err)
	}
	return ws, srv
}

// attachCompute sends an AttachRequest and reads the AttachReply.
func attachCompute(t *testing.T, ws *websocket.Conn, computeID string, actors []computebus.AttachDeclaration) computebus.AttachReply {
	t.Helper()
	req := computebus.Frame{
		Type: computebus.FrameAttach,
		Attach: &computebus.AttachRequest{
			APIKey:       "test-key",
			ComputeID:    computeID,
			Declarations: actors,
		},
	}
	raw, _ := computebus.Encode(req)
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
	if reply.Type != computebus.FrameAttachReply || reply.Reply == nil {
		t.Fatalf("expected attach_reply, got %+v", reply)
	}
	return *reply.Reply
}

// --- tests ---

func TestAttach_Accepted(t *testing.T) {
	rig := newTestRig("test-key")
	ws, srv := dialFleet(t, rig)
	defer srv.Close()
	defer func() { _ = ws.Close() }()

	reply := attachCompute(t, ws, "compute-1", []computebus.AttachDeclaration{
		{ActorID: "agent:alpha", Kind: actor.KindAgent, Binding: actor.BindingEmbedded},
	})
	if !reply.Accepted {
		t.Fatalf("attach rejected: %s", reply.Reason)
	}
	if reply.ChannelID != testChannelID {
		t.Errorf("channel = %q, want %q", reply.ChannelID, testChannelID)
	}

	// Verify membership registration.
	adds := rig.membership.getAdds()
	if len(adds) != 1 || adds[0].ID != "agent:alpha" {
		t.Errorf("membership adds = %+v, want [agent:alpha]", adds)
	}

	// Verify the actor is hosted as a port in actorrt.
	if _, ok := rig.runtime.Stat("agent:alpha"); !ok {
		t.Error("agent:alpha not hosted in actorrt after attach")
	}
}

func TestAttach_BadAPIKey(t *testing.T) {
	rig := newTestRig("secret")
	ws, srv := dialFleet(t, rig)
	defer srv.Close()
	defer func() { _ = ws.Close() }()

	req := computebus.Frame{
		Type: computebus.FrameAttach,
		Attach: &computebus.AttachRequest{
			APIKey:    "wrong-key",
			ComputeID: "compute-bad",
		},
	}
	raw, _ := computebus.Encode(req)
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

func TestDispatch_VirtualPipeRelay(t *testing.T) {
	rig := newTestRig("test-key")
	ws, srv := dialFleet(t, rig)
	defer srv.Close()
	defer func() { _ = ws.Close() }()

	reply := attachCompute(t, ws, "compute-1", []computebus.AttachDeclaration{
		{ActorID: "agent:relay", Kind: actor.KindAgent},
	})
	if !reply.Accepted {
		t.Fatalf("attach rejected: %s", reply.Reason)
	}

	// Deliver an envelope to the actor via actorrt. The Deliverer enqueues it
	// into the port's sendq, the port writeLoop writes it as ipc.KindDeliver
	// to the net.Pipe, the relay goroutine reads it and translates to
	// FrameDispatch on WS.
	env := &message.Envelope{
		ID:        "dispatch-001",
		ChannelID: testChannelID,
		Kind:      message.KindRequest,
		Type:      "test.request",
		Audience:  message.Audience{"agent:relay"},
	}
	_, err := rig.deliver.Deliver([]actor.ActorID{"agent:relay"}, env)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}

	// Read the dispatched frame from WS.
	_ = ws.SetReadDeadline(time.Now().Add(2 * time.Second))
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
	if disp.Dispatch.Target != "agent:relay" {
		t.Errorf("dispatch target = %q, want agent:relay", disp.Dispatch.Target)
	}
	if disp.Dispatch.Envelope == nil || disp.Dispatch.Envelope.ID != "dispatch-001" {
		t.Errorf("dispatch envelope ID mismatch: %+v", disp.Dispatch.Envelope)
	}
}

func TestEmit_WritesAndAcks(t *testing.T) {
	rig := newTestRig("")
	ws, srv := dialFleet(t, rig)
	defer srv.Close()
	defer func() { _ = ws.Close() }()

	reply := attachCompute(t, ws, "compute-emit", []computebus.AttachDeclaration{
		{ActorID: "agent:emitter", Kind: actor.KindAgent},
	})
	if !reply.Accepted {
		t.Fatalf("attach rejected: %s", reply.Reason)
	}

	// Send an emit frame.
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
	raw, _ := computebus.Encode(emitFrame)
	if err := ws.WriteMessage(websocket.TextMessage, raw); err != nil {
		t.Fatalf("write emit: %v", err)
	}

	// Read ack.
	_ = ws.SetReadDeadline(time.Now().Add(2 * time.Second))
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

	// Verify the envelope was written to truth.
	last := rig.writer.lastWrite()
	if last == nil || last.ID != "resp-001" {
		t.Errorf("expected write of resp-001, got %v", last)
	}
}

func TestDeath_PipeCloseTriggersOnDown(t *testing.T) {
	rig := newTestRig("")
	ws, srv := dialFleet(t, rig)
	defer srv.Close()
	defer func() { _ = ws.Close() }()

	reply := attachCompute(t, ws, "compute-death", []computebus.AttachDeclaration{
		{ActorID: "agent:dying", Kind: actor.KindAgent},
	})
	if !reply.Accepted {
		t.Fatalf("attach rejected: %s", reply.Reason)
	}

	// Verify the actor is hosted.
	if _, ok := rig.runtime.Stat("agent:dying"); !ok {
		t.Fatal("agent:dying not hosted after attach")
	}

	// Send death frame.
	deathFrame := computebus.Frame{
		Type:  computebus.FrameDeath,
		Death: &computebus.DeathFrame{Actor: "agent:dying", Cause: "panic"},
	}
	raw, _ := computebus.Encode(deathFrame)
	if err := ws.WriteMessage(websocket.TextMessage, raw); err != nil {
		t.Fatalf("write death: %v", err)
	}

	// Wait for OnDown to fire (pipe close -> port EOF -> die -> publishDown).
	deadline := time.After(2 * time.Second)
	for {
		down := rig.getDownActors()
		for _, id := range down {
			if id == "agent:dying" {
				// Verify the actor is no longer hosted.
				if _, ok := rig.runtime.Stat("agent:dying"); ok {
					t.Error("agent:dying still hosted after death")
				}
				return
			}
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for OnDown(agent:dying)")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestDisconnect_BatchOnDown(t *testing.T) {
	rig := newTestRig("")
	ws, srv := dialFleet(t, rig)
	defer srv.Close()

	reply := attachCompute(t, ws, "compute-batch", []computebus.AttachDeclaration{
		{ActorID: "agent:a", Kind: actor.KindAgent},
		{ActorID: "agent:b", Kind: actor.KindAgent},
	})
	if !reply.Accepted {
		t.Fatalf("attach rejected: %s", reply.Reason)
	}

	// Both actors should be hosted.
	for _, id := range []actor.ActorID{"agent:a", "agent:b"} {
		if _, ok := rig.runtime.Stat(id); !ok {
			t.Fatalf("%s not hosted after attach", id)
		}
	}

	// Close WS (simulate disconnect).
	_ = ws.Close()

	// Wait for both OnDown events.
	deadline := time.After(2 * time.Second)
	for {
		down := rig.getDownActors()
		if containsAll(down, "agent:a", "agent:b") {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for batch OnDown; got %v", down)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestMultipleActors_IndependentDelivery(t *testing.T) {
	rig := newTestRig("")
	ws, srv := dialFleet(t, rig)
	defer srv.Close()
	defer func() { _ = ws.Close() }()

	reply := attachCompute(t, ws, "compute-multi", []computebus.AttachDeclaration{
		{ActorID: "agent:x", Kind: actor.KindAgent},
		{ActorID: "agent:y", Kind: actor.KindAgent},
	})
	if !reply.Accepted {
		t.Fatalf("attach rejected: %s", reply.Reason)
	}

	// Deliver to each actor independently.
	for _, target := range []actor.ActorID{"agent:x", "agent:y"} {
		env := &message.Envelope{
			ID:        message.ID("msg-for-" + string(target)),
			ChannelID: testChannelID,
			Kind:      message.KindRequest,
			Audience:  message.Audience{target},
		}
		_, err := rig.deliver.Deliver([]actor.ActorID{target}, env)
		if err != nil {
			t.Fatalf("deliver to %s: %v", target, err)
		}
	}

	// Read both dispatch frames.
	_ = ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	seen := map[actor.ActorID]bool{}
	for i := 0; i < 2; i++ {
		_, raw, err := ws.ReadMessage()
		if err != nil {
			t.Fatalf("read dispatch %d: %v", i, err)
		}
		fr, err := computebus.Decode(raw)
		if err != nil {
			t.Fatalf("decode dispatch %d: %v", i, err)
		}
		if fr.Type != computebus.FrameDispatch || fr.Dispatch == nil {
			t.Fatalf("expected dispatch, got %+v", fr)
		}
		seen[fr.Dispatch.Target] = true
	}
	if !seen["agent:x"] || !seen["agent:y"] {
		t.Errorf("expected dispatches to both actors, got %v", seen)
	}
}

func TestNoAPIKey_AnyKeyAccepted(t *testing.T) {
	rig := newTestRig("") // no api-key enforcement
	ws, srv := dialFleet(t, rig)
	defer srv.Close()
	defer func() { _ = ws.Close() }()

	// Attach with a random key should be accepted.
	req := computebus.Frame{
		Type: computebus.FrameAttach,
		Attach: &computebus.AttachRequest{
			APIKey:    "anything",
			ComputeID: "compute-any",
		},
	}
	raw, _ := computebus.Encode(req)
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
	if reply.Reply == nil || !reply.Reply.Accepted {
		t.Fatal("expected accepted when no api-key configured")
	}
}

// containsAll checks that ids contains all of the wanted actor ids.
func containsAll(ids []actor.ActorID, wanted ...actor.ActorID) bool {
	set := make(map[actor.ActorID]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	for _, w := range wanted {
		if !set[w] {
			return false
		}
	}
	return true
}
