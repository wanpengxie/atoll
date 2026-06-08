package host

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/wire/computebus"
)

// --- test helpers ---

// echoActor responds by writing back via the writer passed at construction.
type echoActor struct {
	writer *UplinkWriter
}

func (e *echoActor) Receive(ctx context.Context, env *message.Envelope) error {
	_, _ = e.writer.Write(ctx, &message.Envelope{
		ID:        message.ID("resp-" + string(env.ID)),
		ChannelID: env.ChannelID,
		Kind:      message.KindResponse,
		Type:      env.Type,
		ParentID:  env.ID,
		Sender:    message.Sender{ID: "tool1", Kind: actor.KindTool},
		Audience:  message.Audience{env.Sender.ID},
	})
	return nil
}

// panicActor dies on the first envelope it receives.
type panicActor struct{}

func (panicActor) Receive(_ context.Context, _ *message.Envelope) error { panic("boom") }

// noopActor does nothing.
type noopActor struct{}

func (noopActor) Receive(_ context.Context, _ *message.Envelope) error { return nil }

// --- tests ---

// TestUplinkWriter_ForwardsToEmit proves UplinkWriter.Write sends an EmitFrame
// to the injected EmitFunc and returns the EmitAck as a WriteResult.
func TestUplinkWriter_ForwardsToEmit(t *testing.T) {
	var captured computebus.EmitFrame
	emit := func(_ context.Context, ef computebus.EmitFrame) (computebus.EmitAck, error) {
		captured = ef
		return computebus.EmitAck{MessageID: "msg-123"}, nil
	}

	w := NewUplinkWriter("tool1", emit)
	env := &message.Envelope{ID: "env-1", Kind: message.KindResponse}
	res, err := w.Write(context.Background(), env)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if res.MessageID != "msg-123" {
		t.Fatalf("MessageID = %q, want msg-123", res.MessageID)
	}
	if captured.Source != "tool1" {
		t.Fatalf("EmitFrame.Source = %q, want tool1", captured.Source)
	}
	if captured.Envelope != env {
		t.Fatal("EmitFrame.Envelope should be the same pointer")
	}
}

// TestUplinkWriter_RejectReason proves a reject reason from EmitAck surfaces
// in the WriteResult.
func TestUplinkWriter_RejectReason(t *testing.T) {
	emit := func(_ context.Context, _ computebus.EmitFrame) (computebus.EmitAck, error) {
		return computebus.EmitAck{RejectReason: "harness_terminal_duplicate"}, nil
	}
	w := NewUplinkWriter("tool1", emit)
	res, err := w.Write(context.Background(), &message.Envelope{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.RejectReason != "harness_terminal_duplicate" {
		t.Fatalf("RejectReason = %q, want harness_terminal_duplicate", res.RejectReason)
	}
}

// TestUplinkWriter_EmitError proves an Err string in EmitAck becomes a Go error.
func TestUplinkWriter_EmitError(t *testing.T) {
	emit := func(_ context.Context, _ computebus.EmitFrame) (computebus.EmitAck, error) {
		return computebus.EmitAck{Err: "store broken"}, nil
	}
	w := NewUplinkWriter("tool1", emit)
	_, err := w.Write(context.Background(), &message.Envelope{})
	if err == nil || err.Error() != "store broken" {
		t.Fatalf("expected error 'store broken', got %v", err)
	}
}

// TestUplinkWriter_EmitFuncError proves a transport error from EmitFunc
// propagates to Write.
func TestUplinkWriter_EmitFuncError(t *testing.T) {
	emit := func(_ context.Context, _ computebus.EmitFrame) (computebus.EmitAck, error) {
		return computebus.EmitAck{}, errors.New("ws closed")
	}
	w := NewUplinkWriter("tool1", emit)
	_, err := w.Write(context.Background(), &message.Envelope{})
	if err == nil || err.Error() != "ws closed" {
		t.Fatalf("expected 'ws closed', got %v", err)
	}
}

// TestHost_Dispatch proves inbound DispatchFrame routes to the hosted cell.
func TestHost_Dispatch(t *testing.T) {
	got := make(chan message.ID, 1)
	emit := func(_ context.Context, ef computebus.EmitFrame) (computebus.EmitAck, error) {
		got <- ef.Envelope.ID
		return computebus.EmitAck{MessageID: ef.Envelope.ID}, nil
	}

	h := New(emit, nil)
	defer h.Stop()

	w := h.Install("tool1", &echoActor{writer: NewUplinkWriter("tool1", emit)})
	_ = w // writer is for the actor

	err := h.Dispatch(computebus.DispatchFrame{
		Target: "tool1",
		Envelope: &message.Envelope{
			ID:        "req-1",
			ChannelID: "ch1",
			Kind:      message.KindRequest,
			Type:      "test.echo",
			Sender:    message.Sender{ID: "human1", Kind: actor.KindHuman},
			Audience:  message.Audience{"tool1"},
		},
	})
	if err != nil {
		t.Fatalf("Dispatch error: %v", err)
	}

	select {
	case id := <-got:
		if id != "resp-req-1" {
			t.Fatalf("emitted id = %q, want resp-req-1", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cell never processed the dispatched envelope")
	}
}

// TestHost_DeathPropagation proves that when a hosted cell panics, Host
// propagates the death UP via DeathFunc.
func TestHost_DeathPropagation(t *testing.T) {
	var mu sync.Mutex
	var deadActor actor.ActorID
	deaths := make(chan struct{}, 1)
	deathFn := func(a actor.ActorID, _ string) {
		mu.Lock()
		deadActor = a
		mu.Unlock()
		deaths <- struct{}{}
	}

	h := New(nil, deathFn)
	defer h.Stop()

	h.Install("doomed", panicActor{})

	// Deliver triggers the panic.
	_ = h.Dispatch(computebus.DispatchFrame{
		Target:   "doomed",
		Envelope: &message.Envelope{Kind: message.KindRequest, Type: "x.kill", ChannelID: "ch"},
	})

	select {
	case <-deaths:
		mu.Lock()
		got := deadActor
		mu.Unlock()
		if got != "doomed" {
			t.Fatalf("DeathFunc reported actor=%q, want doomed", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Host never propagated the death UP -- PresenceWatcher not wired")
	}
}

// TestHost_InstallMultiple proves multiple cells can coexist.
func TestHost_InstallMultiple(t *testing.T) {
	h := New(nil, nil)
	defer h.Stop()

	h.Install("a1", noopActor{})
	h.Install("a2", noopActor{})

	// Both should accept dispatch without error.
	for _, id := range []actor.ActorID{"a1", "a2"} {
		err := h.Dispatch(computebus.DispatchFrame{
			Target:   id,
			Envelope: &message.Envelope{Kind: message.KindEvent, Type: "test.ping", ChannelID: "ch"},
		})
		if err != nil {
			t.Fatalf("Dispatch to %s error: %v", id, err)
		}
	}
}
