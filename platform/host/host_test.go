package host

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// --- test helpers ---

// recordingWriter captures every envelope a cell emits (the cell's pen in
// production is the link's ipc.RemoteWriter; here we capture in memory).
type recordingWriter struct {
	mu  sync.Mutex
	out []message.Envelope
}

func (w *recordingWriter) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	w.mu.Lock()
	w.out = append(w.out, *env)
	w.mu.Unlock()
	return harness.WriteResult{MessageID: env.ID}, nil
}

func (w *recordingWriter) written() []message.Envelope {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]message.Envelope, len(w.out))
	copy(out, w.out)
	return out
}

// echoActor responds by writing back via the injected writer.
type echoActor struct {
	writer harness.Writer
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

// TestHost_Dispatch proves an inbound envelope routes to the hosted cell's
// mailbox and the cell's emit reaches its injected writer.
func TestHost_Dispatch(t *testing.T) {
	w := &recordingWriter{}
	h := New()
	defer h.Stop()

	h.Install("tool1", &echoActor{writer: w}, nil)

	err := h.Dispatch("tool1", &message.Envelope{
		ID:        "req-1",
		ChannelID: "ch1",
		Kind:      message.KindRequest,
		Type:      "test.echo",
		Sender:    message.Sender{ID: "human1", Kind: actor.KindHuman},
		Audience:  message.Audience{"tool1"},
	})
	if err != nil {
		t.Fatalf("Dispatch error: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		got := w.written()
		if len(got) >= 1 {
			if got[0].ID != "resp-req-1" {
				t.Fatalf("emitted id = %q, want resp-req-1", got[0].ID)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("cell never processed the dispatched envelope")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestHost_DeathPropagation proves a hosted cell's abnormal death fires the
// installed downHandler (the link layer closes that actor's stream UP).
func TestHost_DeathPropagation(t *testing.T) {
	var mu sync.Mutex
	var cause string
	deaths := make(chan struct{}, 1)

	h := New()
	defer h.Stop()

	h.Install("doomed", panicActor{}, func(c string) {
		mu.Lock()
		cause = c
		mu.Unlock()
		deaths <- struct{}{}
	})

	_ = h.Dispatch("doomed", &message.Envelope{Kind: message.KindRequest, Type: "x.kill", ChannelID: "ch"})

	select {
	case <-deaths:
		mu.Lock()
		got := cause
		mu.Unlock()
		if got == "" {
			t.Fatal("downHandler received empty cause for a panicking cell")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Host never fired the downHandler -- PresenceWatcher not wired")
	}
}

// TestHost_InstallMultiple proves multiple cells coexist and each accepts
// dispatch independently.
func TestHost_InstallMultiple(t *testing.T) {
	h := New()
	defer h.Stop()

	h.Install("a1", noopActor{}, nil)
	h.Install("a2", noopActor{}, nil)

	for _, id := range []actor.ActorID{"a1", "a2"} {
		err := h.Dispatch(id, &message.Envelope{Kind: message.KindEvent, Type: "test.ping", ChannelID: "ch"})
		if err != nil {
			t.Fatalf("Dispatch to %s error: %v", id, err)
		}
	}
}
