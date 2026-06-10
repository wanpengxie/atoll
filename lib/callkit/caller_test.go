package callkit_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/lib/callkit"
	"github.com/wanpengxie/ActOS/protocol/message"
)

// stubWriter is a minimal IPCWriter for testing.
type stubWriter struct {
	written []message.Envelope
	err     error
}

func (s *stubWriter) WriteEnvelope(_ context.Context, env message.Envelope) error {
	if s.err != nil {
		return s.err
	}
	s.written = append(s.written, env)
	return nil
}

func TestSubmitRegistersFutureAndWritesEnvelope(t *testing.T) {
	c := callkit.NewClient()
	w := &stubWriter{}
	env := message.Envelope{
		ID:   "req-submit-1",
		Kind: message.KindRequest,
	}
	if err := c.Submit(context.Background(), w, env, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(w.written) != 1 {
		t.Fatalf("expected 1 written envelope, got %d", len(w.written))
	}
	if w.written[0].ID != env.ID {
		t.Fatalf("expected written envelope ID %q, got %q", env.ID, w.written[0].ID)
	}
	// Future should be registered.
	if !c.Futures.Registered(env.ID) {
		t.Fatal("expected future to be registered after Submit")
	}
}

func TestSubmitWriteErrorCancelsFuture(t *testing.T) {
	c := callkit.NewClient()
	writeErr := errors.New("write failed")
	w := &stubWriter{err: writeErr}
	env := message.Envelope{
		ID:   "req-submit-err",
		Kind: message.KindRequest,
	}
	err := c.Submit(context.Background(), w, env, false)
	if !errors.Is(err, writeErr) {
		t.Fatalf("expected write error, got %v", err)
	}
	if c.Futures.Registered(env.ID) {
		t.Fatal("expected future to be cancelled after write error")
	}
}

func TestCallerAwaitReturnsFinal(t *testing.T) {
	c := callkit.NewClient()
	w := &stubWriter{}
	env := message.Envelope{
		ID:   "req-await-1",
		Kind: message.KindRequest,
	}
	err := c.Submit(context.Background(), w, env, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Deliver a final response in a goroutine.
	respPayload, _ := json.Marshal(map[string]string{"status": "completed"})
	respEnv := &message.Envelope{
		ID:       "resp-await-1",
		Kind:     message.KindResponse,
		ParentID: env.ID,
		Payload:  respPayload,
	}

	done := make(chan struct{})
	var gotEnv *message.Envelope
	var gotOK bool
	go func() {
		defer close(done)
		gotEnv, gotOK, _ = c.Await(context.Background(), env.ID, 2*time.Second)
	}()
	time.Sleep(50 * time.Millisecond)
	c.Deliver(respEnv)
	<-done

	if !gotOK {
		t.Fatal("expected ok=true")
	}
	if gotEnv == nil || gotEnv.ID != respEnv.ID {
		t.Fatal("expected the delivered response envelope")
	}
}

func TestCallerAbandonRemovesFuture(t *testing.T) {
	c := callkit.NewClient()
	w := &stubWriter{}
	env := message.Envelope{
		ID:   "req-abandon",
		Kind: message.KindRequest,
	}
	err := c.Submit(context.Background(), w, env, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c.Abandon(env.ID)
	if c.Futures.Registered(env.ID) {
		t.Fatal("expected future to be removed after Abandon")
	}
}

func TestCallerPending(t *testing.T) {
	c := callkit.NewClient()
	w := &stubWriter{}
	for _, id := range []message.ID{"a", "b"} {
		if err := c.Submit(context.Background(), w, message.Envelope{ID: id, Kind: message.KindRequest}, false); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	pending := c.Pending()
	if len(pending) != 2 {
		t.Fatalf("expected 2 pending, got %d", len(pending))
	}
}
