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
	result, err := c.Submit(context.Background(), w, env, 5000, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequestID != env.ID {
		t.Fatalf("expected request id %q, got %q", env.ID, result.RequestID)
	}
	if !result.Ack.Accepted {
		t.Fatal("expected ack.accepted=true")
	}
	if result.Ack.Status != "accepted" {
		t.Fatalf("expected ack.status=accepted, got %q", result.Ack.Status)
	}
	if result.Ack.EstWaitMs != 5000 {
		t.Fatalf("expected ack.est_wait_ms=5000, got %d", result.Ack.EstWaitMs)
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
	_, err := c.Submit(context.Background(), w, env, 0, false)
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
	_, err := c.Submit(context.Background(), w, env, 0, true)
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
	_, err := c.Submit(context.Background(), w, env, 0, false)
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
		_, err := c.Submit(context.Background(), w, message.Envelope{ID: id, Kind: message.KindRequest}, 0, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	pending := c.Pending()
	if len(pending) != 2 {
		t.Fatalf("expected 2 pending, got %d", len(pending))
	}
}

func TestResolveFastPathWindow(t *testing.T) {
	tests := []struct {
		name           string
		typeTimeout    time.Duration
		defaultTimeout time.Duration
		waitUnbounded  bool
		want           time.Duration
	}{
		{
			name:           "bounded, type timeout > fast path",
			typeTimeout:    30 * time.Second,
			defaultTimeout: 30 * time.Second,
			waitUnbounded:  false,
			want:           callkit.FastPathWindow,
		},
		{
			name:           "bounded, type timeout < fast path",
			typeTimeout:    5 * time.Second,
			defaultTimeout: 30 * time.Second,
			waitUnbounded:  false,
			want:           5 * time.Second,
		},
		{
			name:           "unbounded uses type timeout",
			typeTimeout:    60 * time.Second,
			defaultTimeout: 30 * time.Second,
			waitUnbounded:  true,
			want:           60 * time.Second,
		},
		{
			name:           "zero type timeout uses default",
			typeTimeout:    0,
			defaultTimeout: 20 * time.Second,
			waitUnbounded:  true,
			want:           20 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := callkit.ResolveFastPathWindow(tt.typeTimeout, tt.defaultTimeout, tt.waitUnbounded)
			if got != tt.want {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAckResult(t *testing.T) {
	ack := callkit.AckDescriptor{
		RequestID: "req-ack",
		Accepted:  true,
		Status:    "accepted",
		EstWaitMs: 15000,
	}
	rv := callkit.AckResult("call_actor", ack)
	if rv.Name != "call_actor" {
		t.Fatalf("expected name call_actor, got %q", rv.Name)
	}
	if rv.Value["status"] != "accepted" {
		t.Fatalf("expected status=accepted, got %v", rv.Value["status"])
	}
	if rv.Value["accepted"] != true {
		t.Fatalf("expected accepted=true, got %v", rv.Value["accepted"])
	}
	if rv.Value["request_id"] != "req-ack" {
		t.Fatalf("expected request_id=req-ack, got %v", rv.Value["request_id"])
	}
}
