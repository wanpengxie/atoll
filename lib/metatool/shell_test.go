package metatool

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// shellNoopPen is a harness.Pen double that accepts every write — the
// correlator-mechanism tests never exercise the write door.
type shellNoopPen struct{}

func (shellNoopPen) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	return harness.WriteResult{MessageID: env.ID}, nil
}

// newTestShell builds a Shell wired to a no-op write seam for correlator
// mechanism tests (Deliver/Await/Pending in isolation; the outbound build +
// emit path is exercised by execute_test.go through the meta-tools).
func newTestShell() *Shell {
	return NewShell(ShellConfig{
		Pen:        shellNoopPen{},
		Clock:      func() time.Time { return time.UnixMilli(0) },
		EnvelopeID: func(nowMs int64) message.ID { return message.ID(fmt.Sprintf("req-%d", nowMs)) },
	})
}

// finalEnvelope builds a response envelope with status=completed payload,
// parented to the given id.
func finalEnvelope(parentID message.ID) *message.Envelope {
	payload, _ := json.Marshal(map[string]string{"status": "completed"})
	return &message.Envelope{
		ID:       message.ID("resp-" + parentID),
		Kind:     message.KindResponse,
		ParentID: parentID,
		Payload:  payload,
	}
}

// provisionalEnvelope builds a response envelope with a non-final status.
func provisionalEnvelope(parentID message.ID) *message.Envelope {
	payload, _ := json.Marshal(map[string]string{"status": "processing"})
	return &message.Envelope{
		ID:       message.ID("prov-" + parentID),
		Kind:     message.KindResponse,
		ParentID: parentID,
		Payload:  payload,
	}
}

func TestShell_RegisterAndDeliverFinal(t *testing.T) {
	s := newTestShell()
	id := message.ID("req-1")
	s.register(id, "actor:test", false)

	// expectsAwait=false and state=awaitNotStarted → default branch → not
	// consumed (nobody is actually awaiting).
	if consumed := s.Deliver(finalEnvelope(id)); consumed {
		t.Fatalf("expected not consumed for !expectsAwait with no Await, got consumed")
	}
}

func TestShell_RegisterWithExpectsAwaitAndDeliverFinal(t *testing.T) {
	s := newTestShell()
	id := message.ID("req-2")
	s.register(id, "actor:test", true) // expectsAwait=true → buffer for future Await

	if consumed := s.Deliver(finalEnvelope(id)); !consumed {
		t.Fatalf("expected consumed, got not consumed")
	}
}

func TestShell_DeliverProvisionalToActiveAwaiter(t *testing.T) {
	s := newTestShell()
	id := message.ID("req-3")
	s.register(id, "actor:test", true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Await(context.Background(), id, 2*time.Second)
	}()
	time.Sleep(50 * time.Millisecond)

	if consumed := s.Deliver(provisionalEnvelope(id)); !consumed {
		t.Fatalf("expected consumed for provisional to active awaiter, got not consumed")
	}

	// The provisional must NOT wake the Await goroutine; deliver a final to unblock.
	if consumed := s.Deliver(finalEnvelope(id)); !consumed {
		t.Fatalf("expected consumed for final to active awaiter, got not consumed")
	}
	<-done
}

func TestShell_DeliverWithoutRegister(t *testing.T) {
	s := newTestShell()
	if consumed := s.Deliver(finalEnvelope(message.ID("unknown"))); consumed {
		t.Fatalf("expected not consumed, got consumed")
	}
}

func TestShell_DeliverNilEnvelope(t *testing.T) {
	s := newTestShell()
	if consumed := s.Deliver(nil); consumed {
		t.Fatalf("expected not consumed for nil envelope, got consumed")
	}
}

func TestShell_RegisterAwaitDeliverFinal(t *testing.T) {
	s := newTestShell()
	id := message.ID("req-4")
	s.register(id, "actor:test", true)

	env := finalEnvelope(id)
	var (
		gotEnv *message.Envelope
		gotOK  bool
		gotErr error
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		gotEnv, gotOK, gotErr = s.Await(context.Background(), id, 2*time.Second)
	}()
	time.Sleep(50 * time.Millisecond)
	s.Deliver(env)
	<-done

	if gotErr != nil {
		t.Fatalf("unexpected error: %v", gotErr)
	}
	if !gotOK {
		t.Fatal("expected ok=true")
	}
	if gotEnv == nil || gotEnv.ID != env.ID {
		t.Fatalf("expected envelope ID %q, got %v", env.ID, gotEnv)
	}
}

func TestShell_AwaitTimeout(t *testing.T) {
	s := newTestShell()
	id := message.ID("req-5")
	s.register(id, "actor:test", true)

	env, ok, err := s.Await(context.Background(), id, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok || env != nil {
		t.Fatalf("expected ok=false/nil env on timeout, got ok=%v env=%v", ok, env)
	}
}

func TestShell_AwaitContextCancelled(t *testing.T) {
	s := newTestShell()
	id := message.ID("req-ctx")
	s.register(id, "actor:test", true)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	env, ok, err := s.Await(ctx, id, 5*time.Second)
	if err == nil {
		t.Fatal("expected context error")
	}
	if ok || env != nil {
		t.Fatalf("expected ok=false/nil env on cancel, got ok=%v env=%v", ok, env)
	}
}

func TestShell_AwaitZeroWindow(t *testing.T) {
	s := newTestShell()
	id := message.ID("req-zero")
	s.register(id, "actor:test", true)

	env, ok, err := s.Await(context.Background(), id, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok || env != nil {
		t.Fatalf("expected ok=false/nil env for zero window, got ok=%v env=%v", ok, env)
	}
}

func TestShell_AwaitUnregistered(t *testing.T) {
	s := newTestShell()
	env, ok, err := s.Await(context.Background(), message.ID("nope"), time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok || env != nil {
		t.Fatalf("expected ok=false/nil env for unregistered id, got ok=%v env=%v", ok, env)
	}
}

func TestShell_AbandonRemovesFuture(t *testing.T) {
	s := newTestShell()
	id := message.ID("req-6")
	s.register(id, "actor:test", true)
	if !s.InFlight(id) {
		t.Fatal("expected InFlight=true after register")
	}
	s.Abandon(id)
	if s.InFlight(id) {
		t.Fatal("expected InFlight=false after abandon")
	}
}

func TestShell_PendingReturnsRegisteredIDs(t *testing.T) {
	s := newTestShell()
	ids := []message.ID{"a", "b", "c"}
	for _, id := range ids {
		s.register(id, "actor:test", false)
	}
	pending := s.Pending()
	if len(pending) != 3 {
		t.Fatalf("expected 3 pending, got %d", len(pending))
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i] < pending[j] })
	for i, id := range ids {
		if pending[i] != id {
			t.Fatalf("pending[%d] = %q, want %q", i, pending[i], id)
		}
	}
}

func TestShell_PendingEmptyByDefault(t *testing.T) {
	s := newTestShell()
	if len(s.Pending()) != 0 {
		t.Fatal("expected empty pending list")
	}
}

func TestShell_RegisterDuplicateIsNoop(t *testing.T) {
	s := newTestShell()
	id := message.ID("dup")
	s.register(id, "actor:test", true)
	s.register(id, "actor:test", false) // should not overwrite
	if len(s.Pending()) != 1 {
		t.Fatal("expected exactly 1 pending after duplicate register")
	}
}

func TestShell_DeliverFinalBufferedBeforeAwait(t *testing.T) {
	s := newTestShell()
	id := message.ID("req-buf")
	s.register(id, "actor:test", true)

	if consumed := s.Deliver(finalEnvelope(id)); !consumed {
		t.Fatalf("expected consumed, got not consumed")
	}

	gotEnv, ok, err := s.Await(context.Background(), id, time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok || gotEnv == nil {
		t.Fatalf("expected buffered final, got ok=%v env=%v", ok, gotEnv)
	}
}

// TestShell_BufferedFinalNotStrandedByTimeout pins the timeout-vs-buffered-
// final race: when both the timer and a buffered final are ready, Await must
// ALWAYS return the final — never strand it as a ghost in-flight entry.
func TestShell_BufferedFinalNotStrandedByTimeout(t *testing.T) {
	for i := 0; i < 100; i++ {
		s := newTestShell()
		id := message.ID(fmt.Sprintf("req-race-%d", i))
		s.register(id, "actor:test", true)
		s.Deliver(finalEnvelope(id)) // buffered before Await parks

		env, ok, err := s.Await(context.Background(), id, time.Nanosecond)
		if err != nil {
			t.Fatalf("iter %d: err %v", i, err)
		}
		if !ok || env == nil {
			t.Fatalf("iter %d: buffered final stranded by timeout", i)
		}
		if s.InFlight(id) {
			t.Fatalf("iter %d: ghost in-flight entry survived", i)
		}
	}
}
