package metatool

import (
	"context"
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/protocol/message"
)

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

func TestRegisterAndDeliverFinal(t *testing.T) {
	rc := NewRequestCorrelator()
	id := message.ID("req-1")
	rc.Register(id, false)

	env := finalEnvelope(id)
	disp := rc.Deliver(env)
	// expectsAwait=false and state=awaitNotStarted → default branch → NoActiveWaiter
	// because nobody is actually awaiting (expectsAwait=false).
	if disp != NoActiveWaiter {
		t.Fatalf("expected NoActiveWaiter for !expectsAwait with no Await, got %v", disp)
	}
}

func TestRegisterWithExpectsAwaitAndDeliverFinal(t *testing.T) {
	rc := NewRequestCorrelator()
	id := message.ID("req-2")
	rc.Register(id, true) // expectsAwait=true → buffer for future Await

	env := finalEnvelope(id)
	disp := rc.Deliver(env)
	if disp != DeliveredToWaiter {
		t.Fatalf("expected DeliveredToWaiter, got %v", disp)
	}
}

func TestDeliverProvisionalToActiveAwaiter(t *testing.T) {
	rc := NewRequestCorrelator()
	id := message.ID("req-3")
	rc.Register(id, true)

	// Start Await in a goroutine — it will set state to awaitActive.
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Window long enough that the provisional won't time out the await.
		rc.Await(context.Background(), id, 2*time.Second)
	}()
	// Give the goroutine time to enter Await (sets state to awaitActive).
	time.Sleep(50 * time.Millisecond)

	provEnv := provisionalEnvelope(id)
	disp := rc.Deliver(provEnv)
	if disp != DeliveredToWaiter {
		t.Fatalf("expected DeliveredToWaiter for provisional to active awaiter, got %v", disp)
	}

	// The provisional should NOT wake the Await goroutine (it swallows provisionals).
	// Deliver a final to unblock it.
	finalEnv := finalEnvelope(id)
	disp = rc.Deliver(finalEnv)
	if disp != DeliveredToWaiter {
		t.Fatalf("expected DeliveredToWaiter for final to active awaiter, got %v", disp)
	}
	<-done
}

func TestDeliverWithoutRegister(t *testing.T) {
	rc := NewRequestCorrelator()
	env := finalEnvelope(message.ID("unknown"))
	disp := rc.Deliver(env)
	if disp != NoActiveWaiter {
		t.Fatalf("expected NoActiveWaiter, got %v", disp)
	}
}

func TestDeliverNilEnvelope(t *testing.T) {
	rc := NewRequestCorrelator()
	disp := rc.Deliver(nil)
	if disp != NoActiveWaiter {
		t.Fatalf("expected NoActiveWaiter for nil envelope, got %v", disp)
	}
}

func TestRegisterAwaitDeliverFinal(t *testing.T) {
	rc := NewRequestCorrelator()
	id := message.ID("req-4")
	rc.Register(id, true)

	env := finalEnvelope(id)
	var (
		gotEnv *message.Envelope
		gotOK  bool
		gotErr error
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		gotEnv, gotOK, gotErr = rc.Await(context.Background(), id, 2*time.Second)
	}()
	// Give Await time to enter the select.
	time.Sleep(50 * time.Millisecond)
	rc.Deliver(env)
	<-done

	if gotErr != nil {
		t.Fatalf("unexpected error: %v", gotErr)
	}
	if !gotOK {
		t.Fatal("expected ok=true")
	}
	if gotEnv == nil {
		t.Fatal("expected non-nil envelope")
	}
	if gotEnv.ID != env.ID {
		t.Fatalf("expected envelope ID %q, got %q", env.ID, gotEnv.ID)
	}
}

func TestAwaitTimeout(t *testing.T) {
	rc := NewRequestCorrelator()
	id := message.ID("req-5")
	rc.Register(id, true)

	env, ok, err := rc.Await(context.Background(), id, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false on timeout")
	}
	if env != nil {
		t.Fatal("expected nil envelope on timeout")
	}
}

func TestAwaitContextCancelled(t *testing.T) {
	rc := NewRequestCorrelator()
	id := message.ID("req-ctx")
	rc.Register(id, true)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	env, ok, err := rc.Await(ctx, id, 5*time.Second)
	if err == nil {
		t.Fatal("expected context error")
	}
	if ok {
		t.Fatal("expected ok=false on context cancel")
	}
	if env != nil {
		t.Fatal("expected nil envelope on context cancel")
	}
}

func TestAwaitZeroWindow(t *testing.T) {
	rc := NewRequestCorrelator()
	id := message.ID("req-zero")
	rc.Register(id, true)

	env, ok, err := rc.Await(context.Background(), id, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false for zero window")
	}
	if env != nil {
		t.Fatal("expected nil envelope for zero window")
	}
}

func TestAwaitUnregistered(t *testing.T) {
	rc := NewRequestCorrelator()
	env, ok, err := rc.Await(context.Background(), message.ID("nope"), time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false for unregistered id")
	}
	if env != nil {
		t.Fatal("expected nil envelope for unregistered id")
	}
}

func TestCancelRemovesFuture(t *testing.T) {
	rc := NewRequestCorrelator()
	id := message.ID("req-6")
	rc.Register(id, true)
	if !rc.Registered(id) {
		t.Fatal("expected Registered=true after Register")
	}
	rc.Cancel(id)
	if rc.Registered(id) {
		t.Fatal("expected Registered=false after Cancel")
	}
}

func TestPendingReturnsRegisteredIDs(t *testing.T) {
	rc := NewRequestCorrelator()
	ids := []message.ID{"a", "b", "c"}
	for _, id := range ids {
		rc.Register(id, false)
	}
	pending := rc.Pending()
	if len(pending) != 3 {
		t.Fatalf("expected 3 pending, got %d", len(pending))
	}
	sort.Slice(pending, func(i, j int) bool {
		return pending[i] < pending[j]
	})
	for i, id := range ids {
		if pending[i] != id {
			t.Fatalf("pending[%d] = %q, want %q", i, pending[i], id)
		}
	}
}

func TestPendingEmptyByDefault(t *testing.T) {
	rc := NewRequestCorrelator()
	if len(rc.Pending()) != 0 {
		t.Fatal("expected empty pending list")
	}
}

func TestRegisterDuplicateIsNoop(t *testing.T) {
	rc := NewRequestCorrelator()
	id := message.ID("dup")
	rc.Register(id, true)
	rc.Register(id, false) // should not overwrite
	if len(rc.Pending()) != 1 {
		t.Fatal("expected exactly 1 pending after duplicate register")
	}
}

func TestDeliverFinalBufferedBeforeAwait(t *testing.T) {
	// Register with expectsAwait=true, deliver the final BEFORE Await starts.
	// The final should be buffered in the channel and returned when Await is called.
	rc := NewRequestCorrelator()
	id := message.ID("req-buf")
	rc.Register(id, true)

	env := finalEnvelope(id)
	disp := rc.Deliver(env)
	if disp != DeliveredToWaiter {
		t.Fatalf("expected DeliveredToWaiter, got %v", disp)
	}

	// Now Await should pick up the buffered final immediately.
	gotEnv, ok, err := rc.Await(context.Background(), id, time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true from buffered final")
	}
	if gotEnv == nil || gotEnv.ID != env.ID {
		t.Fatalf("expected buffered envelope, got %v", gotEnv)
	}
}
