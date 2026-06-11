package platform_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

const closureTestChannelID = channel.ID("test-closure")

// ---------------------------------------------------------------------------
// Test actors
// ---------------------------------------------------------------------------

// panicOnReceive is a cell that panics on any envelope — simulates abnormal
// death so the runtime publishes the presence-down edge and the channelkit
// OnDown materialises receiver_unavailable (closure author #3).
type panicOnReceive struct{}

func (panicOnReceive) Receive(context.Context, *message.Envelope) error { panic("boom") }

// silentActor receives the request, arms a Caller (author #2 timer), and
// deliberately never responds. The caller-scoped timer fires and writes the
// unanswered_timeout terminal.
type silentActor struct {
	caller *behavior.Caller
}

func (a *silentActor) Receive(_ context.Context, env *message.Envelope) error {
	if env.Kind == message.KindRequest {
		a.caller.Arm(env)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// newClosureHome assembles a full platform.ChannelHome backed by a temp sqlite
// and returns it plus the channel stores for pre-registering actors.
func newClosureHome(t *testing.T) *platform.ChannelHome {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "closure.sqlite")
	ch, err := platform.NewChannelHome(platform.HomeConfig{
		ChannelID: closureTestChannelID,
		DBPath:    dbPath,
	})
	if err != nil {
		t.Fatalf("NewChannelHome: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close() })
	return ch
}

// registerActor seeds a membership row so the harness accepts envelopes from /
// addressed to this actor.
func registerActor(t *testing.T, ch *platform.ChannelHome, id actor.ActorID, kind actor.Kind) {
	t.Helper()
	err := ch.Membership().Insert(context.Background(), storespec.Record{
		ID: id, Kind: kind, CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("register actor %s: %v", id, err)
	}
}

// writeRequest writes a kind=request envelope through the gateway (= full
// commit pipeline: harness -> append -> notify; the delivery tap then delivers
// the committed row to its audience cells asynchronously).
func writeRequest(
	t *testing.T,
	ch *platform.ChannelHome,
	senderID actor.ActorID,
	senderKind actor.Kind,
	targetID actor.ActorID,
	reqType string,
	expiresAt *int64,
) message.ID {
	t.Helper()
	id := message.ID(uuid.NewString())
	env := &message.Envelope{
		ID:         id,
		TS:         time.Now().UnixMilli(),
		ChannelID:  closureTestChannelID,
		Sender:     message.Sender{Kind: senderKind, ID: senderID},
		Kind:       message.KindRequest,
		Type:       reqType,
		Payload:    json.RawMessage(`{}`),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{targetID},
		ExpiresAt:  expiresAt,
	}

	ctx := harness.CtxWithCaller(context.Background(), harness.CallerContext{
		ActorID:   senderID,
		ChannelID: closureTestChannelID,
	})
	res, err := ch.Gateway().SendMessage(ctx, env)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if !res.Accepted() {
		t.Fatalf("SendMessage rejected: %s (%s)", res.RejectReason, res.RejectDetail)
	}
	return id
}

// pollForTerminal waits up to timeout for a kind=response with the given
// parentID to appear in the channel log. Returns the terminal envelope or
// fails the test.
func pollForTerminal(t *testing.T, ch *platform.ChannelHome, parentID message.ID, timeout time.Duration) message.Envelope {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rows, err := ch.Gateway().ListMessages(ctx, 0, 1000)
		if err != nil {
			t.Fatalf("ListMessages: %v", err)
		}
		for _, row := range rows {
			if row.Envelope.Kind == message.KindResponse && row.Envelope.ParentID == parentID {
				return row.Envelope
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no terminal response for parent_id=%s within %v", parentID, timeout)
	return message.Envelope{} // unreachable
}

// ---------------------------------------------------------------------------
// Test 1: Author #3 — actor death -> receiver_unavailable
// ---------------------------------------------------------------------------

// TestClosure_Author3_ActorDeath_MaterialisesReceiverUnavailable is the
// platform-level integration test for closure author #3: a cell panics ->
// presence-down edge -> channelkit OnDown -> MaterialiseReceiverUnavailable
// writes a system-authored receiver_unavailable terminal into truth.
//
// The full commit pipeline is exercised: write -> harness -> sqlite append ->
// notify -> delivery tap delivers to cell -> cell panic -> death edge -> closure.
func TestClosure_Author3_ActorDeath_MaterialisesReceiverUnavailable(t *testing.T) {
	ch := newClosureHome(t)

	callerID := actor.ActorID("user:caller")
	workerID := actor.ActorID("agent:worker")

	// 1. Register both actors in membership.
	registerActor(t, ch, callerID, actor.KindHuman)
	registerActor(t, ch, workerID, actor.KindAgent)

	// 2. Spawn the panic actor cell in the runtime.
	ch.Runtime().Spawn(workerID, panicOnReceive{})

	// 3. Write a request addressed to the worker through the full pipeline.
	//    The delivery tap delivers it to the worker cell, which panics.
	reqID := writeRequest(t, ch, callerID, actor.KindHuman, workerID, "test.do", nil)

	// 4. Wait for the receiver_unavailable terminal to appear in truth.
	//    The chain: panic -> cell death -> publishDown -> OnDown ->
	//    MaterialiseReceiverUnavailable -> harness.Write -> sqlite append.
	term := pollForTerminal(t, ch, reqID, 5*time.Second)

	// 5. Verify the terminal.
	if term.Kind != message.KindResponse {
		t.Fatalf("terminal kind=%s, want response", term.Kind)
	}
	if term.ParentID != reqID {
		t.Fatalf("terminal parent_id=%s, want %s", term.ParentID, reqID)
	}
	if term.Sender.ID != actor.SystemActorID {
		t.Fatalf("terminal sender.id=%s, want %s (system substrate-death author)", term.Sender.ID, actor.SystemActorID)
	}
	if term.Sender.Kind != actor.KindSystem {
		t.Fatalf("terminal sender.kind=%s, want system", term.Sender.Kind)
	}

	// Verify payload carries status=failed + reason=receiver_unavailable.
	var payload struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(term.Payload, &payload); err != nil {
		t.Fatalf("unmarshal terminal payload: %v (raw=%s)", err, term.Payload)
	}
	if payload.Status != "failed" {
		t.Fatalf("terminal payload.status=%q, want failed", payload.Status)
	}
	if payload.Reason != string(message.TerminalReceiverUnavailable) {
		t.Fatalf("terminal payload.reason=%q, want receiver_unavailable", payload.Reason)
	}

	// Verify audience targets the original caller (response flows back to
	// whoever sent the request).
	if len(term.Audience) != 1 || term.Audience[0] != callerID {
		t.Fatalf("terminal audience=%v, want [%s]", term.Audience, callerID)
	}
}

// ---------------------------------------------------------------------------
// Test 2: Author #2 — caller timeout -> unanswered_timeout
// ---------------------------------------------------------------------------

// TestClosure_Author2_CallerTimeout_MaterialisesUnansweredTimeout is the
// platform-level integration test for closure author #2: a request carries
// ExpiresAt, the receiving actor never responds, the caller-scoped timer
// fires and writes an unanswered_timeout terminal into truth.
//
// The Caller is an actor-private behaviour primitive. This test wires a
// silentActor that arms the Caller on receipt but never sends a response.
// The timer's fireTimeout writes through the same harness pipeline.
func TestClosure_Author2_CallerTimeout_MaterialisesUnansweredTimeout(t *testing.T) {
	ch := newClosureHome(t)

	callerID := actor.ActorID("user:caller")
	workerID := actor.ActorID("agent:silent")

	// 1. Register both actors in membership.
	registerActor(t, ch, callerID, actor.KindHuman)
	registerActor(t, ch, workerID, actor.KindAgent)

	// 2. Build a silentActor with a Caller wired to the gateway writer. The
	//    Caller's fireTimeout will write the unanswered_timeout terminal
	//    through the harness. The sender for the terminal is the CALLER (the
	//    one who issued the request and owns the timer), matching the author#2
	//    contract: caller self-close.
	sa := &silentActor{
		caller: behavior.NewCaller(
			message.Sender{Kind: actor.KindHuman, ID: callerID},
			gatewayWriter(ch.Gateway()),
			time.Now,
		),
	}

	// 3. Spawn the silent actor cell.
	ch.Runtime().Spawn(workerID, sa)

	// 4. Write a request with a short ExpiresAt (now + 300ms).
	deadline := time.Now().Add(300 * time.Millisecond).UnixMilli()
	reqID := writeRequest(t, ch, callerID, actor.KindHuman, workerID, "test.ask", &deadline)

	// 5. Wait for the unanswered_timeout terminal. The Caller timer fires
	//    ~300ms after the request was armed, then writes through the harness.
	term := pollForTerminal(t, ch, reqID, 5*time.Second)

	// 6. Verify the terminal.
	if term.Kind != message.KindResponse {
		t.Fatalf("terminal kind=%s, want response", term.Kind)
	}
	if term.ParentID != reqID {
		t.Fatalf("terminal parent_id=%s, want %s", term.ParentID, reqID)
	}
	// Author #2 = the caller itself (caller self-close).
	if term.Sender.ID != callerID {
		t.Fatalf("terminal sender.id=%s, want %s (caller self-close)", term.Sender.ID, callerID)
	}

	var payload struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(term.Payload, &payload); err != nil {
		t.Fatalf("unmarshal terminal payload: %v (raw=%s)", err, term.Payload)
	}
	if payload.Status != "failed" {
		t.Fatalf("terminal payload.status=%q, want failed", payload.Status)
	}
	if payload.Reason != string(message.TerminalUnansweredTimeout) {
		t.Fatalf("terminal payload.reason=%q, want unanswered_timeout", payload.Reason)
	}
}

// gatewayWriter adapts *platform.Gateway (SendMessage) to harness.Writer.
type gwWriter struct{ gw *platform.Gateway }

func gatewayWriter(gw *platform.Gateway) harness.Writer { return &gwWriter{gw: gw} }

func (w *gwWriter) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	return w.gw.SendMessage(ctx, env)
}

