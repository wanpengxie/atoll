package platform_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
)

const closureTestChannelID = channel.ID("test-closure")

// ---------------------------------------------------------------------------
// Test actors
// ---------------------------------------------------------------------------

// panicOnReceive is a cell that panics on any envelope — simulates abnormal
// death so the runtime publishes the down edge and the channelkit
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

// penCell is a no-op cell whose only purpose is to capture the welded Pen the
// substrate Mints for it at admission. In the sealed-pen world a write gate is
// reachable ONLY as an actor's own welded pen (no Home.Gate / no Minter outside
// platform), so a test that needs to write truth AS some actor spawns that actor
// and uses the pen the factory received — exactly the production admission path.
type penCell struct{ pen harness.Pen }

func (penCell) Receive(context.Context, *message.Envelope) error { return nil }

// spawnWithPen admits id as an embodiment-bearing cell and returns the Pen welded to
// (id, channelID). This is the only legitimate way a platform_test obtains a pen:
// through the same Spawn(factory) admission an agent/tool/human goes through.
func spawnWithPen(t *testing.T, ch *platform.Home, id actor.ActorID, kind actor.Kind) harness.Pen {
	t.Helper()
	if err := ch.Admit(context.Background(), id, kind); err != nil {
		t.Fatalf("admit %s: %v", id, err)
	}
	var pen harness.Pen
	if err := ch.Spawn(context.Background(), id, kind, platform.ActorFactory{Legacy: func(p harness.Pen) actorrt.Actor {
		pen = p
		return penCell{pen: p}
	}}); err != nil {
		t.Fatalf("spawn pen actor %s: %v", id, err)
	}
	return pen
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// newClosureHome assembles a full platform.Home backed by a temp sqlite.
func newClosureHome(t *testing.T) *platform.Home {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "closure.sqlite")
	ch, err := platform.Open(platform.HomeConfig{
		ChannelID: closureTestChannelID,
		DBPath:    dbPath,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close() })
	return ch
}

// registerActor seeds a membership row (Admit — the pure-membership primitive) so
// the harness accepts envelopes from / addressed to this actor without embodying a
// cell.
func registerActor(t *testing.T, ch *platform.Home, id actor.ActorID, kind actor.Kind) {
	t.Helper()
	if err := ch.Admit(context.Background(), id, kind); err != nil {
		t.Fatalf("register actor %s: %v", id, err)
	}
}

// writeRequest writes a kind=request envelope through the caller's welded pen (=
// full commit pipeline: harness -> append -> notify; the delivery tap then
// delivers the committed row to its audience cells asynchronously). Sender.ID /
// ChannelID are left EMPTY — the pen injects the welded (caller, channel); a non-
// empty self-report would be rejected fail-fast.
func writeRequest(
	t *testing.T,
	pen harness.Pen,
	targetID actor.ActorID,
	reqType string,
	expiresAt *int64,
) message.ID {
	t.Helper()
	id := message.ID(uuid.NewString())
	env := &message.Envelope{
		ID:         id,
		TS:         time.Now().UnixMilli(),
		Kind:       message.KindRequest,
		Type:       reqType,
		Payload:    json.RawMessage(`{}`),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{targetID},
		ExpiresAt:  expiresAt,
	}

	res, err := pen.Write(context.Background(), env)
	if err != nil {
		t.Fatalf("pen.Write: %v", err)
	}
	if !res.Accepted() {
		t.Fatalf("SendMessage rejected: %s (%s)", res.RejectReason, res.RejectDetail)
	}
	return id
}

// pollForTerminal waits up to timeout for a kind=response with the given
// parentID to appear in the channel log. Returns the terminal envelope or
// fails the test.
func pollForTerminal(t *testing.T, ch *platform.Home, parentID message.ID, timeout time.Duration) message.Envelope {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rows, err := ch.View().ReadAfterSeq(ctx, 0, 1000)
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
// down edge -> channelkit OnDown -> MaterialiseReceiverUnavailable
// writes a system-authored receiver_unavailable terminal into truth.
//
// The full commit pipeline is exercised: write -> harness -> sqlite append ->
// notify -> delivery tap delivers to cell -> cell panic -> death edge -> closure.
func TestClosure_Author3_ActorDeath_MaterialisesReceiverUnavailable(t *testing.T) {
	ch := newClosureHome(t)

	callerID := actor.ActorID("user:caller")
	workerID := actor.ActorID("agent:worker")

	// 1. Admit the caller as a pen-bearing cell (its welded pen writes the request)
	//    and register the worker in membership.
	callerPen := spawnWithPen(t, ch, callerID, actor.KindHuman)
	registerActor(t, ch, workerID, actor.KindAgent)

	// 2. Spawn the panic actor cell (membership already seeded above; Spawn
	//    reactivates + places the cell).
	if err := ch.Spawn(context.Background(), workerID, actor.KindAgent, platform.ActorFactory{Legacy: func(harness.Pen) actorrt.Actor {
		return panicOnReceive{}
	}}); err != nil {
		t.Fatalf("spawn worker cell: %v", err)
	}

	// 3. Write a request addressed to the worker through the full pipeline.
	//    The delivery tap delivers it to the worker cell, which panics.
	reqID := writeRequest(t, callerPen, workerID, "test.do", nil)

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

	// 1. Admit the caller as a pen-bearing cell. Its welded pen is BOTH the request
	//    writer AND the Caller's terminal author — author #2 is caller self-close,
	//    so the timeout terminal is committed under the caller's own welded
	//    identity (no sender passed; the pen welds it). This is exactly how a real
	//    caller actor holds its Caller primitive.
	callerPen := spawnWithPen(t, ch, callerID, actor.KindHuman)
	registerActor(t, ch, workerID, actor.KindAgent)

	// 2. Build a silentActor with a Caller wired to the CALLER's welded pen. The
	//    Caller's fireTimeout writes the unanswered_timeout terminal through that
	//    pen, so sender == callerID by construction (caller self-close).
	sa := &silentActor{
		caller: behavior.NewCaller(callerPen, time.Now, nil),
	}

	// 3. Spawn the silent actor cell (membership already seeded; Spawn places it).
	if err := ch.Spawn(context.Background(), workerID, actor.KindAgent, platform.ActorFactory{Legacy: func(harness.Pen) actorrt.Actor {
		return sa
	}}); err != nil {
		t.Fatalf("spawn silent cell: %v", err)
	}

	// 4. Write a request with a short ExpiresAt (now + 300ms).
	deadline := time.Now().Add(300 * time.Millisecond).UnixMilli()
	reqID := writeRequest(t, callerPen, workerID, "test.ask", &deadline)

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
