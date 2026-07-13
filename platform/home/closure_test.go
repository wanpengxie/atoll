package home_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/goleak"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/platform/internal/hostcommon"
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

// silentActor receives requests and deliberately never responds — the
// receiver half of the author#2 fixture (期12 S6: the old form armed a
// behavior.Caller here; the fast-path timer now lives in the CALLER's own
// actorbase callLedger, where production callers actually hold it).
type silentActor struct{}

func (silentActor) Receive(context.Context, *message.Envelope) error { return nil }

// penCell is a no-op cell whose only purpose is to capture the welded Pen the
// substrate Mints for it at admission. In the sealed-pen world a write gate is
// reachable ONLY as an actor's own welded pen (no Home.Gate / no Minter outside
// platform), so a test that needs to write truth AS some actor spawns that actor
// and uses the pen the factory received — exactly the production admission path.
type penCell struct{ pen harness.Pen }

func (penCell) Receive(context.Context, *message.Envelope) error { return nil }

// spawnWithPen admits id as an embodiment-bearing cell and returns the Pen welded to
// (id, channelID). This is the only legitimate way a home_test obtains a pen:
// through the same Spawn(factory) admission an agent/tool/human goes through.
func spawnWithPen(t *testing.T, ch *home.Home, id *actor.ActorID, kind actor.Kind) harness.Pen {
	t.Helper()
	var pen harness.Pen
	minted, err := home.SpawnForTest(ch, *id, kind, platform.ActorFactory{Legacy: func(p harness.Pen) actorrt.Actor {
		pen = p
		return penCell{pen: p}
	}})
	if err != nil {
		t.Fatalf("spawn pen actor %s: %v", *id, err)
	}
	*id = minted
	return pen
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// newClosureHome assembles a full home.Home backed by a temp sqlite.
func newClosureHome(t *testing.T) *home.Home {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "closure.sqlite")
	ch, err := home.Open(home.Config{
		ChannelID: closureTestChannelID,
		DBPath:    dbPath,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := ch.Close(); err != nil {
			t.Errorf("Home.Close: %v", err)
		}
		if err := goleak.Find(); err != nil {
			t.Errorf("post-close goroutines: %v", err)
		}
	})
	return ch
}

// registerActor seeds a membership row (Admit — the pure-membership primitive) so
// the harness accepts envelopes from / addressed to this actor without embodying a
// cell.
func registerActor(t *testing.T, ch *home.Home, id *actor.ActorID, kind actor.Kind) {
	t.Helper()
	minted, err := home.AdmitForTest(ch, strings.ReplaceAll(string(*id), ":", "-"), kind)
	if err != nil {
		t.Fatalf("register actor %s: %v", *id, err)
	}
	*id = minted
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
func pollForTerminal(t *testing.T, ch *home.Home, parentID message.ID, timeout time.Duration) message.Envelope {
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
// Test 1 (C7 新测② · 崩溃在册者不被 RU、由 sweepExpired 按 deadline 关)
// ---------------------------------------------------------------------------

// TestClosure_CrashedButRegistered_ClosedByDeadlineNotEdge locks the C7 拔根
// semantic change: a cell that crashes while still a REGISTERED member is NOT
// closed by the death edge — liveness absence is no longer a terminal-write
// dependency (a successor incarnation may yet be re-embodied). Its stranded
// request is closed only when its declared deadline passes, by the expiry reaper,
// with reason=unanswered_timeout (the writer moves from closure to expiry). The
// 快速失败延迟=TTL cost is asserted here.
//
// The full commit pipeline still runs: write -> harness -> sqlite append ->
// notify -> delivery tap delivers to cell -> cell panic -> death edge (skipped,
// worker still a member) -> deadline -> expiry reaper.
func TestClosure_CrashedButRegistered_ClosedByDeadlineNotEdge(t *testing.T) {
	// Fast ticker so the automatic reconcile sweep (which includes sweepExpired)
	// closes the request shortly after its short declared deadline.
	ch := newSweepingHome(t, 30*time.Millisecond)

	callerID := actor.ActorID("user:caller")
	workerID := actor.ActorID("agent:worker")

	// 1. Admit the caller as a pen-bearing cell and register the worker.
	callerPen := spawnWithPen(t, ch, &callerID, actor.KindHuman)
	registerActor(t, ch, &workerID, actor.KindAgent)

	// 2. Spawn the panic actor cell — it dies on delivery, firing the death edge,
	//    but the worker stays a registered member throughout (never deregistered).
	workerID, err := home.SpawnForTest(ch, workerID, actor.KindAgent, platform.ActorFactory{Legacy: func(harness.Pen) actorrt.Actor {
		return panicOnReceive{}
	}})
	if err != nil {
		t.Fatalf("spawn worker cell: %v", err)
	}

	// 3. Request with a SHORT declared deadline. Delivery -> panic -> death edge ->
	//    the closure predicate finds the worker still registered -> NO
	//    receiver_unavailable. Only the reaper closes it, at the deadline.
	exp := time.Now().UnixMilli() + 200
	reqID := writeRequest(t, callerPen, workerID, "test.do", &exp)

	// 4. The terminal that eventually appears must be the expiry reaper's
	//    unanswered_timeout — NEVER the death edge's receiver_unavailable (the
	//    per-request UNIQUE index means whichever wrote first is the only terminal,
	//    so a wrong edge-close would surface here).
	term := pollForTerminal(t, ch, reqID, 5*time.Second)

	if term.Kind != message.KindResponse {
		t.Fatalf("terminal kind=%s, want response", term.Kind)
	}
	if term.ParentID != reqID {
		t.Fatalf("terminal parent_id=%s, want %s", term.ParentID, reqID)
	}
	if term.Sender.ID != actor.SystemActorID {
		t.Fatalf("terminal sender.id=%s, want %s (system expiry author)", term.Sender.ID, actor.SystemActorID)
	}

	var payload struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(term.Payload, &payload); err != nil {
		t.Fatalf("unmarshal terminal payload: %v (raw=%s)", err, term.Payload)
	}
	if payload.Reason == string(message.TerminalReceiverUnavailable) {
		t.Fatal("崩溃在册者被 death 边沿误关 (receiver_unavailable) — C7 拔根回归")
	}
	if payload.Status != "failed" || payload.Reason != string(message.TerminalUnansweredTimeout) {
		t.Fatalf("terminal payload=%s, want failed/unanswered_timeout (deadline reaper)", term.Payload)
	}

	// Audience still targets the original caller (the reaper reflects the request).
	if len(term.Audience) != 1 || term.Audience[0] != callerID {
		t.Fatalf("terminal audience=%v, want [%s]", term.Audience, callerID)
	}
}

// ---------------------------------------------------------------------------
// Test 2: Author #2 — caller timeout -> unanswered_timeout
// ---------------------------------------------------------------------------

// TestClosure_Author2_CallerTimeout_MaterialisesUnansweredTimeout is the
// platform-level integration test for author#2's FAST PATH (期12 迁形): a
// LIVE caller's own engine (actorbase callLedger — where every production
// caller's timer actually lives; the old form borrowed the since-拆删
// behavior.Caller helper) times out an unanswered Call and self-closes with
// unanswered_timeout under the caller's own welded identity. The durable
// guarantee for a GONE caller is the expiry reaper's — asserted separately
// in TestExpiryReaper_SystemAuthoredAcrossRestart.
func TestClosure_Author2_CallerTimeout_MaterialisesUnansweredTimeout(t *testing.T) {
	ch := newClosureHome(t)

	callerID := actor.ActorID("agent:caller")
	workerID := actor.ActorID("agent:silent")

	// 1. The silent receiver: live, admitted, never answers.
	registerActor(t, ch, &workerID, actor.KindAgent)
	workerID, err := home.SpawnForTest(ch, workerID, actor.KindAgent, platform.ActorFactory{Legacy: func(harness.Pen) actorrt.Actor {
		return silentActor{}
	}})
	if err != nil {
		t.Fatalf("spawn silent cell: %v", err)
	}

	// 2. The caller: a REAL actorbase engine cell whose Proc issues one
	//    Sys.Call on a trigger event. Its callLedger arms the 300ms author#2
	//    timer (TimeoutResolver hook) and fires the terminal through the
	//    cell's own welded pen — caller self-close by construction.
	callerFactory := hostcommon.CapsFactory(func(caps actorcaps.Caps) actorrt.Actor {
		hooks := actorbase.Hooks{TimeoutResolver: func(actor.ActorID, string) (time.Duration, bool) {
			return 300 * time.Millisecond, true
		}}
		return actorbase.New(caps, hooks, actorbase.Def{
			Doc: "test caller: one Sys.Call per trigger event",
			New: func() (actorbase.Proc, error) {
				return func(sys actorbase.Sys) error {
					for {
						msg, err := sys.Recv()
						if err != nil {
							return nil
						}
						if msg.Type == "closure.go" {
							if _, cerr := sys.Call(workerID, "test.ask", map[string]any{}); cerr != nil {
								return cerr
							}
						}
					}
				}, nil
			},
		})
	})
	callerID, err = home.SpawnForTest(ch, callerID, actor.KindAgent, callerFactory)
	if err != nil {
		t.Fatalf("spawn caller cell: %v", err)
	}

	// 3. Trigger the Call with an event from a third pen.
	triggerID := actor.ActorID("user:trigger")
	triggerPen := spawnWithPen(t, ch, &triggerID, actor.KindHuman)
	trigEnv, err := behavior.BuildEvent(time.Now, behavior.EventSpec{
		Type: "closure.go", Audience: message.Audience{callerID},
	})
	if err != nil {
		t.Fatalf("build trigger: %v", err)
	}
	if res, werr := triggerPen.Write(context.Background(), trigEnv); werr != nil || !res.Accepted() {
		t.Fatalf("trigger write = (%+v, %v)", res, werr)
	}

	// 4. Find the caller's request in the log (its engine authored it).
	var reqID message.ID
	findDeadline := time.Now().Add(5 * time.Second)
	for reqID == "" {
		rows, rerr := ch.View().ReadAfterSeq(context.Background(), 0, 1000)
		if rerr != nil {
			t.Fatalf("ReadAfterSeq: %v", rerr)
		}
		for _, r := range rows {
			if r.Envelope.Kind == message.KindRequest && r.Envelope.Type == "test.ask" && r.Envelope.Sender.ID == callerID {
				reqID = r.Envelope.ID
			}
		}
		if time.Now().After(findDeadline) {
			t.Fatal("caller engine never authored the Call request")
		}
		if reqID == "" {
			time.Sleep(10 * time.Millisecond)
		}
	}

	// 5. Wait for the unanswered_timeout terminal — the caller engine's own
	//    author#2 timer fires ~300ms in and writes through the harness.
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
