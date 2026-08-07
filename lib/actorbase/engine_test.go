package actorbase

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// --- fakes: a stub Caps built entirely from this package + protocol/runtime
// leaf vocabulary — zero platform dependency (spec DoD⑥). ------------------

// fakePen is a harness.Pen double: it welds a fixed self identity onto every
// written envelope (mirroring boundPen's fail-fast weld) and records every
// write for assertions.
type fakePen struct {
	mu      sync.Mutex
	self    actor.ActorID
	written []*message.Envelope
	reject  harness.HarnessRejectReason // when set, every Write is rejected with this reason
}

func (p *fakePen) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.reject != "" {
		// A rejected write still names the message it judged — the harness does
		// (a terminal duplicate reports the id of the terminal already in truth),
		// and behavior.Respond's idempotent absorption hands that id back to its
		// caller as the write's receipt.
		return harness.WriteResult{MessageID: env.ID, RejectReason: p.reject}, nil
	}
	env.Sender.ID = p.self
	p.written = append(p.written, env)
	return harness.WriteResult{MessageID: env.ID}, nil
}

func (p *fakePen) last() *message.Envelope {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.written) == 0 {
		return nil
	}
	return p.written[len(p.written)-1]
}

func (p *fakePen) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.written)
}

// fakeAccess is an accessdoor.ResourceAccessHandle double never meaningfully
// exercised by these tests (State()/Resource() are thin pass-throughs;
// covering the pass-through shape is enough here). It satisfies the WIDE
// resource face so the same value can back both engine.access (Access field,
// needs Create/Stat/List) and engine.state (State field, only ever needs
// Invoke — a wider double is harmless there, Go interfaces are structural).
type fakeAccess struct{}

func (fakeAccess) Invoke(_ context.Context, op access.Operation, id resource.ResourceID, args []byte) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{Value: args}, nil
}

func (fakeAccess) Create(_ context.Context, id resource.ResourceID, spec resourcespec.CreateSpec, initial []byte) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{Value: initial}, nil
}

func (fakeAccess) Stat(_ context.Context, id resource.ResourceID) (accessdoor.StatResult, error) {
	return accessdoor.StatResult{}, nil
}

func (fakeAccess) List(_ context.Context, q accessdoor.ListQuery) (accessdoor.ListPage, error) {
	return accessdoor.ListPage{}, nil
}
func (fakeAccess) Open(context.Context, resource.ResourceID, access.Operation) (accessdoor.FileAccess, accessdoor.Outcome, error) {
	return accessdoor.FileAccess{}, accessdoor.Outcome{}, accessdoor.ErrFileCapabilityUnavailable
}
func (fakeAccess) Redeem(context.Context, accessdoor.FileRoute) (accessdoor.FileAccess, error) {
	return accessdoor.FileAccess{}, accessdoor.ErrFileCapabilityUnavailable
}

func TestServerResourceFaceRejectsUnavailableFileByteCapability(t *testing.T) {
	// A server-hosted actor still receives the uniform ResourceHandle surface,
	// but its local bound handle intentionally has no FileOpener capability.
	// The call must fail honestly at the capability boundary, not disappear
	// from the API or panic through a nil arm.
	adapter := resourceAdapter{h: fakeAccess{}, ctx: context.Background}
	if _, _, err := adapter.Open("file:remote-only", access.OpRead); !errors.Is(err, accessdoor.ErrFileCapabilityUnavailable) {
		t.Fatalf("server file Open err=%v, want capability unavailable", err)
	}
}

// fakeSchedule is a schedule.ScheduleHandle double: Schedule/Cancel are
// recorded, never actually fired (the engine's own timers are what these
// tests exercise, not the schedule arm).
type fakeSchedule struct{}

func (fakeSchedule) Schedule(_ context.Context, _ schedule.ScheduleReq) (schedule.TimerID, error) {
	return "timer-1", nil
}
func (fakeSchedule) Cancel(_ context.Context, _ schedule.TimerID) error { return nil }
func (fakeSchedule) Ack(_ context.Context, _ schedule.TimerID) error    { return nil }

type failingAckSchedule struct {
	mu    sync.Mutex
	calls []schedule.TimerID
	err   error
}

func (*failingAckSchedule) Schedule(context.Context, schedule.ScheduleReq) (schedule.TimerID, error) {
	return "", nil
}
func (*failingAckSchedule) Cancel(context.Context, schedule.TimerID) error { return nil }
func (s *failingAckSchedule) Ack(_ context.Context, id schedule.TimerID) error {
	s.mu.Lock()
	s.calls = append(s.calls, id)
	s.mu.Unlock()
	return s.err
}

// fakeSpawn is an actorcaps.LifecycleHandle double.
type fakeSpawn struct{}

func (fakeSpawn) Fork(_ context.Context, _ message.ID, spec actorcaps.ForkSpec) (actor.ActorID, error) {
	return actor.ActorID("child/" + spec.NameHint), nil
}
func (fakeSpawn) EndSelf(context.Context, actorcaps.EndSelfRequest) error {
	return nil
}

// fakeActorContext is an actorrt.ActorContext double.
type fakeActorContext struct {
	self actor.ActorID

	mu   sync.Mutex
	pubs []struct {
		kind actorrt.ObsKind
		val  actorrt.ObsValue
	}
}

func (f *fakeActorContext) Self() actor.ActorID { return f.self }
func (f *fakeActorContext) PublishObs(kind actorrt.ObsKind, val actorrt.ObsValue) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pubs = append(f.pubs, struct {
		kind actorrt.ObsKind
		val  actorrt.ObsValue
	}{kind, val})
}
func (f *fakeActorContext) obsCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pubs)
}

// obsKindCounts tallies published obs by kind — the precise assertion F5/F8
// tests need (a bare non-zero count cannot tell reject_lane_overflow from
// closure_fault from a stale-delivery drop).
func (f *fakeActorContext) obsKindCounts() map[actorrt.ObsKind]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	counts := map[actorrt.ObsKind]int{}
	for _, p := range f.pubs {
		counts[p.kind]++
	}
	return counts
}

// newTestEngine builds an *engine directly (bypassing New) so tests can
// shrink the ledger/queue capacities and inject a deterministic clock —
// same-package whitebox construction, zero platform import.
func newTestEngine(t *testing.T, pen *fakePen, hooks Hooks, serveCap, queueCap int) *engine {
	t.Helper()
	e := &engine{
		pen:       pen,
		access:    fakeAccess{},
		state:     fakeAccess{},
		sched:     fakeSchedule{},
		lifecycle: fakeSpawn{},
		hooks:     hooks,
		clockFn:   time.Now,
		queueCap:  queueCap,
	}
	e.serve = newServeLedger(e.life, serveCap)
	e.call = newCallLedger(e.life, e.pen, e.clockFn, hooks, e.closureFault)
	e.workQ = newWorkDeque(queueCap)
	e.rejectQ = make(chan *message.Envelope, queueCap)
	e.rejectStop = make(chan struct{})
	return e
}

func newRequestEnv(id message.ID, expiresInMs int64) *message.Envelope {
	env := &message.Envelope{ID: id, Kind: message.KindRequest, Type: "test.req"}
	if expiresInMs >= 0 {
		exp := time.Now().Add(time.Duration(expiresInMs) * time.Millisecond).UnixMilli()
		env.ExpiresAt = &exp
	}
	return env
}

func TestAutomaticTimerAckFailureIsObservedAndLeftRetryable(t *testing.T) {
	e := newTestEngine(t, &fakePen{self: "actor:test"}, Hooks{}, 8, 8)
	sched := &failingAckSchedule{err: errors.New("transient ack failure")}
	actx := &fakeActorContext{self: "actor:test"}
	e.sched = sched
	e.actorCtx = actx
	e.pendingTimer = "timer:durable-1"
	e.completePendingTimer()
	sched.mu.Lock()
	calls := append([]schedule.TimerID(nil), sched.calls...)
	sched.mu.Unlock()
	if !reflect.DeepEqual(calls, []schedule.TimerID{"durable-1"}) {
		t.Fatalf("Ack calls=%v", calls)
	}
	if got := actx.obsKindCounts()[ObsTimerAckFault]; got != 1 {
		t.Fatalf("timer ack fault obs=%d", got)
	}
}

// TestServeAutomaticTimerAckFiresOnHandlerSuccessNotOnError closes the gap
// serve_test.go's fakeSys left open (spec S8/DoD 4: "Serve 道 = handler 正常
// 返回"): fakeSys never implemented settleTimer, so dispatch's type-asserted
// settle hook silently no-op'd there and no test ever proved Serve's
// dispatch loop actually reaches the engine's real ack口 (settleTimer →
// ackTimerObserved → ackTimer → sched.Ack). This drives a fired-timer-shaped
// delivery through the REAL engine (not fakeSys) via Receive/Recv/dispatch —
// the exact path Serve(routes) itself runs — and asserts against a recording
// schedule fake: a handler that returns nil settles (real Ack call fires);
// a handler that returns an error must NOT settle (no Ack call at all, so
// the fired truth survives for redelivery, DoD 4's "处理中失败→不销").
func TestServeAutomaticTimerAckFiresOnHandlerSuccessNotOnError(t *testing.T) {
	e := newTestEngine(t, &fakePen{self: "actor:test"}, Hooks{}, 8, 8)
	sched := &failingAckSchedule{} // err=nil: every delegated Ack call succeeds and is recorded.
	e.sched = sched
	e.lifeCtx = context.Background()
	e.occupant.Store(int32(occupantRunning))

	// Handler success: dispatch must settle true → real Ack call to sched.
	okEnv := &message.Envelope{ID: "timer:fired-ok", Kind: message.KindEvent, Type: "tick"}
	if err := e.Receive(context.Background(), okEnv); err != nil {
		t.Fatalf("Receive(ok): %v", err)
	}
	msg, err := e.Recv()
	if err != nil {
		t.Fatalf("Recv(ok): %v", err)
	}
	dispatch(e, msg, map[string]Handler{
		"tick": func(ctx context.Context, msg Msg) (any, error) { return "ok", nil },
	})
	sched.mu.Lock()
	calls := append([]schedule.TimerID(nil), sched.calls...)
	sched.mu.Unlock()
	if !reflect.DeepEqual(calls, []schedule.TimerID{"fired-ok"}) {
		t.Fatalf("Ack calls after handler success = %v, want [fired-ok]", calls)
	}

	// Handler error: dispatch must settle false → NO Ack call, fired truth
	// left intact for redelivery.
	errEnv := &message.Envelope{ID: "timer:fired-err", Kind: message.KindEvent, Type: "boom"}
	if err := e.Receive(context.Background(), errEnv); err != nil {
		t.Fatalf("Receive(err): %v", err)
	}
	msg, err = e.Recv()
	if err != nil {
		t.Fatalf("Recv(err): %v", err)
	}
	dispatch(e, msg, map[string]Handler{
		"boom": func(ctx context.Context, msg Msg) (any, error) { return nil, errors.New("boom") },
	})
	sched.mu.Lock()
	calls = append([]schedule.TimerID(nil), sched.calls...)
	sched.mu.Unlock()
	if !reflect.DeepEqual(calls, []schedule.TimerID{"fired-ok"}) {
		t.Fatalf("Ack calls after handler error = %v, want unchanged [fired-ok] (no ack on failure)", calls)
	}
}

// --- serve ledger: deadline-close and late-write judgement -----------------

func TestServeLedger_DeadlineFiresThenLedgerEmpty(t *testing.T) {
	life := func() context.Context { return context.Background() }
	l := newServeLedger(life, 8)
	env := newRequestEnv("req-1", 20)
	if !l.admit(env) {
		t.Fatal("expected admit to succeed")
	}
	if l.len() != 1 {
		t.Fatalf("expected 1 admitted entry, got %d", l.len())
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for l.len() != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if l.len() != 0 {
		t.Fatalf("expected ledger empty after deadline, got len=%d", l.len())
	}
	if !l.isClosed(env.ID) {
		t.Fatal("expected entry closed after deadline")
	}
}

func TestEngine_LateReplyAfterDeadlineIsErrRequestClosed(t *testing.T) {
	pen := &fakePen{self: "actor:test"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()
	env := newRequestEnv("req-2", 20)
	if !e.serve.admit(env) {
		t.Fatal("expected admit to succeed")
	}
	ctx, _ := e.serve.ctxFor(env.ID)
	msg := NewMsg(OriginMailbox, ctx, *env)

	deadline := time.Now().Add(500 * time.Millisecond)
	for e.serve.len() != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	_, err := e.Reply(msg, map[string]string{"ok": "true"})
	if !errors.Is(err, ErrRequestClosed) {
		t.Fatalf("expected ErrRequestClosed, got %v", err)
	}
	if pen.count() != 0 {
		t.Fatalf("expected no write to land for a closed entry, got %d writes", pen.count())
	}
}

func TestEngine_ReplyClosesEntry(t *testing.T) {
	pen := &fakePen{self: "actor:test"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()
	env := newRequestEnv("req-3", -1)
	e.serve.admit(env)
	ctx, _ := e.serve.ctxFor(env.ID)
	msg := NewMsg(OriginMailbox, ctx, *env)

	if _, err := e.Reply(msg, map[string]string{"greeting": "hello"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.serve.len() != 0 {
		t.Fatalf("expected entry closed after Reply, got len=%d", e.serve.len())
	}
	if pen.count() != 1 {
		t.Fatalf("expected exactly one write, got %d", pen.count())
	}
	// A second Reply against the now-closed entry is late.
	if _, err := e.Reply(msg, "again"); !errors.Is(err, ErrRequestClosed) {
		t.Fatalf("expected ErrRequestClosed on second Reply, got %v", err)
	}
}

// TestEngine_CancelRequestClosesEntryAndCancelsMsgCtx is spec §5 DoD④'s
// "投递穿透 msg.Ctx Done" — the cell-hosted proof: cell.cancelRequest's
// one-hop handoff to a RequestCanceller-implementing occupant (§1.4) IS
// engine.CancelRequest, which must both close the serve ledger entry AND
// cancel the ctx a delivered Msg's Ctx() already carries (ledger_serve.go's
// close() firing the entry's own cancel — a Proc parked on msg.Ctx().Done()
// must actually unblock, not just have its ledger row vanish).
func TestEngine_CancelRequestClosesEntryAndCancelsMsgCtx(t *testing.T) {
	pen := &fakePen{self: "actor:test"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()
	env := newRequestEnv("req-cancel", -1) // no ExpiresAt: only a delivered cancel can close it
	if !e.serve.admit(env) {
		t.Fatal("expected admit to succeed")
	}
	ctx, ok := e.serve.ctxFor(env.ID)
	if !ok {
		t.Fatal("expected ctxFor to resolve the admitted entry")
	}
	msg := NewMsg(OriginMailbox, ctx, *env)

	select {
	case <-msg.Ctx().Done():
		t.Fatal("msg.Ctx() done before any cancel was delivered")
	default:
	}

	var _ actorrt.RequestCanceller = e // engine.CancelRequest IS the RequestCanceller hook cell.cancelRequest one-hop-delivers to.
	e.CancelRequest(env.ID)

	select {
	case <-msg.Ctx().Done():
	case <-time.After(time.Second):
		t.Fatal("msg.Ctx() never cancelled after engine.CancelRequest")
	}
	if e.serve.len() != 0 {
		t.Fatalf("expected serve ledger entry closed after CancelRequest, got len=%d", e.serve.len())
	}
	// Idempotent: a redundant cancel/late-write on an already-closed entry.
	if _, err := e.Reply(msg, "late"); !errors.Is(err, ErrRequestClosed) {
		t.Fatalf("expected ErrRequestClosed on Reply after cancel, got %v", err)
	}
}

// --- call ledger / Sys.Call / JobTable: await_result and Wait share one
// account -------------------------------------------------------------------

func responseEnv(parentID message.ID, status string) *message.Envelope {
	payload, _ := json.Marshal(map[string]string{"status": status})
	return &message.Envelope{
		ID:       message.ID("resp-" + parentID),
		Kind:     message.KindResponse,
		ParentID: parentID,
		Payload:  payload,
	}
}

func TestEngine_CallWaitReceivesMatchedFinal(t *testing.T) {
	pen := &fakePen{self: "actor:caller"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()

	pending, err := e.Call("actor:callee", "greet", map[string]string{"hi": "1"})
	if err != nil {
		t.Fatalf("unexpected Call error: %v", err)
	}
	sent := pen.last()
	if sent == nil {
		t.Fatal("expected a written outbound request")
	}

	done := make(chan struct{})
	var gotMsg Msg
	var gotErr error
	go func() {
		defer close(done)
		gotMsg, gotErr = pending.Wait(context.Background(), 2*time.Second)
	}()
	time.Sleep(20 * time.Millisecond)

	e.call.match(responseEnv(sent.ID, message.StatusCompleted))
	<-done

	if gotErr != nil {
		t.Fatalf("unexpected Wait error: %v", gotErr)
	}
	if gotMsg.ParentID != sent.ID {
		t.Fatalf("expected matched response, got %+v", gotMsg)
	}
}

func TestEngine_JobTableAwaitSameAccountAsWait(t *testing.T) {
	pen := &fakePen{self: "actor:caller"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()

	id, err := e.Submit(behavior.RequestSpec{
		Type:     "greet",
		Audience: message.Audience{"actor:callee"},
	})
	if err != nil {
		t.Fatalf("unexpected Submit error: %v", err)
	}

	// Final lands BEFORE Await parks — must not be stranded (buffered-final
	// race, spec's reconcile guarantee).
	e.call.match(responseEnv(id, message.StatusCompleted))

	env, ok, err := e.Await(context.Background(), id, time.Second)
	if err != nil {
		t.Fatalf("unexpected Await error: %v", err)
	}
	if !ok || env == nil {
		t.Fatalf("expected buffered final delivered, got ok=%v env=%v", ok, env)
	}
}

// --- reject lane: overflow gets an overloaded terminal; a full reject lane
// leaves it to the caller's own author#2 ------------------------------------

func TestEngine_ServeLedgerFullRoutesToRejectLane(t *testing.T) {
	pen := &fakePen{self: "actor:test"}
	e := newTestEngine(t, pen, Hooks{}, 1 /*serveCap*/, 8)
	e.lifeCtx = context.Background()
	e.occupant.Store(int32(occupantRunning))
	go e.runRejectLane()
	defer close(e.rejectStop)

	// Fill the one serve-ledger slot.
	if err := e.Receive(context.Background(), newRequestEnv("req-a", -1)); err != nil {
		t.Fatalf("unexpected Receive error: %v", err)
	}
	// This one has no room to Admit — must route to the reject lane.
	if err := e.Receive(context.Background(), newRequestEnv("req-b", -1)); err != nil {
		t.Fatalf("unexpected Receive error: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for pen.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	last := pen.last()
	if last == nil {
		t.Fatal("expected the reject lane to write an overloaded terminal")
	}
	var payload struct {
		ErrorCode string `json:"error_code"`
	}
	_ = json.Unmarshal(last.Payload, &payload)
	if payload.ErrorCode != "overloaded" {
		t.Fatalf("expected error_code=overloaded, got %+v (payload=%s)", payload, last.Payload)
	}
}

// TestEngine_RejectLaneFullDropsAndObsRecords (F8) genuinely exercises the
// reject-lane-overflow path with REAL small capacities (serveCap=1, queueCap=2)
// — the old form passed serveCap=0, which newServeLedger silently remapped to
// 256, so all requests Admitted and the overflow it "verified" was the work
// queue's, never the reject lane's. Here: req-1 fills the one serve slot;
// req-2/req-3 fill the reject queue (cap=2); req-4 has nowhere left and must
// record EXACTLY the reject_lane_overflow obs (nothing else publishes obs on
// this path, so the assertion is precise).
func TestEngine_RejectLaneFullDropsAndObsRecords(t *testing.T) {
	pen := &fakePen{self: "actor:test"}
	actx := &fakeActorContext{self: "actor:test"}
	e := newTestEngine(t, pen, Hooks{}, 1 /*serveCap*/, 2 /*queueCap → rejectQ cap*/)
	e.lifeCtx = context.Background()
	e.actorCtx = actx
	e.occupant.Store(int32(occupantRunning))
	// Reject lane goroutine deliberately NOT started so rejectQ never drains.
	for i, id := range []message.ID{"req-1", "req-2", "req-3", "req-4"} {
		if err := e.Receive(context.Background(), newRequestEnv(id, -1)); err != nil {
			t.Fatalf("unexpected Receive error on %s (i=%d): %v", id, i, err)
		}
	}
	if got := actx.obsKindCounts(); got[actorrt.ObsKind("actorbase.reject_lane_overflow")] != 1 {
		t.Fatalf("expected exactly one reject_lane_overflow obs, got kinds=%v", got)
	}
}

// --- call ledger: author#2 durable timeout + pending.Cancel disposition ----

func TestEngine_CallTimeoutWritesUnansweredTimeoutAndClosesEntry(t *testing.T) {
	pen := &fakePen{self: "actor:caller"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()

	spec := behavior.RequestSpec{
		Type:      "greet",
		Audience:  message.Audience{"actor:callee"},
		ExpiresAt: nil,
	}
	// Force a short deadline directly through submit's ExpiresAt resolution
	// path (Hooks.TimeoutResolver) rather than sleeping out DefaultTimeout.
	e.hooks.TimeoutResolver = func(actor.ActorID, string) (time.Duration, bool) {
		return 20 * time.Millisecond, true
	}
	id, err := e.Submit(spec)
	if err != nil {
		t.Fatalf("unexpected Submit error: %v", err)
	}

	// fireTimeout claims the entry (deletes it under lock — the at-most-once
	// guard against a racing Match) BEFORE it writes the terminal through the
	// pen, so polling on list()==0 alone is not proof the write has landed
	// yet (a benign race, not a production defect: the delete is the claim,
	// the write is the effect, and nothing here promises they're atomic).
	// Poll on the actual write count instead (Submit already wrote the
	// request itself, so wait for a SECOND write — the terminal).
	writeCountBefore := pen.count()
	deadline := time.Now().Add(time.Second)
	for pen.count() == writeCountBefore && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if len(e.call.list()) != 0 {
		t.Fatal("expected call ledger entry closed after author#2 timeout fired")
	}
	last := pen.last()
	if last == nil {
		t.Fatal("expected author#2 to write the caller's own unanswered_timeout terminal")
	}
	var payload struct {
		ErrorCode string `json:"error_code"`
	}
	_ = json.Unmarshal(last.Payload, &payload)
	if payload.ErrorCode != string(message.TerminalUnansweredTimeout) {
		t.Fatalf("expected error_code=%s, got %+v", message.TerminalUnansweredTimeout, payload)
	}

	// A response that races in after the self-close is a benign no-op — the
	// entry is already gone, Match must not resurrect or double-write it.
	writesBefore := pen.count()
	e.call.match(responseEnv(id, message.StatusCompleted))
	if pen.count() != writesBefore {
		t.Fatalf("expected no further write from a post-timeout race match, got %d writes (was %d)", pen.count(), writesBefore)
	}
}

func TestEngine_PendingCancelSelfClosesAndSkipsCancellerWhenNil(t *testing.T) {
	pen := &fakePen{self: "actor:caller"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8) // Hooks{} — Canceller nil, honest degrade
	e.lifeCtx = context.Background()

	pending, err := e.Call("actor:callee", "greet", map[string]string{"hi": "1"})
	if err != nil {
		t.Fatalf("unexpected Call error: %v", err)
	}

	if err := pending.Cancel(); err != nil {
		t.Fatalf("unexpected Cancel error: %v", err)
	}
	if len(e.call.list()) != 0 {
		t.Fatal("expected the call ledger entry closed by Cancel")
	}
	last := pen.last()
	if last == nil {
		t.Fatal("expected Cancel to write the caller's own self-closed terminal")
	}
	var payload struct {
		ErrorCode string `json:"error_code"`
		Cancelled bool   `json:"cancelled"`
	}
	_ = json.Unmarshal(last.Payload, &payload)
	if payload.ErrorCode != string(message.TerminalUnansweredTimeout) || !payload.Cancelled {
		t.Fatalf("expected a cancelled unanswered_timeout terminal, got %+v", payload)
	}

	// A second Cancel against an already-closed entry is an idempotent no-op.
	if err := pending.Cancel(); err != nil {
		t.Fatalf("expected idempotent no-op on double Cancel, got %v", err)
	}
}

func TestEngine_PendingCancelInvokesCancellerHookWhenWired(t *testing.T) {
	pen := &fakePen{self: "actor:caller"}
	var gotTarget actor.ActorID
	var gotID message.ID
	calls := 0
	hooks := Hooks{Canceller: func(target actor.ActorID, id message.ID) {
		calls++
		gotTarget = target
		gotID = id
	}}
	e := newTestEngine(t, pen, hooks, 8, 8)
	e.lifeCtx = context.Background()

	pending, err := e.Call("actor:callee", "greet", map[string]string{"hi": "1"})
	if err != nil {
		t.Fatalf("unexpected Call error: %v", err)
	}
	sent := pen.last()

	if err := pending.Cancel(); err != nil {
		t.Fatalf("unexpected Cancel error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected Hooks.Canceller invoked exactly once, got %d", calls)
	}
	if gotTarget != actor.ActorID("actor:callee") || gotID != sent.ID {
		t.Fatalf("expected Canceller(target=actor:callee, id=%s), got target=%s id=%s", sent.ID, gotTarget, gotID)
	}
}

// --- occupant arc: death broadcast timing + quiet/loud raw exit code -------

func TestEngine_DyingReportsQuietOnNilReturn(t *testing.T) {
	pen := &fakePen{self: "actor:test"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actx := &fakeActorContext{self: "actor:test"}

	def := Def{New: func() (Proc, error) {
		return func(sys Sys) error { return nil }, nil
	}}
	e.def = def
	if err := e.Start(ctx, actx); err != nil {
		t.Fatalf("unexpected Start error: %v", err)
	}

	select {
	case err := <-e.Dying():
		if err != nil {
			t.Fatalf("expected quiet (nil) exit, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Dying()")
	}
}

func TestEngine_DyingReportsLoudOnErrorReturn(t *testing.T) {
	pen := &fakePen{self: "actor:test"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actx := &fakeActorContext{self: "actor:test"}

	boom := errors.New("boom")
	e.def = Def{New: func() (Proc, error) {
		return func(sys Sys) error { return boom }, nil
	}}
	if err := e.Start(ctx, actx); err != nil {
		t.Fatalf("unexpected Start error: %v", err)
	}

	select {
	case err := <-e.Dying():
		if !errors.Is(err, boom) {
			t.Fatalf("expected loud exit %v, got %v", boom, err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Dying()")
	}
}

// --- New: the caps→Sys assembly seam itself ---------------------------------

func TestNew_WeldsCapsIntoALiveActor(t *testing.T) {
	pen := &fakePen{self: "actor:test"}
	caps := actorcaps.Caps{
		Pen:       pen,
		Access:    fakeAccess{},
		State:     fakeAccess{},
		Schedule:  fakeSchedule{},
		Lifecycle: fakeSpawn{},
	}
	done := make(chan struct{})
	a := New(caps, Hooks{}, Def{New: func() (Proc, error) {
		return func(sys Sys) error {
			close(done)
			return nil
		}, nil
	}})

	starter, ok := a.(actorrt.Starter)
	if !ok {
		t.Fatal("expected New's result to implement actorrt.Starter")
	}
	if err := starter.Start(context.Background(), &fakeActorContext{self: "actor:test"}); err != nil {
		t.Fatalf("unexpected Start error: %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the Proc to run")
	}
}

func TestEngine_StopDrainsWorkerBeforeReturning(t *testing.T) {
	pen := &fakePen{self: "actor:test"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	ctx, cancel := context.WithCancel(context.Background())
	actx := &fakeActorContext{self: "actor:test"}

	started := make(chan struct{})
	e.def = Def{New: func() (Proc, error) {
		return func(sys Sys) error {
			close(started)
			<-sys.Life().Done()
			return nil
		}, nil
	}}
	if err := e.Start(ctx, actx); err != nil {
		t.Fatalf("unexpected Start error: %v", err)
	}
	<-started
	cancel() // simulate cell teardown cancelling the process-life ctx

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = e.Stop(context.Background())
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after the worker drained")
	}
}

// TestEngine_LifecycleMethodsReturnErrUnsupportedWhenLifecycleNil pins the
// defensive capability-absence contract: a deliberately incomplete host must
// answer ErrUnsupported, never panic on a nil lifecycle handle.
func TestEngine_LifecycleMethodsReturnErrUnsupportedWhenLifecycleNil(t *testing.T) {
	pen := &fakePen{self: "actor:daemon-hosted"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifecycle = nil

	if _, err := e.Fork("fork-1", actorcaps.ForkSpec{
		Kind: actor.KindTool, Class: "worker", NameHint: "hint",
	}); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Fork with nil lifecycle err = %v, want ErrUnsupported", err)
	}
	if err := e.End(); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("End with nil lifecycle err = %v, want ErrUnsupported", err)
	}
}

// TestEngine_StopAbandonsStuckWorkerOnBudget pins the bounded-stop contract
// (purity 手动档, owner 拍 5s at the CELL call site; here the budget is the
// test's own short ctx): a worker that never drains must not pin Stop forever
// — past the ctx budget Stop abandons the join, reports the leak as an error,
// and still runs its ledger/occupant teardown. 审查两问执行件: "卡住等多久、
// 谁收尾" now has a mechanical answer.
func TestEngine_StopAbandonsStuckWorkerOnBudget(t *testing.T) {
	pen := &fakePen{self: "actor:test"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	actx := &fakeActorContext{self: "actor:test"}

	started := make(chan struct{})
	release := make(chan struct{})
	e.def = Def{New: func() (Proc, error) {
		return func(sys Sys) error {
			close(started)
			<-release // stuck occupant: ignores Life() entirely
			return nil
		}, nil
	}}
	if err := e.Start(context.Background(), actx); err != nil {
		t.Fatalf("unexpected Start error: %v", err)
	}
	<-started

	stopCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.Stop(stopCtx) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Stop must report the abandoned join, got nil")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("abandonment must wrap the budget ctx error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop still blocked past its ctx budget — the unbounded join is back")
	}
	close(release) // unblock the worker: the background waiter finishes teardown order
}

// TestNormaliseTimerPayloadFoldsAbsentPayloadToZeroLength pins the fold that
// keeps an absent timer payload legal at FIRE time.
//
// The Scheduler's buildFireEnvelope substitutes the proto baseline `{}` only
// when len(payload)==0. json.Marshal(nil) yields the four bytes `null`, which
// is non-empty, so it sails through to the harness — which rejects null
// payloads. The failure surfaces at fire time (Memory timer silently dropped,
// Durable timer parked in a dead row), never at Schedule time, which is why it
// went unnoticed: a test that only asserts "Schedule returned no error" cannot
// see it.
func TestNormaliseTimerPayloadFoldsAbsentPayloadToZeroLength(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []byte
		want int // wanted len; 0 means "fire path will substitute {}"
	}{
		{"marshalled nil", []byte("null"), 0},
		{"nil slice", nil, 0},
		{"empty slice", []byte{}, 0},
		{"real payload survives", []byte(`{"a":1}`), 7},
		{"json null STRING is not the null literal", []byte(`"null"`), 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := len(normaliseTimerPayload(tc.in)); got != tc.want {
				t.Fatalf("normaliseTimerPayload(%s) len = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}
