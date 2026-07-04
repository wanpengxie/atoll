package actorcaps_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/timerspec"
)

// ---------------------------------------------------------------------
// A minimal fake schedule.Engine wiring, mirroring runtime/schedule's own
// test doubles (unexported there, so this package needs its own) — just
// enough to arm a REAL engine and observe a REAL fire, never a stand-in for
// ScheduleCallTo/ParseScheduledCall themselves.
// ---------------------------------------------------------------------

// fakeTimerStore is a bare in-memory timerspec.TimerStore. Confined to this
// _test.go file: archtest's timerspec wall (archtest/timer_purity_test.go)
// exempts _test.go files exactly so a downstream package can build its own
// engine-level test fixture without opening a production import edge.
type fakeTimerStore struct {
	mu   sync.Mutex
	rows map[timerspec.TimerID]timerspec.TimerRow
}

func newFakeTimerStore() *fakeTimerStore {
	return &fakeTimerStore{rows: make(map[timerspec.TimerID]timerspec.TimerRow)}
}

func (s *fakeTimerStore) Insert(ctx context.Context, row timerspec.TimerRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[row.ID] = row
	return nil
}

func (s *fakeTimerStore) Delete(ctx context.Context, id timerspec.TimerID) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, existed := s.rows[id]
	delete(s.rows, id)
	return existed, nil
}

func (s *fakeTimerStore) Due(ctx context.Context, now int64, limit int) ([]timerspec.TimerRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []timerspec.TimerRow
	for _, r := range s.rows {
		if r.FireAt <= now {
			out = append(out, r)
		}
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *fakeTimerStore) NextFireAt(ctx context.Context) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var next int64
	ok := false
	for _, r := range s.rows {
		if !ok || r.FireAt < next {
			next, ok = r.FireAt, true
		}
	}
	return next, ok, nil
}

func (s *fakeTimerStore) CancelOwned(ctx context.Context, id timerspec.TimerID, author actor.ActorID) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[id]
	if !ok || r.AuthorID != author {
		return false, nil
	}
	delete(s.rows, id)
	return true, nil
}

var _ timerspec.TimerStore = (*fakeTimerStore)(nil)

// fakeFireSink records every fired envelope, welding env.Sender the way the
// REAL platform fireSink does via its minted Pen (scheduler.go: `pen :=
// s.minter.Mint(author, kind, s.chID); pen.Write(ctx, env)`) — the welding
// itself is out of scope here (that is platform's job), but the shape it
// produces on the wire is exactly what ParseScheduledCall's self-authorship
// check consumes, so the fixture reproduces that one bit faithfully.
type fakeFireSink struct {
	mu    sync.Mutex
	calls []*message.Envelope
}

func (f *fakeFireSink) Append(ctx context.Context, author actor.ActorID, env *message.Envelope) error {
	f.mu.Lock()
	env.Sender = message.Sender{ID: author}
	f.calls = append(f.calls, env)
	f.mu.Unlock()
	return nil
}

func (f *fakeFireSink) lastCall() *message.Envelope {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[len(f.calls)-1]
}

func (f *fakeFireSink) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

var _ schedule.FireSink = (*fakeFireSink)(nil)

// fakeReviver always succeeds — these tests exercise BindIdentity, whose
// only Revive requirement is EnsureLive returning nil before Append runs.
type fakeReviver struct{}

func (fakeReviver) EnsureLive(ctx context.Context, id actor.ActorID) error { return nil }

var _ schedule.Reviver = fakeReviver{}

// fixedClock is a deterministic schedule.Clock frozen at one instant. A
// past-relative-to-now FireAt is legal (schedule/types.go: "a millisecond
// before vs after the deadline" must not be two different behaviours) and
// fires on the engine's very first tick — this fixture never needs to
// Advance(), it only needs Now() to never silently be the real wall clock
// (schedule.Deps requires a Clock precisely so a test can never fall back
// to one by accident).
type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

func (c fixedClock) NewAlarm(deadline time.Time) schedule.Timer {
	ch := make(chan time.Time, 1)
	if !deadline.After(c.now) {
		ch <- c.now
	}
	return fixedTimer{ch: ch}
}

type fixedTimer struct{ ch chan time.Time }

func (t fixedTimer) C() <-chan time.Time { return t.ch }
func (t fixedTimer) Stop() bool          { return true }

var _ schedule.Clock = fixedClock{}

// newTestEngine assembles a real schedule.Engine over the fakes above.
func newTestEngine(t *testing.T, sink *fakeFireSink, clock fixedClock) (schedule.Minter, *schedule.Engine) {
	t.Helper()
	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	t.Cleanup(rt.StopAll)
	minter, engine, err := schedule.New(schedule.Deps{
		Store:  newFakeTimerStore(),
		Fire:   sink,
		Host:   rt,
		Revive: fakeReviver{},
		Clock:  clock,
	})
	if err != nil {
		t.Fatalf("schedule.New: %v", err)
	}
	engine.Start()
	t.Cleanup(engine.Close)
	return minter, engine
}

func waitForCall(t *testing.T, sink *fakeFireSink) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for sink.callCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("fire never observed within the deadline")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestScheduleCallToArmFireParseCloses: the full paved-path loop —
// ScheduleCallTo arms a self-targeted closure, the (fake-clocked, real)
// engine fires it, and ParseScheduledCall decodes the SAME target/reqType/
// payload/correlation back out of the fired envelope.
func TestScheduleCallToArmFireParseCloses(t *testing.T) {
	const self = actor.ActorID("scheduler-actor")
	const target = actor.ActorID("callee-actor")

	clock := fixedClock{now: time.UnixMilli(1_000_000)}
	sink := &fakeFireSink{}
	minter, _ := newTestEngine(t, sink, clock)
	handle := minter.Mint(self)

	_, err := actorcaps.ScheduleCallTo(context.Background(), handle, schedule.BindIdentity,
		clock.Now().UnixMilli()-1, target, "demo.req", []byte("hello"), "corr-1")
	if err != nil {
		t.Fatalf("ScheduleCallTo: %v", err)
	}

	waitForCall(t, sink)
	env := sink.lastCall()

	call, ok := actorcaps.ParseScheduledCall(self, env)
	if !ok {
		t.Fatalf("ParseScheduledCall(self, fired env) = false, want true")
	}
	if call.Target != target {
		t.Fatalf("call.Target = %q, want %q", call.Target, target)
	}
	if call.ReqType != "demo.req" {
		t.Fatalf("call.ReqType = %q, want demo.req", call.ReqType)
	}
	if string(call.Payload) != "hello" {
		t.Fatalf("call.Payload = %q, want hello", call.Payload)
	}
	if string(env.CorrelationID) != "corr-1" {
		t.Fatalf("env.CorrelationID = %q, want corr-1", env.CorrelationID)
	}
	if env.Type != actorcaps.ScheduledCallType {
		t.Fatalf("env.Type = %q, want %q", env.Type, actorcaps.ScheduledCallType)
	}
}

// TestParseScheduledCallRejectsForgedSender: ANY member can write its own
// pen with Type=lib.schedule_call and an audience naming the victim — that
// is not a late timer fire, and ParseScheduledCall must not treat it as one.
func TestParseScheduledCallRejectsForgedSender(t *testing.T) {
	const self = actor.ActorID("victim")
	const forger = actor.ActorID("not-victim")

	forged := &message.Envelope{
		Type:    actorcaps.ScheduledCallType,
		Sender:  message.Sender{ID: forger},
		Payload: []byte(`{"target":"anyone","req_type":"evil","payload":null}`),
	}

	if call, ok := actorcaps.ParseScheduledCall(self, forged); ok {
		t.Fatalf("ParseScheduledCall accepted a forged sender, got %+v", call)
	}
}

// TestParseScheduledCallRejectsWrongType: an ordinary self-authored event of
// a different Type is not a scheduled call either — only the exact wire Type
// this facade owns should ever decode.
func TestParseScheduledCallRejectsWrongType(t *testing.T) {
	const self = actor.ActorID("self-actor")
	env := &message.Envelope{
		Type:    "some.other.event",
		Sender:  message.Sender{ID: self},
		Payload: []byte(`{"target":"anyone","req_type":"x","payload":null}`),
	}
	if call, ok := actorcaps.ParseScheduledCall(self, env); ok {
		t.Fatalf("ParseScheduledCall accepted a non-schedule Type, got %+v", call)
	}
}
