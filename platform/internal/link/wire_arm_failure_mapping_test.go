package link

// Error MAPPING across a real wire round trip: which of the three settlements
// (definite error / unconfirmed / host verdict) each arm surfaces, and in what
// vocabulary.
//
// The rule the substrate is held to (governance: "跨 wire 中断→OutcomeUnknown,
// 绝不伪造成功"; port/host build spec: "emit=err / access=outcome_unknown /
// schedule=err"):
//
//	provably NOT executed  → a plain Go error on every arm
//	genuinely UNCONFIRMED  → access/create: the outcome_unknown VERDICT with a
//	                         nil error; schedule + the read-only Query arms:
//	                         a plain Go error
//
// The asymmetry is deliberate, not an oversight: access carries a mutation
// whose result nobody can name, and access.FailureReason has a slot for
// exactly that; the time axis has no unknown-verdict slot (at-least-once
// semantics already permit the caller to retry), and Stat/List are read-only
// and freely retryable with no unknown member in QueryReject to lie through.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// TestWirePreSendCancelIsDefiniteErrorNotOutcomeUnknown: a context already
// cancelled when the call starts means the frame never leaves the process, so
// the operation PROVABLY did not execute. Every arm must say so with a plain
// error — reporting outcome_unknown here would be a false alarm that pushes a
// caller into reconciliation work for an op that never happened.
func TestWirePreSendCancelIsDefiniteErrorNotOutcomeUnknown(t *testing.T) {
	t.Parallel()
	ing := &wireArmIngress{}
	rig := newWireArmRig(t, ing)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	out, err := rig.access.Invoke(ctx, access.OpWrite, "res:doc", []byte("v"), nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Invoke err = %v, want context.Canceled", err)
	}
	if out.RejectReason == access.OutcomeUnknown {
		t.Fatal("a pre-send cancel was reported as outcome_unknown (it provably never executed)")
	}
	if !reflect0Outcome(out) {
		t.Fatalf("Invoke returned a non-zero outcome %+v alongside a definite error", out)
	}

	stateOut, err := rig.state.Invoke(ctx, access.OpWrite, "res:self", nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("state Invoke err = %v, want context.Canceled", err)
	}
	if stateOut.RejectReason == access.OutcomeUnknown {
		t.Fatal("a pre-send cancel on the state face was reported as outcome_unknown")
	}

	createOut, err := rig.access.Create(ctx, "res:new", accessdoor.CreateSpec{Kind: accessdoor.KindKV}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Create err = %v, want context.Canceled", err)
	}
	if createOut.RejectReason == access.OutcomeUnknown {
		t.Fatal("a pre-send cancel on Create was reported as outcome_unknown")
	}

	if _, err := rig.access.Stat(ctx, "res:doc"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stat err = %v, want context.Canceled", err)
	}
	if _, err := rig.access.List(ctx, accessdoor.ListQuery{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("List err = %v, want context.Canceled", err)
	}
	if id, err := rig.sched.Schedule(ctx, schedule.ScheduleReq{Type: "t"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Schedule err = %v (id %q), want context.Canceled", err, id)
	}
	if err := rig.sched.Cancel(ctx, "timer:x"); !errors.Is(err, context.Canceled) {
		t.Fatalf("timer Cancel err = %v, want context.Canceled", err)
	}

	// "Provably did not execute" is the claim, so prove it: drain the home end
	// and assert not one frame ever arrived.
	rig.drainHome()
	if got := len(rig.rawAccessFrames()); got != 0 {
		t.Fatalf("%d access frames crossed on a pre-send cancel, want 0", got)
	}
	if got := len(rig.rawScheduleFrames()); got != 0 {
		t.Fatalf("%d schedule frames crossed on a pre-send cancel, want 0", got)
	}
	if got := len(ing.recordedAccess()) + len(ing.recordedSchedule()); got != 0 {
		t.Fatalf("%d home door calls happened on a pre-send cancel, want 0", got)
	}
}

// TestWirePostSendCancelMapsAccessToOutcomeUnknown: once the frame is on the
// wire the home may already be applying it, so a cancellation leaves the
// result genuinely unnameable. Access/Create must surface that as the
// outcome_unknown VERDICT with a nil error (the honest "I cannot tell you"),
// never as a plain cancellation — which a caller would read as "it did not
// happen" and safely retry, double-applying a mutation.
func TestWirePostSendCancelMapsAccessToOutcomeUnknown(t *testing.T) {
	t.Parallel()

	t.Run("invoke", func(t *testing.T) {
		ing, entered, release := wireArmParkedAccess()
		t.Cleanup(release)
		rig := newWireArmRig(t, ing)

		ctx, cancel := context.WithCancel(context.Background())
		type settled struct {
			out accessdoor.Outcome
			err error
		}
		done := make(chan settled, 1)
		go func() {
			out, err := rig.access.Invoke(ctx, access.OpWrite, "res:doc", []byte("v"), nil)
			done <- settled{out, err}
		}()
		// The home door has the operation in hand: it is unambiguously in flight.
		awaitWireArm(t, entered, "the home door to receive the invocation")
		cancel()

		select {
		case s := <-done:
			if s.err != nil {
				t.Fatalf("post-send cancel returned err %v, want the outcome_unknown verdict", s.err)
			}
			if s.out.RejectReason != access.OutcomeUnknown {
				t.Fatalf("RejectReason = %q, want outcome_unknown", s.out.RejectReason)
			}
			if s.out.Accepted() {
				t.Fatal("an unconfirmed operation was reported as accepted")
			}
		case <-time.After(wireArmTimeout):
			t.Fatal("the cancelled in-flight Invoke never settled")
		}
	})

	t.Run("create", func(t *testing.T) {
		ing, entered, release := wireArmParkedAccess()
		t.Cleanup(release)
		rig := newWireArmRig(t, ing)

		ctx, cancel := context.WithCancel(context.Background())
		type settled struct {
			out accessdoor.Outcome
			err error
		}
		done := make(chan settled, 1)
		go func() {
			out, err := rig.access.Create(ctx, "res:new", accessdoor.CreateSpec{Kind: accessdoor.KindKV}, []byte("i"))
			done <- settled{out, err}
		}()
		awaitWireArm(t, entered, "the home door to receive the create")
		cancel()

		select {
		case s := <-done:
			if s.err != nil {
				t.Fatalf("post-send cancel on Create returned err %v, want outcome_unknown", s.err)
			}
			if s.out.RejectReason != access.OutcomeUnknown {
				t.Fatalf("Create RejectReason = %q, want outcome_unknown", s.out.RejectReason)
			}
		case <-time.After(wireArmTimeout):
			t.Fatal("the cancelled in-flight Create never settled")
		}
	})

	t.Run("stat_stays_a_plain_error", func(t *testing.T) {
		// The read-only contrast: an unconfirmed Stat has nothing to be
		// unknown ABOUT (it mutated nothing and may be retried freely), and
		// QueryReject has no unknown member to lie through — so it must come
		// back as a plain error, never as a fabricated StatResult.
		ing, entered, release := wireArmParkedAccess()
		t.Cleanup(release)
		rig := newWireArmRig(t, ing)

		ctx, cancel := context.WithCancel(context.Background())
		type settled struct {
			stat accessdoor.StatResult
			err  error
		}
		done := make(chan settled, 1)
		go func() {
			stat, err := rig.access.Stat(ctx, "res:doc")
			done <- settled{stat, err}
		}()
		awaitWireArm(t, entered, "the home door to receive the stat")
		cancel()

		select {
		case s := <-done:
			if s.err == nil {
				t.Fatalf("post-send cancel on Stat returned a fabricated result %+v", s.stat)
			}
			if s.stat.Reject != "" {
				t.Fatalf("Stat invented reject verdict %q on an unconfirmed call", s.stat.Reject)
			}
		case <-time.After(wireArmTimeout):
			t.Fatal("the cancelled in-flight Stat never settled")
		}
	})
}

// TestWireTransportDeathMapsAccessToOutcomeUnknown is the real-world producer
// of the unknown verdict: the home dies with an operation in flight. The
// daemon read loop's teardown closes every arm, and the access proxy turns
// that settlement into outcome_unknown — both for the call caught in flight
// and for any call made afterwards against the dead arm (a dead arm must keep
// speaking the same honest word, not degrade into a bare error).
func TestWireTransportDeathMapsAccessToOutcomeUnknown(t *testing.T) {
	t.Parallel()
	ing, entered, release := wireArmParkedAccess()
	t.Cleanup(release)
	rig := newWireArmRig(t, ing)

	type settled struct {
		out accessdoor.Outcome
		err error
	}
	done := make(chan settled, 1)
	go func() {
		out, err := rig.access.Invoke(context.Background(), access.OpWrite, "res:doc", []byte("v"), nil)
		done <- settled{out, err}
	}()
	awaitWireArm(t, entered, "the home door to receive the invocation")

	rig.killTransport()

	select {
	case s := <-done:
		if s.err != nil {
			t.Fatalf("in-flight Invoke at transport death returned err %v, want outcome_unknown", s.err)
		}
		if s.out.RejectReason != access.OutcomeUnknown {
			t.Fatalf("RejectReason = %q, want outcome_unknown", s.out.RejectReason)
		}
		if s.out.Accepted() {
			t.Fatal("transport death was reported as an accepted outcome")
		}
	case <-time.After(wireArmTimeout):
		t.Fatal("the in-flight Invoke never settled after transport death")
	}

	// After the arm is dead, the same word: never a silent success, never a
	// bare transport error a caller might mistake for "it did not happen".
	after, err := rig.access.Invoke(context.Background(), access.OpRead, "res:doc", nil, nil)
	if err != nil {
		t.Fatalf("Invoke on a dead arm returned err %v, want outcome_unknown", err)
	}
	if after.RejectReason != access.OutcomeUnknown {
		t.Fatalf("dead-arm RejectReason = %q, want outcome_unknown", after.RejectReason)
	}
	afterCreate, err := rig.access.Create(
		context.Background(), "res:new", accessdoor.CreateSpec{Kind: accessdoor.KindKV}, nil,
	)
	if err != nil {
		t.Fatalf("Create on a dead arm returned err %v, want outcome_unknown", err)
	}
	if afterCreate.RejectReason != access.OutcomeUnknown {
		t.Fatalf("dead-arm Create RejectReason = %q, want outcome_unknown", afterCreate.RejectReason)
	}
	afterState, err := rig.state.Invoke(context.Background(), access.OpWrite, "res:self", nil, nil)
	if err != nil {
		t.Fatalf("state Invoke on a dead arm returned err %v, want outcome_unknown", err)
	}
	if afterState.RejectReason != access.OutcomeUnknown {
		t.Fatalf("dead-arm state RejectReason = %q, want outcome_unknown", afterState.RejectReason)
	}
}

// TestWireTransportDeathMapsScheduleAndQueryToPlainError is the asymmetric
// counterpart of the test above, on the SAME failure: the time axis and the
// read-only Query arms have no unknown-verdict slot, so a transport death
// there is a plain error carrying the arm's closed sentinel — and, critically,
// no minted TimerID and no fabricated page come back with it.
func TestWireTransportDeathMapsScheduleAndQueryToPlainError(t *testing.T) {
	t.Parallel()
	ing, entered, release := wireArmParkedSchedule()
	t.Cleanup(release)
	rig := newWireArmRig(t, ing)

	type settled struct {
		id  schedule.TimerID
		err error
	}
	done := make(chan settled, 1)
	go func() {
		id, err := rig.sched.Schedule(context.Background(), schedule.ScheduleReq{
			Home: schedule.TimerHomeDurable, Type: "domain.reminder", CorrelationID: "corr-1",
		})
		done <- settled{id, err}
	}()
	awaitWireArm(t, entered, "the home door to receive the schedule")

	rig.killTransport()

	select {
	case s := <-done:
		if s.err == nil {
			t.Fatalf("in-flight Schedule at transport death returned success (id %q)", s.id)
		}
		if !errors.Is(s.err, errRelayClosed) {
			t.Fatalf("Schedule err = %v, want the relay closed sentinel", s.err)
		}
		if s.id != "" {
			t.Fatalf("Schedule returned TimerID %q on an unconfirmed call", s.id)
		}
	case <-time.After(wireArmTimeout):
		t.Fatal("the in-flight Schedule never settled after transport death")
	}

	// Dead-arm behaviour on both sides of the asymmetry, side by side.
	if id, err := rig.sched.Schedule(context.Background(), schedule.ScheduleReq{Type: "t"}); err == nil {
		t.Fatalf("Schedule on a dead arm succeeded with id %q", id)
	} else if !errors.Is(err, errRelayClosed) {
		t.Fatalf("dead-arm Schedule err = %v, want the relay closed sentinel", err)
	}
	if err := rig.sched.Cancel(context.Background(), "timer:x"); !errors.Is(err, errRelayClosed) {
		t.Fatalf("dead-arm timer Cancel err = %v, want the relay closed sentinel", err)
	}
	if err := rig.sched.Ack(context.Background(), "timer:x"); !errors.Is(err, errRelayClosed) {
		t.Fatalf("dead-arm timer Ack err = %v, want the relay closed sentinel", err)
	}
	if _, err := rig.access.Stat(context.Background(), "res:doc"); !errors.Is(err, errRelayClosed) {
		t.Fatalf("dead-arm Stat err = %v, want the relay closed sentinel", err)
	}
	if _, err := rig.access.List(context.Background(), accessdoor.ListQuery{}); !errors.Is(err, errRelayClosed) {
		t.Fatalf("dead-arm List err = %v, want the relay closed sentinel", err)
	}
	// ...while the access arm on the very same dead link keeps answering with
	// the verdict word instead of an error.
	out, err := rig.access.Invoke(context.Background(), access.OpWrite, "res:doc", nil, nil)
	if err != nil || out.RejectReason != access.OutcomeUnknown {
		t.Fatalf("dead-arm Invoke = (%+v, %v), want (outcome_unknown, nil)", out, err)
	}
}

// reflect0Outcome reports whether an Outcome carries nothing at all — the
// shape an arm must return alongside a definite error.
func reflect0Outcome(o accessdoor.Outcome) bool {
	return o.Value == nil && !o.Found && o.RejectReason == "" && o.Route == nil
}
