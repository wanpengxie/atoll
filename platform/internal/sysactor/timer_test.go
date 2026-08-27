package sysactor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// fakeTimers records what the gate decided to ask for. The port is where the
// subject lands, so recording it is how the subject rules are asserted.
type fakeTimers struct {
	setFor    []actor.ActorID
	setReq    []TimerSet
	cancelFor []actor.ActorID
	cancelID  []string
	listFor   []actor.ActorID
	pending   map[actor.ActorID][]TimerInfo
	cancelled map[string]bool
	setErr    error
}

func (f *fakeTimers) Set(_ context.Context, subject actor.ActorID, req TimerSet) (TimerHandle, error) {
	f.setFor = append(f.setFor, subject)
	f.setReq = append(f.setReq, req)
	if f.setErr != nil {
		return TimerHandle{}, f.setErr
	}
	return TimerHandle{ID: "new-timer", FireAt: req.FireAt}, nil
}

func (f *fakeTimers) Cancel(_ context.Context, subject actor.ActorID, id string) (bool, error) {
	f.cancelFor = append(f.cancelFor, subject)
	f.cancelID = append(f.cancelID, id)
	if f.cancelled != nil && f.cancelled[id] {
		return false, nil
	}
	for _, t := range f.pending[subject] {
		if t.ID == id {
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeTimers) List(_ context.Context, subject actor.ActorID) ([]TimerInfo, error) {
	f.listFor = append(f.listFor, subject)
	return f.pending[subject], nil
}

func timerActor(t *testing.T, port TimerPort) *SystemActor {
	t.Helper()
	return New(Deps{
		Authority: memberRegistry{active: map[actor.ActorID]bool{
			"agent:caller:1": true, "agent:other:1": true,
		}},
		Clock:  func() time.Time { return time.UnixMilli(1_000_000) },
		Timers: port,
	})
}

// An alarm belongs to the actor the harness welded onto the request, not to
// anything the payload says. The default subject is therefore the sender, and
// the relative duration is resolved against the channel's clock before the port
// ever sees it.
func TestTimerSetArmsForTheAuthenticatedSender(t *testing.T) {
	port := &fakeTimers{}
	sys := &failSys{}
	timerActor(t, port).handle(sys, requestMsg("t1", message.TypeSystemTimerSet,
		[]byte(`{"duration_ms":5000,"msg_type":"standup","payload":{"note":"hi"}}`)))

	if len(sys.fails) != 0 {
		t.Fatalf("unexpected failure: %+v", sys.fails)
	}
	if len(port.setFor) != 1 || port.setFor[0] != "agent:caller:1" {
		t.Fatalf("subject=%v, want the request sender", port.setFor)
	}
	req := port.setReq[0]
	if req.FireAt != 1_005_000 {
		t.Fatalf("fire_at=%d, want clock+duration", req.FireAt)
	}
	if req.Type != "standup" || string(req.Payload) != `{"note":"hi"}` {
		t.Fatalf("req=%+v, want the author's own type and bytes", req)
	}
	if req.Home != timerHomeDurable {
		t.Fatalf("home=%q, want durable by default", req.Home)
	}
}

// Setting an alarm on somebody else is the power to make them wake up and work.
// That is a different grant from "may use timers" and is refused, while READING
// another member's pending alarms is allowed: a channel is one permission
// boundary and a pending intent is not a secret inside it.
func TestTimerSubjectIsWriteRestrictedButReadOpen(t *testing.T) {
	t.Run("set for another member is refused", func(t *testing.T) {
		port := &fakeTimers{}
		sys := &failSys{}
		timerActor(t, port).handle(sys, requestMsg("t2", message.TypeSystemTimerSet,
			[]byte(`{"duration_ms":5000,"msg_type":"x","subject":"agent:other:1"}`)))
		if len(sys.fails) != 1 || sys.fails[0].code != "forbidden" {
			t.Fatalf("fails=%+v, want forbidden", sys.fails)
		}
		if len(port.setFor) != 0 {
			t.Fatalf("port was called for %v despite refusal", port.setFor)
		}
	})

	t.Run("list for another member is served", func(t *testing.T) {
		port := &fakeTimers{pending: map[actor.ActorID][]TimerInfo{
			"agent:other:1": {{ID: "a", Home: "durable", FireAt: 42, Type: "standup"}},
		}}
		sys := &failSys{}
		timerActor(t, port).handle(sys, requestMsg("t3", message.TypeSystemTimerList,
			[]byte(`{"subject":"agent:other:1"}`)))
		if len(sys.fails) != 0 {
			t.Fatalf("unexpected failure: %+v", sys.fails)
		}
		if len(port.listFor) != 1 || port.listFor[0] != "agent:other:1" {
			t.Fatalf("listFor=%v", port.listFor)
		}
		value, _ := sys.replies[0].v.(map[string]any)
		rows, _ := value["timers"].([]map[string]any)
		if len(rows) != 1 || rows[0]["timer_id"] != "a" || rows[0]["msg_type"] != "standup" {
			t.Fatalf("reply=%+v", value)
		}
		if _, leaked := rows[0]["payload"]; leaked {
			t.Fatal("a list read must not carry the alarm's payload")
		}
	})

	t.Run("list for a stranger is refused", func(t *testing.T) {
		port := &fakeTimers{}
		sys := &failSys{}
		timerActor(t, port).handle(sys, requestMsg("t4", message.TypeSystemTimerList,
			[]byte(`{"subject":"agent:ghost:9"}`)))
		if len(sys.fails) != 1 || sys.fails[0].code != "actor_not_in_channel" {
			t.Fatalf("fails=%+v", sys.fails)
		}
	})
}

// WHEN has exactly two spellings and they are mutually exclusive: accepting
// both would leave the substrate to guess which one the author meant.
func TestTimerSetRefusesAmbiguousOrMissingInstant(t *testing.T) {
	for _, tc := range []struct{ name, payload string }{
		{"both", `{"duration_ms":5000,"fire_at":9999,"msg_type":"x"}`},
		{"neither", `{"msg_type":"x"}`},
		{"negative duration", `{"duration_ms":-1,"msg_type":"x"}`},
		{"no msg_type", `{"duration_ms":5000}`},
		{"unknown home", `{"duration_ms":5000,"msg_type":"x","home":"disk"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			port := &fakeTimers{}
			sys := &failSys{}
			timerActor(t, port).handle(sys, requestMsg("t5", message.TypeSystemTimerSet, []byte(tc.payload)))
			if len(sys.fails) != 1 || sys.fails[0].code != "bad_payload" {
				t.Fatalf("fails=%+v, want bad_payload", sys.fails)
			}
			if len(port.setFor) != 0 {
				t.Fatal("a malformed request must not reach the port")
			}
		})
	}
}

// Cancel passes the store's non-leaking verdict straight through: an id that
// fired, never existed, or belongs to someone else are all existed=false, so no
// caller can probe for another member's alarms by watching the answer change.
func TestTimerCancelReportsExistenceWithoutLeaking(t *testing.T) {
	port := &fakeTimers{pending: map[actor.ActorID][]TimerInfo{
		"agent:caller:1": {{ID: "mine", FireAt: 10}},
	}}
	for _, tc := range []struct {
		id   string
		want bool
	}{{"mine", true}, {"someone-elses", false}, {"never-existed", false}} {
		sys := &failSys{}
		timerActor(t, port).handle(sys, requestMsg("t6", message.TypeSystemTimerCancel,
			[]byte(`{"timer_id":"`+tc.id+`"}`)))
		if len(sys.fails) != 0 {
			t.Fatalf("%s: unexpected failure %+v", tc.id, sys.fails)
		}
		value, _ := sys.replies[0].v.(map[string]any)
		if value["existed"] != tc.want {
			t.Fatalf("%s: existed=%v, want %v", tc.id, value["existed"], tc.want)
		}
	}
}

// Reset moves WHEN and keeps WHAT: the pending row supplies the type and home,
// so a caller cannot quietly rewrite what an alarm will say by "moving" it.
// Because the store has no update, it answers with a new id and names the one
// it replaced rather than pretending the id survived.
func TestTimerResetKeepsTheMessageAndNamesTheReplacedID(t *testing.T) {
	port := &fakeTimers{pending: map[actor.ActorID][]TimerInfo{
		"agent:caller:1": {{ID: "old", Home: "memory", FireAt: 10, Type: "standup"}},
	}}
	sys := &failSys{}
	timerActor(t, port).handle(sys, requestMsg("t7", message.TypeSystemTimerReset,
		[]byte(`{"timer_id":"old","duration_ms":9000}`)))

	if len(sys.fails) != 0 {
		t.Fatalf("unexpected failure: %+v", sys.fails)
	}
	if len(port.cancelID) != 1 || port.cancelID[0] != "old" {
		t.Fatalf("cancelled=%v, want the old id", port.cancelID)
	}
	req := port.setReq[0]
	if req.Type != "standup" || req.Home != "memory" {
		t.Fatalf("re-armed as %+v, want the pending row's own type and home", req)
	}
	if req.FireAt != 1_009_000 {
		t.Fatalf("fire_at=%d, want clock+duration", req.FireAt)
	}
	value, _ := sys.replies[0].v.(map[string]any)
	if value["timer_id"] != "new-timer" || value["replaced"] != "old" {
		t.Fatalf("reply=%+v", value)
	}
}

// An alarm that already rang cannot be un-rung, so reset fails the whole word
// rather than silently turning into "armed a new one".
func TestTimerResetRefusesAVanishedAlarm(t *testing.T) {
	port := &fakeTimers{}
	sys := &failSys{}
	timerActor(t, port).handle(sys, requestMsg("t8", message.TypeSystemTimerReset,
		[]byte(`{"timer_id":"gone","duration_ms":9000}`)))
	if len(sys.fails) != 1 || sys.fails[0].code != "timer_gone" {
		t.Fatalf("fails=%+v, want timer_gone", sys.fails)
	}
	if len(port.setFor) != 0 {
		t.Fatal("a vanished alarm must not be quietly re-armed")
	}
}

// A non-member gets the same refusal the other control words give, and an
// unfilled injection point synthesizes nothing at all (the caller's own closure
// reaps the request) rather than inventing a failure the operator never wired.
func TestTimerGateRefusesStrangersAndStaysInertWhenUnwired(t *testing.T) {
	t.Run("stranger", func(t *testing.T) {
		sys := &failSys{}
		New(Deps{Authority: memberRegistry{}, Timers: &fakeTimers{}}).
			handle(sys, requestMsg("t9", message.TypeSystemTimerList, []byte(`{}`)))
		if len(sys.fails) != 1 || sys.fails[0].code != unauthorizedSenderCode {
			t.Fatalf("fails=%+v", sys.fails)
		}
	})

	t.Run("unwired", func(t *testing.T) {
		sys := &failSys{}
		New(Deps{Authority: memberRegistry{active: map[actor.ActorID]bool{"agent:caller:1": true}}}).
			handle(sys, requestMsg("t10", message.TypeSystemTimerSet, []byte(`{"duration_ms":1,"msg_type":"x"}`)))
		if len(sys.fails) != 0 || len(sys.replies) != 0 {
			t.Fatalf("unwired port synthesized fails=%+v replies=%+v", sys.fails, sys.replies)
		}
	})
}

// A port error keeps its chosen code instead of collapsing into internal_error.
func TestTimerPortErrorKeepsItsCode(t *testing.T) {
	port := &fakeTimers{setErr: &OperateError{Code: "authority_unavailable", Detail: "scheduler down"}}
	sys := &failSys{}
	timerActor(t, port).handle(sys, requestMsg("t11", message.TypeSystemTimerSet,
		[]byte(`{"duration_ms":5000,"msg_type":"x"}`)))
	if len(sys.fails) != 1 || sys.fails[0].code != "authority_unavailable" {
		t.Fatalf("fails=%+v", sys.fails)
	}
	var oe *OperateError
	if !errors.As(port.setErr, &oe) {
		t.Fatal("fixture is not an OperateError")
	}
}
