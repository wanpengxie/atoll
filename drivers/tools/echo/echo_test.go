package echo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// fakeSys is a minimal actorbase.Sys double: it embeds the (nil) interface so
// every verb this Proc never touches stays unimplemented (a call would nil-
// panic, failing the test loudly rather than silently no-op'ing), and
// overrides only the verbs echo's run actually calls — the say path needs
// Recv/Reply/Fail plus the boot-time State read and PublishObs; the
// capability-face tour additionally records Progress/Emit/After/CancelTimer.
// It feeds a fixed sequence of Msg deliveries then returns errStop (the
// Recv-error loop-termination contract, spec §1.3).
type fakeSys struct {
	actorbase.Sys

	queue      []actorbase.Msg
	at         int
	replies    []replyCall
	fails      []failCall
	progresses []replyCall
	events     []behavior.EventSpec
	timers     []timerCall
	cancels    []schedule.TimerID
	state      map[resource.ResourceID][]byte
}

type replyCall struct {
	msg actorbase.Msg
	v   any
}

type failCall struct {
	msg          actorbase.Msg
	code, detail string
}

type timerCall struct {
	d       time.Duration
	msgType string
	payload any
	home    schedule.TimerHome
}

var errStop = errors.New("fakeSys: queue drained")

// defaultCfg mirrors what a zero-config declaration yields: parseConfig(nil).
func defaultCfg(t *testing.T) Config {
	t.Helper()
	cfg, err := parseConfig(nil)
	if err != nil {
		t.Fatalf("parseConfig(nil) = %v, want defaults", err)
	}
	return cfg
}

func (f *fakeSys) Recv() (actorbase.Msg, error) {
	if f.at >= len(f.queue) {
		return actorbase.Msg{}, errStop
	}
	m := f.queue[f.at]
	f.at++
	return m, nil
}

func (f *fakeSys) Reply(msg actorbase.Msg, v any) (message.ID, error) {
	f.replies = append(f.replies, replyCall{msg, v})
	return msg.ID, nil
}

func (f *fakeSys) Fail(msg actorbase.Msg, code, detail string, _ ...map[string]any) (message.ID, error) {
	f.fails = append(f.fails, failCall{msg, code, detail})
	return msg.ID, nil
}

func (f *fakeSys) Progress(msg actorbase.Msg, _ string, v any) (message.ID, error) {
	f.progresses = append(f.progresses, replyCall{msg, v})
	return msg.ID, nil
}

func (f *fakeSys) Emit(spec behavior.EventSpec) (message.ID, error) {
	f.events = append(f.events, spec)
	return spec.ID, nil
}

func (f *fakeSys) After(d time.Duration, msgType string, payload any, home schedule.TimerHome) (schedule.TimerID, error) {
	f.timers = append(f.timers, timerCall{d, msgType, payload, home})
	return schedule.TimerID(fmt.Sprintf("timer-%d", len(f.timers))), nil
}

func (f *fakeSys) CancelTimer(id schedule.TimerID) error {
	f.cancels = append(f.cancels, id)
	return nil
}

func (f *fakeSys) PublishObs(actorrt.ObsKind, actorrt.ObsValue) error { return nil }

func (f *fakeSys) State() actorbase.StateHandle { return fakeState{f} }

type fakeState struct{ f *fakeSys }

func (s fakeState) Get(id resource.ResourceID) (accessdoor.Outcome, error) {
	v, ok := s.f.state[id]
	return accessdoor.Outcome{Value: v, Found: ok}, nil
}

func (s fakeState) Put(id resource.ResourceID, args []byte) (accessdoor.Outcome, error) {
	if s.f.state == nil {
		s.f.state = map[resource.ResourceID][]byte{}
	}
	s.f.state[id] = args
	return accessdoor.Outcome{}, nil
}

func (s fakeState) Del(id resource.ResourceID) (accessdoor.Outcome, error) {
	delete(s.f.state, id)
	return accessdoor.Outcome{}, nil
}

var _ actorbase.Sys = (*fakeSys)(nil)

func requestMsg(id, typ string, payload any) actorbase.Msg {
	raw, _ := json.Marshal(map[string]any{"body": payload})
	return actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID:      message.ID(id),
		Kind:    message.KindRequest,
		Type:    typ,
		Payload: raw,
	})
}

func TestRun_SayRepliesWithPayloadVerbatim(t *testing.T) {
	msg := requestMsg("req-1", TypeSay, map[string]any{"text": "ping"})
	sys := &fakeSys{queue: []actorbase.Msg{msg}}

	if err := run(sys, defaultCfg(t)); !errors.Is(err, errStop) {
		t.Fatalf("run returned %v, want errStop", err)
	}
	if len(sys.replies) != 1 {
		t.Fatalf("replies = %d, want 1", len(sys.replies))
	}
	if len(sys.fails) != 0 {
		t.Fatalf("fails = %d, want 0", len(sys.fails))
	}
	got := sys.replies[0]
	if got.msg.ID != msg.ID {
		t.Fatalf("reply msg id = %q, want %q", got.msg.ID, msg.ID)
	}
	raw, ok := got.v.(json.RawMessage)
	if !ok {
		t.Fatalf("reply value type = %T, want json.RawMessage", got.v)
	}
	if string(raw) != string(msg.Payload) {
		t.Fatalf("reply payload = %s, want verbatim %s", raw, msg.Payload)
	}
}

func TestRun_UnknownTypeFailsTypeUnsupported(t *testing.T) {
	msg := requestMsg("req-2", "echo.nope", map[string]any{})
	sys := &fakeSys{queue: []actorbase.Msg{msg}}

	if err := run(sys, defaultCfg(t)); !errors.Is(err, errStop) {
		t.Fatalf("run returned %v, want errStop", err)
	}
	if len(sys.fails) != 1 {
		t.Fatalf("fails = %d, want 1", len(sys.fails))
	}
	if len(sys.replies) != 0 {
		t.Fatalf("replies = %d, want 0", len(sys.replies))
	}
	got := sys.fails[0]
	if got.code != "type_unsupported" {
		t.Fatalf("fail code = %q, want type_unsupported", got.code)
	}
}

func TestRun_LoopEndsByPropagatingRecvError(t *testing.T) {
	sys := &fakeSys{}
	if err := run(sys, defaultCfg(t)); !errors.Is(err, errStop) {
		t.Fatalf("run returned %v, want errStop on an empty queue", err)
	}
}

func eventMsg(id, typ string, payload any) actorbase.Msg {
	raw, _ := json.Marshal(payload)
	return actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID:      message.ID(id),
		Kind:    message.KindEvent,
		Type:    typ,
		Payload: raw,
	})
}

// The tour's core account shape: one request held OPEN across Recv
// iterations — Progress at arming, terminal only when the timer fire comes
// home as an event.
func TestRun_CountdownHoldsAccountUntilFireSettlesIt(t *testing.T) {
	start := requestMsg("req-cd", TypeCountdownStart, startPayload{Seconds: 3, Note: "tea"})
	fire := eventMsg("timer:1", TypeCountdownFire, firePayload{Origin: start.ID})
	sys := &fakeSys{queue: []actorbase.Msg{start, fire}}

	if err := run(sys, defaultCfg(t)); !errors.Is(err, errStop) {
		t.Fatalf("run returned %v, want errStop", err)
	}
	if len(sys.progresses) != 1 || sys.progresses[0].msg.ID != start.ID {
		t.Fatalf("progresses = %+v, want exactly one on %q", sys.progresses, start.ID)
	}
	if len(sys.timers) != 1 || sys.timers[0].home != schedule.TimerHomeDurable || sys.timers[0].msgType != TypeCountdownFire {
		t.Fatalf("timers = %+v, want one durable %s", sys.timers, TypeCountdownFire)
	}
	if len(sys.events) != 1 {
		t.Fatalf("events = %+v, want exactly one", sys.events)
	}
	// The spec carries a Cause rather than a correlation field, and the
	// correlation it stands for only appears once the envelope is built.
	armed, err := behavior.BuildEvent(func() time.Time { return time.UnixMilli(0) }, sys.events[0])
	if err != nil {
		t.Fatalf("BuildEvent from the emitted spec: %v", err)
	}
	if armed.ParentID != start.ID || armed.CorrelationID != start.ID {
		t.Fatalf("armed event parent/correlation = %q/%q, want both %q", armed.ParentID, armed.CorrelationID, start.ID)
	}
	if sys.events[0].Audience != nil {
		t.Fatalf("countdown-armed audience=%#v, want pure-log event", sys.events[0].Audience)
	}
	if len(sys.replies) != 1 || sys.replies[0].msg.ID != start.ID {
		t.Fatalf("replies = %+v, want the fire to settle %q", sys.replies, start.ID)
	}
	if len(sys.fails) != 0 {
		t.Fatalf("fails = %+v, want none", sys.fails)
	}
}

// Abort dismantles the timer and settles the held account with a cancelled
// failure; the abort request gets its own separate terminal.
func TestRun_CountdownAbortCancelsTimerAndFailsHeldAccount(t *testing.T) {
	start := requestMsg("req-cd2", TypeCountdownStart, startPayload{Seconds: 60, Note: "slow"})
	abort := requestMsg("req-ab", TypeCountdownAbort, map[string]any{})
	sys := &fakeSys{queue: []actorbase.Msg{start, abort}}

	if err := run(sys, defaultCfg(t)); !errors.Is(err, errStop) {
		t.Fatalf("run returned %v, want errStop", err)
	}
	if len(sys.cancels) != 1 {
		t.Fatalf("cancels = %+v, want the armed timer dismantled", sys.cancels)
	}
	if len(sys.fails) != 1 || sys.fails[0].msg.ID != start.ID || sys.fails[0].code != "cancelled" {
		t.Fatalf("fails = %+v, want one cancelled terminal on %q", sys.fails, start.ID)
	}
	if len(sys.replies) != 1 || sys.replies[0].msg.ID != abort.ID {
		t.Fatalf("replies = %+v, want abort's own ok on %q", sys.replies, abort.ID)
	}
}
