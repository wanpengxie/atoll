package base

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// --- fakes -------------------------------------------------------------------

type fakeState struct{ m map[resource.ResourceID][]byte }

func newFakeState() *fakeState { return &fakeState{m: map[resource.ResourceID][]byte{}} }

func (s *fakeState) Get(id resource.ResourceID) (accessdoor.Outcome, error) {
	v, ok := s.m[id]
	return accessdoor.Outcome{Value: v, Found: ok}, nil
}
func (s *fakeState) Put(id resource.ResourceID, args []byte) (accessdoor.Outcome, error) {
	s.m[id] = append([]byte(nil), args...)
	return accessdoor.Outcome{}, nil
}
func (s *fakeState) Del(id resource.ResourceID) (accessdoor.Outcome, error) {
	delete(s.m, id)
	return accessdoor.Outcome{}, nil
}

type emitRecord struct {
	typ      string
	payload  map[string]any
	audience []actor.ActorID
}

type failRecord struct{ code, detail string }

// fakeSys is a minimal actorbase.Sys the base Proc runs against — scripted Recv
// deliveries drained from a pre-filled, closed inbox (ErrRecvDone on drain),
// plus capture of Emit/Reply/Fail/State.
type fakeSys struct {
	self  actor.ActorID
	life  context.Context
	inbox chan actorbase.Msg
	state *fakeState

	emits   []emitRecord
	replies []any
	fails   []failRecord
}

func newFakeSys(self actor.ActorID, msgs ...actorbase.Msg) *fakeSys {
	inbox := make(chan actorbase.Msg, len(msgs))
	for _, m := range msgs {
		inbox <- m
	}
	close(inbox)
	return &fakeSys{self: self, life: context.Background(), inbox: inbox, state: newFakeState()}
}

func (s *fakeSys) Reply(_ actorbase.Msg, v any) (message.ID, error) {
	s.replies = append(s.replies, v)
	return "reply", nil
}
func (s *fakeSys) Fail(_ actorbase.Msg, code, detail string) (message.ID, error) {
	s.fails = append(s.fails, failRecord{code: code, detail: detail})
	return "fail", nil
}
func (s *fakeSys) Progress(_ actorbase.Msg, _ any) (message.ID, error) { return "", nil }
func (s *fakeSys) Emit(msgType string, payload any, audience ...actor.ActorID) (message.ID, error) {
	m, _ := payload.(map[string]any)
	s.emits = append(s.emits, emitRecord{typ: msgType, payload: m, audience: audience})
	return "emit", nil
}
func (s *fakeSys) Call(actor.ActorID, string, any) (actorbase.Pending, error) { return nil, nil }
func (s *fakeSys) State() actorbase.StateHandle                              { return s.state }
func (s *fakeSys) Resource() actorbase.ResourceHandle                        { return nil }
func (s *fakeSys) After(time.Duration, string, any) (schedule.TimerID, error) {
	return "", nil
}
func (s *fakeSys) CancelTimer(schedule.TimerID) error               { return nil }
func (s *fakeSys) Fork(string, string) (actor.ActorID, error)       { return "", nil }
func (s *fakeSys) DespawnChild(actor.ActorID) error                 { return nil }
func (s *fakeSys) PublishObs(actorrt.ObsKind, actorrt.ObsValue) error { return nil }
func (s *fakeSys) Self() actor.ActorID                              { return s.self }
func (s *fakeSys) Life() context.Context                           { return s.life }
func (s *fakeSys) Recv() (actorbase.Msg, error) {
	msg, ok := <-s.inbox
	if !ok {
		return actorbase.Msg{}, actorbase.ErrRecvDone
	}
	return msg, nil
}

var _ actorbase.Sys = (*fakeSys)(nil)

// stubEngine records Turn calls and emits scripted Outputs — the §1 skeleton's
// unit-test seam (the base is engine-agnostic; the stub never touches an SDK).
type stubEngine struct {
	seed       []byte
	describe   introspect.Describe
	checkpoint []byte
	turnErr    error
	outputs    []Output

	turns  []Trigger
	closed bool
}

func (e *stubEngine) Turn(_ context.Context, tr Trigger, sink Sink) error {
	e.turns = append(e.turns, tr)
	for _, o := range e.outputs {
		if err := sink.Emit(o); err != nil {
			return err
		}
	}
	return e.turnErr
}
func (e *stubEngine) Describe() introspect.Describe { return e.describe }
func (e *stubEngine) Checkpoint() []byte            { return e.checkpoint }
func (e *stubEngine) Close() error                  { e.closed = true; return nil }

// --- helpers -----------------------------------------------------------------

func eventMsg(sender actor.ActorID, text string) actorbase.Msg {
	payload, _ := json.Marshal(map[string]any{"text": text})
	var env message.Envelope
	env.ID = "trigger-1"
	env.Kind = message.KindEvent
	env.Type = "user.text"
	env.Sender = message.Sender{ID: sender}
	env.Payload = payload
	return actorbase.NewMsg(context.Background(), env)
}

func describeMsg(typeSel string) actorbase.Msg {
	payload, _ := json.Marshal(introspect.DescribeRequest{Type: typeSel})
	var env message.Envelope
	env.ID = "describe-1"
	env.Kind = message.KindRequest
	env.Type = introspect.QueryDescribe
	env.Sender = message.Sender{ID: "asker"}
	env.Payload = payload
	return actorbase.NewMsg(context.Background(), env)
}

func runProc(t *testing.T, self actor.ActorID, eng *stubEngine, seedState map[resource.ResourceID][]byte, msgs ...actorbase.Msg) (*fakeSys, error) {
	t.Helper()
	sys := newFakeSys(self, msgs...)
	if seedState != nil {
		for k, v := range seedState {
			sys.state.m[k] = v
		}
	}
	cfg := Config{NewEngine: func(seed []byte) (Engine, error) {
		eng.seed = seed
		return eng, nil
	}}
	proc := newProc(cfg)
	err := proc(sys)
	// ErrRecvDone is the normal drain termination (the loop-termination contract,
	// same as echo's Proc) — not a failure the tests should trip on.
	if errors.Is(err, actorbase.ErrRecvDone) {
		err = nil
	}
	return sys, err
}

// --- tests -------------------------------------------------------------------

func TestTurnEmitsTerminalOutput(t *testing.T) {
	eng := &stubEngine{outputs: []Output{{Final: true, Text: "hi back", NextAction: "done"}}}
	sys, err := runProc(t, "agent:me", eng, nil, eventMsg("user:alice", "hello"))
	if err != nil {
		t.Fatalf("proc: %v", err)
	}
	if len(eng.turns) != 1 {
		t.Fatalf("want 1 turn, got %d", len(eng.turns))
	}
	if got := eng.turns[0].Index; got != 1 {
		t.Fatalf("turn index = %d, want 1", got)
	}
	if len(sys.emits) != 1 {
		t.Fatalf("want 1 emit, got %d", len(sys.emits))
	}
	e := sys.emits[0]
	if e.typ != eventType {
		t.Fatalf("emit type = %q, want %q", e.typ, eventType)
	}
	if e.payload["text"] != "hi back" || e.payload["next_action"] != "done" {
		t.Fatalf("emit payload = %v", e.payload)
	}
	if e.payload["turn_index"] != 1 {
		t.Fatalf("turn_index = %v, want 1", e.payload["turn_index"])
	}
	if len(e.audience) != 1 || e.audience[0] != actor.ActorID("user:alice") {
		t.Fatalf("audience = %v, want [user:alice]", e.audience)
	}
	if !eng.closed {
		t.Fatalf("engine not closed on teardown")
	}
}

func TestIntermediateThenTerminal(t *testing.T) {
	eng := &stubEngine{outputs: []Output{
		{Final: false, NextAction: "continue", Extra: map[string]any{"step_index": 1}},
		{Final: true, Text: "done text", NextAction: "done"},
	}}
	sys, err := runProc(t, "agent:me", eng, nil, eventMsg("user:bob", "go"))
	if err != nil {
		t.Fatalf("proc: %v", err)
	}
	if len(sys.emits) != 2 {
		t.Fatalf("want 2 emits (intermediate + terminal), got %d", len(sys.emits))
	}
	if sys.emits[0].payload["step_index"] != 1 {
		t.Fatalf("intermediate extra not merged: %v", sys.emits[0].payload)
	}
	if sys.emits[1].payload["text"] != "done text" {
		t.Fatalf("terminal text wrong: %v", sys.emits[1].payload)
	}
}

func TestDescribeStampsSelfAndAnswers(t *testing.T) {
	eng := &stubEngine{describe: introspect.Describe{
		ActorID:     "SHOULD_BE_OVERWRITTEN",
		Description: "the brain",
		SkillDoc:    "# agent",
	}}
	sys, err := runProc(t, "agent:me", eng, nil, describeMsg(""))
	if err != nil {
		t.Fatalf("proc: %v", err)
	}
	if len(eng.turns) != 0 {
		t.Fatalf("describe must not become a turn, got %d turns", len(eng.turns))
	}
	if len(sys.replies) != 1 {
		t.Fatalf("want 1 reply, got %d", len(sys.replies))
	}
	d, ok := sys.replies[0].(introspect.Describe)
	if !ok {
		t.Fatalf("reply is %T, want introspect.Describe", sys.replies[0])
	}
	if d.ActorID != "agent:me" {
		t.Fatalf("ActorID = %q, want stamped sys.Self() agent:me", d.ActorID)
	}
	if d.Description != "the brain" {
		t.Fatalf("Description not carried from engine: %q", d.Description)
	}
}

func TestDescribeUnknownTypeFails(t *testing.T) {
	eng := &stubEngine{describe: introspect.Describe{Description: "x"}}
	sys, err := runProc(t, "agent:me", eng, nil, describeMsg("no.such.type"))
	if err != nil {
		t.Fatalf("proc: %v", err)
	}
	if len(sys.fails) != 1 || sys.fails[0].code != "type_unsupported" {
		t.Fatalf("want type_unsupported fail, got %v", sys.fails)
	}
}

func TestSelfEmissionIgnored(t *testing.T) {
	eng := &stubEngine{outputs: []Output{{Final: true, Text: "x"}}}
	sys, err := runProc(t, "agent:me", eng, nil, eventMsg("agent:me", "my own echo"))
	if err != nil {
		t.Fatalf("proc: %v", err)
	}
	if len(eng.turns) != 0 {
		t.Fatalf("self-authored message must not become a turn")
	}
	if len(sys.emits) != 0 {
		t.Fatalf("no emit expected, got %d", len(sys.emits))
	}
}

func TestCheckpointPersistedPerTurn(t *testing.T) {
	eng := &stubEngine{checkpoint: []byte(`{"session":"s1"}`), outputs: []Output{{Final: true, Text: "x"}}}
	sys, err := runProc(t, "agent:me", eng, nil, eventMsg("user:c", "one"))
	if err != nil {
		t.Fatalf("proc: %v", err)
	}
	got := sys.state.m[resumeSeedKey]
	if string(got) != `{"session":"s1"}` {
		t.Fatalf("checkpoint not persisted, got %q", string(got))
	}
}

func TestNoCheckpointWhenNil(t *testing.T) {
	eng := &stubEngine{checkpoint: nil, outputs: []Output{{Final: true, Text: "x"}}}
	sys, err := runProc(t, "agent:me", eng, nil, eventMsg("user:c", "one"))
	if err != nil {
		t.Fatalf("proc: %v", err)
	}
	if _, ok := sys.state.m[resumeSeedKey]; ok {
		t.Fatalf("nil checkpoint must not write state")
	}
}

func TestResumeSeedReadAtBoot(t *testing.T) {
	eng := &stubEngine{}
	seed := map[resource.ResourceID][]byte{resumeSeedKey: []byte(`{"session":"prev"}`)}
	_, err := runProc(t, "agent:me", eng, seed, eventMsg("user:c", "one"))
	if err != nil {
		t.Fatalf("proc: %v", err)
	}
	if string(eng.seed) != `{"session":"prev"}` {
		t.Fatalf("engine boot seed = %q, want prior session", string(eng.seed))
	}
}

func TestBootFailureIsLoudDeath(t *testing.T) {
	sys := newFakeSys("agent:me", eventMsg("user:c", "one"))
	bootErr := errors.New("no api key")
	proc := newProc(Config{NewEngine: func([]byte) (Engine, error) { return nil, bootErr }})
	err := proc(sys)
	if err == nil || !errors.Is(err, bootErr) {
		t.Fatalf("boot failure must be loud death, got %v", err)
	}
}

func TestTurnPlumbingErrorPropagates(t *testing.T) {
	plumb := errors.New("emit rejected")
	eng := &stubEngine{turnErr: plumb}
	_, err := runProc(t, "agent:me", eng, nil, eventMsg("user:c", "one"))
	if !errors.Is(err, plumb) {
		t.Fatalf("plumbing error must propagate as loud death, got %v", err)
	}
}

func TestTurnCanceledIsQuiet(t *testing.T) {
	eng := &stubEngine{turnErr: context.Canceled}
	_, err := runProc(t, "agent:me", eng, nil, eventMsg("user:c", "one"))
	if err != nil {
		t.Fatalf("context.Canceled from Turn must be quiet寿终, got %v", err)
	}
}

func TestDefRequiresNewEngine(t *testing.T) {
	if _, err := Def("doc", Config{}); err == nil {
		t.Fatalf("Def must reject a nil NewEngine")
	}
	if _, err := Def("doc", Config{NewEngine: func([]byte) (Engine, error) { return &stubEngine{}, nil }}); err != nil {
		t.Fatalf("Def with NewEngine: %v", err)
	}
}
