package base

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// scriptedState answers Get from a fixed script, one outcome per call; the
// last entry repeats.
type scriptedState struct {
	script []accessdoor.Outcome
	calls  int
}

func (s *scriptedState) Get(resource.ResourceID) (accessdoor.Outcome, error) {
	i := s.calls
	if i >= len(s.script) {
		i = len(s.script) - 1
	}
	s.calls++
	return s.script[i], nil
}
func (s *scriptedState) Put(resource.ResourceID, []byte) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}
func (s *scriptedState) Del(resource.ResourceID) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}

type seedSys struct{ state *scriptedState }

func (s seedSys) State() actorbase.StateHandle { return s.state }
func (seedSys) Self() actor.ActorID            { return "agent:test" }

// A daemon-hosted actor boots while its outbound link is still coming up; the
// state door answers OutcomeUnknown until then. The seed read must wait it out
// instead of silently starting a fresh provider session.
func TestReadSeedWaitsOutTransportUnknownThenReturnsPersistedSeed(t *testing.T) {
	unknown := accessdoor.Outcome{RejectReason: access.OutcomeUnknown}
	found := accessdoor.Outcome{Found: true, Value: []byte("session-1")}
	state := &scriptedState{script: []accessdoor.Outcome{unknown, unknown, unknown, found}}
	got := readSeed(context.Background(), seedSys{state})
	if string(got) != "session-1" {
		t.Fatalf("seed=%q calls=%d", got, state.calls)
	}
	if state.calls != 4 {
		t.Fatalf("calls=%d want 4 (three unknown then found)", state.calls)
	}
}

// A resolved "no seed" is definitive: no retry, no delay.
func TestReadSeedReturnsAtOnceWhenSeedIsAbsent(t *testing.T) {
	for name, out := range map[string]accessdoor.Outcome{
		"accepted-empty": {Found: false},
		"not-found":      {RejectReason: access.ResourceNotFound},
	} {
		state := &scriptedState{script: []accessdoor.Outcome{out}}
		start := time.Now()
		if got := readSeed(context.Background(), seedSys{state}); got != nil {
			t.Fatalf("%s: seed=%q", name, got)
		}
		if state.calls != 1 || time.Since(start) > catchupRetryInterval {
			t.Fatalf("%s: calls=%d elapsed=%s", name, state.calls, time.Since(start))
		}
	}
}

// A link that never comes up must not hold boot past the catch-up budget.
func TestReadSeedGivesUpAfterBudget(t *testing.T) {
	state := &scriptedState{script: []accessdoor.Outcome{{RejectReason: access.OutcomeUnknown}}}
	start := time.Now()
	if got := readSeed(context.Background(), seedSys{state}); got != nil {
		t.Fatalf("seed=%q", got)
	}
	if elapsed := time.Since(start); elapsed < catchupQueryBudget || elapsed > catchupQueryBudget+time.Second {
		t.Fatalf("elapsed=%s want ≈%s", elapsed, catchupQueryBudget)
	}
}
