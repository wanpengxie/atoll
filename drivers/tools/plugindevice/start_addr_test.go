package plugindevice

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// A daemon-hosted actor boots while its outbound link is still coming up, and
// until it is the state door honestly answers outcome_unknown rather than faking
// either answer. Reading once there and falling back silently undoes an address
// an operator set — a setting that persisted correctly and still did not survive
// a restart, with nothing anywhere saying why.
//
// The agent driver hit this first (drivers/agents/base/persist.go) and its
// answer is the one copied here: retry the verdicts that mean "cannot say yet",
// return at once on the ones that mean something.

type scriptedState struct {
	mu      sync.Mutex
	replies []accessdoor.Outcome
	errs    []error
	n       int
}

func (s *scriptedState) Get(resource.ResourceID) (accessdoor.Outcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.n
	if i >= len(s.replies) {
		i = len(s.replies) - 1
	}
	s.n++
	var err error
	if i < len(s.errs) {
		err = s.errs[i]
	}
	return s.replies[i], err
}
func (s *scriptedState) Put(resource.ResourceID, []byte) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}
func (s *scriptedState) Del(resource.ResourceID) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}
func (s *scriptedState) calls() int { s.mu.Lock(); defer s.mu.Unlock(); return s.n }

type stateSys struct {
	actorbase.Sys
	state actorbase.StateHandle
}

func (s stateSys) State() actorbase.StateHandle { return s.state }

type capturedLog struct{ lines []string }

func (l *capturedLog) Info(msg string, _ ...any)  { l.lines = append(l.lines, msg) }
func (l *capturedLog) Warn(msg string, _ ...any)  { l.lines = append(l.lines, msg) }
func (l *capturedLog) Error(msg string, _ ...any) { l.lines = append(l.lines, msg) }
func (l *capturedLog) saw(msg string) bool {
	for _, line := range l.lines {
		if line == msg {
			return true
		}
	}
	return false
}

func TestAnAddressSurvivesABootThatRacesTheLink(t *testing.T) {
	state := &scriptedState{replies: []accessdoor.Outcome{
		{RejectReason: access.OutcomeUnknown},            // link not up yet
		{RejectReason: access.OutcomeUnknown},            // still not
		{Found: true, Value: []byte("100.64.0.7:10086")}, // and now it can say
	}}
	log := &capturedLog{}
	got := StartAddr(context.Background(), stateSys{state: state}, "127.0.0.1:10086", log)
	if got != "100.64.0.7:10086" {
		t.Fatalf("addr=%q, want the stored one — a boot that raced the link must not lose it", got)
	}
	if state.calls() != 3 {
		t.Fatalf("read %d times, want it to have waited for an answer", state.calls())
	}
	if !log.saw("plugindevice.stored_addr_restored_after_link") {
		t.Fatalf("the wait was not reported: %v", log.lines)
	}
}

// A resolved answer is definitive and must not be waited on. "Nothing was ever
// set" is the ordinary first boot, and delaying every cold start by the retry
// budget to re-confirm it would be pure cost.
func TestAResolvedAnswerIsTakenAtOnce(t *testing.T) {
	for name, reply := range map[string]accessdoor.Outcome{
		"never set":         {RejectReason: access.ResourceNotFound},
		"resolved as empty": {Found: false},
	} {
		t.Run(name, func(t *testing.T) {
			state := &scriptedState{replies: []accessdoor.Outcome{reply}}
			log := &capturedLog{}
			start := time.Now()
			got := StartAddr(context.Background(), stateSys{state: state}, "127.0.0.1:10086", log)
			if got != "127.0.0.1:10086" {
				t.Fatalf("addr=%q, want the default", got)
			}
			if state.calls() != 1 {
				t.Fatalf("read %d times, want exactly one — this answer is final", state.calls())
			}
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Fatalf("waited %v on an answer that was already given", elapsed)
			}
			if !log.saw("plugindevice.stored_addr_absent") {
				t.Fatalf("a first boot should say so plainly: %v", log.lines)
			}
		})
	}
}

// A door that never comes up must not hold the actor at the gate forever. It
// gives up inside its budget, says so, and boots on the default — listening
// somewhere is better than listening nowhere.
func TestADoorThatNeverAnswersDoesNotBlockBootForever(t *testing.T) {
	state := &scriptedState{replies: []accessdoor.Outcome{{RejectReason: access.OutcomeUnknown}}}
	log := &capturedLog{}
	start := time.Now()
	got := StartAddr(context.Background(), stateSys{state: state}, "127.0.0.1:10086", log)
	if got != "127.0.0.1:10086" {
		t.Fatalf("addr=%q, want the default", got)
	}
	if elapsed := time.Since(start); elapsed > 3*startAddrBudget {
		t.Fatalf("waited %v, well past the budget", elapsed)
	}
	if !log.saw("plugindevice.stored_addr_unreadable") {
		t.Fatalf("giving up must be said out loud: %v", log.lines)
	}
}

// A cancelled life stops the wait immediately: an actor being torn down should
// not spend its budget asking a door it will never use.
func TestGivingUpWhenTheActorIsGoingAway(t *testing.T) {
	state := &scriptedState{replies: []accessdoor.Outcome{{RejectReason: access.OutcomeUnknown}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if got := StartAddr(ctx, stateSys{state: state}, "127.0.0.1:10086", &capturedLog{}); got != "127.0.0.1:10086" {
		t.Fatalf("addr=%q, want the default", got)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("waited %v after the actor was cancelled", elapsed)
	}
}

// A stored address that no longer validates is ignored with a loud line rather
// than taking the actor down: refusing to start is far harder to recover from
// than listening somewhere unexpected.
func TestAnUnusableStoredAddressIsRefusedNotFatal(t *testing.T) {
	state := &scriptedState{replies: []accessdoor.Outcome{{Found: true, Value: []byte("0.0.0.0:10086")}}}
	log := &capturedLog{}
	if got := StartAddr(context.Background(), stateSys{state: state}, "127.0.0.1:10086", log); got != "127.0.0.1:10086" {
		t.Fatalf("addr=%q, want the default", got)
	}
	if !log.saw("plugindevice.stored_addr_rejected") {
		t.Fatalf("the rejection must be visible: %v", log.lines)
	}
	_ = strings.TrimSpace("")
}
