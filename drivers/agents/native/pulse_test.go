package native

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

var errPulseTimer = errors.New("timer unavailable")

type pulseTestSys struct {
	*testSys
	inbox  []actorbase.Msg
	arms   int
	failAt int
}

func (s *pulseTestSys) Recv() (actorbase.Msg, error) {
	if len(s.inbox) == 0 {
		return actorbase.Msg{}, io.EOF
	}
	m := s.inbox[0]
	s.inbox = s.inbox[1:]
	return m, nil
}

func (s *pulseTestSys) After(d time.Duration, typ string, _ any, home schedule.TimerHome) (schedule.TimerID, error) {
	s.arms++
	if s.arms == s.failAt {
		return "", errPulseTimer
	}
	if d != 10*time.Second || typ != pulseType || home != schedule.TimerHomeMemory {
		return "", fmt.Errorf("unexpected timer: %s %s %s", d, typ, home)
	}
	if s.arms <= 2 {
		s.inbox = append(s.inbox, pulseEvent(s.Self(), typ))
	}
	return schedule.TimerID(fmt.Sprintf("pulse-%d", s.arms)), nil
}

func pulseEvent(sender actor.ActorID, typ string) actorbase.Msg {
	return actorbase.NewBodyMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID: "timer-event", Kind: message.KindEvent, Type: typ, Sender: message.Sender{ID: sender}, Payload: []byte(`{}`),
	})
}

func TestRunHandlesRecurringTimerEvents(t *testing.T) {
	for _, failAt := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) {
			sys := &pulseTestSys{testSys: newTestSys(newTestState()), failAt: failAt}
			// Neither another member's pulse nor unrelated events may run maintenance.
			sys.inbox = []actorbase.Msg{pulseEvent("agent:other:1", pulseType), pulseEvent(sys.Self(), "unrelated")}
			err := run(sys, Config{})
			wantErr, wantArms := io.EOF, 3
			if failAt != 0 {
				wantErr, wantArms = errPulseTimer, failAt
			}
			if !errors.Is(err, wantErr) || sys.arms != wantArms {
				t.Fatalf("err=%v arms=%d, want %v/%d", err, sys.arms, wantErr, wantArms)
			}
			if len(sys.replies) != 0 || len(sys.fails) != 0 {
				t.Fatalf("timer events received request responses: replies=%v fails=%v", sys.replies, sys.fails)
			}
		})
	}
}
