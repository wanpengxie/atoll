package agentmain

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
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

var errPulseTimer = errors.New("timer unavailable")

type pulseTestSys struct {
	actorbase.Sys
	inbox               []actorbase.Msg
	arms, scans, failAt int
}

func (*pulseTestSys) Self() actor.ActorID                { return "tool:main:1" }
func (*pulseTestSys) Life() context.Context              { return context.Background() }
func (*pulseTestSys) Resource() actorbase.ResourceHandle { return pulseResource{} }
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
	if d != 10*time.Second || typ != pulse || home != schedule.TimerHomeMemory {
		return "", fmt.Errorf("unexpected timer: %s %s %s", d, typ, home)
	}
	if s.arms <= 2 {
		s.inbox = append(s.inbox, mainPulseEvent(s.Self(), typ))
	}
	return schedule.TimerID(fmt.Sprintf("pulse-%d", s.arms)), nil
}

type mainTestView struct {
	actorcaps.LedgerView
	read func(context.Context, actorcaps.LedgerRead) (actorcaps.LedgerSnapshot, error)
}

func (v mainTestView) Read(ctx context.Context, q actorcaps.LedgerRead) (actorcaps.LedgerSnapshot, error) {
	return v.read(ctx, q)
}
func (s *pulseTestSys) View() actorcaps.LedgerView {
	return mainTestView{read: func(context.Context, actorcaps.LedgerRead) (actorcaps.LedgerSnapshot, error) {
		s.scans++
		return actorcaps.LedgerSnapshot{}, nil
	}}
}

type pulseResource struct{ actorbase.ResourceHandle }

func (pulseResource) Read(resource.ResourceID) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{Found: true, Value: []byte(`{"messages":[]}`)}, nil
}

func mainPulseEvent(sender actor.ActorID, typ string) actorbase.Msg {
	return actorbase.NewBodyMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID: "timer-event", Kind: message.KindEvent, Type: typ, Sender: message.Sender{ID: sender}, Payload: []byte(`{}`),
	})
}

func TestProcHandlesRecurringTimerEvents(t *testing.T) {
	for _, failAt := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) {
			sys := &pulseTestSys{failAt: failAt}
			sys.inbox = []actorbase.Msg{mainPulseEvent("tool:other:1", pulse), mainPulseEvent(sys.Self(), "unrelated")}
			err := proc(Config{Session: "main"})(sys)
			// Each maintenance pass reads main policies and archived branches.
			wantErr, wantArms, wantScans := io.EOF, 3, 4
			if failAt != 0 {
				wantErr, wantArms, wantScans = errPulseTimer, failAt, 2*(failAt-1)
			}
			if !errors.Is(err, wantErr) || sys.arms != wantArms || sys.scans != wantScans {
				t.Fatalf("err=%v arms=%d scans=%d, want %v/%d/%d", err, sys.arms, sys.scans, wantErr, wantArms, wantScans)
			}
			// Reply/Fail are deliberately unimplemented: replying to an event fails this test.
		})
	}
}
