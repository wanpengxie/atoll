package agentmain

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

type pulseTestSys struct {
	actorbase.Sys
	inbox  []actorbase.Msg
	recvCh chan actorbase.Msg
	scans  int
	arms   int
	scanCh chan struct{}
}

func (*pulseTestSys) Self() actor.ActorID                { return "tool:main:1" }
func (*pulseTestSys) Life() context.Context              { return context.Background() }
func (*pulseTestSys) Resource() actorbase.ResourceHandle { return pulseResource{} }
func (s *pulseTestSys) Recv() (actorbase.Msg, error) {
	if s.recvCh != nil {
		msg, ok := <-s.recvCh
		if !ok {
			return actorbase.Msg{}, io.EOF
		}
		return msg, nil
	}
	if len(s.inbox) == 0 {
		return actorbase.Msg{}, io.EOF
	}
	msg := s.inbox[0]
	s.inbox = s.inbox[1:]
	return msg, nil
}
func (*pulseTestSys) After(time.Duration, string, any, schedule.TimerHome) (schedule.TimerID, error) {
	panic("private maintenance must not use the channel scheduler")
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
		s.scanCh <- struct{}{}
		return actorcaps.LedgerSnapshot{}, nil
	}}
}

type pulseResource struct{ actorbase.ResourceHandle }

func (pulseResource) Read(resource.ResourceID) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{Found: true, Value: []byte(`{"messages":[]}`)}, nil
}

func TestProcUsesPrivateMaintenanceTicks(t *testing.T) {
	sys := &pulseTestSys{recvCh: make(chan actorbase.Msg), scanCh: make(chan struct{}, 4)}
	ticks := make(chan time.Time, 2)
	ticks <- time.Now()
	ticks <- time.Now()
	done := make(chan error, 1)
	go func() { done <- runWithTicks(sys, Config{Session: "main"}, ticks) }()
	for range 4 {
		select {
		case <-sys.scanCh:
		case <-time.After(time.Second):
			t.Fatal("maintenance tick was not handled")
		}
	}
	close(sys.recvCh)
	if err := <-done; !errors.Is(err, io.EOF) {
		t.Fatalf("run error = %v", err)
	}
	if sys.scans != 4 {
		t.Fatalf("ledger scans = %d, want 4", sys.scans)
	}
}
