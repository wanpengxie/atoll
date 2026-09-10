package native

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

type pulseTestSys struct {
	*testSys
	recv chan actorbase.Msg
}

func (s *pulseTestSys) Recv() (actorbase.Msg, error) {
	select {
	case msg, ok := <-s.recv:
		if !ok {
			return actorbase.Msg{}, io.EOF
		}
		return msg, nil
	case <-s.Life().Done():
		return actorbase.Msg{}, s.Life().Err()
	}
}

func (*pulseTestSys) After(time.Duration, string, any, schedule.TimerHome) (schedule.TimerID, error) {
	panic("private maintenance must not use the channel scheduler")
}

func TestRunUsesPrivateMaintenanceTicks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	base := newTestSys(newTestState())
	base.Sys = lifeSys{Context: ctx}
	sys := &pulseTestSys{testSys: base, recv: make(chan actorbase.Msg)}
	ticks := make(chan time.Time, 2)
	ticks <- time.Now()
	ticks <- time.Now()
	done := make(chan error, 1)
	go func() { done <- runWithTicks(sys, Config{}, ticks) }()
	time.Sleep(10 * time.Millisecond)
	close(sys.recv)
	if err := <-done; !errors.Is(err, io.EOF) {
		t.Fatalf("run error = %v", err)
	}
	if len(sys.events) != 0 || len(sys.posts) != 0 {
		t.Fatalf("private ticks wrote channel messages: events=%v posts=%v", sys.events, sys.posts)
	}
}

type lifeSys struct {
	actorbase.Sys
	context.Context
}

func (s lifeSys) Life() context.Context { return s.Context }
