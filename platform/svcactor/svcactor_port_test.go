package svcactor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform/peerproto"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestPortCallAfterCloseReturnsChannelClosed(t *testing.T) {
	port := NewPort()
	port.Close()
	result, err := port.Call(context.Background(), "caller", peerproto.Request{})
	if err != nil || result.Fail == nil || result.Fail.Code != "channel_closed" {
		t.Fatalf("close result=%+v err=%v", result, err)
	}
}

func TestPortCloseReleasesReceivedPendingRequestWithChannelClosed(t *testing.T) {
	port := NewPort()
	received := make(chan struct{})
	go func() {
		_, err := port.receive(context.Background())
		if err == nil {
			close(received)
		}
	}()
	done := make(chan peerproto.Result, 1)
	go func() {
		result, _ := port.Call(context.Background(), "caller", peerproto.Request{})
		done <- result
	}()
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("request was not received")
	}
	port.Close()
	select {
	case result := <-done:
		if result.Fail == nil || result.Fail.Code != "channel_closed" {
			t.Fatalf("close result=%+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("pending call remained blocked after close")
	}
}

type portPending struct {
	started   chan struct{}
	cancelled chan struct{}
}

func (p *portPending) RequestID() message.ID { return "local-request" }
func (p *portPending) Wait(ctx context.Context, _ time.Duration) (actorbase.Msg, error) {
	close(p.started)
	<-ctx.Done()
	return actorbase.Msg{}, ctx.Err()
}
func (p *portPending) Cancel() error {
	select {
	case <-p.cancelled:
	default:
		close(p.cancelled)
	}
	return nil
}

type portSys struct {
	actorbase.Sys
	pending *portPending
	life    context.Context
}

func (s *portSys) Call(actor.ActorID, string, any) (actorbase.Pending, error) { return s.pending, nil }
func (s *portSys) Life() context.Context                                      { return s.life }

func TestPortCloseCancelsDispatchPendingAndStopsOldGenerationWorker(t *testing.T) {
	port := NewPort()
	pending := &portPending{started: make(chan struct{}), cancelled: make(chan struct{})}
	life, cancelLife := context.WithCancel(context.Background())
	defer cancelLife()
	sys := &portSys{pending: pending, life: life}
	deps := svcDeps("codex")
	deps.Port = port
	workerDone := make(chan struct{})
	go func() {
		servePort(sys, deps)
		close(workerDone)
	}()
	callDone := make(chan peerproto.Result, 1)
	go func() {
		result, _ := port.Call(context.Background(), "caller", peerproto.Request{Origin: peerproto.Origin{Channel: "caller", Actor: "actor", RequestID: "remote"}, Type: "work", Payload: []byte(`{}`)})
		callDone <- result
	}()
	<-pending.started
	port.Close()
	select {
	case result := <-callDone:
		// Inside the generation-close window the caller gets a definite
		// failure either way: `channel_closed` from the port, or
		// `receiver_unavailable` when the old worker's cancelled wait wins
		// the race to answer first. Both mean "retry"; neither is silence.
		if result.Fail == nil || (result.Fail.Code != "channel_closed" && result.Fail.Code != "receiver_unavailable") {
			t.Fatalf("close result=%+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("caller did not observe closed generation")
	}
	select {
	case <-pending.cancelled:
	case <-time.After(time.Second):
		t.Fatal("dispatch left its pending call open")
	}
	select {
	case <-workerDone:
	case <-time.After(time.Second):
		t.Fatal("old generation worker did not exit")
	}
}

func TestDispatchCancelsPendingFromCallerContextOrWorkerLife(t *testing.T) {
	for _, tc := range []struct {
		name    string
		trigger func(context.CancelFunc, context.CancelFunc)
	}{
		{name: "caller context", trigger: func(cancelCaller, _ context.CancelFunc) { cancelCaller() }},
		{name: "worker life", trigger: func(_, cancelLife context.CancelFunc) { cancelLife() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			port := NewPort()
			defer port.Close()
			pending := &portPending{started: make(chan struct{}), cancelled: make(chan struct{})}
			life, cancelLife := context.WithCancel(context.Background())
			defer cancelLife()
			caller, cancelCaller := context.WithCancel(context.Background())
			defer cancelCaller()
			sys := &portSys{pending: pending, life: life}
			deps := svcDeps("codex")
			deps.Port = port
			result := make(chan peerproto.Result, 1)
			go func() {
				result <- dispatch(caller, sys, deps, "caller", peerproto.Request{Origin: peerproto.Origin{Channel: "caller", Actor: "actor", RequestID: "remote"}, Type: "work", Payload: []byte(`{}`)})
			}()
			<-pending.started
			tc.trigger(cancelCaller, cancelLife)
			select {
			case got := <-result:
				if got.Fail == nil || got.Fail.Code != "receiver_unavailable" {
					t.Fatalf("result=%+v", got)
				}
			case <-time.After(time.Second):
				t.Fatal("dispatch did not leave pending wait")
			}
			select {
			case <-pending.cancelled:
			case <-time.After(time.Second):
				t.Fatal("pending was not cancelled")
			}
		})
	}
}

func TestPortWithoutSvcactorWaitsForCallerContextAndRemainsUsable(t *testing.T) {
	port := NewPort()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := port.Call(ctx, "caller", peerproto.Request{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("call error=%v", err)
	}

	served := make(chan struct{})
	go func() {
		req, err := port.receive(context.Background())
		if err == nil {
			req.done <- peerproto.Result{Body: []byte(`{"ok":true}`)}
		}
		close(served)
	}()
	result, err := port.Call(context.Background(), channel.ID("caller"), peerproto.Request{})
	if err != nil || string(result.Body) != `{"ok":true}` {
		t.Fatalf("reused port result=%+v err=%v", result, err)
	}
	<-served
}
