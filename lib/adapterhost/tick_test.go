package adapterhost

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
)

// TestReapExpired_BoundsInflight proves the reaper clears in-flight requests
// past their deadline, keeps live ones, and leaves no-deadline ones alone
// (止泄漏). The in-flight cache IS the pending tracker (env.expires_at is the
// deadline; no parallel correlation entry).
func TestReapExpired_BoundsInflight(t *testing.T) {
	ms := func(v int64) *int64 { return &v }
	a := &adapterActor{inflight: map[behavior.CorrelationKey]*message.Envelope{}}
	a.inflight["exp"] = &message.Envelope{ID: "exp", ExpiresAt: ms(100)}
	a.inflight["live"] = &message.Envelope{ID: "live", ExpiresAt: ms(10000)}
	a.inflight["nodeadline"] = &message.Envelope{ID: "nodeadline"} // ExpiresAt nil
	a.reapExpired(500)
	if _, ok := a.inflight["exp"]; ok {
		t.Error("expired in-flight request NOT reaped (leak)")
	}
	if _, ok := a.inflight["live"]; !ok {
		t.Error("live in-flight request wrongly reaped")
	}
	if _, ok := a.inflight["nodeadline"]; !ok {
		t.Error("no-deadline request wrongly reaped (must stay until terminal)")
	}
}

// hbModule records Heartbeat calls (proves the self-tick really drives it).
type hbModule struct{ called int64 }

func (*hbModule) Declares() behavior.Declaration {
	return behavior.Declaration{Name: "hb", ActorID: "t"}
}
func (*hbModule) Init(context.Context, *behavior.ModuleContext) error { return nil }
func (*hbModule) Shutdown(context.Context) error                      { return nil }
func (*hbModule) Handle(context.Context, *message.Envelope) error     { return nil }
func (*hbModule) OnExternalCallback(context.Context, []byte) error    { return nil }
func (m *hbModule) Heartbeat(context.Context) (behavior.HeartbeatReport, error) {
	atomic.AddInt64(&m.called, 1)
	return behavior.HeartbeatReport{Available: true}, nil
}

// TestSelfSchedule_TickerDrivesHeartbeat proves the cell self-schedules: Start
// arms a ticker → self.Deliver(tick) → Receive → onTick → module.Heartbeat,
// all on the cell goroutine. Was dead code (Start dropped ActorContext).
func TestSelfSchedule_TickerDrivesHeartbeat(t *testing.T) {
	mod := &hbModule{}
	a := &adapterActor{
		self: "t", module: mod, declaration: behavior.Declaration{ActorID: "t"},
		clock: time.Now, channelID: "ch", tickEvery: 5 * time.Millisecond,
		chain:    &recChain{},
		inflight: map[behavior.CorrelationKey]*message.Envelope{},
	}
	rt := actorrt.New(actorrt.Config{})
	rt.Spawn("t", a)
	deadline := time.Now().Add(1 * time.Second)
	for atomic.LoadInt64(&mod.called) == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	rt.StopAll()
	if atomic.LoadInt64(&mod.called) == 0 {
		t.Fatal("ticker never drove Heartbeat — cell self-schedule NOT wired (Start dropped ActorContext)")
	}
	_ = actor.SystemActorID // keep actor import
}
