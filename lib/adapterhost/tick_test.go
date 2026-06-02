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

// TestReapExpired_BoundsCorrelation proves the reaper really clears expired
// pending + done correlation (止泄漏 — was //nolint:unused dead code).
func TestReapExpired_BoundsCorrelation(t *testing.T) {
	a := &adapterActor{correlation: map[behavior.CorrelationKey]behavior.CorrelationEntry{}}
	a.correlation["exp"] = behavior.CorrelationEntry{RequestID: "exp", State: behavior.CorrelationPending, ExpiresAt: 100}
	a.correlation["live"] = behavior.CorrelationEntry{RequestID: "live", State: behavior.CorrelationPending, ExpiresAt: 10000}
	a.correlation["done"] = behavior.CorrelationEntry{RequestID: "done", State: behavior.CorrelationDone}
	a.reapExpired(500)
	if _, ok := a.correlation["exp"]; ok {
		t.Error("expired pending correlation NOT reaped (leak)")
	}
	if _, ok := a.correlation["done"]; ok {
		t.Error("done correlation NOT reaped (leak)")
	}
	if _, ok := a.correlation["live"]; !ok {
		t.Error("live pending correlation wrongly reaped")
	}
}

// hbModule records Heartbeat calls (proves the self-tick really drives it).
type hbModule struct{ called int64 }

func (*hbModule) Declares() behavior.Declaration {
	return behavior.Declaration{Name: "hb", ActorID: "t", Types: []string{"x"}}
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
		chain:       &recChain{},
		correlation: map[behavior.CorrelationKey]behavior.CorrelationEntry{},
		inflight:    map[behavior.CorrelationKey]*message.Envelope{},
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
