package adapterhost

import (
	"context"
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

// tickModule is a do-nothing module: the self-tick now drives ONLY the reaper
// (there is no Heartbeater — status is advisory, not polled).
type tickModule struct{}

func (*tickModule) Declares() behavior.Declaration {
	return behavior.Declaration{Name: "tk", ActorID: "t"}
}
func (*tickModule) Init(context.Context, *behavior.ModuleContext) error { return nil }
func (*tickModule) Shutdown(context.Context) error                      { return nil }
func (*tickModule) Handle(context.Context, *message.Envelope) error     { return nil }

// TestSelfSchedule_TickerDrivesReaper proves the cell self-schedules: Start arms
// a ticker → self.Deliver(tick) → Receive → onTick → reapExpired, all on the
// cell goroutine. Seed an already-expired in-flight request; after the ticker
// fires it must be gone. StopAll establishes happens-before so the post-stop map
// read is race-free (the cell goroutine has drained).
func TestSelfSchedule_TickerDrivesReaper(t *testing.T) {
	a := &adapterActor{
		self: "t", module: &tickModule{}, declaration: behavior.Declaration{ActorID: "t"},
		clock: time.Now, channelID: "ch", tickEvery: 5 * time.Millisecond,
		chain:    &recChain{},
		inflight: map[behavior.CorrelationKey]*message.Envelope{},
	}
	exp := int64(100) // far in the past vs clock().UnixMilli()
	a.inflight["exp"] = &message.Envelope{ID: "exp", ExpiresAt: &exp}
	rt := actorrt.New(actorrt.Config{})
	rt.Spawn("t", a)
	time.Sleep(60 * time.Millisecond) // several tick periods
	rt.StopAll()
	if _, ok := a.inflight["exp"]; ok {
		t.Fatal("ticker never drove the reaper — cell self-schedule NOT wired (Start dropped ActorContext)")
	}
	_ = actor.SystemActorID // keep actor import
}
