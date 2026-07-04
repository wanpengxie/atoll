package actorrt

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// observerActor is the PUSH producer: it publishes a snapshot when it receives
// work.
type observerActor struct {
	self ActorContext
}

func (o *observerActor) Start(_ context.Context, self ActorContext) error {
	o.self = self
	return nil
}
func (o *observerActor) Receive(_ context.Context, _ *message.Envelope) error {
	o.self.PublishObs("quota.consumed", ObsValue("7"))
	return nil
}

// obsCollector is the consumer end of the obs PUSH channel.
type obsCollector struct {
	mu     sync.Mutex
	got    []ObsKind
	notify chan struct{}
}

func (c *obsCollector) OnObs(_ context.Context, _ actor.ActorID, kind ObsKind, _ ObsValue) {
	c.mu.Lock()
	c.got = append(c.got, kind)
	c.mu.Unlock()
	if c.notify != nil {
		c.notify <- struct{}{}
	}
}

// TestPublishObsFanout: an actor publishing via ActorContext.PublishObs reaches
// a registered ObsWatcher (obs push/actor). No watcher → no-op (not asserted
// here, but publishObs over an empty slice is a plain no-op).
func TestPublishObsFanout(t *testing.T) {
	t.Parallel()
	col := &obsCollector{notify: make(chan struct{}, 1)}
	rt, del := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.WatchObs("a", col)
	rt.Spawn("a", actor.KindAgent, static(&observerActor{}))

	if _, err := del.Deliver([]actor.ActorID{"a"}, env("trigger")); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	select {
	case <-col.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("PublishObs never reached the ObsWatcher")
	}
	col.mu.Lock()
	defer col.mu.Unlock()
	if len(col.got) != 1 || col.got[0] != "quota.consumed" {
		t.Fatalf("OnObs got %+v, want one quota.consumed", col.got)
	}
}

// TestUnwatchObs_StopsDelivery (H5): the symmetric un-registration removes the
// watcher from fanout — a publish after UnwatchObs must not reach it (the
// append-only registry's mirror-image half, so a watcher's registration does
// not outlive whatever tracked it).
func TestUnwatchObs_StopsDelivery(t *testing.T) {
	t.Parallel()
	col := &obsCollector{notify: make(chan struct{}, 1)}
	rt, del := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.WatchObs("a", col)
	rt.UnwatchObs("a", col)
	rt.Spawn("a", actor.KindAgent, static(&observerActor{}))

	if _, err := del.Deliver([]actor.ActorID{"a"}, env("trigger")); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	select {
	case <-col.notify:
		t.Fatal("PublishObs reached the ObsWatcher after UnwatchObs")
	case <-time.After(200 * time.Millisecond):
	}
	col.mu.Lock()
	defer col.mu.Unlock()
	if len(col.got) != 0 {
		t.Fatalf("OnObs got %+v after UnwatchObs, want none", col.got)
	}
}

// TestUnwatchObs_NilAndNotFound_NoOp: nil watcher and an unregistered watcher
// are both safe no-ops (mirrors WatchObs's nil guard).
func TestUnwatchObs_NilAndNotFound_NoOp(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.UnwatchObs("a", nil)
	rt.UnwatchObs("a", &obsCollector{})
}
