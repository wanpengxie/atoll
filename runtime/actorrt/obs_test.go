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
	gens   []Incarnation
	notify chan struct{}
}

func (c *obsCollector) OnObs(_ context.Context, _ actor.ActorID, incarnation Incarnation, kind ObsKind, _ ObsValue) {
	c.mu.Lock()
	c.got = append(c.got, kind)
	c.gens = append(c.gens, incarnation)
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
	rt.WatchObsAll(col)
	_, _, _ = rt.SpawnIfAbsent("a", actor.KindAgent, static(&observerActor{}))

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
	if len(col.gens) != 1 || col.gens[0].ID() != "a" || !rt.IsLive(col.gens[0]) {
		t.Fatalf("OnObs incarnation = %+v, want actor a's live generation", col.gens)
	}
}

// TestWatchObsAllReceivesEveryProducer proves the population subscription does
// not require consumers to mirror producer identities.
func TestWatchObsAllReceivesEveryProducer(t *testing.T) {
	t.Parallel()
	col := &obsCollector{notify: make(chan struct{}, 2)}
	rt, del := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.WatchObsAll(col)
	_, _, _ = rt.SpawnIfAbsent("a", actor.KindAgent, static(&observerActor{}))
	_, _, _ = rt.SpawnIfAbsent("b", actor.KindAgent, static(&observerActor{}))

	if _, err := del.Deliver([]actor.ActorID{"a", "b"}, env("trigger")); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	for range 2 {
		select {
		case <-col.notify:
		case <-time.After(2 * time.Second):
			t.Fatal("population watcher did not receive both producers")
		}
	}
	col.mu.Lock()
	defer col.mu.Unlock()
	if len(col.got) != 2 {
		t.Fatalf("OnObs got %d observations, want 2", len(col.got))
	}
}
