package actorrt

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// observerActor implements the actor-source obs PULL hook (Observer) and the
// PUSH producer (publishes a snapshot when it receives work).
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
func (o *observerActor) Observe(_ context.Context, kind ObsKind) (ObsValue, error) {
	if kind == "quota.limit" {
		return ObsValue("100"), nil
	}
	return nil, ErrObsUnsupported
}

// TestObserveRoutesToObserver: obs pull/actor routes the opaque kind to the
// actor's Observer hook, which self-answers (non-truth, out-of-band).
func TestObserveRoutesToObserver(t *testing.T) {
	t.Parallel()
	rt, _, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.Spawn("a", &observerActor{})

	val, err := rt.Observe(context.Background(), "a", "quota.limit")
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if string(val) != "100" {
		t.Fatalf("Observe quota.limit = %q, want 100", val)
	}
	// A kind the actor does not expose → ErrObsUnsupported (no-op).
	if _, err := rt.Observe(context.Background(), "a", "secret.business"); err != ErrObsUnsupported {
		t.Fatalf("Observe(undeclared) err = %v, want ErrObsUnsupported", err)
	}
}

// TestObserveUnsupportedAndNotHosted: an actor without the Observer hook is a
// no-op (ErrObsUnsupported); an unhosted id is ErrNotHosted.
func TestObserveUnsupportedAndNotHosted(t *testing.T) {
	t.Parallel()
	rt, _, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.Spawn("plain", newRecordActor()) // no Observer

	if _, err := rt.Observe(context.Background(), "plain", "anything"); err != ErrObsUnsupported {
		t.Fatalf("Observe(non-observer) err = %v, want ErrObsUnsupported", err)
	}
	if _, err := rt.Observe(context.Background(), "ghost", "anything"); err != ErrNotHosted {
		t.Fatalf("Observe(unhosted) err = %v, want ErrNotHosted", err)
	}
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
	rt, del, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.WatchObs("a", col)
	rt.Spawn("a", &observerActor{})

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
