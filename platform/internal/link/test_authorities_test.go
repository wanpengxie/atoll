package link_test

import (
	"context"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// testAuthorities is a complete in-memory authority assembly for substrate
// tests. It deliberately drives the same coordinator -> composition/registry
// revalidation path as production; it is not a constructor fallback.
type testAuthorities struct {
	mu      sync.Mutex
	rows    map[actor.ActorID]storespec.CompositionRecord
	records map[actor.ActorID]storespec.Record
	ports   map[actor.ActorID]testPortEntry
}

type testPortEntry struct {
	owner link.PortOwner
	inc   actorrt.Incarnation
}

type blockingPortIndex struct {
	link.PortIndex
	entered chan<- struct{}
	release <-chan struct{}
}

func (x blockingPortIndex) Register(owner link.PortOwner, inc actorrt.Incarnation) {
	close(x.entered)
	<-x.release
	x.PortIndex.Register(owner, inc)
}

func newTestAuthorities() *testAuthorities {
	return &testAuthorities{
		rows:    map[actor.ActorID]storespec.CompositionRecord{},
		records: map[actor.ActorID]storespec.Record{},
		ports:   map[actor.ActorID]testPortEntry{},
	}
}

func (a *testAuthorities) ApplyComputeDeclaration(_ context.Context, _ link.PortOwner, daemonID string, in []storespec.ComputeDeclaration) ([]storespec.ComputeDeclaration, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	nextRows := make(map[actor.ActorID]storespec.CompositionRecord, len(in))
	nextRecords := make(map[actor.ActorID]storespec.Record, len(in))
	out := make([]storespec.ComputeDeclaration, 0, len(in))
	for _, d := range in {
		if d.Binding == "" {
			d.Binding = actor.BindingRuntimeInboundViaRelay
		}
		nextRows[d.ActorID] = storespec.CompositionRecord{
			InstanceID: d.ActorID, Placement: storespec.PlacementDaemon,
			DesiredHost: daemonID, Epoch: d.Epoch,
		}
		nextRecords[d.ActorID] = storespec.Record{
			ID: d.ActorID, Kind: d.Kind, Binding: d.Binding, Host: daemonID,
		}
		out = append(out, d)
	}
	a.rows, a.records = nextRows, nextRecords
	return out, nil
}

func (a *testAuthorities) LookupComposition(_ context.Context, id actor.ActorID) (storespec.CompositionRecord, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	r, ok := a.rows[id]
	return r, ok, nil
}

func (a *testAuthorities) LookupCompositionPrincipal(context.Context, string) (storespec.CompositionRecord, bool, error) {
	return storespec.CompositionRecord{}, false, nil
}

func (a *testAuthorities) ListComposition(context.Context) ([]storespec.CompositionRecord, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]storespec.CompositionRecord, 0, len(a.rows))
	for _, row := range a.rows {
		out = append(out, row)
	}
	return out, nil
}

func (a *testAuthorities) DefaultComposition(context.Context) (actor.ActorID, bool, error) {
	return "", false, nil
}

func (a *testAuthorities) Lookup(_ context.Context, id actor.ActorID) (storespec.Record, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	r, ok := a.records[id]
	return r, ok, nil
}

func (a *testAuthorities) Exists(ctx context.Context, id actor.ActorID) (bool, error) {
	_, ok, err := a.Lookup(ctx, id)
	return ok, err
}

func (a *testAuthorities) ListActive(context.Context) ([]storespec.Record, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]storespec.Record, 0, len(a.records))
	for _, rec := range a.records {
		out = append(out, rec)
	}
	return out, nil
}

func (*testAuthorities) LockAndValidate(context.Context, string, channel.ID) (func(), error) {
	return func() {}, nil
}

func (a *testAuthorities) Register(owner link.PortOwner, inc actorrt.Incarnation) {
	a.mu.Lock()
	a.ports[inc.ID()] = testPortEntry{owner: owner, inc: inc}
	a.mu.Unlock()
}

func (a *testAuthorities) Remove(owner link.PortOwner, inc actorrt.Incarnation) {
	a.mu.Lock()
	if cur, ok := a.ports[inc.ID()]; ok && cur.owner == owner && cur.inc == inc {
		delete(a.ports, inc.ID())
	}
	a.mu.Unlock()
}

func (a *testAuthorities) Take(owner link.PortOwner, id actor.ActorID) (actorrt.Incarnation, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	cur, ok := a.ports[id]
	if !ok || cur.owner != owner {
		return actorrt.Incarnation{}, false
	}
	delete(a.ports, id)
	return cur.inc, true
}

func (a *testAuthorities) TakeOwner(owner link.PortOwner) []actorrt.Incarnation {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []actorrt.Incarnation
	for id, cur := range a.ports {
		if cur.owner == owner {
			out = append(out, cur.inc)
			delete(a.ports, id)
		}
	}
	return out
}

func newTestAcceptor(t *testing.T, cfg link.Config) *link.Acceptor {
	t.Helper()
	auth := newTestAuthorities()
	if cfg.Declarations == nil {
		cfg.Declarations = auth
	}
	if cfg.Composition == nil {
		cfg.Composition = auth
	}
	if cfg.Registry == nil {
		cfg.Registry = auth
	}
	if cfg.DaemonAuthority == nil {
		cfg.DaemonAuthority = auth
	}
	if cfg.ActorLock == nil {
		cfg.ActorLock = func(actor.ActorID) func() { return func() {} }
	}
	if cfg.PortIndex == nil {
		cfg.PortIndex = auth
	}
	acc, err := link.NewAcceptor(cfg)
	if err != nil {
		t.Fatalf("NewAcceptor: %v", err)
	}
	return acc
}
