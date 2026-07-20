package link_test

import (
	"context"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// testAuthorities is a complete in-memory authority assembly for substrate
// tests. It deliberately drives the same coordinator -> composition/registry
// revalidation path as production; it is not a constructor fallback.
type testAuthorities struct {
	mu    sync.Mutex
	rows  map[actor.ActorID]storespec.ActorControlRow
	ports map[actor.ActorID]testPortEntry
}

type testPortEntry struct {
	owner link.PortOwner
	inc   actorrt.Incarnation
}

type testStateHandles struct{ access accessdoor.AccessMinter }

func (h testStateHandles) AdmitRun(actor.ActorID) error { return nil }
func (h testStateHandles) EndBatch([]actor.ActorID)     {}
func (h testStateHandles) Resolve(_ context.Context, stamp storespec.AuthorStamp) (accessdoor.AccessHandle, error) {
	if h.access == nil {
		return nil, accessdoor.ErrStateHandleUnavailable
	}
	return h.access.MintState(stamp), nil
}

type blockingPortIndex struct {
	link.PortIndex
	entered chan<- struct{}
	release <-chan struct{}
}

type validAttachmentFence struct{}

func (validAttachmentFence) Valid() bool { return true }

type blockingSecondDaemonValidation struct {
	inner   func(context.Context, string) error
	entered chan<- struct{}
	release <-chan struct{}
	mu      sync.Mutex
	calls   int
}

func (x *blockingSecondDaemonValidation) Validate(ctx context.Context, daemonID string) error {
	x.mu.Lock()
	x.calls++
	call := x.calls
	x.mu.Unlock()
	if call == 2 { // first validates the link attach; second is the first actor stream.
		close(x.entered)
		select {
		case <-x.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return x.inner(ctx, daemonID)
}

func (x blockingPortIndex) Register(owner link.PortOwner, inc actorrt.Incarnation, ticket string, version int64) bool {
	close(x.entered)
	<-x.release
	return x.PortIndex.Register(owner, inc, ticket, version)
}

func newTestAuthorities() *testAuthorities {
	return &testAuthorities{
		rows:  map[actor.ActorID]storespec.ActorControlRow{},
		ports: map[actor.ActorID]testPortEntry{},
	}
}

func (a *testAuthorities) ValidateAttachment(_ context.Context, _ link.PortOwner, daemonID string, in []storespec.ComputeDeclaration) ([]storespec.ComputeDeclaration, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	nextRows := make(map[actor.ActorID]storespec.ActorControlRow, len(in))
	out := make([]storespec.ComputeDeclaration, 0, len(in))
	for _, d := range in {
		if d.Binding == "" {
			d.Binding = actor.BindingRuntimeInboundViaRelay
		}
		nextRows[d.ActorID] = storespec.ActorControlRow{
			ID: d.ActorID, Kind: d.Kind, Binding: d.Binding,
			CurrentDeclVersion: d.Version,
			Placement:          storespec.Placement{Kind: storespec.PlacementDaemon, Host: daemonID},
		}
		out = append(out, d)
	}
	a.rows = nextRows
	return out, nil
}

func (*testAuthorities) PrepareAttachmentFence(context.Context, actor.ActorID, string, int64) (link.AttachmentFence, error) {
	return validAttachmentFence{}, nil
}

func (a *testAuthorities) LookupActive(_ context.Context, id actor.ActorID) (storespec.ActorControlRow, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	r, ok := a.rows[id]
	return r, ok, nil
}

func (a *testAuthorities) ListActive(context.Context) ([]storespec.ActorControlRow, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]storespec.ActorControlRow, 0, len(a.rows))
	for _, row := range a.rows {
		out = append(out, row)
	}
	return out, nil
}

func (a *testAuthorities) WorldOf(ctx context.Context, id actor.ActorID) (storespec.ActorWorld, bool, error) {
	_, ok, err := a.LookupActive(ctx, id)
	return storespec.WorldDurable, ok, err
}

func (a *testAuthorities) CheckAuthor(ctx context.Context, stamp storespec.AuthorStamp) (storespec.AuthorVerdict, error) {
	row, ok, err := a.LookupActive(ctx, stamp.ID)
	if err != nil || !ok {
		return storespec.AuthorNotMember, err
	}
	if row.CurrentDeclVersion != stamp.BirthVersion {
		return storespec.AuthorVersionStale, nil
	}
	return storespec.AuthorOK, nil
}

func (a *testAuthorities) Register(owner link.PortOwner, inc actorrt.Incarnation, _ string, _ int64) bool {
	a.mu.Lock()
	a.ports[inc.ID()] = testPortEntry{owner: owner, inc: inc}
	a.mu.Unlock()
	return true
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

func (a *testAuthorities) ExpireOwner(link.PortOwner) {}

func newTestAcceptor(t *testing.T, cfg link.Config) *link.Acceptor {
	t.Helper()
	auth := newTestAuthorities()
	if cfg.Declarations == nil {
		cfg.Declarations = auth
	}
	if cfg.Authority == nil {
		cfg.Authority = auth
	}
	if cfg.CanAttach == nil {
		cfg.CanAttach = func(context.Context, string) error { return nil }
	}
	if cfg.ActorLock == nil {
		cfg.ActorLock = func(actor.ActorID) func() { return func() {} }
	}
	if cfg.PortIndex == nil {
		cfg.PortIndex = auth
	}
	if cfg.StateHandles == nil {
		cfg.StateHandles = testStateHandles{access: cfg.Access}
	}
	acc, err := link.NewAcceptor(cfg)
	if err != nil {
		t.Fatalf("NewAcceptor: %v", err)
	}
	return acc
}
