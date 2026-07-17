package accessdoor

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type resolverAuthority struct {
	worlds map[actor.ActorID]storespec.ActorWorld
}

func (a resolverAuthority) LookupActive(_ context.Context, id actor.ActorID) (storespec.ActorControlRow, bool, error) {
	_, ok := a.worlds[id]
	return storespec.ActorControlRow{ID: id, CurrentDeclVersion: 1}, ok, nil
}
func (a resolverAuthority) ListActive(context.Context) ([]storespec.ActorControlRow, error) {
	return nil, nil
}
func (a resolverAuthority) WorldOf(_ context.Context, id actor.ActorID) (storespec.ActorWorld, bool, error) {
	w, ok := a.worlds[id]
	return w, ok, nil
}
func (a resolverAuthority) CheckAuthor(context.Context, storespec.AuthorStamp) (storespec.AuthorVerdict, error) {
	return storespec.AuthorOK, nil
}

type resolverMinter struct{ authority storespec.ActorAuthority }

func (resolverMinter) Mint(storespec.AuthorStamp) ResourceAccessHandle { return nil }
func (m resolverMinter) MintState(stamp storespec.AuthorStamp) AccessHandle {
	return newMemoryStateHandle(stamp, m.authority)
}

func TestStateHandleResolverDispatchAndRunLifetime(t *testing.T) {
	ctx := context.Background()
	authority := resolverAuthority{worlds: map[actor.ActorID]storespec.ActorWorld{
		"declared": storespec.WorldDurable,
		"forked":   storespec.WorldRun,
	}}
	resolver, err := NewStateHandleResolver(authority, resolverMinter{authority: authority})
	if err != nil {
		t.Fatal(err)
	}
	if err := resolver.AdmitRun("forked"); err != nil {
		t.Fatal(err)
	}
	first, err := resolver.Resolve(ctx, "forked")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := first.Invoke(ctx, access.OpCreate, resource.ResourceID("done"), []byte("yes"), nil); err != nil || !out.Accepted() {
		t.Fatalf("create = (%+v,%v)", out, err)
	}
	// A later embodiment resolves the same completed handle and sees the run
	// state written by its predecessor.
	second, err := resolver.Resolve(ctx, "forked")
	if err != nil {
		t.Fatal(err)
	}
	out, err := second.Invoke(ctx, access.OpRead, resource.ResourceID("done"), nil, nil)
	if err != nil || string(out.Value) != "yes" {
		t.Fatalf("successor read = (%+v,%v)", out, err)
	}
	resolver.EndBatch([]actor.ActorID{"forked"})
	if _, err := resolver.Resolve(ctx, "forked"); !errors.Is(err, ErrStateHandleUnavailable) {
		t.Fatalf("Resolve after End = %v", err)
	}
	if _, err := resolver.Resolve(ctx, "missing"); !errors.Is(err, ErrStateHandleUnavailable) {
		t.Fatalf("Resolve missing = %v", err)
	}
	if _, err := resolver.Resolve(ctx, "declared"); err != nil {
		t.Fatalf("Resolve durable = %v", err)
	}
}
