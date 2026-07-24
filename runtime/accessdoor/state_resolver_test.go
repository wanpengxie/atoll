package accessdoor

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/identitystore"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type resolverHomes map[actor.ActorID]identitystore.Home

func (h resolverHomes) HomeOf(
	_ context.Context,
	id actor.ActorID,
) (identitystore.Home, bool, error) {
	home, ok := h[id]
	return home, ok, nil
}

type resolverMinter struct{}

func (resolverMinter) MintAdmitted(storespec.IdentityAdmission) ResourceAccessHandle {
	return nil
}
func (resolverMinter) MintStateAdmitted(admission storespec.IdentityAdmission) AccessHandle {
	return boundStateHandle{
		door:  &door{deps: Deps{State: newMemStateStore()}},
		owner: admission.ID, admitted: true,
	}
}

func TestStateHandleResolverConfinesHomeAndCrossesReplacement(t *testing.T) {
	ctx := context.Background()
	homes := resolverHomes{
		"declared": identitystore.HomeDurable,
		"forked":   identitystore.HomeMemory,
	}
	resolver, err := NewStateHandleResolver(
		homes, resolverMinter{},
	)
	if err != nil {
		t.Fatal(err)
	}
	firstBinding, err := resolver.ResolvePhysical(ctx, "forked")
	if err != nil {
		t.Fatal(err)
	}
	admission := storespec.IdentityAdmission{
		ID: "forked", Kind: actor.KindAgent,
	}
	first := firstBinding.MintAdmitted(admission)
	if out, err := first.Invoke(ctx, access.OpCreate, resource.ResourceID("done"), []byte("yes"), nil); err != nil || !out.Accepted() {
		t.Fatalf("create = (%+v,%v)", out, err)
	}
	// A later incarnation resolves the same ActorID home and sees predecessor
	// state without receiving any identity-home tag.
	secondBinding, err := resolver.ResolvePhysical(ctx, "forked")
	if err != nil {
		t.Fatal(err)
	}
	second := secondBinding.MintAdmitted(admission)
	out, err := second.Invoke(ctx, access.OpRead, resource.ResourceID("done"), nil, nil)
	if err != nil || string(out.Value) != "yes" {
		t.Fatalf("successor read = (%+v,%v)", out, err)
	}
	delete(homes, "forked")
	resolver.EndBatch([]actor.ActorID{"forked"})
	// The call admitted before End remains attached to the stable physical
	// handle it already selected; End cannot redirect it to durable storage or
	// revoke the accepted sliding window.
	out, err = first.Invoke(
		ctx, access.OpRead, resource.ResourceID("done"), nil, nil,
	)
	if err != nil || string(out.Value) != "yes" {
		t.Fatalf("admitted handle after End = (%+v,%v)", out, err)
	}
	if _, err := resolver.ResolvePhysical(ctx, "forked"); !errors.Is(err, ErrStateHandleUnavailable) {
		t.Fatalf("ResolvePhysical after End = %v", err)
	}
	if _, err := resolver.ResolvePhysical(ctx, "missing"); !errors.Is(err, ErrStateHandleUnavailable) {
		t.Fatalf("ResolvePhysical missing = %v", err)
	}
	if _, err := resolver.ResolvePhysical(ctx, "declared"); err != nil {
		t.Fatalf("ResolvePhysical durable = %v", err)
	}
}
