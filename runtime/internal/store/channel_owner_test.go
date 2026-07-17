package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func ownerBundle(principal string, at int64) storespec.AdmitBundle {
	return storespec.AdmitBundle{
		Kind: actor.KindHuman, Principal: principal, Role: storespec.RoleOwner,
		Class: "human", Placement: storespec.NewServerPlacement(), CreatedAt: at,
	}
}

func TestChannelOwnerRolePersistenceUniquenessAndIdempotency(t *testing.T) {
	cs := openTestChannel(t)
	ctx := context.Background()
	first, err := cs.DeclAdmission.AdmitDeclared(ctx, ownerBundle("owner-a", 1))
	if err != nil || !first.Created {
		t.Fatalf("first owner = (%+v,%v)", first, err)
	}
	row, ok, err := cs.Declared.LookupDeclaredActive(ctx, first.ID)
	if err != nil || !ok || row.Role != storespec.RoleOwner || row.Kind != actor.KindHuman || row.Sponsor != actor.SystemActorID {
		t.Fatalf("owner row = (%+v,%v,%v)", row, ok, err)
	}
	second, err := cs.DeclAdmission.AdmitDeclared(ctx, ownerBundle("owner-a", 2))
	if err != nil || second.Created || second.ID != first.ID {
		t.Fatalf("idempotent owner = (%+v,%v)", second, err)
	}
	if _, err := cs.DeclAdmission.AdmitDeclared(ctx, ownerBundle("owner-b", 3)); err == nil {
		t.Fatal("second active owner unexpectedly succeeded")
	}
	invalid := ownerBundle("agent-owner", 4)
	invalid.Kind = actor.KindAgent
	if _, err := cs.DeclAdmission.AdmitDeclared(ctx, invalid); err == nil {
		t.Fatal("non-human owner unexpectedly succeeded")
	}
}

func TestAdmitDeclaredPrincipalCollisionNeverUpgradesRole(t *testing.T) {
	cs := openTestChannel(t)
	ctx := context.Background()
	neutral, err := cs.DeclAdmission.AdmitDeclared(ctx, storespec.AdmitBundle{
		Kind: actor.KindHuman, Principal: "same", Class: "human",
		Placement: storespec.NewServerPlacement(), CreatedAt: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	converged, err := cs.DeclAdmission.AdmitDeclared(ctx, ownerBundle("same", 2))
	if err != nil || converged.ID != neutral.ID || converged.Created {
		t.Fatalf("collision = (%+v,%v)", converged, err)
	}
	row, _, _ := cs.Declared.LookupDeclaredActive(ctx, neutral.ID)
	if row.Role != storespec.RoleNone {
		t.Fatalf("principal collision upgraded role to %q", row.Role)
	}
}

func TestEndCascadeProtectsOwnerFromBothBundleHalves(t *testing.T) {
	for _, tc := range []struct {
		name string
		make func(actor.ActorID) storespec.CascadeBundle
	}{
		{"ids", func(id actor.ActorID) storespec.CascadeBundle {
			return storespec.CascadeBundle{IDs: []actor.ActorID{id}, EndedAt: 2}
		}},
		{"envelopes", func(id actor.ActorID) storespec.CascadeBundle {
			return storespec.CascadeBundle{EndedAt: 2, Envelopes: []storespec.CascadeEnvelope{{Target: id, EndedBy: actor.SystemActorID}}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cs := openTestChannel(t)
			ctx := context.Background()
			owner, err := cs.DeclAdmission.AdmitDeclared(ctx, ownerBundle("owner", 1))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := cs.Cascade.EndCascade(ctx, tc.make(owner.ID)); !errors.Is(err, storespec.ErrChannelOwnerProtected) {
				t.Fatalf("EndCascade err=%v, want owner sentinel", err)
			}
			if _, ok, err := cs.Declared.LookupDeclaredActive(ctx, owner.ID); err != nil || !ok {
				t.Fatalf("owner changed after rejected cascade: ok=%v err=%v", ok, err)
			}
		})
	}
}

func TestSetGrantMissingResourceReturnsSentinelForReplaceAndRevoke(t *testing.T) {
	cs := openTestChannel(t)
	for _, ops := range [][]access.Operation{{access.OpRead}, nil} {
		err := cs.Resources.SetGrant(context.Background(), "missing", access.Grant{
			GranteeKind: access.GranteeActor, Grantee: "human:a:1", Ops: ops,
		})
		if !errors.Is(err, resourcespec.ErrResourceNotFound) {
			t.Fatalf("SetGrant ops=%v err=%v, want ErrResourceNotFound", ops, err)
		}
	}
}
