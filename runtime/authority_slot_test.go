package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type slotAuthority struct{}

func (slotAuthority) LookupActive(context.Context, actor.ActorID) (storespec.ActorControlRow, bool, error) {
	return storespec.ActorControlRow{ID: "actor:a"}, true, nil
}
func (slotAuthority) ListActive(context.Context) ([]storespec.ActorControlRow, error) {
	return nil, nil
}
func (slotAuthority) IsActive(context.Context, actor.ActorID) (bool, error) {
	return true, nil
}
func (slotAuthority) AdmitIdentity(
	context.Context,
	actor.ActorID,
) (storespec.IdentityAdmission, bool, error) {
	return storespec.IdentityAdmission{ID: "actor:a", Kind: actor.KindAgent}, true, nil
}
func (slotAuthority) ResourceActorFacts(
	context.Context,
	actor.ActorID,
) (storespec.ResourceActorFacts, error) {
	return storespec.ResourceActorFacts{Active: true}, nil
}

func TestActorAuthoritySlotFailsClosedAndBindsOnce(t *testing.T) {
	ctx := context.Background()
	slot := newActorAuthoritySlot()
	if _, _, err := slot.LookupActive(ctx, "actor:a"); !errors.Is(err, ErrActorAuthorityUnbound) {
		t.Fatalf("pre-bind lookup err = %v", err)
	}
	if err := slot.Bind(slotAuthority{}); err != nil {
		t.Fatalf("first Bind: %v", err)
	}
	if err := slot.Bind(slotAuthority{}); !errors.Is(err, ErrActorAuthorityAlreadyBound) {
		t.Fatalf("second Bind err = %v", err)
	}
	if row, ok, err := slot.LookupActive(ctx, "actor:a"); err != nil || !ok || row.ID != "actor:a" {
		t.Fatalf("bound lookup = (%+v,%v,%v)", row, ok, err)
	}
}
