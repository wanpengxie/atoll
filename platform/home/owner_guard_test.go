package home

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// The owner terminal guard is the ONLY protection line left after the store
// cascade chokepoint was deleted, so it must be exercised for real: with a
// genesis owner in place, a management remove aimed at the owner's human actor
// is refused at the door and the actor stays active; a non-owner target passes
// the same door untouched.
func TestOwnerTerminalGuardRefusesAtTheDoor(t *testing.T) {
	const ownerPrincipal = "alice@example.com"
	h, err := Open(Config{
		ChannelID:            "owner-guard",
		DBPath:               filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver:  routingResolver{},
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		Bootstrap:            true,
		Genesis: &storespec.ChannelGenesis{
			ChannelID: "owner-guard", Type: "channel",
			OwnerPrincipal: ownerPrincipal, CreatedAt: time.Now().UnixMilli(),
		},
		BootstrapOwnerPrincipal: ownerPrincipal,
		BootstrapDeclarations: []DeclareRequest{{
			SourceDeclID: "decl:probe", Class: "routing-live",
			Placement: storespec.NewServerPlacement(), Kind: actor.KindAgent,
			CreatedAt: time.Now().UnixMilli(),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	ctx := context.Background()

	identities, err := h.controller.ActiveIdentities()
	if err != nil {
		t.Fatal(err)
	}
	var ownerID, agentID actor.ActorID
	for _, identity := range identities {
		switch identity.Kind {
		case actor.KindHuman:
			ownerID = identity.ID
		case actor.KindAgent:
			agentID = identity.ID
		}
	}
	if ownerID == "" || agentID == "" {
		t.Fatalf("bootstrap incomplete: owner=%q agent=%q", ownerID, agentID)
	}

	// The owner is protected: refused with the typed door error, still active.
	_, err = h.opEntry.Remove(ctx, channel.RemoveRequest{
		Ref: "remove-owner", Target: ownerID, InitiatorActorID: agentID,
	})
	var opErr *channel.OperationError
	if !errors.As(err, &opErr) || opErr.Code != channel.ErrCodeProtectedActor {
		t.Fatalf("removing the owner: err=%v, want %s", err, channel.ErrCodeProtectedActor)
	}
	if active, err := h.controller.IsActive(ctx, ownerID); err != nil || !active {
		t.Fatalf("owner survived check: active=%v err=%v", active, err)
	}

	// A non-owner member passes the same door.
	result, err := h.opEntry.Remove(ctx, channel.RemoveRequest{
		Ref: "remove-agent", Target: agentID, InitiatorActorID: agentID,
	})
	if err != nil {
		t.Fatalf("removing a non-owner member: %v", err)
	}
	if len(result.Removed) != 1 || result.Removed[0] != agentID {
		t.Fatalf("removed=%v, want [%s]", result.Removed, agentID)
	}
}
