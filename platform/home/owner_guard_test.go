package home

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type ownedRoutingResolver struct {
	routingResolver
	owner string
}

func (r ownedRoutingResolver) ResolveDeclaration(ctx context.Context, ch channel.ID, source string) (channelspec.DeclarationFacts, error) {
	facts, err := r.routingResolver.ResolveDeclaration(ctx, ch, source)
	if err != nil {
		return channelspec.DeclarationFacts{}, err
	}
	facts.OwnerPrincipal = r.owner
	facts.Visibility = "private"
	return facts, nil
}

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
		BootstrapHumanPrincipals: []string{ownerPrincipal},
		BootstrapDeclarations: []DeclareRequest{{
			SourceDeclID: "decl-probe", Seed: "decl-probe", Class: "routing-live",
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
	_, err = h.opEntry.remove(ctx, removeRequest{
		Target: ownerID, InitiatorActorID: agentID,
	})
	var opErr *channelspec.OperationError
	if !errors.As(err, &opErr) || opErr.Code != channelspec.ErrCodeProtectedActor {
		t.Fatalf("removing the owner: err=%v, want %s", err, channelspec.ErrCodeProtectedActor)
	}
	if active, err := h.controller.IsActive(ctx, ownerID); err != nil || !active {
		t.Fatalf("owner survived check: active=%v err=%v", active, err)
	}

	// A non-owner member passes the same door.
	result, err := h.opEntry.remove(ctx, removeRequest{
		Target: agentID, InitiatorActorID: agentID,
	})
	if err != nil {
		t.Fatalf("removing a non-owner member: %v", err)
	}
	if len(result.Removed) != 1 || result.Removed[0] != agentID {
		t.Fatalf("removed=%v, want [%s]", result.Removed, agentID)
	}
}

func TestOwnerTerminalGuardRecognizesAgentPrincipal(t *testing.T) {
	const ownerPrincipal = "steward"
	h, err := Open(Config{
		ChannelID:            "agent-owner-guard",
		DBPath:               filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver:  routingResolver{},
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		Bootstrap:            true,
		Genesis: &storespec.ChannelGenesis{
			ChannelID: "agent-owner-guard", Type: "channel",
			OwnerPrincipal: ownerPrincipal, CreatedAt: time.Now().UnixMilli(),
		},
		BootstrapDeclarations: []DeclareRequest{
			{
				SourceDeclID: "decl-steward", Seed: "steward", Class: "routing-live",
				Placement: storespec.NewServerPlacement(), Kind: actor.KindAgent,
				Principal: ownerPrincipal, CreatedAt: time.Now().UnixMilli(),
			},
			{
				SourceDeclID: "decl-worker", Seed: "worker", Class: "routing-live",
				Placement: storespec.NewServerPlacement(), Kind: actor.KindAgent,
				CreatedAt: time.Now().UnixMilli(),
			},
		},
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
	var ownerID actor.ActorID
	for _, identity := range identities {
		facts, active, factsErr := h.controller.ActorFacts(ctx, identity.ID)
		if factsErr == nil && active && facts.Principal == ownerPrincipal {
			ownerID = identity.ID
		}
	}
	if ownerID == "" {
		t.Fatal("agent owner seat missing")
	}
	resourceFacts, err := h.actors.ResourceActorFacts(ctx, ownerID)
	if err != nil || !resourceFacts.Owner {
		t.Fatalf("explicit principal-bound agent owner facts=%+v err=%v", resourceFacts, err)
	}
	_, err = h.opEntry.remove(ctx, removeRequest{Target: ownerID, InitiatorActorID: ownerID})
	var opErr *channelspec.OperationError
	if !errors.As(err, &opErr) || opErr.Code != channelspec.ErrCodeProtectedActor {
		t.Fatalf("removing agent owner: err=%v, want %s", err, channelspec.ErrCodeProtectedActor)
	}
}

func TestDeclarationOwnerDoesNotBecomeAgentSeatPrincipal(t *testing.T) {
	const ownerPrincipal = "steward"
	resolver := ownedRoutingResolver{owner: ownerPrincipal}
	h, err := Open(completeHomeTestConfig(Config{
		ChannelID:           "declaration-owner-is-not-seat-principal",
		DBPath:              filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver: resolver, IntroductionResolver: resolver,
		ReconcileInterval: time.Hour, Bootstrap: true,
		Genesis: &storespec.ChannelGenesis{
			ChannelID: "declaration-owner-is-not-seat-principal", Type: "channel",
			OwnerPrincipal: ownerPrincipal, CreatedAt: time.Now().UnixMilli(),
		},
		BootstrapHumanPrincipals: []string{ownerPrincipal},
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	ctx := context.Background()

	identities, err := h.controller.ActiveIdentities()
	if err != nil {
		t.Fatal(err)
	}
	var ownerID actor.ActorID
	for _, identity := range identities {
		if identity.Kind == actor.KindHuman {
			ownerID = identity.ID
		}
	}
	if ownerID == "" {
		t.Fatal("human owner seat missing")
	}

	command, err := h.resolveIntroduction(ctx, "codex-template", "", ownerID)
	if err != nil {
		t.Fatal(err)
	}
	if command.Principal != "" {
		t.Fatalf("ordinary declaration birth principal=%q, want empty", command.Principal)
	}
	introduced, err := h.actors.Introduce(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	facts, active, err := h.actors.ActorFacts(ctx, introduced.ActorID)
	if err != nil || !active || facts.Principal != "" {
		t.Fatalf("introduced facts=%+v active=%v err=%v", facts, active, err)
	}
	resourceFacts, err := h.actors.ResourceActorFacts(ctx, introduced.ActorID)
	if err != nil || resourceFacts.Owner {
		t.Fatalf("declaration instance resource facts=%+v err=%v", resourceFacts, err)
	}
	removed, err := h.opEntry.remove(ctx, removeRequest{Target: introduced.ActorID, InitiatorActorID: ownerID})
	if err != nil {
		t.Fatalf("remove declaration instance: %v", err)
	}
	if len(removed.Removed) != 1 || removed.Removed[0] != introduced.ActorID {
		t.Fatalf("removed=%v, want [%s]", removed.Removed, introduced.ActorID)
	}
}
