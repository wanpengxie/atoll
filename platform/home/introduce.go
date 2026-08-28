package home

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// resolveIntroduction is the command-front business resolution for one
// declaration admission: fetch the declaration, judge its visibility against
// the initiator, resolve the class kind, and pick the placement host. Policy
// lives here, at the door; the Controller command that follows is mechanical.
// desiredHost names which of this channel's bound devices should run the
// member. It is asked HERE, at the seating, and not on the declaration: a
// declaration is a recipe and travels between channels, while a device is bound
// per channel — the same template seated in two channels may well belong on two
// different machines. Empty means "no preference", and the channel applies its
// default (see resolveDaemonPlacement) or refuses when it has no defensible one.
func (h *Home) resolveIntroduction(
	ctx context.Context,
	declID string,
	desiredHost string,
	initiator actor.ActorID,
) (actorctl.IntroduceRequest, error) {
	if declID == "" || initiator == "" {
		return actorctl.IntroduceRequest{}, &channelspec.OperationError{
			Code: channelspec.ErrCodeBadPayload, Detail: "decl_id and initiator_actor_id required",
		}
	}
	if h.resolver == nil {
		return actorctl.IntroduceRequest{}, &channelspec.OperationError{
			Code: channelspec.ErrCodeAuthorityUnavailable, Detail: "introduction resolver unavailable", Retryable: true,
		}
	}
	initiatorFacts, active, err := h.actors.ActorFacts(ctx, initiator)
	if err != nil {
		return actorctl.IntroduceRequest{}, err
	}
	if !active {
		return actorctl.IntroduceRequest{}, &channelspec.OperationError{
			Code: channelspec.ErrCodeMemberInactive, Detail: "initiator is not an active member",
		}
	}

	resolveCtx, cancel := context.WithTimeout(ctx, introductionResolveTimeout)
	facts, err := h.resolver.ResolveDeclaration(resolveCtx, h.channelID, declID)
	cancel()
	if err != nil {
		code, retryable := channelspec.ErrCodeAuthorityUnavailable, true
		if errors.Is(err, channelspec.ErrDeclarationNotFound) {
			code, retryable = channelspec.ErrCodeDeclNotFound, false
		}
		return actorctl.IntroduceRequest{}, &channelspec.OperationError{
			Code: code, Detail: err.Error(), Retryable: retryable,
		}
	}
	if facts.Name == "" {
		return actorctl.IntroduceRequest{}, &channelspec.OperationError{
			Code:   channelspec.ErrCodeDeclNotFound,
			Detail: "declaration " + declID + " has no name, so the member it would seat could not be named; give the declaration a name with system.actor.template.set",
		}
	}
	admitCtx, admitCancel := context.WithTimeout(ctx, introductionResolveTimeout)
	err = h.resolver.AdmitIntroduction(admitCtx, h.channelID, facts)
	admitCancel()
	if err != nil {
		code, retryable := channelspec.ErrCodeAuthorityUnavailable, true
		if errors.Is(err, channelspec.ErrTargetNotServing) {
			code, retryable = channelspec.ErrCodeForbidden, false
		}
		if errors.Is(err, channelspec.ErrDeclarationNotFound) {
			code, retryable = channelspec.ErrCodeDeclNotFound, false
		}
		return actorctl.IntroduceRequest{}, &channelspec.OperationError{Code: code, Detail: err.Error(), Retryable: retryable}
	}
	// A non-public declaration is its owner's to place. Attribution follows the
	// same two-level rule as the registrar: an explicit principal wins; an actor
	// without one acts for the owner of its source declaration. The actor facts
	// come from the roster and declaration ownership comes from the space read
	// port, so the door accepts neither as a caller assertion.
	initiatorPrincipal, err := channelspec.ResolveActorPrincipal(ctx, channelspec.ActorFacts{
		Principal: initiatorFacts.Principal, SourceDeclID: initiatorFacts.SourceDeclID,
		Kind: initiatorFacts.Kind, Active: active,
	}, func(ctx context.Context, sourceDeclID string) (string, bool, error) {
		if sourceDeclID == declID {
			return facts.OwnerPrincipal, true, nil
		}
		lookupCtx, lookupCancel := context.WithTimeout(ctx, introductionResolveTimeout)
		declaration, lookupErr := h.resolver.ResolveDeclaration(lookupCtx, h.channelID, sourceDeclID)
		lookupCancel()
		if errors.Is(lookupErr, channelspec.ErrDeclarationNotFound) {
			return "", false, nil
		}
		if lookupErr != nil {
			return "", false, lookupErr
		}
		return declaration.OwnerPrincipal, true, nil
	})
	if err != nil {
		return actorctl.IntroduceRequest{}, &channelspec.OperationError{
			Code: channelspec.ErrCodeAuthorityUnavailable, Detail: err.Error(), Retryable: true,
		}
	}
	if facts.Visibility != "public" &&
		(initiatorPrincipal == "" || initiatorPrincipal != facts.OwnerPrincipal) {
		return actorctl.IntroduceRequest{}, &channelspec.OperationError{
			Code: channelspec.ErrCodeForbidden, Detail: "declaration is private",
		}
	}

	kindCtx, kindCancel := context.WithTimeout(ctx, introductionResolveTimeout)
	kind, found, err := h.resolver.ClassKind(kindCtx, facts.Class)
	kindCancel()
	if err != nil {
		return actorctl.IntroduceRequest{}, &channelspec.OperationError{
			Code: channelspec.ErrCodeAuthorityUnavailable, Detail: err.Error(), Retryable: true,
		}
	}
	if !found || kind == actor.KindHuman || kind == actor.KindSystem {
		return actorctl.IntroduceRequest{}, &channelspec.OperationError{
			Code: channelspec.ErrCodeUnknownClass, Detail: "unknown class " + facts.Class,
		}
	}

	placementCtx, placementCancel := context.WithTimeout(ctx, introductionResolveTimeout)
	placementKind, found, err := h.resolver.ClassPlacement(placementCtx, facts.Class)
	placementCancel()
	if err != nil {
		return actorctl.IntroduceRequest{}, &channelspec.OperationError{Code: channelspec.ErrCodeAuthorityUnavailable, Detail: err.Error(), Retryable: true}
	}
	if !found {
		return actorctl.IntroduceRequest{}, &channelspec.OperationError{Code: channelspec.ErrCodeUnknownClass, Detail: "unknown class " + facts.Class}
	}
	var placement storespec.Placement
	switch placementKind {
	case channelspec.PlacementServer:
		// A server-placed class runs where the channel runs; there is no machine
		// to choose, so naming one is a misunderstanding worth saying out loud
		// rather than ignoring.
		if desiredHost != "" {
			return actorctl.IntroduceRequest{}, &channelspec.OperationError{
				Code:   channelspec.ErrCodeInvalidDesiredHost,
				Detail: "class " + facts.Class + " runs on the server, not on a device, so it cannot be given a desired_host",
			}
		}
		placement = storespec.NewServerPlacement()
	case channelspec.PlacementDaemon:
		placement, err = h.resolveDaemonPlacement(ctx, desiredHost)
	default:
		err = &channelspec.OperationError{Code: channelspec.ErrCodeUnknownClass, Detail: "class has invalid placement"}
	}
	if err != nil {
		return actorctl.IntroduceRequest{}, err
	}
	// The birth name comes from the declaration's name, never from its id: the
	// id may be opaque (a peer declaration is keyed by the target channel's
	// uuid) while the name is the readable word members address each other by.
	// An ordinary declaration birth carries no explicit principal. Declaration
	// ownership controls the recipe and can supply derived attribution where a
	// policy asks for it; it does not mean every instance of that recipe is the
	// owner's identity. Principal-bound agents enter only through a trusted
	// identity-carrying genesis/initial-seat path.
	return actorctl.IntroduceRequest{
		DeclID: declID, Seed: facts.Name, Kind: kind, Singleton: facts.Singleton, Placement: placement,
		Definition: storespec.ActorDefinition{
			Class:  facts.Class,
			Config: append(json.RawMessage(nil), facts.Config...),
		},
	}, nil
}

// resolveDaemonPlacement settles which machine runs a daemon-placed member.
//
// A named host is CHECKED, never trusted: it must be a device bound to this
// channel, or the seating is refused. Naming a device the channel cannot reach
// would produce a member that is declared, placed, and permanently absent.
//
// An unnamed host resolves to the node's local device, which is the same
// default channel genesis applies (platform/lagoon: a rendered daemon
// placement starts at channelspec.LocalDeviceID). Runtime seating used to take
// the FIRST BOUND device instead, and that was a guess dressed as a default:
// binding order carries no intent, so on a channel with a laptop and a phone
// attached the answer depended on which had been attached first. Naming the
// local device instead is a default the caller can predict and the node
// guarantees, and it keeps one rule for both admission paths.
//
// When even that is unavailable — the local device is not bound here and more
// than one remote device is — there is no defensible pick, so the seating is
// refused and the candidates are named. Refusing is cheap; a member silently
// seated on the wrong person's laptop is not.
func (h *Home) resolveDaemonPlacement(ctx context.Context, desiredHost string) (storespec.Placement, error) {
	bound, err := h.registryBindings.ListBoundDeviceIDs(ctx, h.channelID)
	if err != nil {
		return storespec.Placement{}, err
	}
	if len(bound) == 0 {
		return storespec.Placement{}, &channelspec.OperationError{
			Code: channelspec.ErrCodeInvalidDesiredHost, Detail: "daemon is not bound to this channel",
		}
	}
	if desiredHost == "" {
		return h.defaultDaemonPlacement(bound)
	}
	for _, id := range bound {
		if id == desiredHost {
			return storespec.NewDaemonPlacement(desiredHost)
		}
	}
	return storespec.Placement{}, &channelspec.OperationError{
		Code:   channelspec.ErrCodeInvalidDesiredHost,
		Detail: "device " + desiredHost + " is not attached to this channel; attach it with system.device.attach, or list this channel's devices and name one of those",
	}
}

// defaultDaemonPlacement answers the unnamed case against a non-empty bound
// set: the local device when this channel has it, the sole device when there is
// exactly one, and otherwise nothing — because any pick among peers would be
// arbitrary and the caller is the only one who knows which machine they meant.
func (h *Home) defaultDaemonPlacement(bound []string) (storespec.Placement, error) {
	for _, id := range bound {
		if id == channelspec.LocalDeviceID {
			return storespec.NewDaemonPlacement(id)
		}
	}
	if len(bound) == 1 {
		return storespec.NewDaemonPlacement(bound[0])
	}
	return storespec.Placement{}, &channelspec.OperationError{
		Code: channelspec.ErrCodeInvalidDesiredHost,
		Detail: "this channel has " + strconv.Itoa(len(bound)) + " devices attached and none of them is the node's local device, so there is no default: name one with desired_host (" + strings.Join(bound, ", ") + ")",
	}
}
