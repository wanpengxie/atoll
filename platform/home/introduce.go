package home

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// resolveIntroduction is the command-front business resolution for one
// declaration admission: fetch the declaration, judge its visibility against
// the initiator, resolve the class kind, and pick the placement host. Policy
// lives here, at the door; the Controller command that follows is mechanical.
func (h *Home) resolveIntroduction(
	ctx context.Context,
	declID string,
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
		placement = storespec.NewServerPlacement()
	case channelspec.PlacementDaemon:
		placement, err = h.resolveDaemonPlacement(ctx)
	default:
		err = &channelspec.OperationError{Code: channelspec.ErrCodeUnknownClass, Detail: "class has invalid placement"}
	}
	if err != nil {
		return actorctl.IntroduceRequest{}, err
	}
	principal := ""
	if kind == actor.KindAgent {
		principal = facts.OwnerPrincipal
	}
	// The birth name comes from the declaration's name, never from its id: the
	// id may be opaque (a peer declaration is keyed by the target channel's
	// uuid) while the name is the readable word members address each other by.
	return actorctl.IntroduceRequest{
		DeclID: declID, Seed: facts.Name, Kind: kind, Principal: principal, Singleton: facts.Singleton, Placement: placement,
		Definition: storespec.ActorDefinition{
			Class:  facts.Class,
			Config: append(json.RawMessage(nil), facts.Config...),
		},
	}, nil
}

// resolveDaemonPlacement picks the placement host for a declaration-backed
// actor: the first bound daemon.
func (h *Home) resolveDaemonPlacement(ctx context.Context) (storespec.Placement, error) {
	bound, err := h.registryBindings.ListBoundDeviceIDs(ctx, h.channelID)
	if err != nil {
		return storespec.Placement{}, err
	}
	if len(bound) == 0 {
		return storespec.Placement{}, &channelspec.OperationError{
			Code: channelspec.ErrCodeInvalidDesiredHost, Detail: "daemon is not bound to this channel",
		}
	}
	return storespec.NewDaemonPlacement(bound[0])
}
