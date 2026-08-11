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
	principal string,
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
	// A non-public declaration is its owner's to place, and ownership is a
	// principal fact. The initiator's principal is read here, from the roster —
	// the door does not get to assert it. An initiator that carries no principal
	// (every non-human admission: the store forbids them one) can therefore own
	// nothing, and the empty-vs-empty comparison that would otherwise admit it
	// is refused explicitly rather than left to depend on space-side invariants
	// keeping every declaration owner non-empty.
	if facts.Visibility != "public" &&
		(initiatorFacts.Principal == "" || initiatorFacts.Principal != facts.OwnerPrincipal) {
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

	placement, err := h.resolveDaemonPlacement(ctx)
	if err != nil {
		return actorctl.IntroduceRequest{}, err
	}
	if principal != "" && kind != actor.KindAgent {
		return actorctl.IntroduceRequest{}, &channelspec.OperationError{Code: channelspec.ErrCodeBadPayload, Detail: "only an agent declaration may carry principal"}
	}
	return actorctl.IntroduceRequest{
		DeclID: declID, Kind: kind, Principal: principal, Placement: placement,
		Definition: storespec.ActorDefinition{
			Class:  facts.Class,
			Config: append(json.RawMessage(nil), facts.Config...),
		},
	}, nil
}

// resolveDaemonPlacement picks the placement host for a declaration-backed
// actor: the first bound daemon.
func (h *Home) resolveDaemonPlacement(ctx context.Context) (storespec.Placement, error) {
	bound, err := h.registryBindings.ListBoundDevices(ctx, h.channelID)
	if err != nil {
		return storespec.Placement{}, err
	}
	if len(bound) == 0 {
		return storespec.Placement{}, &channelspec.OperationError{
			Code: channelspec.ErrCodeInvalidDesiredHost, Detail: "daemon is not bound to this channel",
		}
	}
	return storespec.NewDaemonPlacement(bound[0].ID)
}
