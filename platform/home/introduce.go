package home

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
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
		return actorctl.IntroduceRequest{}, &channel.OperationError{
			Code: channel.ErrCodeBadPayload, Detail: "decl_id and initiator_actor_id required",
		}
	}
	if h.resolver == nil {
		return actorctl.IntroduceRequest{}, &channel.OperationError{
			Code: channel.ErrCodeAuthorityUnavailable, Detail: "introduction resolver unavailable", Retryable: true,
		}
	}
	initiatorFacts, active, err := h.actors.ActorFacts(ctx, initiator)
	if err != nil {
		return actorctl.IntroduceRequest{}, err
	}
	if !active {
		return actorctl.IntroduceRequest{}, &channel.OperationError{
			Code: channel.ErrCodeMemberInactive, Detail: "initiator is not an active member",
		}
	}

	resolveCtx, cancel := context.WithTimeout(ctx, introductionResolveTimeout)
	facts, err := h.resolver.ResolveDeclaration(resolveCtx, h.channelID, declID)
	cancel()
	if err != nil {
		code, retryable := channel.ErrCodeAuthorityUnavailable, true
		if errors.Is(err, channel.ErrDeclarationNotFound) {
			code, retryable = channel.ErrCodeDeclNotFound, false
		}
		return actorctl.IntroduceRequest{}, &channel.OperationError{
			Code: code, Detail: err.Error(), Retryable: retryable,
		}
	}
	// A non-public declaration is its owner's to place, and ownership is a
	// principal fact. The initiator's principal is read here, from the roster —
	// the door does not get to assert it. An initiator that carries no principal
	// (every non-human admission: the store forbids them one) can therefore own
	// nothing, and the empty-vs-empty comparison that would otherwise admit it
	// is refused explicitly rather than left to depend on realm-side invariants
	// keeping every declaration owner non-empty.
	if facts.Visibility != "public" &&
		(initiatorFacts.Principal == "" || initiatorFacts.Principal != facts.OwnerPrincipal) {
		return actorctl.IntroduceRequest{}, &channel.OperationError{
			Code: channel.ErrCodeForbidden, Detail: "declaration is private",
		}
	}

	kindCtx, kindCancel := context.WithTimeout(ctx, introductionResolveTimeout)
	kind, found, err := h.resolver.ClassKind(kindCtx, facts.Class)
	kindCancel()
	if err != nil {
		return actorctl.IntroduceRequest{}, &channel.OperationError{
			Code: channel.ErrCodeAuthorityUnavailable, Detail: err.Error(), Retryable: true,
		}
	}
	if !found || kind == actor.KindHuman || kind == actor.KindSystem {
		return actorctl.IntroduceRequest{}, &channel.OperationError{
			Code: channel.ErrCodeUnknownClass, Detail: "unknown class " + facts.Class,
		}
	}

	placement, err := h.resolveDaemonPlacement(ctx)
	if err != nil {
		return actorctl.IntroduceRequest{}, err
	}
	return actorctl.IntroduceRequest{
		DeclID: declID, Kind: kind, Placement: placement,
		Definition: storespec.ActorDefinition{
			Class:  facts.Class,
			Config: append(json.RawMessage(nil), facts.Config...),
		},
	}, nil
}

// resolveDaemonPlacement picks the placement host for a declaration-backed
// actor: the first bound daemon.
func (h *Home) resolveDaemonPlacement(ctx context.Context) (storespec.Placement, error) {
	bound, err := h.bindings.ListBoundDaemons(ctx)
	if err != nil {
		return storespec.Placement{}, err
	}
	if len(bound) == 0 {
		return storespec.Placement{}, &channel.OperationError{
			Code: channel.ErrCodeInvalidDesiredHost, Detail: "daemon is not bound to this channel",
		}
	}
	return storespec.NewDaemonPlacement(string(bound[0]))
}
