package home

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

const defaultRoutingAgentSource = "sys:boost"

var ErrRoutingUnavailable = errors.New("channel routing unavailable")

func (h *Home) resolveAudience(ctx context.Context, env *message.Envelope) error {
	defaultID, hasDefault, err := h.View().DefaultAgent(ctx)
	if err != nil {
		return err
	}
	if hasDefault && defaultID != "" {
		// The pointer is channel configuration, never a dead actor's belonging,
		// so terminal leaves it dangling on purpose. A pointer at a
		// deregistered actor reads as UNCONFIGURED and falls back to boost; a
		// pointer at a member who is merely absent right now keeps refusing, so
		// "a configured default never silently changes destination" still holds
		// for the case it actually protects.
		member, memberErr := h.actors.IsActive(ctx, defaultID)
		if memberErr != nil {
			return memberErr
		}
		if member {
			if _, live := h.View().Stat(defaultID); live {
				env.Audience = message.Audience{defaultID}
				env.Kind = message.KindRequest
				return nil
			}
			return ErrRoutingUnavailable
		}
	}
	boost, hasBoost, err := h.View().DeclaredBySourceOne(ctx, defaultRoutingAgentSource)
	if err != nil {
		return err
	}
	if hasBoost {
		if _, live := h.View().Stat(boost.ID); live {
			env.Audience = message.Audience{boost.ID}
			env.Kind = message.KindRequest
			return nil
		}
		return ErrRoutingUnavailable
	}
	rows, err := h.View().ActiveActors(ctx)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.Kind == actor.KindHuman {
			env.Audience = append(env.Audience, row.ID)
		}
	}
	if len(env.Audience) == 0 {
		return ErrRoutingUnavailable
	}
	env.Kind = message.KindEvent
	return nil
}
