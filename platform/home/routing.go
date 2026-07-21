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
	if defaultID != "" {
		if _, live := h.View().Stat(defaultID); live {
			env.Audience = message.Audience{defaultID}
			env.Kind = message.KindRequest
			return nil
		}
	}
	// A configured default is an explicit routing decision. If its current
	// embodiment is unavailable, fail this request without silently changing
	// the destination to boost (or a human broadcast). Boost is only the
	// fallback for channels that have no configured default.
	if hasDefault {
		return ErrRoutingUnavailable
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
	rows, err := h.View().ListActors(ctx)
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
