package home

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/protocol/message"
)

const defaultRoutingAgentSource = "sys:boost"

var ErrRoutingUnavailable = errors.New("channel routing unavailable")

// ErrBoostMissing is the fixed error routing returns when the fallback terminus
// itself is gone. boost carries NO protection gate: it can be ended like any
// other actor (including by its own EndSelf), and nothing resurrects it. When
// it is absent the channel has no fallback destination left, so the message is
// refused loudly rather than silently swallowed or fanned out somewhere else.
// Recovery is an explicit management action.
var ErrBoostMissing = errors.New(
	"channel routing unavailable: boost has been terminated — re-declare boost or restart the channel")

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
	boosts, err := h.View().DeclaredInstances(ctx, defaultRoutingAgentSource)
	if err != nil {
		return err
	}
	if len(boosts) == 0 {
		return ErrBoostMissing
	}
	boostID := boosts[0]
	if _, live := h.View().Stat(boostID); !live {
		return ErrRoutingUnavailable
	}
	env.Audience = message.Audience{boostID}
	env.Kind = message.KindRequest
	return nil
}
