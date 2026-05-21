package channelaccess

import (
	"context"
	"errors"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// ErrDenied is returned when a route cannot prove channel membership.
var ErrDenied = errors.New("channelaccess: access denied")

// Authorizer validates that an authenticated user may access a channel.
type Authorizer interface {
	AuthorizeChannelAccess(ctx context.Context, channelID, userID string) error
}

// MemberActorResolver resolves the channel-local actor_id for an
// authenticated human user. Visibility checks must use actor_id, not the
// global user_id.
type MemberActorResolver interface {
	MemberActorID(ctx context.Context, channelID, userID string) (string, error)
}

// Require fails closed when no authorizer is wired or membership lookup fails.
func Require(ctx context.Context, auth Authorizer, channelID, userID string) error {
	if auth == nil {
		return ErrDenied
	}
	if err := auth.AuthorizeChannelAccess(ctx, channelID, userID); err != nil {
		return ErrDenied
	}
	return nil
}

// RequireMemberActor returns the caller's channel-local actor_id after
// proving channel membership.
func RequireMemberActor(ctx context.Context, auth Authorizer, channelID, userID string) (string, error) {
	if err := Require(ctx, auth, channelID, userID); err != nil {
		return "", err
	}
	resolver, ok := auth.(MemberActorResolver)
	if !ok {
		return "", ErrDenied
	}
	actorID, err := resolver.MemberActorID(ctx, channelID, userID)
	if err != nil || actorID == "" {
		return "", ErrDenied
	}
	return actorID, nil
}

// VisibleToActor is coagent's default user-facing projection policy for
// cached history and WS fan-out.
func VisibleToActor(env message.Envelope, memberActorID string) bool {
	switch env.Visibility {
	case message.VisibilityPublic:
		return true
	case message.VisibilityPrivate:
		target := actor.ActorID(memberActorID)
		if env.Sender.ID == target {
			return true
		}
		for _, audienceID := range env.Audience {
			if audienceID == target {
				return true
			}
		}
		return false
	case message.VisibilitySystem:
		return false
	default:
		return false
	}
}
