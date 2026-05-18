package channelaccess

import (
	"context"
	"errors"
)

// ErrDenied is returned when a route cannot prove channel membership.
var ErrDenied = errors.New("channelaccess: access denied")

// Authorizer validates that an authenticated user may access a channel.
type Authorizer interface {
	AuthorizeChannelAccess(ctx context.Context, channelID, userID string) error
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
