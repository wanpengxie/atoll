package lifecycle

import (
	"context"
	"errors"
	"fmt"

	"github.com/coagent-ai/coagent/kernel/channel"
)

// UnloadReason classifies why a channel is being unloaded.
type UnloadReason string

const (
	UnloadIdle   UnloadReason = "idle"   // long inactivity → free resources
	UnloadOrphan UnloadReason = "orphan" // server marked orphan
	UnloadStale  UnloadReason = "stale"  // server marked stale
)

// Unloader closes a channel's in-memory state (open *sql.DB, push pump,
// scheduler timers). The actual close is performed by callbacks the
// caller registers — Unloader just orchestrates the invocations and
// records the reason.
type Unloader struct {
	closeFns map[channel.ID][]func() error
}

// NewUnloader returns an Unloader.
func NewUnloader() *Unloader {
	return &Unloader{closeFns: make(map[channel.ID][]func() error)}
}

// Register adds a teardown function to be called on Unload.
func (u *Unloader) Register(channelID channel.ID, fn func() error) {
	if fn == nil {
		return
	}
	u.closeFns[channelID] = append(u.closeFns[channelID], fn)
}

// Unload runs all registered teardown functions for the channel and
// then forgets them. Errors are collected and returned as a single
// joined error.
func (u *Unloader) Unload(ctx context.Context, channelID channel.ID, reason UnloadReason) error {
	fns, ok := u.closeFns[channelID]
	if !ok {
		return nil
	}
	delete(u.closeFns, channelID)
	var errs []error
	for i := len(fns) - 1; i >= 0; i-- {
		if err := fns[i](); err != nil {
			errs = append(errs, fmt.Errorf("lifecycle: unload %s (reason=%s): %w", channelID, reason, err))
		}
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
	}
	return errors.Join(errs...)
}
