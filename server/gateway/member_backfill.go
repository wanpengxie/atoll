package gateway

import (
	"context"
	"fmt"
	"time"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/placement"
)

func (a *App) syncActiveChannelMembers(ctx context.Context) error {
	active, err := a.placements.ListByState(ctx, placement.StateActive)
	if err != nil {
		return err
	}
	var firstErr error
	for _, p := range active {
		if err := a.syncPlacementMembers(ctx, p); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (a *App) syncChannelMembersForDaemon(ctx context.Context, daemonID placement.DaemonID) error {
	active, err := a.placements.ListByState(ctx, placement.StateActive)
	if err != nil {
		return err
	}
	var firstErr error
	for _, p := range active {
		if p.DaemonID != daemonID {
			continue
		}
		if err := a.syncPlacementMembers(ctx, p); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (a *App) syncPlacementMembers(ctx context.Context, p placement.Placement) error {
	members, err := a.catalog.ListChannelMembers(ctx, string(p.ChannelID))
	if err != nil {
		return fmt.Errorf("gateway: list channel members for %s: %w", p.ChannelID, err)
	}
	if len(members) == 0 {
		return nil
	}
	if err := a.OnChannelMembersChanged(ctx, string(p.ChannelID), members, nil); err != nil {
		return fmt.Errorf("gateway: replay channel members for %s: %w", p.ChannelID, err)
	}
	return nil
}

func (a *App) syncChannelMembersForDaemonAsync(daemonID placement.DaemonID) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := a.syncChannelMembersForDaemon(ctx, daemonID); err != nil {
			pkgLogger.Warn().Err(err).
				Str("event", "catalog.member_backfill_failed_after_register").
				Str("daemon_id", string(daemonID)).
				Msg("catalog member backfill failed after daemon register")
		}
	}()
}

func (a *App) syncChannelMembersForChannelAsync(channelID string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		p, ok, err := a.placements.Get(ctx, channel.ID(channelID))
		if err != nil || !ok || p.State != placement.StateActive {
			if err != nil {
				pkgLogger.Warn().Err(err).
					Str("event", "catalog.member_backfill_failed_after_held_report").
					Str("channel_id", channelID).
					Msg("catalog member backfill placement lookup failed")
			}
			return
		}
		if err := a.syncPlacementMembers(ctx, p); err != nil {
			pkgLogger.Warn().Err(err).
				Str("event", "catalog.member_backfill_failed_after_held_report").
				Str("channel_id", channelID).
				Msg("catalog member backfill failed after held report")
		}
	}()
}
