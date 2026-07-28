package app

import (
	"context"
	"time"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// EntitlementRoute is a durable human membership route. Observer traffic uses
// the per-channel SSE/HTTP read plane and never appears in the gateway route set.
type EntitlementRoute struct {
	Channel   channel.ID
	Bundle    channelhost.Bundle
	SubjectID actor.ActorID
}

// reconcilePrincipalChannel maintains the realm-side, rebuildable membership
// projection from membrane truth. A missing directory row suppresses the
// pre-publication genesis poke; publish writes the owner projection atomically.
func (a *App) reconcilePrincipalChannel(ctx context.Context, chID channel.ID, principal string) {
	exists, err := a.channelExists(ctx, string(chID))
	if err != nil {
		a.logger.Warn("membership projection directory read failed", "channel", chID, "principal", principal, "err", err)
		return
	}
	if !exists {
		return
	}
	bundle, ok := a.host.Acquire(chID)
	if !ok {
		return
	}
	id, found, err := bundle.View().ResolvePrincipal(ctx, actor.KindHuman, principal)
	if err != nil {
		a.logger.Warn("membership projection reconcile failed", "channel", chID, "principal", principal, "err", err)
		return
	}
	if found {
		_, err = a.db.ExecContext(ctx, `INSERT INTO principal_channels(principal,channel_id,actor_id,updated_at)
			VALUES (?,?,?,?) ON CONFLICT(principal,channel_id) DO UPDATE SET actor_id=excluded.actor_id,updated_at=excluded.updated_at`,
			principal, string(chID), string(id), time.Now().UnixMilli())
	} else {
		_, err = a.db.ExecContext(ctx, `DELETE FROM principal_channels WHERE principal=? AND channel_id=?`, principal, string(chID))
	}
	if err != nil {
		a.logger.Warn("membership projection write failed", "channel", chID, "principal", principal, "err", err)
	}
}

// sweepMembershipProjection is the projection's low-frequency third maintenance
// layer (after same-transaction writes and targeted pokes): orphan rows whose
// channel left the directory are dropped, then every serving channel's
// KindHuman roster is two-way diffed against principal_channels. It repairs
// drift no event will revisit — a poke that fired while the channel was
// unavailable, or a member row nothing has listed since. Closed channels
// cannot change membership, so serving channels plus the boot pass are
// complete coverage; the boot pass doubles as the rebuild path.
func (a *App) sweepMembershipProjection(ctx context.Context) {
	if _, err := a.db.ExecContext(ctx, `DELETE FROM principal_channels WHERE channel_id NOT IN (SELECT id FROM channels)`); err != nil {
		a.logger.Warn("membership sweep orphan cleanup failed", "err", err)
	}
	ids, err := a.directoryChannelIDs(ctx)
	if err != nil {
		a.logger.Warn("membership sweep directory read failed", "err", err)
		return
	}
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		a.sweepChannelMembership(ctx, id)
	}
}

// sweepChannelMembership serializes with admission-driven membership writes via
// the per-channel lock, so a join committing mid-diff cannot be deleted as stale.
func (a *App) sweepChannelMembership(ctx context.Context, chID channel.ID) {
	release := a.channelLocks.lock(string(chID))
	defer release()
	bundle, ok := a.host.Acquire(chID)
	if !ok {
		return
	}
	roster, err := bundle.View().HumanRoster(ctx)
	if err != nil {
		a.logger.Warn("membership sweep roster read failed", "channel", chID, "err", err)
		return
	}
	truth := make(map[string]string)
	for _, entry := range roster {
		truth[entry.Principal] = string(entry.ActorID)
	}
	projRows, err := a.db.QueryContext(ctx, `SELECT principal,actor_id FROM principal_channels WHERE channel_id=?`, string(chID))
	if err != nil {
		a.logger.Warn("membership sweep projection read failed", "channel", chID, "err", err)
		return
	}
	projected := make(map[string]string)
	for projRows.Next() {
		var principal, actorID string
		if err := projRows.Scan(&principal, &actorID); err != nil {
			projRows.Close()
			a.logger.Warn("membership sweep projection read failed", "channel", chID, "err", err)
			return
		}
		projected[principal] = actorID
	}
	projRows.Close()
	if err := projRows.Err(); err != nil {
		a.logger.Warn("membership sweep projection read failed", "channel", chID, "err", err)
		return
	}
	now := time.Now().UnixMilli()
	for principal, actorID := range truth {
		if projected[principal] == actorID {
			continue
		}
		if _, err := a.db.ExecContext(ctx, `INSERT INTO principal_channels(principal,channel_id,actor_id,updated_at)
			VALUES (?,?,?,?) ON CONFLICT(principal,channel_id) DO UPDATE SET actor_id=excluded.actor_id,updated_at=excluded.updated_at`,
			principal, string(chID), actorID, now); err != nil {
			a.logger.Warn("membership sweep write failed", "channel", chID, "principal", principal, "err", err)
		}
	}
	for principal := range projected {
		if _, live := truth[principal]; live {
			continue
		}
		if _, err := a.db.ExecContext(ctx, `DELETE FROM principal_channels WHERE principal=? AND channel_id=?`, principal, string(chID)); err != nil {
			a.logger.Warn("membership sweep delete failed", "channel", chID, "principal", principal, "err", err)
		}
	}
}

func (a *App) EntitlementSnapshot(ctx context.Context, principal string) ([]EntitlementRoute, []channel.ID, error) {
	rows, err := a.db.QueryContext(ctx, `SELECT pc.channel_id,pc.actor_id
		FROM principal_channels pc JOIN channels c ON c.id=pc.channel_id
		WHERE pc.principal=? ORDER BY pc.channel_id`, principal)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	type membership struct {
		channel channel.ID
		actor   actor.ActorID
	}
	var memberships []membership
	for rows.Next() {
		var ch, id string
		if err := rows.Scan(&ch, &id); err != nil {
			return nil, nil, err
		}
		memberships = append(memberships, membership{channel.ID(ch), actor.ActorID(id)})
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	routes := make([]EntitlementRoute, 0, len(memberships))
	var failed []channel.ID
	for _, membership := range memberships {
		bundle, ok := a.host.Acquire(membership.channel)
		if !ok {
			failed = append(failed, membership.channel)
			continue
		}
		id, found, err := bundle.View().ResolvePrincipal(ctx, actor.KindHuman, principal)
		if err != nil {
			failed = append(failed, membership.channel)
			continue
		}
		if !found || id != membership.actor {
			// Projection over-grant is never trusted. Repair it and omit the route.
			a.reconcilePrincipalChannel(ctx, membership.channel, principal)
			continue
		}
		routes = append(routes, EntitlementRoute{Channel: membership.channel, Bundle: bundle, SubjectID: id})
	}
	return routes, failed, nil
}
