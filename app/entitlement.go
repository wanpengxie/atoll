package app

import (
	"context"

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

func (a *App) EntitlementSnapshot(ctx context.Context, principal string) ([]EntitlementRoute, []channel.ID, error) {
	rows, err := a.db.QueryContext(ctx, `SELECT pc.channel_id,pc.actor_id
		FROM principal_channels pc JOIN channels c ON c.id=pc.channel_id
		WHERE pc.principal=? AND c.status='present' ORDER BY pc.channel_id`, principal)
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
		id, found, err := bundle.View().ResolvePrincipal(ctx, principal)
		if err != nil {
			failed = append(failed, membership.channel)
			continue
		}
		if !found || id != membership.actor {
			// Projection over-grant is never trusted. Repair it and omit the route.
			repairID := id
			if !found {
				repairID = membership.actor
			}
			if err := a.relations.ReconcilePrincipal(ctx, membership.channel, principal, repairID, found); err != nil {
				a.logger.Warn("membership relation repair failed", "channel", membership.channel, "principal", principal, "err", err)
			}
			continue
		}
		routes = append(routes, EntitlementRoute{Channel: membership.channel, Bundle: bundle, SubjectID: id})
	}
	return routes, failed, nil
}
