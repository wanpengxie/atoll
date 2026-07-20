package app

import (
	"context"
	"time"

	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// EntitlementRoute is a durable human membership route. Observer traffic uses
// the per-channel SSE/HTTP read plane and never appears in the gateway route set.
type EntitlementRoute struct {
	Channel   channel.ID
	Home      *home.Home // migrated to channelhost.GatewayHitch in S4
	Access    string
	SubjectID actor.ActorID
}

// reconcilePrincipalChannel maintains the realm-side, rebuildable membership
// projection from membrane truth. A missing directory row suppresses the
// pre-publication genesis poke; publish writes the owner projection atomically.
func (a *App) reconcilePrincipalChannel(ctx context.Context, chID channel.ID, principal string) {
	if !a.channelExists(ctx, string(chID)) {
		return
	}
	h := a.getHome(chID)
	if h == nil {
		return
	}
	id, found, err := h.ResolvePrincipal(ctx, actor.KindHuman, principal)
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
		h := a.getHome(membership.channel)
		if h == nil {
			failed = append(failed, membership.channel)
			continue
		}
		id, found, err := h.ResolvePrincipal(ctx, actor.KindHuman, principal)
		if err != nil {
			failed = append(failed, membership.channel)
			continue
		}
		if !found || id != membership.actor {
			// Projection over-grant is never trusted. Repair it and omit the route.
			a.reconcilePrincipalChannel(ctx, membership.channel, principal)
			continue
		}
		routes = append(routes, EntitlementRoute{Channel: membership.channel, Home: h, Access: "member", SubjectID: id})
	}
	return routes, failed, nil
}
