package app

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

// human.go carries the app-layer HUMAN ROUTING POLICY — the one product-domain
// decision the substrate deliberately leaves to the host: given a raw client
// intent with no explicit audience, WHO should it go to (the channel's default
// agent / a boost floor / a group broadcast) and as WHICH kind. This is domain
// policy (default_agent / boost-floor failover / group-chat), reading the app DB
// and Home.View (live roster) — actors/ and platform/ must not carry it.
//
// The WRITE itself is NOT here: a person writes truth ONLY through the subjectgate
// frame path onto their own cell's welded pen (a submit frame → the cell's
// identity-dimension Sys verb), never through an app-held write gate (sealed-pen:
// the app holds no Minter, no bare Pen). This file resolves routing and hands the
// result to the gateway (via ResolveRoutingForGateway); the cell welds identity
// and commits.

// submitInput is the user's RAW intent (the parsed HTTP body) — NOT a ready
// envelope. Routing resolution turns its (possibly empty) audience into a
// concrete audience + kind; identity is left for the door's pen to weld.
type submitInput struct {
	ID         string
	Type       string
	Kind       string
	Payload    json.RawMessage
	Audience   []string
	Visibility string
	ParentID   string
}

// routingError carries a per-request routing condition (no reachable brain / a
// down default agent) back to the HTTP layer as a 503 to the SENDING user — it is
// NOT written into the channel as truth. (errors.As-friendly.)
type routingError struct {
	detail string
}

func (e *routingError) Error() string { return e.detail }

// ResolveRoutingForGateway is the gateway 期 S3 injection adapter (design §5.3:
// routing政策留 app, 组装根注入路由解析面). The gateway core (drivers/gateway) holds
// this method value as its Routing面; the assembly root bridges it (app → drivers is
// fenced). It wraps resolveRouting, mapping a per-request routingError into the
// retryable-detail return so the gateway emits an unavailable error frame (never
// writing the condition as truth), and a genuine failure into err. audienceIn is the
// client's explicit audience (empty → policy resolves it).
func (a *App) ResolveRoutingForGateway(ctx context.Context, chID channel.ID, audienceIn []actor.ActorID, kindIn message.Kind) ([]actor.ActorID, message.Kind, string, error) {
	in := submitInput{Kind: string(kindIn)}
	for _, id := range audienceIn {
		in.Audience = append(in.Audience, string(id))
	}
	audience, kind, err := a.resolveRouting(ctx, chID, in)
	if err != nil {
		var re *routingError
		if errors.As(err, &re) {
			return nil, kind, re.detail, nil
		}
		return nil, kind, "", err
	}
	return audience, kind, "", nil
}

// resolveRouting reproduces the channel's no-audience routing policy: an explicit
// audience is honoured as-is; otherwise default_agent is the INTENT pointer,
// resolved against live PRESENCE (View.Stat, the substrate's authoritative
// embodiment self-read) with the agent:boost floor as the failover target:
//   - default_agent points at a PRESENT agent   → agent-centric: request to it.
//   - else the channel HAS a boost floor:
//     boost present → failover to boost;  boost absent → channel CANNOT serve (503).
//   - else (no boost AND no default set)         → group-chat: broadcast to humans.
//   - else (no boost, default set but absent)    → no reachable brain (503).
//
// Liveness is PRESENCE not membership (§4.5 axes must not cross-train): a
// brain-dead default (member registry still lists it, but its cell has no live
// embodiment) is 503, never a silent 201 into a black hole.
//
// "cannot serve" / "no brain" are per-request conditions for the SENDING user —
// returned as a routingError, NEVER written as a channel envelope.
func (a *App) resolveRouting(ctx context.Context, chID channel.ID, in submitInput) ([]actor.ActorID, message.Kind, error) {
	audience := make([]actor.ActorID, 0, len(in.Audience))
	for _, id := range in.Audience {
		audience = append(audience, actor.ActorID(id))
	}
	kind := message.Kind(in.Kind)
	if len(audience) > 0 {
		return audience, kind, nil
	}

	home := a.getHome(chID)
	if home == nil {
		return nil, kind, &routingError{detail: "channel unavailable"}
	}
	view := home.View()

	var da string
	_ = a.db.QueryRowContext(ctx,
		`SELECT COALESCE(default_agent, '') FROM channels WHERE id = ?`, string(chID)).Scan(&da)

	daLive := false
	if da != "" {
		_, daLive = view.Stat(actor.ActorID(da))
	}
	if daLive {
		return []actor.ActorID{actor.ActorID(da)}, message.KindRequest, nil
	}

	var boostID string
	_ = a.db.QueryRowContext(ctx,
		`SELECT instance_id FROM channel_actors WHERE channel_id=? AND principal=?`, string(chID), defaultAgentPrincipal).Scan(&boostID)
	_, boostLive := view.Stat(actor.ActorID(boostID))
	hasBoost, berr := a.channelHasInstance(ctx, string(chID), boostID)
	if berr != nil {
		return nil, kind, berr
	}
	switch {
	case hasBoost && boostLive:
		return []actor.ActorID{actor.ActorID(boostID)}, message.KindRequest, nil
	case hasBoost:
		// boost floor introduced but its cell is not present → channel cannot serve.
		return nil, kind, &routingError{detail: "the channel's default/fallback agent is down"}
	case da == "":
		// no floor + no default → pure group-chat: broadcast to human MEMBERS
		// (membership axis, not presence — an event lands in each human's log inbox).
		actors, lerr := view.ListActors(ctx)
		if lerr != nil {
			// fail closed: do not silently downgrade routing on a transient view failure.
			return nil, kind, lerr
		}
		for _, ac := range actors {
			if ac.Kind == actor.KindHuman {
				audience = append(audience, ac.ID)
			}
		}
		return audience, message.KindEvent, nil
	default:
		// da was set but its brain is absent, and no boost floor exists.
		return nil, kind, &routingError{detail: "the channel's default agent is down and no fallback is configured"}
	}
}
