package app

import (
	"context"
	"errors"
	"time"

	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// EntitlementRoute is one channel a principal is currently entitled to (连接模型勘误期
// §3.2 解析面 输出定形), in APP-OWNED DTO form. The gateway's EntitlementResolver seam
// is defined in drivers/gateway and consumes gateway.Route; app → drivers is fenced, so
// the app returns its OWN shape and the assembly root (cmd/server) bridges DTO→DTO
// (照 WSGateway 既有形). Access is the flat class the bridge maps to gateway.AccessClass.
type EntitlementRoute struct {
	Channel   channel.ID
	Home      *home.Home
	Access    string // "member" (户籍) | "observer" (workspace 观众/读资格)
	SubjectID actor.ActorID // member only; empty for an observer
	CheckedAt time.Time
}

// EntitlementFailure carries a per-channel resolution failure (spec §3.2 部分失败
// 语义): "查得坏消息" for exactly this channel — the gateway serves it from the last
// good snapshot within T_stale, then pauses. Distinct from a channel simply absent
// from routes (= confirmed no eligibility, retire now).
type EntitlementFailure struct {
	Channel channel.ID
	Err     error
}

// errChannelHomeNotOpen is the per-channel transient: the directory lists the channel
// (the principal is a workspace member) but its universe is not open yet (getHome==nil)
// — a retryable "unavailable", rides the lease, never a confirmed "no eligibility".
var errChannelHomeNotOpen = errors.New("app: channel home not open")

// EntitlementSnapshot resolves the full set of channels principal is currently
// entitled to (连接模型勘误期 §3.2 解析面): every channel in a workspace the principal
// belongs to (workspace member = 观众/observer 读资格), upgraded to member (户籍/write
// eligibility) when the channel's Home resolves the principal to an active subject.
// The gateway injects this (bridged) and calls it live per frame/batch — it is the
// function of 户籍/ACL truth, not a connection attribute.
//
//   - routes: every entitled channel (member ∪ observer). A channel absent from BOTH
//     routes and failed = confirmed no eligibility → the gateway retires it immediately.
//   - failed: per-channel query failure (or home-not-open transient) → rides T_stale.
//   - err: a whole-snapshot failure (the directory query itself) → the entire prior
//     snapshot rides its lease.
func (a *App) EntitlementSnapshot(ctx context.Context, principal string) ([]EntitlementRoute, []EntitlementFailure, error) {
	// Every channel in any workspace the principal is a member of (observer candidates).
	rows, err := a.db.QueryContext(ctx,
		`SELECT c.id FROM channels c
		 JOIN workspace_members wm ON wm.workspace_id = c.workspace_id
		 WHERE wm.user_id = ?`, principal)
	if err != nil {
		return nil, nil, err
	}
	var chIDs []string
	for rows.Next() {
		var id string
		if scanErr := rows.Scan(&id); scanErr != nil {
			// P2-10 (六轮终审): a row-scan failure gives no channel id to attribute a
			// ChannelFailure to, so silently `continue`-ing here does not degrade that
			// one channel to "查得坏消息" (failed, rides T_stale) — it makes it vanish
			// from BOTH routes and failed, which the gateway reads as "confirmed no
			// eligibility" and retires IMMEDIATELY (表①). A directory read this broken
			// cannot be trusted to have enumerated the full channel set either, so this
			// escalates to a whole-snapshot failure (rides the lease) rather than
			// silently dropping one unidentified channel out of the result.
			rows.Close()
			return nil, nil, scanErr
		}
		chIDs = append(chIDs, id)
	}
	rows.Close()
	if rErr := rows.Err(); rErr != nil {
		return nil, nil, rErr
	}

	now := a.now()
	var routes []EntitlementRoute
	var failed []EntitlementFailure
	for _, id := range chIDs {
		chID := channel.ID(id)
		h := a.getHome(chID)
		if h == nil {
			// Directory has it (workspace member) but the home is not open — transient.
			failed = append(failed, EntitlementFailure{Channel: chID, Err: errChannelHomeNotOpen})
			continue
		}
		subjectID, found, rerr := h.ResolvePrincipal(ctx, actor.KindHuman, principal)
		if rerr != nil {
			failed = append(failed, EntitlementFailure{Channel: chID, Err: rerr})
			continue
		}
		if found {
			routes = append(routes, EntitlementRoute{
				Channel: chID, Home: h, Access: "member", SubjectID: subjectID, CheckedAt: now,
			})
			continue
		}
		// Workspace member but not a channel member → observer (read/tail only).
		routes = append(routes, EntitlementRoute{
			Channel: chID, Home: h, Access: "observer", CheckedAt: now,
		})
	}
	return routes, failed, nil
}

// now is the app's wall clock (production time.Now; a test may not need to override it,
// but keeping it a method mirrors the gateway's injectable clock so CheckedAt anchors
// consistently with the lease logic).
func (a *App) now() time.Time { return time.Now() }
