package app

import (
	"context"
	"database/sql"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// composition.go is the组合域 (composition-domain) supply half the app injects
// into a channel home: the reconcile ring's desired source (server-placed intent)
// and its factory table (id→ActorFactory). The user域 (per-channel human members)
// is NOT here — its authority lives only inside the channel's own registry, so the
// platform ring derives it internally (chicken-egg: the app cannot enumerate a
// per-channel human member; see Home.reconcileActivation). The app only speaks the
// composition domain.

// compositionRow is one channel_actors instance with its resolved config layers
// (per-channel over the global agents declaration). The SINGLE row shape
// serverCompositionRows / daemonCompositionRows / compositionBuilder all read (A12: one
// query, one scan, one build — no手搓 duplication).
type compositionRow struct {
	instanceID, class, channelCfg, globalCfg string
}

// compositionSelect is the shared SELECT + LEFT JOIN (global config overlay) every
// composition read uses; each caller appends its own predicate (placement / host /
// instance). The overlay is UNDER the per-channel config (mergeConfig); a non-agent
// class never matches the join (no overlay), which is correct.
const compositionSelect = `SELECT ca.instance_id, ca.class, COALESCE(ca.config_json, ''), COALESCE(a.config_json, '')
	   FROM channel_actors ca
	   LEFT JOIN agents a ON ca.instance_id = 'agent:' || a.id
	  WHERE ca.channel_id = ?`

// scanCompositionRows drains rows into compositionRow (shared scan; a per-row scan
// error skips that row, never aborts the set).
func scanCompositionRows(rows *sql.Rows) ([]compositionRow, error) {
	var out []compositionRow
	for rows.Next() {
		var r compositionRow
		if err := rows.Scan(&r.instanceID, &r.class, &r.channelCfg, &r.globalCfg); err != nil {
			continue
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// serverCompositionRows reads the channel's server-placed composition (the set the
// home embodies as in-process cells).
func (a *App) serverCompositionRows(chID channel.ID) ([]compositionRow, error) {
	rows, err := a.db.Query(compositionSelect+` AND ca.placement = ?`, string(chID), placementServer)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCompositionRows(rows)
}

// daemonCompositionRows reads the channel's daemon-placed composition assigned to
// one daemon (desired_host filter resolves G4: two daemons each pull only their
// own rows).
func (a *App) daemonCompositionRows(chID channel.ID, daemonID string) ([]compositionRow, error) {
	rows, err := a.db.Query(compositionSelect+` AND ca.placement = ? AND ca.desired_host = ?`,
		string(chID), placementDaemon, daemonID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCompositionRows(rows)
}

// instanceCompositionRow reads ONE server-placed instance by id (the reconcile
// ring's per-id factory resolve, compositionBuilder.Lookup). Server-scoped: the
// home builder only ever embodies server-placed cells; a daemon-placed instance is
// built by the daemon's own plan-poll builder.
func (a *App) instanceCompositionRow(chID channel.ID, instanceID actor.ActorID) (compositionRow, bool, error) {
	rows, err := a.db.Query(compositionSelect+` AND ca.placement = ? AND ca.instance_id = ?`,
		string(chID), placementServer, string(instanceID))
	if err != nil {
		return compositionRow{}, false, err
	}
	defer rows.Close()
	got, err := scanCompositionRows(rows)
	if err != nil {
		return compositionRow{}, false, err
	}
	if len(got) == 0 {
		return compositionRow{}, false, nil
	}
	return got[0], true, nil
}

// buildInstance is the shared build装配: one compositionRow → the ActorDecl the
// host spawns (mergeConfig + registry.Build). The engine IS ca.class (per-channel
// concrete class); server placement carries no WorkspaceDir so the agent class
// derives the server-embedded Situation.
func (a *App) buildInstance(chID channel.ID, r compositionRow) (platform.ActorDecl, error) {
	return registry.Build(r.class, registry.InstanceSpec{
		ID:     actor.ActorID(r.instanceID),
		Config: mergeConfig(r.globalCfg, r.channelCfg),
	}, registry.Deps{
		ChannelID: chID,
		Logger:    a.logger,
	})
}

// compositionDesired is the组合域 DesiredSource injected into a home: it yields the
// channel's server-placed intent rows as AlwaysOn desired members. It is INTENT
// only — the platform ring intersects it with durable membership (desired = intent
// ∩ membership), so a row whose Admit never landed never embodies. Kind is the
// class's declared kind (registry.ClassKind, a pure pre-Build table lookup); an
// unknown class is skipped (its kind is unknowable, so it is not activatable).
type compositionDesired struct {
	app  *App
	chID channel.ID
}

func (d compositionDesired) Members(context.Context) ([]actorrt.DesiredMember, error) {
	rows, err := d.app.serverCompositionRows(d.chID)
	if err != nil {
		return nil, err
	}
	out := make([]actorrt.DesiredMember, 0, len(rows))
	for _, r := range rows {
		kind, ok := registry.ClassKind(r.class)
		if !ok {
			continue
		}
		out = append(out, actorrt.DesiredMember{
			ID:        actor.ActorID(r.instanceID),
			Kind:      kind,
			Lifecycle: actorrt.LifecycleAlwaysOn,
		})
	}
	return out, nil
}

// compositionBuilder is the组合域 CapsFactoryBuilder injected into a home: it
// resolves a durable member id to its ActorFactory by reading the channel_actors
// row and building it (the same read+build the reconcile ring's activation does,
// resolved for the builder instead of eagerly spawned). A build failure (missing creds, unknown class) is (zero,false) — the
// ring records it infeasible and retries next tick. LookupByClass is always
// not-found: fork is deferred (A-P2=A), so the home never forks a domain child.
type compositionBuilder struct {
	app  *App
	chID channel.ID
}

func (b compositionBuilder) Lookup(id actor.ActorID) (platform.ActorFactory, bool) {
	row, ok, err := b.app.instanceCompositionRow(b.chID, id)
	if err != nil {
		b.app.logger.Warn("app: composition builder lookup", "channel", string(b.chID), "instance", string(id), "err", err.Error())
		return platform.ActorFactory{}, false
	}
	if !ok {
		return platform.ActorFactory{}, false
	}
	decl, err := b.app.buildInstance(b.chID, row)
	if err != nil {
		b.app.logger.Debug("app: composition builder build", "channel", string(b.chID), "instance", string(id), "reason", err.Error())
		return platform.ActorFactory{}, false
	}
	return decl.Factory, true
}

func (b compositionBuilder) LookupByClass(string) (platform.ActorFactory, bool) {
	return platform.ActorFactory{}, false
}
