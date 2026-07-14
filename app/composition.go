package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// compositionResolver is the world half injected into Home. Channel-local
// intent is supplied by Home from its own database; this resolver performs only
// the actor_decls lookup/config overlay and registry construction.
type compositionResolver struct{ app *App }

func (r compositionResolver) ResolveComposition(ctx context.Context, chID channel.ID, row storespec.CompositionRecord) (platform.ActorDecl, bool, error) {
	global := ""
	if !strings.HasPrefix(row.DeclID, "sys:") {
		err := r.app.db.QueryRowContext(ctx,
			`SELECT COALESCE(config_json,'') FROM actor_decls WHERE id=? AND deleted_at IS NULL`, row.DeclID).Scan(&global)
		if err == sql.ErrNoRows {
			return platform.ActorDecl{}, false, nil
		}
		if err != nil {
			return platform.ActorDecl{}, false, err
		}
	}
	decl, err := registry.Build(row.Class, registry.InstanceSpec{
		ID: row.InstanceID, Config: mergeConfig(global, row.ConfigJSON),
	}, registry.Deps{ChannelID: chID, Logger: r.app.logger})
	if err != nil {
		return platform.ActorDecl{}, false, nil
	}
	return decl, true, nil
}

func (r compositionResolver) BuildClass(chID channel.ID, childID actor.ActorID, class string, config json.RawMessage) (platform.ActorFactory, bool) {
	decl, err := registry.Build(class, registry.InstanceSpec{ID: childID, Config: config}, registry.Deps{ChannelID: chID, Logger: r.app.logger})
	if err != nil {
		return platform.ActorFactory{}, false
	}
	return decl.Factory, true
}
