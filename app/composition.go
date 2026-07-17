package app

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
)

// compositionResolver is the world half injected into Home. Channel-local
// intent is supplied by Home from its own database; this resolver performs only
// the actor_decls lookup/config overlay and registry construction.
type compositionResolver struct{ app *App }

func (r compositionResolver) BuildClass(chID channel.ID, childID actor.ActorID, class string, config json.RawMessage) (platform.ActorFactory, bool) {
	decl, err := registry.Build(class, registry.InstanceSpec{ID: childID, Config: config}, registry.Deps{ChannelID: chID, Logger: r.app.logger})
	if err != nil {
		return platform.ActorFactory{}, false
	}
	return decl.Factory, true
}

func (r compositionResolver) ResolveSourceConfig(ctx context.Context, sourceID string, config json.RawMessage) (json.RawMessage, error) {
	global := ""
	if sourceID != "" && !strings.HasPrefix(sourceID, "sys:") {
		if err := r.app.db.QueryRowContext(ctx, `SELECT COALESCE(config_json,'') FROM actor_decls WHERE id=? AND deleted_at IS NULL`, sourceID).Scan(&global); err != nil {
			return nil, err
		}
	}
	return mergeConfig(global, string(config)), nil
}
