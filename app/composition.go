package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/spacetool"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
)

// compositionResolver is the world half injected into Home. Channel-local
// intent is supplied by Home from its own database; this resolver performs only
// space-current declaration/overlay/daemon reads and registry construction.
type compositionResolver struct{ app *App }

func (r compositionResolver) BuildClass(chID channel.ID, childID actor.ActorID, class string, config json.RawMessage) (platform.ActorFactory, bool) {
	if class == spaceToolClass {
		return platform.ActorFactory{Proc: spacetool.Def(spaceOps{app: r.app})}, true
	}
	decl, err := registry.Build(class, registry.InstanceSpec{ID: childID, Config: config}, registry.Deps{ChannelID: chID, Logger: r.app.logger})
	if err != nil {
		r.app.logger.Error("actor class build failed",
			"channel", chID, "actor", childID, "class", class, "err", err)
		return platform.ActorFactory{}, false
	}
	return decl.Factory, true
}

func (r compositionResolver) ResolveDeclaration(ctx context.Context, chID channel.ID, declID string) (channelspec.DeclarationFacts, error) {
	var owner, visibility, class string
	var global sql.NullString
	var deleted sql.NullInt64
	if err := r.app.db.QueryRowContext(ctx, `SELECT owner,visibility,default_class,config_json,deleted_at FROM actor_decls WHERE id=?`, declID).
		Scan(&owner, &visibility, &class, &global, &deleted); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return channelspec.DeclarationFacts{}, channelspec.ErrDeclarationNotFound
		}
		return channelspec.DeclarationFacts{}, err
	}
	if deleted.Valid {
		return channelspec.DeclarationFacts{}, channelspec.ErrDeclarationNotFound
	}
	config := global.String
	var overlay sql.NullString
	err := r.app.db.QueryRowContext(ctx, `SELECT config_json FROM channel_decl_overlays WHERE channel_id=? AND decl_id=?`, string(chID), declID).Scan(&overlay)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return channelspec.DeclarationFacts{}, err
	}
	if err == nil && overlay.Valid {
		config = overlay.String
	}
	var raw json.RawMessage
	if config != "" {
		raw = json.RawMessage(config)
	}
	return channelspec.DeclarationFacts{OwnerPrincipal: owner, Visibility: visibility, Class: class, Config: raw}, nil
}

func (r compositionResolver) ClassKind(_ context.Context, class string) (actor.Kind, bool, error) {
	if class == spaceToolClass {
		return actor.KindTool, true, nil
	}
	kind, ok := registry.ClassKind(class)
	if !ok {
		return "", false, nil
	}
	return kind, true, nil
}
