package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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

func (r compositionResolver) ResolveDeclaration(ctx context.Context, chID channel.ID, declID string) (channel.DeclarationFacts, error) {
	var owner, visibility, class string
	var global sql.NullString
	var deleted sql.NullInt64
	if err := r.app.db.QueryRowContext(ctx, `SELECT owner,visibility,default_class,config_json,deleted_at FROM actor_decls WHERE id=?`, declID).
		Scan(&owner, &visibility, &class, &global, &deleted); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return channel.DeclarationFacts{}, channel.ErrDeclarationNotFound
		}
		return channel.DeclarationFacts{}, err
	}
	if deleted.Valid {
		return channel.DeclarationFacts{}, channel.ErrDeclarationNotFound
	}
	config := global.String
	var overlay sql.NullString
	err := r.app.db.QueryRowContext(ctx, `SELECT config_json FROM channel_decl_overlays WHERE channel_id=? AND decl_id=?`, string(chID), declID).Scan(&overlay)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return channel.DeclarationFacts{}, err
	}
	if err == nil && overlay.Valid {
		config = overlay.String
	}
	tx, err := r.app.db.BeginTx(ctx, nil)
	if err != nil {
		return channel.DeclarationFacts{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO decl_render_state(channel_id,decl_id,render_seq) VALUES (?,?,1)`, string(chID), declID); err != nil {
		return channel.DeclarationFacts{}, err
	}
	var seq int64
	if err := tx.QueryRowContext(ctx, `SELECT render_seq FROM decl_render_state WHERE channel_id=? AND decl_id=?`, string(chID), declID).Scan(&seq); err != nil {
		return channel.DeclarationFacts{}, err
	}
	if err := tx.Commit(); err != nil {
		return channel.DeclarationFacts{}, err
	}
	var raw json.RawMessage
	if strings.TrimSpace(config) != "" {
		raw = json.RawMessage(config)
	}
	snapshot, err := (channel.RenderedSnapshot{
		Class: class, Config: raw, Placement: channel.Placement{Kind: channel.PlacementDaemon}, RenderSeq: seq,
	}).Seal()
	if err != nil {
		return channel.DeclarationFacts{}, err
	}
	return channel.DeclarationFacts{OwnerPrincipal: owner, Visibility: visibility, DefaultClass: class, Rendered: snapshot}, nil
}

func (r compositionResolver) ClassKind(_ context.Context, class string) (actor.Kind, error) {
	kind, ok := registry.ClassKind(class)
	if !ok {
		return "", fmt.Errorf("unknown class %q", class)
	}
	return kind, nil
}
