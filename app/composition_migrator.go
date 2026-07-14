package app

import (
	"context"
	"fmt"
	"time"

	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// importLegacyComposition is the idempotent stop-the-world bridge while the app
// source table still exists. This file is the retired-symbol gate's explicit
// migrator allowlist; production consumers outside it may not name that source.
func (a *App) importLegacyComposition(ctx context.Context, chID channel.ID, h *home.Home) error {
	rows, err := a.db.QueryContext(ctx, `SELECT ca.instance_id,ca.principal, ca.class, COALESCE(ca.config_json,''), ca.placement,
		COALESCE(ca.desired_host,''), ca.restart_epoch,
		CASE WHEN COALESCE(c.default_agent,'')=ca.instance_id THEN 1 ELSE 0 END
		FROM channel_actors ca JOIN channels c ON c.id=ca.channel_id WHERE ca.channel_id=?`, string(chID))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var legacyID, principal, class, cfg, placement, desired string
		var epoch int64
		var isDefault int
		if err := rows.Scan(&legacyID, &principal, &class, &cfg, &placement, &desired, &epoch, &isDefault); err != nil {
			return err
		}
		kind, ok := registry.ClassKind(class)
		if !ok || (kind != actor.KindAgent && kind != actor.KindTool) {
			continue
		}
		declID := principal
		if principal == defaultAgentPrincipal {
			declID = "sys:boost"
		} else {
			var n int
			if err := a.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM actor_decls WHERE id=? AND deleted_at IS NULL`, declID).Scan(&n); err != nil {
				return err
			}
			if n == 0 {
				continue
			}
		}
		var cfgPtr *string
		if cfg != "" {
			value := cfg
			cfgPtr = &value
		}
		if desired != "" {
			var valid int
			if err := a.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM daemons d JOIN daemon_channels dc ON dc.daemon_id=d.id WHERE d.id=? AND dc.channel_id=?`, desired, string(chID)).Scan(&valid); err != nil {
				return err
			}
			if valid == 0 {
				desired = ""
			}
		}
		rec, _, _, err := h.IntroduceComposition(ctx, storespec.CompositionIntroduce{
			DeclID: declID, Principal: principal, Class: class, ConfigJSON: cfgPtr,
			Placement: storespec.Placement(placement), DesiredHost: desired,
			MakeDefault: isDefault == 1, Kind: kind, At: time.Now().UnixMilli(),
		})
		if err != nil {
			return err
		}
		for rec.Epoch < epoch {
			if _, err := h.RestartInstanceDirect(ctx, rec.InstanceID); err != nil {
				return err
			}
			rec.Epoch++
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	composition, err := h.Composition(ctx)
	if err != nil {
		return err
	}
	live := make(map[actor.ActorID]struct{}, len(composition))
	for _, row := range composition {
		live[row.InstanceID] = struct{}{}
	}
	registryRows, err := h.View().ListActors(ctx)
	if err != nil {
		return err
	}
	for _, row := range registryRows {
		if row.Kind != actor.KindAgent && row.Kind != actor.KindTool {
			continue
		}
		if _, ok := live[row.ID]; !ok {
			if err := h.Remove(ctx, row.ID); err != nil {
				return err
			}
		}
	}
	return h.MarkCompositionMigrated(ctx, time.Now().UnixMilli())
}

func (a *App) loadChannels() error {
	var sourceTable int
	if err := a.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='channel_actors'`).Scan(&sourceTable); err != nil {
		return err
	}
	rows, err := a.db.Query(`SELECT id, db_path FROM channels`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id, dbPath string
		if err := rows.Scan(&id, &dbPath); err != nil {
			a.closeLoadedHomes()
			return err
		}
		h, err := a.createHome(channel.ID(id), dbPath)
		if err != nil {
			a.closeLoadedHomes()
			return fmt.Errorf("channel %s: %w", id, err)
		}
		if sourceTable != 0 {
			if err := a.importLegacyComposition(context.Background(), channel.ID(id), h); err != nil {
				a.closeLoadedHomes()
				return fmt.Errorf("migrate channel %s: %w", id, err)
			}
		} else if err := h.MarkCompositionMigrated(context.Background(), time.Now().UnixMilli()); err != nil {
			a.closeLoadedHomes()
			return err
		}
	}
	if err := rows.Err(); err != nil {
		a.closeLoadedHomes()
		return err
	}
	if err := rows.Close(); err != nil {
		a.closeLoadedHomes()
		return err
	}
	if sourceTable != 0 {
		if _, err := a.db.Exec(`DROP TABLE channel_actors`); err != nil {
			a.closeLoadedHomes()
			return fmt.Errorf("retire channel_actors: %w", err)
		}
	}
	_, _ = a.db.Exec(`ALTER TABLE channels DROP COLUMN default_agent`)
	return nil
}
