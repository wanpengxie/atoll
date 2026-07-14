package app

import "database/sql"

// migrateLegacyCompositionShape is the only app-schema source normalizer. It
// is intentionally isolated in a *_migrator.go file so the retired-symbol gate
// can permit upgrade SQL without permitting a live consumer to regress.
func migrateLegacyCompositionShape(db *sql.DB) {
	var legacyComposition int
	_ = db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='channel_actors'`).Scan(&legacyComposition)
	if legacyComposition == 0 {
		return
	}
	_, _ = db.Exec(`ALTER TABLE channels ADD COLUMN default_agent TEXT`)
	_, _ = db.Exec(`ALTER TABLE channel_actors ADD COLUMN placement TEXT NOT NULL DEFAULT 'server'`)
	_, _ = db.Exec(`ALTER TABLE channel_actors ADD COLUMN principal TEXT NOT NULL DEFAULT ''`)
	_, _ = db.Exec(`ALTER TABLE channel_actors ADD COLUMN desired_host TEXT NOT NULL DEFAULT ''`)
	_, _ = db.Exec(`ALTER TABLE channel_actors ADD COLUMN restart_epoch INTEGER NOT NULL DEFAULT 0`)
	_, _ = db.Exec(`UPDATE channel_actors
		SET class = (SELECT default_class FROM actor_decls WHERE 'agent:' || actor_decls.id = channel_actors.instance_id)
		WHERE class = 'agent' AND instance_id IN (SELECT 'agent:' || id FROM actor_decls)`)
	_, _ = db.Exec(`UPDATE channel_actors SET class = 'go-kimi' WHERE class = 'agent'`)
}
