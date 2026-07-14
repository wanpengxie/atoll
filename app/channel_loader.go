package app

import (
	"fmt"

	"github.com/wanpengxie/atoll/protocol/channel"
)

// loadChannels reopens channels already present in the app directory. There is
// no legacy composition source or startup data migration: every directory row
// must point at an existing channel DB carrying the current schema. One failure
// closes all homes opened by this pass and aborts startup.
func (a *App) loadChannels() error {
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
		if _, err := a.openExistingHome(channel.ID(id), dbPath); err != nil {
			a.closeLoadedHomes()
			return fmt.Errorf("channel %s: %w", id, err)
		}
	}
	if err := rows.Err(); err != nil {
		a.closeLoadedHomes()
		return err
	}
	return nil
}
