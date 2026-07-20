package app

import (
	"context"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// loadChannels is a level reconciliation pass. One corrupt/unavailable channel
// is isolated and remains honestly 503; it never prevents the realm from
// starting or the next pass from retrying it.
func (a *App) loadChannels() error {
	rows, err := a.db.Query(`SELECT id,type FROM channels ORDER BY id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var raw, typ string
		if err := rows.Scan(&raw, &typ); err != nil {
			return err
		}
		id := channel.ID(raw)
		if err := a.host.Open(context.Background(), channelhost.OpenSpec{ChannelID: id, ExpectedType: typ}); err != nil {
			a.logger.Warn("channel open reconcile failed", "channel", raw, "err", err)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	entries, err := a.host.Census(context.Background())
	if err != nil {
		a.logger.Warn("channel census failed", "err", err)
		return nil
	}
	for _, entry := range entries {
		if !a.channelExists(context.Background(), string(entry.ChannelID)) {
			a.logger.Warn("orphan channel image", "channel", entry.ChannelID, "state", entry.State)
		}
	}
	return nil
}
