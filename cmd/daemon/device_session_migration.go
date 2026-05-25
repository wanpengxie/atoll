package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog"
	_ "modernc.org/sqlite"
)

// migrateLegacyDeviceMirrorFile retires the daemon-local sqlite mirror.
// The actor-route implementation does not recover any route from this file,
// but reading the old rows once gives operators a useful audit trail before
// we move the file aside. The .bak file is intentionally preserved.
func migrateLegacyDeviceMirrorFile(ctx context.Context, dataDir string, log *zerolog.Logger) error {
	path := filepath.Join(dataDir, "device_sessions.sqlite")
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	rowsSeen, readErr := readLegacyDeviceSessionMirror(ctx, path)
	if log != nil {
		event := log.Info().
			Str("event", "daemon.legacy_device_route_mirror_retired").
			Str("path", path).
			Int("rows_seen", rowsSeen)
		if readErr != nil {
			event = event.Err(readErr)
		}
		event.Msg("legacy device route mirror will be renamed")
	}

	bak, err := nextLegacyMirrorBackupPath(path)
	if err != nil {
		return err
	}
	if err := os.Rename(path, bak); err != nil {
		return fmt.Errorf("rename legacy device route mirror: %w", err)
	}
	if readErr != nil {
		// Preserve boot continuity: once the old file has been backed up,
		// a read failure should not keep the actor-route daemon down.
		return nil
	}
	return nil
}

func readLegacyDeviceSessionMirror(ctx context.Context, path string) (int, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = db.Close() }()

	var exists int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		  FROM sqlite_master
		 WHERE type = 'table' AND name = 'device_session_mirror'`,
	).Scan(&exists); err != nil {
		return 0, err
	}
	if exists == 0 {
		return 0, nil
	}
	rows, err := db.QueryContext(ctx, `
		SELECT adapter_actor_id, device_id, channel_id
		  FROM device_session_mirror`)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()
	count := 0
	for rows.Next() {
		var actorID, deviceID, channelID string
		if err := rows.Scan(&actorID, &deviceID, &channelID); err != nil {
			return count, err
		}
		count++
	}
	return count, rows.Err()
}

func nextLegacyMirrorBackupPath(path string) (string, error) {
	candidate := path + ".bak"
	if _, err := os.Stat(candidate); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return candidate, nil
		}
		return "", err
	}
	stamped := fmt.Sprintf("%s.%s.bak", path, time.Now().Format("20060102T150405"))
	if _, err := os.Stat(stamped); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return stamped, nil
		}
		return "", err
	}
	return "", fmt.Errorf("legacy device route mirror backup path already exists: %s", stamped)
}
