package devicebus

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/pkg/requestctx"
)

var (
	ErrDaemonNotFound      = errors.New("devicebus: daemon not found")
	ErrDaemonActorConflict = errors.New("devicebus: daemon actor already active")
)

type Daemon struct {
	ID            placement.DaemonID
	ChannelID     channel.ID
	OwnerID       string
	Name          string
	APIKey        string
	APIKeyPrefix  string
	Status        string
	Hostname      string
	ProxyVersion  string
	LastHeartbeat int64
	CreatedAt     int64
}

type CreateDaemonInput struct {
	ChannelID channel.ID
	OwnerID   string
	Name      string
}

type ReadyActor struct {
	ActorID       actor.ActorID
	CapabilitySet json.RawMessage
}

type DaemonReadyInput struct {
	Hostname     string
	HostLabel    string
	ProxyVersion string
	Actors       []ReadyActor
}

func (s *Service) CreateDaemon(ctx context.Context, in CreateDaemonInput) (Daemon, string, error) {
	in.Name = strings.TrimSpace(in.Name)
	if in.ChannelID == "" || in.OwnerID == "" || in.Name == "" {
		return Daemon{}, "", fmt.Errorf("devicebus: channel_id + owner_id + name required")
	}
	apiKey, err := s.genAPIKey()
	if err != nil {
		return Daemon{}, "", err
	}
	now := s.nowMs()
	row := Daemon{
		ID:           placement.DaemonID(uuid.NewString()),
		ChannelID:    in.ChannelID,
		OwnerID:      in.OwnerID,
		Name:         in.Name,
		APIKey:       apiKey,
		APIKeyPrefix: apiKeyPrefix(apiKey),
		Status:       "offline",
		CreatedAt:    now,
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO daemons (
		  id, key_hash, channel_id, owner_id, name,
		  api_key, api_key_prefix, status, created_at, last_heartbeat
		)
		VALUES (?, '', ?, ?, ?, ?, ?, 'offline', ?, 0)`,
		string(row.ID), string(row.ChannelID), row.OwnerID, row.Name,
		row.APIKey, row.APIKeyPrefix, row.CreatedAt,
	); err != nil {
		return Daemon{}, "", fmt.Errorf("devicebus: insert daemon: %w", err)
	}
	s.log.Info("devicebus.daemon_created",
		"request_id", requestctx.RequestID(ctx),
		"daemon_id", string(row.ID),
		"channel_id", string(row.ChannelID),
		"user_id", row.OwnerID,
	)
	return row, apiKey, nil
}

func (s *Service) ListDaemons(ctx context.Context, channelID channel.ID) ([]Daemon, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, channel_id, owner_id, name, api_key_prefix, status,
		       COALESCE(hostname,''), COALESCE(proxy_version,''),
		       COALESCE(last_heartbeat,0), created_at
		  FROM daemons
		 WHERE channel_id = ? AND api_key <> ''
		 ORDER BY created_at DESC`, string(channelID))
	if err != nil {
		return nil, fmt.Errorf("devicebus: list daemons: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Daemon
	for rows.Next() {
		row, err := scanDaemonWithoutKey(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("devicebus: list daemons rows: %w", err)
	}
	return out, nil
}

func (s *Service) GetDaemon(ctx context.Context, channelID channel.ID, daemonID placement.DaemonID) (Daemon, error) {
	row, err := s.getDaemon(ctx, `
		SELECT id, channel_id, owner_id, name, api_key, api_key_prefix, status,
		       COALESCE(hostname,''), COALESCE(proxy_version,''),
		       COALESCE(last_heartbeat,0), created_at
		  FROM daemons
		 WHERE channel_id = ? AND id = ? AND api_key <> ''
		 LIMIT 1`, string(channelID), string(daemonID))
	if err != nil {
		return Daemon{}, err
	}
	return row, nil
}

func (s *Service) GetDaemonByAPIKey(ctx context.Context, apiKey string) (Daemon, error) {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return Daemon{}, ErrDaemonNotFound
	}
	return s.getDaemon(ctx, `
		SELECT id, channel_id, owner_id, name, api_key, api_key_prefix, status,
		       COALESCE(hostname,''), COALESCE(proxy_version,''),
		       COALESCE(last_heartbeat,0), created_at
		  FROM daemons
		 WHERE api_key = ? AND api_key <> ''
		 LIMIT 1`, apiKey)
}

func (s *Service) DeleteDaemon(ctx context.Context, channelID channel.ID, daemonID placement.DaemonID) error {
	row, _ := s.GetDaemon(ctx, channelID, daemonID)
	actors, _ := s.ListDaemonActiveActorIDs(ctx, daemonID)
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM daemons WHERE channel_id = ? AND id = ? AND api_key <> ''`,
		string(channelID), string(daemonID),
	)
	if err != nil {
		return fmt.Errorf("devicebus: delete daemon: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrDaemonNotFound
	}
	_ = s.clearDaemonActiveActors(context.WithoutCancel(ctx), daemonID)
	s.closeDaemonConnection(daemonID, true)
	if n := s.proxyDaemonNotifier(); n != nil && row.ID != "" && len(actors) > 0 {
		_ = n.NotifyProxyDaemonOffline(context.WithoutCancel(ctx), row, actors)
	}
	return nil
}

func (s *Service) MarkDaemonOnline(ctx context.Context, daemonID placement.DaemonID) error {
	if _, err := s.db.ExecContext(ctx,
		`UPDATE daemons SET status='online', last_heartbeat=? WHERE id=? AND api_key <> ''`,
		s.nowMs(), string(daemonID),
	); err != nil {
		return fmt.Errorf("devicebus: mark daemon online: %w", err)
	}
	return nil
}

func (s *Service) MarkDaemonOffline(ctx context.Context, daemonID placement.DaemonID) error {
	if _, err := s.db.ExecContext(ctx,
		`UPDATE daemons SET status='offline' WHERE id=? AND api_key <> ''`,
		string(daemonID),
	); err != nil {
		return fmt.Errorf("devicebus: mark daemon offline: %w", err)
	}
	return nil
}

func (s *Service) HeartbeatDaemon(ctx context.Context, daemonID placement.DaemonID) error {
	now := s.nowMs()
	if _, err := s.db.ExecContext(ctx,
		`UPDATE daemons SET last_heartbeat=? WHERE id=? AND api_key <> ''`,
		now, string(daemonID),
	); err != nil {
		return fmt.Errorf("devicebus: heartbeat daemon: %w", err)
	}
	if _, err := s.db.ExecContext(ctx,
		`UPDATE daemon_active_actors SET last_seen_at=? WHERE daemon_id=?`,
		now, string(daemonID),
	); err != nil {
		return fmt.Errorf("devicebus: heartbeat active actors: %w", err)
	}
	return nil
}

func (s *Service) ApplyDaemonReady(ctx context.Context, d Daemon, ready DaemonReadyInput) error {
	if d.ID == "" || d.ChannelID == "" {
		return fmt.Errorf("devicebus: ready daemon identity required")
	}
	if len(ready.Actors) == 0 {
		return fmt.Errorf("devicebus: ready actors required")
	}
	now := s.nowMs()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("devicebus: ready begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM daemon_active_actors WHERE daemon_id = ?`,
		string(d.ID),
	); err != nil {
		return fmt.Errorf("devicebus: ready clear daemon actors: %w", err)
	}

	seen := map[actor.ActorID]struct{}{}
	for _, a := range ready.Actors {
		actorID := actor.ActorID(strings.TrimSpace(string(a.ActorID)))
		if actorID == "" {
			return fmt.Errorf("devicebus: ready actor_id required")
		}
		if _, dup := seen[actorID]; dup {
			return fmt.Errorf("%w: duplicate actor %s", ErrDaemonActorConflict, actorID)
		}
		seen[actorID] = struct{}{}

		var existing string
		err := tx.QueryRowContext(ctx,
			`SELECT daemon_id FROM daemon_active_actors WHERE channel_id=? AND actor_id=?`,
			string(d.ChannelID), string(actorID),
		).Scan(&existing)
		switch {
		case err == nil && existing != string(d.ID):
			return fmt.Errorf("%w: channel=%s actor=%s existing_daemon=%s",
				ErrDaemonActorConflict, d.ChannelID, actorID, existing)
		case err != nil && !errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("devicebus: ready actor conflict lookup: %w", err)
		}

		if _, err := tx.ExecContext(ctx, `
			INSERT INTO daemon_active_actors
			  (channel_id, actor_id, daemon_id, registered_at, last_seen_at)
			VALUES (?, ?, ?, ?, ?)`,
			string(d.ChannelID), string(actorID), string(d.ID), now, now,
		); err != nil {
			return fmt.Errorf("devicebus: ready insert active actor: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE daemons
		   SET status='online',
		       hostname=?,
		       proxy_version=?,
		       last_heartbeat=?
		 WHERE id=? AND api_key <> ''`,
		strings.TrimSpace(ready.Hostname), strings.TrimSpace(ready.ProxyVersion),
		now, string(d.ID),
	); err != nil {
		return fmt.Errorf("devicebus: ready update daemon: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("devicebus: ready commit: %w", err)
	}

	s.mu.Lock()
	s.clearDaemonActorRoutesLocked(d.ID)
	for actorID := range seen {
		s.actorToDaemon[routeKey(d.ChannelID, actorID)] = d.ID
	}
	s.mu.Unlock()
	return nil
}

func (s *Service) clearDaemonActiveActors(ctx context.Context, daemonID placement.DaemonID) error {
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM daemon_active_actors WHERE daemon_id = ?`,
		string(daemonID),
	); err != nil {
		return fmt.Errorf("devicebus: clear daemon active actors: %w", err)
	}
	s.mu.Lock()
	s.clearDaemonActorRoutesLocked(daemonID)
	s.mu.Unlock()
	return nil
}

func (s *Service) ListDaemonActiveActorIDs(ctx context.Context, daemonID placement.DaemonID) ([]actor.ActorID, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT actor_id FROM daemon_active_actors WHERE daemon_id=? ORDER BY actor_id`,
		string(daemonID),
	)
	if err != nil {
		return nil, fmt.Errorf("devicebus: list daemon active actors: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []actor.ActorID
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("devicebus: scan daemon active actor: %w", err)
		}
		out = append(out, actor.ActorID(id))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("devicebus: daemon active actor rows: %w", err)
	}
	return out, nil
}

func (s *Service) getDaemon(ctx context.Context, query string, args ...any) (Daemon, error) {
	var row Daemon
	var id, chID string
	err := s.db.QueryRowContext(ctx, query, args...).Scan(
		&id, &chID, &row.OwnerID, &row.Name, &row.APIKey, &row.APIKeyPrefix,
		&row.Status, &row.Hostname, &row.ProxyVersion, &row.LastHeartbeat,
		&row.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Daemon{}, ErrDaemonNotFound
	}
	if err != nil {
		return Daemon{}, fmt.Errorf("devicebus: get daemon: %w", err)
	}
	row.ID = placement.DaemonID(id)
	row.ChannelID = channel.ID(chID)
	return row, nil
}

type daemonScanner interface {
	Scan(dest ...any) error
}

func scanDaemonWithoutKey(rows daemonScanner) (Daemon, error) {
	var row Daemon
	var id, chID string
	err := rows.Scan(
		&id, &chID, &row.OwnerID, &row.Name, &row.APIKeyPrefix,
		&row.Status, &row.Hostname, &row.ProxyVersion, &row.LastHeartbeat,
		&row.CreatedAt,
	)
	if err != nil {
		return Daemon{}, fmt.Errorf("devicebus: scan daemon: %w", err)
	}
	row.ID = placement.DaemonID(id)
	row.ChannelID = channel.ID(chID)
	return row, nil
}

func (s *Service) genAPIKey() (string, error) {
	buf := make([]byte, 32)
	if _, err := io.ReadFull(s.rng, buf); err != nil {
		return "", fmt.Errorf("devicebus: rng api key: %w", err)
	}
	return "dk_" + hex.EncodeToString(buf), nil
}

func apiKeyPrefix(key string) string {
	if len(key) <= 11 {
		return key
	}
	return key[:11] + "..."
}
