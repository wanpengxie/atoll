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

// Daemon is the owner-scoped proxy daemon row (one row per user-machine
// install). T1-onwards `ChannelID` was the single channel this daemon
// served; T7 (2026-05-27) moves attach to the daemon_channel_attachments
// join table so one install can serve many channels. ChannelID stays for
// legacy reads (always "" for daemons created post-T7) and is no longer
// the routing key — AttachedChannels is.
type Daemon struct {
	ID               placement.DaemonID
	ChannelID        channel.ID
	OwnerID          string
	Name             string
	APIKey           string
	APIKeyPrefix     string
	Status           string
	Hostname         string
	ProxyVersion     string
	LastHeartbeat    int64
	CreatedAt        int64
	AttachedChannels []channel.ID // populated by ws connect; empty after CreateDaemon
}

type CreateDaemonInput struct {
	OwnerID string
	Name    string
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

// CreateDaemon issues a new proxy daemon row scoped to an owner. The
// daemon is "installed but unattached" until callers call
// AttachDaemonToChannel (see UI flow). Legacy column `channel_id` is
// left blank.
func (s *Service) CreateDaemon(ctx context.Context, in CreateDaemonInput) (Daemon, string, error) {
	in.Name = strings.TrimSpace(in.Name)
	if in.OwnerID == "" || in.Name == "" {
		return Daemon{}, "", fmt.Errorf("devicebus: owner_id + name required")
	}
	apiKey, err := s.genAPIKey()
	if err != nil {
		return Daemon{}, "", err
	}
	now := s.nowMs()
	row := Daemon{
		ID:           placement.DaemonID(uuid.NewString()),
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
		VALUES (?, '', '', ?, ?, ?, ?, 'offline', ?, 0)`,
		string(row.ID), row.OwnerID, row.Name,
		row.APIKey, row.APIKeyPrefix, row.CreatedAt,
	); err != nil {
		return Daemon{}, "", fmt.Errorf("devicebus: insert daemon: %w", err)
	}
	s.log.Info("devicebus.daemon_created",
		"request_id", requestctx.RequestID(ctx),
		"daemon_id", string(row.ID),
		"owner_id", row.OwnerID,
	)
	return row, apiKey, nil
}

// AttachDaemonToChannel records that the owner wants `daemon` to serve
// `channelID`. The proxy facade install for the channel runs next time
// the daemon ws connects (or immediately if already connected — handled
// by the caller). Idempotent: re-attaching an existing pair is a no-op.
// Authorization: caller validates ownerID matches the daemon row.
func (s *Service) AttachDaemonToChannel(ctx context.Context, daemonID placement.DaemonID, channelID channel.ID) error {
	if daemonID == "" || channelID == "" {
		return fmt.Errorf("devicebus: daemon_id + channel_id required")
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO daemon_channel_attachments (daemon_id, channel_id, attached_at)
		VALUES (?, ?, ?)`,
		string(daemonID), string(channelID), s.nowMs(),
	)
	if err != nil {
		return fmt.Errorf("devicebus: attach daemon: %w", err)
	}
	return nil
}

// DetachDaemonFromChannel removes the (daemon, channel) attachment.
// Any current daemon_active_actors rows for that channel/daemon pair
// are cleaned up; the daemon ws stays connected.
func (s *Service) DetachDaemonFromChannel(ctx context.Context, daemonID placement.DaemonID, channelID channel.ID) error {
	if daemonID == "" || channelID == "" {
		return fmt.Errorf("devicebus: daemon_id + channel_id required")
	}
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM daemon_channel_attachments WHERE daemon_id=? AND channel_id=?`,
		string(daemonID), string(channelID),
	); err != nil {
		return fmt.Errorf("devicebus: detach daemon: %w", err)
	}
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM daemon_active_actors WHERE daemon_id=? AND channel_id=?`,
		string(daemonID), string(channelID),
	); err != nil {
		return fmt.Errorf("devicebus: detach clear active actors: %w", err)
	}
	s.mu.Lock()
	s.clearDaemonChannelRoutesLocked(daemonID, channelID)
	s.mu.Unlock()
	return nil
}

// HostedActor describes one adapter the daemon advertised in its last
// ready frame. capability_set is the raw JSON payload; UI parses for
// display name + type list.
type HostedActor struct {
	ActorID            actor.ActorID
	CapabilitySet      json.RawMessage
	ActiveChannels     []channel.ID
	FacadeState        string
	FacadeDetail       string
	FacadeUpdatedAt    int64
	ReadyState         string
	ReadyReason        string
	ReadyDetail        json.RawMessage
	ReadinessCheckedAt int64
	LastReadyAt        int64
	LastStateChangeAt  int64
}

// ListDaemonHostedActors returns the adapter manifest the daemon
// advertised in its most recent ready frame, irrespective of channel
// attachments. Drives the global "我的设备" page's adapter chips.
func (s *Service) ListDaemonHostedActors(ctx context.Context, daemonID placement.DaemonID) ([]HostedActor, error) {
	activeChannels, err := s.listDaemonActiveActorChannels(ctx, daemonID)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT actor_id,
		       COALESCE(capability_set,''),
		       COALESCE(facade_state,'unknown'),
		       COALESCE(facade_detail,''),
		       COALESCE(facade_updated_at,0),
		       COALESCE(ready_state,'unknown'),
		       COALESCE(ready_reason,'unknown'),
		       COALESCE(ready_detail,'{}'),
		       COALESCE(readiness_checked_at,0),
		       COALESCE(last_ready_at,0),
		       COALESCE(last_state_change_at,0)
		  FROM daemon_hosted_actors
		 WHERE daemon_id = ?
		 ORDER BY actor_id`, string(daemonID))
	if err != nil {
		return nil, fmt.Errorf("devicebus: list hosted actors: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []HostedActor
	for rows.Next() {
		var id, cap, facadeState, facadeDetail, readyState, readyReason, readyDetail string
		var facadeUpdatedAt, checkedAt, lastReadyAt, lastStateChangeAt int64
		if err := rows.Scan(&id, &cap, &facadeState, &facadeDetail, &facadeUpdatedAt, &readyState, &readyReason, &readyDetail, &checkedAt, &lastReadyAt, &lastStateChangeAt); err != nil {
			return nil, fmt.Errorf("devicebus: scan hosted actor: %w", err)
		}
		out = append(out, HostedActor{
			ActorID:            actor.ActorID(id),
			CapabilitySet:      json.RawMessage(cap),
			ActiveChannels:     append([]channel.ID(nil), activeChannels[actor.ActorID(id)]...),
			FacadeState:        facadeState,
			FacadeDetail:       facadeDetail,
			FacadeUpdatedAt:    facadeUpdatedAt,
			ReadyState:         readyState,
			ReadyReason:        readyReason,
			ReadyDetail:        json.RawMessage(readyDetail),
			ReadinessCheckedAt: checkedAt,
			LastReadyAt:        lastReadyAt,
			LastStateChangeAt:  lastStateChangeAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("devicebus: hosted actor rows: %w", err)
	}
	return out, nil
}

func (s *Service) MarkDaemonFacadeState(ctx context.Context, daemonID placement.DaemonID, actors []actor.ActorID, state, detail string) error {
	if daemonID == "" || len(actors) == 0 {
		return nil
	}
	state = strings.TrimSpace(state)
	if state == "" {
		state = "unknown"
	}
	now := s.nowMs()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("devicebus: facade state begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, actorID := range actors {
		if actorID == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE daemon_hosted_actors
			   SET facade_state=?,
			       facade_detail=?,
			       facade_updated_at=?
			 WHERE daemon_id=? AND actor_id=?`,
			state, strings.TrimSpace(detail), now, string(daemonID), string(actorID),
		); err != nil {
			return fmt.Errorf("devicebus: update facade state %s/%s: %w", daemonID, actorID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("devicebus: facade state commit: %w", err)
	}
	return nil
}

func (s *Service) invalidateDaemonRuntimeState(ctx context.Context, daemonID placement.DaemonID, detail string) error {
	if daemonID == "" {
		return nil
	}
	actors, _ := s.ListDaemonActiveActorIDs(ctx, daemonID)
	if err := s.MarkDaemonOffline(ctx, daemonID); err != nil {
		return err
	}
	if err := s.clearDaemonActiveActors(ctx, daemonID); err != nil {
		return err
	}
	if err := s.MarkDaemonFacadeState(ctx, daemonID, actors, "offline", detail); err != nil {
		return err
	}
	return nil
}

func (s *Service) InvalidateRuntimeProjections(ctx context.Context, reason string) error {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "server runtime projection reset"
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE daemons SET status='offline' WHERE api_key <> ''`); err != nil {
		return fmt.Errorf("devicebus: invalidate daemon status: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM daemon_active_actors`); err != nil {
		return fmt.Errorf("devicebus: invalidate active actors: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE daemon_hosted_actors
		   SET facade_state='offline',
		       facade_detail=?,
		       facade_updated_at=?`,
		reason, s.nowMs(),
	); err != nil {
		return fmt.Errorf("devicebus: invalidate facade state: %w", err)
	}
	s.mu.Lock()
	s.actorToDaemon = map[string]placement.DaemonID{}
	s.mu.Unlock()
	return nil
}

func (s *Service) listDaemonActiveActorChannels(ctx context.Context, daemonID placement.DaemonID) (map[actor.ActorID][]channel.ID, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT actor_id, channel_id
		   FROM daemon_active_actors
		  WHERE daemon_id=?
		  ORDER BY actor_id, channel_id`,
		string(daemonID),
	)
	if err != nil {
		return nil, fmt.Errorf("devicebus: list daemon active actor channels: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[actor.ActorID][]channel.ID{}
	for rows.Next() {
		var actorID, channelID string
		if err := rows.Scan(&actorID, &channelID); err != nil {
			return nil, fmt.Errorf("devicebus: scan daemon active actor channel: %w", err)
		}
		out[actor.ActorID(actorID)] = append(out[actor.ActorID(actorID)], channel.ID(channelID))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("devicebus: active actor channel rows: %w", err)
	}
	return out, nil
}

// ListDaemonAttachments returns the channels currently attached to a
// daemon. Used by ws ready handling to know where to install proxy
// facades.
func (s *Service) ListDaemonAttachments(ctx context.Context, daemonID placement.DaemonID) ([]channel.ID, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT channel_id FROM daemon_channel_attachments WHERE daemon_id=? ORDER BY attached_at`,
		string(daemonID),
	)
	if err != nil {
		return nil, fmt.Errorf("devicebus: list attachments: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []channel.ID
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("devicebus: scan attachment: %w", err)
		}
		out = append(out, channel.ID(id))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("devicebus: attachment rows: %w", err)
	}
	return out, nil
}

// ListDaemonsByOwner returns all daemons owned by ownerID, regardless of
// channel attachments. Drives the UI "我的设备" master list.
func (s *Service) ListDaemonsByOwner(ctx context.Context, ownerID string) ([]Daemon, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, channel_id, owner_id, name, api_key_prefix, status,
		       COALESCE(hostname,''), COALESCE(proxy_version,''),
		       COALESCE(last_heartbeat,0), created_at
		  FROM daemons
		 WHERE owner_id = ? AND api_key <> ''
		 ORDER BY created_at DESC`, ownerID)
	if err != nil {
		return nil, fmt.Errorf("devicebus: list daemons by owner: %w", err)
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
		return nil, fmt.Errorf("devicebus: list daemons by owner rows: %w", err)
	}
	return out, nil
}

// ListDaemons returns daemons currently attached to `channelID` via the
// daemon_channel_attachments join. Used by GET /api/channels/:chID/daemons
// for the per-channel "attached devices" view.
func (s *Service) ListDaemons(ctx context.Context, channelID channel.ID) ([]Daemon, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT d.id, d.channel_id, d.owner_id, d.name, d.api_key_prefix, d.status,
		       COALESCE(d.hostname,''), COALESCE(d.proxy_version,''),
		       COALESCE(d.last_heartbeat,0), d.created_at
		  FROM daemons d
		  JOIN daemon_channel_attachments a ON a.daemon_id = d.id
		 WHERE a.channel_id = ? AND d.api_key <> ''
		 ORDER BY d.created_at DESC`, string(channelID))
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

// GetDaemonByID looks up a single daemon by its UUID (no channel scope).
// Used by attach/detach handlers for owner authorization.
func (s *Service) GetDaemonByID(ctx context.Context, daemonID placement.DaemonID) (Daemon, error) {
	return s.getDaemon(ctx, `
		SELECT id, channel_id, owner_id, name, api_key, api_key_prefix, status,
		       COALESCE(hostname,''), COALESCE(proxy_version,''),
		       COALESCE(last_heartbeat,0), created_at
		  FROM daemons
		 WHERE id = ? AND api_key <> ''
		 LIMIT 1`, string(daemonID))
}

// GetDaemonByAPIKey resolves a presented api-key to the daemon row +
// pre-loads attached channels so the ws handler can install facades per-
// channel in one round trip.
func (s *Service) GetDaemonByAPIKey(ctx context.Context, apiKey string) (Daemon, error) {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return Daemon{}, ErrDaemonNotFound
	}
	row, err := s.getDaemon(ctx, `
		SELECT id, channel_id, owner_id, name, api_key, api_key_prefix, status,
		       COALESCE(hostname,''), COALESCE(proxy_version,''),
		       COALESCE(last_heartbeat,0), created_at
		  FROM daemons
		 WHERE api_key = ? AND api_key <> ''
		 LIMIT 1`, apiKey)
	if err != nil {
		return Daemon{}, err
	}
	attached, err := s.ListDaemonAttachments(ctx, row.ID)
	if err != nil {
		return Daemon{}, err
	}
	row.AttachedChannels = attached
	return row, nil
}

// DeleteDaemon revokes a daemon by id. Owner authorization is the
// caller's responsibility (handler verifies row.OwnerID matches the
// session user). Removes active actor projections across every attached
// channel, closes the ws, and notifies cloud daemon framework so facades
// retire.
func (s *Service) DeleteDaemon(ctx context.Context, daemonID placement.DaemonID) error {
	row, err := s.GetDaemonByID(ctx, daemonID)
	if err != nil {
		return err
	}
	attached, _ := s.ListDaemonAttachments(ctx, daemonID)
	actors, _ := s.ListDaemonActiveActorIDs(ctx, daemonID)
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM daemons WHERE id = ? AND api_key <> ''`,
		string(daemonID),
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
	if notifier := s.proxyDaemonNotifier(); notifier != nil && row.ID != "" && len(actors) > 0 {
		// Pre-T7 the offline notifier knew exactly one channel from
		// row.ChannelID. Now we have N — fire one notification per
		// attachment with the same actor list (each cloud daemon facade
		// for that channel needs to retire its registration).
		for _, chID := range attached {
			rowCopy := row
			rowCopy.ChannelID = chID
			_ = notifier.NotifyProxyDaemonOffline(context.WithoutCancel(ctx), rowCopy, actors)
		}
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

// ApplyDaemonReady commits a ready frame's actor list against every
// channel the daemon is currently attached to. Pre-T7 this wrote one
// (channel, actor) pair per actor; now it writes len(actors) ×
// len(AttachedChannels) — one routing entry per actor instance per
// channel — so a single user-machine install services all attached
// channels with no reconnect. A daemon with zero attachments still
// transitions to status=online (so the UI shows it as installed-but-
// unused) but writes no daemon_active_actors rows; the rows appear on
// the next attach via AttachDaemonToChannel.
func (s *Service) ApplyDaemonReady(ctx context.Context, d Daemon, ready DaemonReadyInput) error {
	if d.ID == "" {
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

	// Refresh the per-daemon hosted-adapter manifest. This is independent
	// of channel attachments — it records "what this machine claims to
	// host" so the global 我的设备 page can show every adapter even when
	// the daemon is not yet attached to any channel.
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM daemon_hosted_actors WHERE daemon_id = ?`,
		string(d.ID),
	); err != nil {
		return fmt.Errorf("devicebus: ready clear hosted actors: %w", err)
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

		// Always record hosted manifest entry, even when no channels
		// attached (UI shows the adapter as available-but-unbound).
		capability := ""
		if len(a.CapabilitySet) > 0 {
			capability = string(a.CapabilitySet)
		}
		facadeState := "not_attached"
		if len(d.AttachedChannels) > 0 {
			facadeState = "pending"
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO daemon_hosted_actors
			  (daemon_id, actor_id, capability_set, last_ready_at, facade_state, facade_updated_at)
			VALUES (?, ?, ?, ?, ?, ?)`,
			string(d.ID), string(actorID), capability, now, facadeState, now,
		); err != nil {
			return fmt.Errorf("devicebus: ready insert hosted actor: %w", err)
		}

		// Insert one row per (attached channel × actor). Skip the
		// loop entirely if the daemon is unattached — it just goes
		// online with no installed actors.
		for _, chID := range d.AttachedChannels {
			var existing string
			err := tx.QueryRowContext(ctx,
				`SELECT daemon_id FROM daemon_active_actors WHERE channel_id=? AND actor_id=?`,
				string(chID), string(actorID),
			).Scan(&existing)
			switch {
			case err == nil && existing != string(d.ID):
				return fmt.Errorf("%w: channel=%s actor=%s existing_daemon=%s",
					ErrDaemonActorConflict, chID, actorID, existing)
			case err != nil && !errors.Is(err, sql.ErrNoRows):
				return fmt.Errorf("devicebus: ready actor conflict lookup: %w", err)
			}

			if _, err := tx.ExecContext(ctx, `
				INSERT INTO daemon_active_actors
				  (channel_id, actor_id, daemon_id, registered_at, last_seen_at)
				VALUES (?, ?, ?, ?, ?)`,
				string(chID), string(actorID), string(d.ID), now, now,
			); err != nil {
				return fmt.Errorf("devicebus: ready insert active actor: %w", err)
			}
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
		for _, chID := range d.AttachedChannels {
			s.actorToDaemon[routeKey(chID, actorID)] = d.ID
		}
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
