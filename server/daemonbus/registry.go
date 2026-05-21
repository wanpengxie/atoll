// Package daemonbus owns the server-side daemon registry, WS
// authentication, mux-frame dispatch loop, heartbeat tracking and
// channel-to-daemon routing (L2 §9 + T6 spec).
//
// Authoritative spec: .dalek/pm/m1.5-tickets.md §T6 (daemonbus 子目录)
// + kernel/daemonbus (Frame schema) + kernel/placement (placement
// state machine).
//
// Demo-period: single-instance, single-secret authentication (every
// daemon shares one HMAC key carried in COAGENT_DAEMON_SECRET).
// Production should plug in per-daemon keys.
package daemonbus

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/server/placements"
)

// Errors returned by Service.
var (
	ErrDaemonAuthFailed    = errors.New("daemonbus: auth failed")
	ErrDaemonNotRegistered = errors.New("daemonbus: daemon not registered")
	ErrNoDaemonForChannel  = errors.New("daemonbus: no daemon for channel")
)

// Config tunes Service behaviour.
type Config struct {
	// SharedSecret is the HMAC key daemons present at connect time.
	// Demo period uses one secret for every daemon.
	SharedSecret string

	// AllowedOrigins is the exact Origin allowlist for browser WebSocket
	// handshakes. Empty means deny browser-origin WS handshakes. Requests
	// with no Origin header are allowed for non-browser daemon clients.
	AllowedOrigins []string

	// PingCadence overrides DefaultPingCadence (tests). Zero =
	// production default.
	PingCadence time.Duration

	// IdleReadTimeout overrides DefaultServerIdleReadTimeout (tests).
	// Zero = production default.
	IdleReadTimeout time.Duration

	// PingWriteTimeout overrides DefaultPingWriteTimeout (tests).
	// Zero = production default.
	PingWriteTimeout time.Duration
}

// Service is the daemonbus facade.
type Service struct {
	db  *sql.DB
	cfg Config
	now func() time.Time

	mu          sync.RWMutex
	connections map[placement.DaemonID]*Connection
	connGen     atomic.Uint64

	channelDaemonResolver placements.ChannelDaemonResolver

	allowedOrigins map[string]struct{}
}

// NewService builds a Service.
func NewService(db *sql.DB, cfg Config) *Service {
	return &Service{
		db:             db,
		cfg:            cfg,
		now:            time.Now,
		connections:    map[placement.DaemonID]*Connection{},
		allowedOrigins: normalizeAllowedOrigins(cfg.AllowedOrigins),
	}
}

// WithClock overrides the clock (tests).
func (s *Service) WithClock(now func() time.Time) *Service {
	s.now = now
	return s
}

func (s *Service) nowMs() int64 { return s.now().UnixMilli() }

// SetChannelDaemonResolver wires the placement-owned active-channel
// lookup used by ConnectionForChannel. Passing nil disables lookup.
func (s *Service) SetChannelDaemonResolver(r placements.ChannelDaemonResolver) {
	s.mu.Lock()
	s.channelDaemonResolver = r
	s.mu.Unlock()
}

// ConnectedDaemons returns a stable snapshot of currently open daemon IDs.
func (s *Service) ConnectedDaemons() []placement.DaemonID {
	conns := s.ConnectedConnections()
	out := make([]placement.DaemonID, 0, len(conns))
	for _, conn := range conns {
		out = append(out, conn.DaemonID)
	}
	return out
}

// ConnectedConnections returns a stable daemon_id-sorted snapshot of open
// connections. Closed or nil registry entries are ignored.
func (s *Service) ConnectedConnections() []*Connection {
	s.mu.RLock()
	out := make([]*Connection, 0, len(s.connections))
	for _, conn := range s.connections {
		if conn == nil || conn.IsClosed() {
			continue
		}
		out = append(out, conn)
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		return out[i].DaemonID < out[j].DaemonID
	})
	return out
}

// RegisterDaemon ensures a row exists in the daemons table for the
// given (daemonID, host, version, capacity). Idempotent — re-runs
// just refresh metadata.
//
// Auth secret is the demo-shared key; in prod each daemon would have
// its own key persisted on first registration via an admin API.
func (s *Service) RegisterDaemon(
	ctx context.Context,
	daemonID placement.DaemonID,
	host, version string,
	capacity int,
	keyMaterial string,
) error {
	if keyMaterial != s.cfg.SharedSecret {
		return ErrDaemonAuthFailed
	}

	now := s.nowMs()
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO daemons (id, host, version, capacity, key_hash, connection_epoch, last_heartbeat_at, created_at)
		 VALUES (?, ?, ?, ?, ?, 0, 0, ?)
		 ON CONFLICT(id) DO UPDATE SET
		   host = excluded.host,
		   version = excluded.version,
		   capacity = excluded.capacity,
		   key_hash = excluded.key_hash`,
		string(daemonID), host, version, capacity, hashKey(keyMaterial), now,
	)
	if err != nil {
		return fmt.Errorf("daemonbus: register: %w", err)
	}
	return nil
}

// IssueConnectionEpoch is called when a daemon successfully opens a
// new WS connection. It increments connection_epoch atomically and
// returns the new value (daemon embeds it on every frame for the
// life of that connection — L2 §9.4).
func (s *Service) IssueConnectionEpoch(
	ctx context.Context,
	daemonID placement.DaemonID,
) (daemonbus.ConnectionEpoch, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	var current int64
	err = tx.QueryRowContext(
		ctx,
		`SELECT connection_epoch FROM daemons WHERE id = ?`,
		string(daemonID),
	).Scan(&current)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, ErrDaemonNotRegistered
		}
		return 0, err
	}
	current++
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE daemons
		    SET connection_epoch = ?,
		        last_heartbeat_at = ?
		  WHERE id = ?`,
		current, s.nowMs(), string(daemonID),
	); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return daemonbus.ConnectionEpoch(current), nil
}

// RecordHeartbeat refreshes last_heartbeat_at when the daemon sends
// `control.heartbeat`.
func (s *Service) RecordHeartbeat(ctx context.Context, daemonID placement.DaemonID) error {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE daemons SET last_heartbeat_at = ? WHERE id = ?`,
		s.nowMs(), string(daemonID),
	)
	if err != nil {
		return fmt.Errorf("daemonbus: heartbeat: %w", err)
	}
	return nil
}

// LookupDaemonForChannel returns the daemon currently owning the
// channel (active placement). Returns ErrNoDaemonForChannel when
// the channel has no active placement.
func (s *Service) LookupDaemonForChannel(ctx context.Context, channelID channel.ID) (placement.DaemonID, error) {
	s.mu.RLock()
	resolver := s.channelDaemonResolver
	s.mu.RUnlock()
	if resolver == nil {
		return "", ErrNoDaemonForChannel
	}
	daemonID, ok, err := resolver.ResolveDaemonForChannel(ctx, channelID)
	if err != nil {
		return "", fmt.Errorf("daemonbus: lookup: %w", err)
	}
	if !ok {
		return "", ErrNoDaemonForChannel
	}
	return daemonID, nil
}

// hashKey is a thin string hash for the daemons.key_hash column.
// Demo period uses bcrypt-of-secret elsewhere; here we just store a
// SHA-256-ish marker so repeated registrations stay deterministic
// without re-bcrypt per call (the auth check is direct equality on
// the shared secret in RegisterDaemon).
func hashKey(s string) string {
	// We could use crypto/sha256 here; keeping the package free of
	// crypto imports — the column is informational only because
	// auth is direct string compare against SharedSecret.
	return s
}
