// Package devicebus owns the server-side actor registration used by
// browser devices to attach to a channel-local adapter actor.
package devicebus

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/pkg/requestctx"
	"github.com/wanpengxie/ActOS/server/channelaccess"
)

const defaultTokenTTL = 30 * 24 * time.Hour

var (
	ErrRegistrationNotFound  = errors.New("devicebus: actor registration not found")
	ErrTokenInvalid          = errors.New("devicebus: invalid token")
	ErrTokenExpired          = errors.New("devicebus: token expired")
	ErrDeviceTypeUnsupported = errors.New("devicebus: device_type unsupported")
)

type Config struct {
	TokenSecret string
	TokenTTL    time.Duration

	AllowedOrigins     []string
	AllowMissingOrigin bool

	PingCadence      time.Duration
	IdleReadTimeout  time.Duration
	PingWriteTimeout time.Duration

	Logger *slog.Logger
}

// LifecycleNotifier receives devicebus connection lifecycle signals
// (register / unregister / token expiry) so the gateway can forward
// them to the owning daemon as `device_transit.lifecycle` frames.
// The notifier is optional; when nil, lifecycle signals are dropped.
type LifecycleNotifier interface {
	NotifyDeviceLifecycle(
		ctx context.Context,
		channelID channel.ID,
		actorID actor.ActorID,
		event devicetransit.LifecycleEvent,
		deviceID string,
		detail string,
	)
}

type Service struct {
	db  *sql.DB
	cfg Config
	now func() time.Time
	rng io.Reader

	tokenTTL time.Duration

	mu      sync.Mutex
	routes  map[string]*Connection
	connGen atomic.Uint64

	accessMu sync.RWMutex
	access   channelaccess.Authorizer

	lifecycleMu sync.RWMutex
	lifecycle   LifecycleNotifier

	allowedOrigins map[string]struct{}
	log            *slog.Logger
}

// SetLifecycleNotifier installs the lifecycle notifier hook. Safe to
// call concurrently with WS handshakes; the latest non-nil notifier
// wins. Passing nil silently disables lifecycle emission.
func (s *Service) SetLifecycleNotifier(n LifecycleNotifier) {
	s.lifecycleMu.Lock()
	s.lifecycle = n
	s.lifecycleMu.Unlock()
}

func (s *Service) lifecycleNotifier() LifecycleNotifier {
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	return s.lifecycle
}

type AccessAuthorizer = channelaccess.Authorizer

func (s *Service) SetAccessAuthorizer(a channelaccess.Authorizer) {
	s.accessMu.Lock()
	s.access = a
	s.accessMu.Unlock()
}

func (s *Service) accessAuthorizer() channelaccess.Authorizer {
	s.accessMu.RLock()
	defer s.accessMu.RUnlock()
	return s.access
}

func NewService(db *sql.DB, cfg Config) *Service {
	ttl := cfg.TokenTTL
	if ttl <= 0 {
		ttl = defaultTokenTTL
	}
	allowed := make(map[string]struct{}, len(cfg.AllowedOrigins))
	for _, origin := range cfg.AllowedOrigins {
		origin = strings.TrimSpace(origin)
		if origin != "" {
			allowed[origin] = struct{}{}
		}
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Service{
		db:             db,
		cfg:            cfg,
		now:            time.Now,
		rng:            rand.Reader,
		tokenTTL:       ttl,
		routes:         map[string]*Connection{},
		allowedOrigins: allowed,
		log:            log.With("subsystem", "devicebus"),
	}
}

func (s *Service) WithClock(now func() time.Time) *Service {
	s.now = now
	return s
}

func (s *Service) nowMs() int64 { return s.now().UnixMilli() }

type ActorRegistration struct {
	ActorID    actor.ActorID
	ChannelID  channel.ID
	UserID     string
	DaemonID   placement.DaemonID
	DeviceID   string
	DeviceType string
	ExpiresAt  int64
	CreatedAt  int64
}

type RegisterInput struct {
	ActorID    actor.ActorID
	ChannelID  channel.ID
	UserID     string
	DaemonID   placement.DaemonID
	DeviceID   string
	DeviceType string
}

type RegisterResult struct {
	Registration     ActorRegistration
	Token            string
	TokenFingerprint string
}

const TokenFingerprintLength = 16

func (s *Service) RegisterActor(ctx context.Context, in RegisterInput) (RegisterResult, error) {
	if in.ActorID == "" {
		in.ActorID = "tool:xhs-adapter"
	}
	if in.ChannelID == "" || in.UserID == "" || in.DaemonID == "" || in.DeviceID == "" {
		return RegisterResult{}, fmt.Errorf("devicebus: actor_id + channel_id + user_id + daemon_id + device_id required")
	}
	if !devicetransit.IsXHSDeviceType(in.DeviceType) {
		s.log.Warn("devicebus.register_rejected",
			"reason", "device_type_unsupported",
			"request_id", requestctx.RequestID(ctx),
			"actor_id", string(in.ActorID),
			"device_id", in.DeviceID,
			"device_type", in.DeviceType,
			"channel_id", string(in.ChannelID),
			"user_id", in.UserID,
			"daemon_id", string(in.DaemonID),
		)
		return RegisterResult{}, ErrDeviceTypeUnsupported
	}
	now := s.nowMs()
	exp := s.now().Add(s.tokenTTL).UnixMilli()
	token, err := s.deriveToken()
	if err != nil {
		return RegisterResult{}, err
	}
	hashed := s.hashToken(token)
	row := ActorRegistration{
		ActorID: in.ActorID, ChannelID: in.ChannelID, UserID: in.UserID,
		DaemonID: in.DaemonID, DeviceID: in.DeviceID, DeviceType: in.DeviceType,
		ExpiresAt: exp, CreatedAt: now,
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RegisterResult{}, fmt.Errorf("devicebus: begin register tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM device_actor_tokens WHERE actor_id = ? AND channel_id = ?`,
		string(row.ActorID), string(row.ChannelID),
	); err != nil {
		return RegisterResult{}, fmt.Errorf("devicebus: replace actor token: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO device_actor_tokens (
		  token_hash, actor_id, channel_id, user_id, daemon_id,
		  device_id, device_type, expires_at, created_at, last_active_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)`,
		hashed, string(row.ActorID), string(row.ChannelID), row.UserID,
		string(row.DaemonID), row.DeviceID, row.DeviceType, row.ExpiresAt, row.CreatedAt,
	); err != nil {
		return RegisterResult{}, fmt.Errorf("devicebus: insert actor token: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return RegisterResult{}, fmt.Errorf("devicebus: commit register: %w", err)
	}
	s.closeCurrentConnection(row.ChannelID, row.ActorID)
	s.log.Info("devicebus.actor_token_issued",
		"request_id", requestctx.RequestID(ctx),
		"actor_id", string(row.ActorID),
		"channel_id", string(row.ChannelID),
		"user_id", row.UserID,
		"daemon_id", string(row.DaemonID),
		"device_id", row.DeviceID,
		"device_type", row.DeviceType,
		"expires_at", row.ExpiresAt,
	)
	return RegisterResult{
		Registration:     row,
		Token:            token,
		TokenFingerprint: fingerprintFromHash(hashed),
	}, nil
}

func (s *Service) GetActor(ctx context.Context, channelID channel.ID, actorID actor.ActorID) (ActorRegistration, error) {
	var row ActorRegistration
	var chID, aID, daemonID string
	err := s.db.QueryRowContext(ctx, `
		SELECT actor_id, channel_id, user_id, daemon_id, device_id,
		       device_type, expires_at, created_at
		  FROM device_actor_tokens
		 WHERE actor_id = ? AND channel_id = ?
		 ORDER BY created_at DESC
		 LIMIT 1`,
		string(actorID), string(channelID),
	).Scan(&aID, &chID, &row.UserID, &daemonID, &row.DeviceID,
		&row.DeviceType, &row.ExpiresAt, &row.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ActorRegistration{}, ErrRegistrationNotFound
		}
		return ActorRegistration{}, err
	}
	row.ActorID = actor.ActorID(aID)
	row.ChannelID = channel.ID(chID)
	row.DaemonID = placement.DaemonID(daemonID)
	return row, nil
}

func (s *Service) RevokeActor(ctx context.Context, channelID channel.ID, actorID actor.ActorID) error {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM device_actor_tokens WHERE actor_id = ? AND channel_id = ?`,
		string(actorID), string(channelID),
	)
	if err != nil {
		return fmt.Errorf("devicebus: revoke actor token: %w", err)
	}
	n, _ := res.RowsAffected()
	s.closeCurrentConnection(channelID, actorID)
	if n == 0 {
		return ErrRegistrationNotFound
	}
	return nil
}

func (s *Service) ExpireDueTokens(ctx context.Context) error {
	now := s.nowMs()
	rows, err := s.db.QueryContext(ctx,
		`SELECT channel_id, actor_id FROM device_actor_tokens WHERE expires_at < ?`,
		now,
	)
	if err != nil {
		return fmt.Errorf("devicebus: list expired actor tokens: %w", err)
	}
	var expired []ActorRegistration
	for rows.Next() {
		var chID, actorID string
		if err := rows.Scan(&chID, &actorID); err != nil {
			_ = rows.Close()
			return fmt.Errorf("devicebus: scan expired actor token: %w", err)
		}
		expired = append(expired, ActorRegistration{ChannelID: channel.ID(chID), ActorID: actor.ActorID(actorID)})
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("devicebus: close expired actor token rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("devicebus: iterate expired actor tokens: %w", err)
	}
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM device_actor_tokens WHERE expires_at < ?`,
		now,
	); err != nil {
		return fmt.Errorf("devicebus: expire actor tokens: %w", err)
	}
	for _, r := range expired {
		s.closeCurrentConnection(r.ChannelID, r.ActorID)
	}
	return nil
}

func (s *Service) ValidateToken(ctx context.Context, actorID actor.ActorID, rawToken string) (ActorRegistration, error) {
	hashed := s.hashToken(rawToken)
	var row ActorRegistration
	var chID, aID, daemonID string
	err := s.db.QueryRowContext(ctx, `
		SELECT actor_id, channel_id, user_id, daemon_id, device_id,
		       device_type, expires_at, created_at
		  FROM device_actor_tokens
		 WHERE actor_id = ? AND token_hash = ?`,
		string(actorID), hashed,
	).Scan(&aID, &chID, &row.UserID, &daemonID, &row.DeviceID,
		&row.DeviceType, &row.ExpiresAt, &row.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			s.log.Warn("devicebus.token_invalid",
				"reason", "token_hash_mismatch",
				"request_id", requestctx.RequestID(ctx),
				"actor_id", string(actorID),
			)
			return ActorRegistration{}, ErrTokenInvalid
		}
		return ActorRegistration{}, err
	}
	row.ActorID = actor.ActorID(aID)
	row.ChannelID = channel.ID(chID)
	row.DaemonID = placement.DaemonID(daemonID)
	if s.nowMs() > row.ExpiresAt {
		_ = s.RevokeActor(ctx, row.ChannelID, row.ActorID)
		s.log.Warn("devicebus.token_invalid",
			"reason", "expired",
			"request_id", requestctx.RequestID(ctx),
			"actor_id", string(row.ActorID),
			"channel_id", string(row.ChannelID),
			"expires_at", row.ExpiresAt,
		)
		return ActorRegistration{}, ErrTokenExpired
	}
	return row, nil
}

func fingerprintFromHash(hashed string) string {
	if len(hashed) <= TokenFingerprintLength {
		return hashed
	}
	return hashed[:TokenFingerprintLength]
}

func (s *Service) deriveToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := io.ReadFull(s.rng, buf); err != nil {
		return "", fmt.Errorf("devicebus: rng: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

func (s *Service) hashToken(raw string) string {
	mac := hmac.New(sha256.New, []byte(s.cfg.TokenSecret))
	_, _ = mac.Write([]byte(raw))
	return hex.EncodeToString(mac.Sum(nil))
}

func routeKey(channelID channel.ID, actorID actor.ActorID) string {
	return string(channelID) + "\x00" + string(actorID)
}
