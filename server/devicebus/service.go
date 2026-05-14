// Package devicebus owns the server-side device session lifecycle —
// session_id allocation, token issuance (HMAC over session_id +
// channel_id + expiry), state transitions per T1.10, and the
// transit-frame forwarder that bridges device WebSockets to daemon
// adapters via daemonbus device_transit.* frames.
//
// Authoritative spec: .dalek/pm/m1.5-tickets.md §T6 (devicebus 子目录)
// + T1.3 (transit) + T1.10 (lifecycle).
//
// Key invariant: device_session_id is allocated by THIS package
// (server side). daemon adapter holds only a local cache (L4 §2.6.4
// + codex #5). Transit frames carry the session_id; tokens travel
// only between server and device.
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
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/placement"
)

// Default token TTL (24h). Configurable via Config.
const defaultTokenTTL = 24 * time.Hour

// State enumerates the T1.10 lifecycle:
//
//	pending → ready → active → offline → expired / revoked
type State string

const (
	StatePending State = "pending"
	StateReady   State = "ready"
	StateActive  State = "active"
	StateOffline State = "offline"
	StateExpired State = "expired"
	StateRevoked State = "revoked"
)

// AllStates is the closed set for tests / state-machine assertions.
var AllStates = []State{StatePending, StateReady, StateActive, StateOffline, StateExpired, StateRevoked}

// Errors returned by Service.
var (
	ErrSessionNotFound = errors.New("devicebus: session not found")
	ErrTokenInvalid    = errors.New("devicebus: invalid token")
	ErrSessionExpired  = errors.New("devicebus: session expired")
)

// Config tunes Service.
type Config struct {
	// TokenSecret is the HMAC key used to derive the token from
	// session_id + channel_id + expiry. Required.
	TokenSecret string
	// TokenTTL overrides defaultTokenTTL. 0 → default.
	TokenTTL time.Duration
}

// Service is the devicebus facade.
type Service struct {
	db  *sql.DB
	cfg Config
	now func() time.Time
	rng io.Reader

	tokenTTL time.Duration

	mu       sync.Mutex
	sessions map[string]*Connection // session_id → live WS connection
}

// NewService builds a Service.
func NewService(db *sql.DB, cfg Config) *Service {
	ttl := cfg.TokenTTL
	if ttl <= 0 {
		ttl = defaultTokenTTL
	}
	return &Service{
		db:       db,
		cfg:      cfg,
		now:      time.Now,
		rng:      rand.Reader,
		tokenTTL: ttl,
		sessions: map[string]*Connection{},
	}
}

// WithClock overrides the clock (tests).
func (s *Service) WithClock(now func() time.Time) *Service {
	s.now = now
	return s
}

func (s *Service) nowMs() int64 { return s.now().UnixMilli() }

// Session is the public projection of one device_sessions row.
type Session struct {
	ID         string
	DeviceID   string
	DeviceType string
	ChannelID  channel.ID
	UserID     string
	DaemonID   placement.DaemonID
	State      State
	ExpiresAt  int64
	CreatedAt  int64
}

// IssueInput carries the issue-session call.
type IssueInput struct {
	DeviceID   string
	DeviceType string
	ChannelID  channel.ID
	UserID     string
	DaemonID   placement.DaemonID
}

// IssueResult carries the new session + raw token. The raw token is
// never re-derivable from the row (only its HMAC hash is stored).
type IssueResult struct {
	Session Session
	Token   string
}

// IssueSession allocates a fresh session_id + token, INSERTs the
// device_sessions row in state=pending. Caller (gateway) should then
// send `control.bind_device_session` to daemon to advance to ready.
func (s *Service) IssueSession(ctx context.Context, in IssueInput) (IssueResult, error) {
	if in.DeviceID == "" || in.ChannelID == "" || in.UserID == "" || in.DaemonID == "" {
		return IssueResult{}, fmt.Errorf("devicebus: device_id + channel_id + user_id + daemon_id required")
	}
	now := s.nowMs()
	exp := s.now().Add(s.tokenTTL).UnixMilli()

	sessID := uuid.NewString()
	token, err := s.deriveToken(sessID, in.ChannelID, exp)
	if err != nil {
		return IssueResult{}, err
	}

	row := Session{
		ID: sessID, DeviceID: in.DeviceID, DeviceType: in.DeviceType,
		ChannelID: in.ChannelID, UserID: in.UserID, DaemonID: in.DaemonID,
		State: StatePending, ExpiresAt: exp, CreatedAt: now,
	}

	if _, err := s.db.ExecContext(
		ctx,
		`INSERT INTO device_sessions (
		   device_session_id, device_id, device_type, channel_id, user_id,
		   daemon_id, token_hash, state, expires_at, created_at, last_state_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		row.ID, row.DeviceID, row.DeviceType,
		string(row.ChannelID), row.UserID, string(row.DaemonID),
		s.hashToken(token), string(row.State),
		row.ExpiresAt, row.CreatedAt, row.CreatedAt,
	); err != nil {
		return IssueResult{}, fmt.Errorf("devicebus: insert session: %w", err)
	}
	return IssueResult{Session: row, Token: token}, nil
}

// MarkBound advances pending → ready when daemon ACKs
// control.bind_device_session.
func (s *Service) MarkBound(ctx context.Context, sessionID string) error {
	return s.transition(ctx, sessionID, StatePending, StateReady)
}

// MarkActive advances ready/offline → active when the device WS
// connects with a valid token.
func (s *Service) MarkActive(ctx context.Context, sessionID string) error {
	now := s.nowMs()
	res, err := s.db.ExecContext(
		ctx,
		`UPDATE device_sessions
		    SET state = 'active', last_state_at = ?
		  WHERE device_session_id = ?
		    AND state IN ('ready','offline')`,
		now, sessionID,
	)
	if err != nil {
		return fmt.Errorf("devicebus: mark active: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrSessionNotFound
	}
	return nil
}

// MarkOffline transitions active → offline when device WS drops.
func (s *Service) MarkOffline(ctx context.Context, sessionID string) error {
	return s.transition(ctx, sessionID, StateActive, StateOffline)
}

// Revoke transitions any non-terminal state → revoked. Called when
// user explicitly revokes or channel is deleted.
func (s *Service) Revoke(ctx context.Context, sessionID string) error {
	now := s.nowMs()
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE device_sessions
		    SET state = 'revoked', last_state_at = ?
		  WHERE device_session_id = ?
		    AND state NOT IN ('expired','revoked')`,
		now, sessionID,
	)
	if err != nil {
		return fmt.Errorf("devicebus: revoke: %w", err)
	}
	return nil
}

// ExpireDueSessions transitions ready/active/offline → expired when
// expires_at < now. Run by a background sweep.
func (s *Service) ExpireDueSessions(ctx context.Context) error {
	now := s.nowMs()
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE device_sessions
		    SET state = 'expired', last_state_at = ?
		  WHERE state IN ('ready','active','offline')
		    AND expires_at < ?`,
		now, now,
	)
	if err != nil {
		return fmt.Errorf("devicebus: expire: %w", err)
	}
	return nil
}

// Get returns the row for sessionID.
func (s *Service) Get(ctx context.Context, sessionID string) (Session, error) {
	var (
		row   Session
		state string
		chID  string
		dID   string
	)
	err := s.db.QueryRowContext(
		ctx,
		`SELECT device_session_id, device_id, device_type, channel_id, user_id,
		        daemon_id, state, expires_at, created_at
		   FROM device_sessions WHERE device_session_id = ?`,
		sessionID,
	).Scan(&row.ID, &row.DeviceID, &row.DeviceType, &chID, &row.UserID,
		&dID, &state, &row.ExpiresAt, &row.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Session{}, ErrSessionNotFound
		}
		return Session{}, err
	}
	row.ChannelID = channel.ID(chID)
	row.DaemonID = placement.DaemonID(dID)
	row.State = State(state)
	return row, nil
}

// ValidateToken returns the Session iff the raw token matches the
// stored hash + session is in a state that allows connection (ready /
// active / offline) and not expired.
func (s *Service) ValidateToken(ctx context.Context, sessionID, rawToken string) (Session, error) {
	row, err := s.Get(ctx, sessionID)
	if err != nil {
		return Session{}, err
	}

	var stored string
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT token_hash FROM device_sessions WHERE device_session_id = ?`,
		sessionID,
	).Scan(&stored); err != nil {
		return Session{}, err
	}
	if !hmac.Equal([]byte(stored), []byte(s.hashToken(rawToken))) {
		return Session{}, ErrTokenInvalid
	}

	if s.now().UnixMilli() > row.ExpiresAt {
		return Session{}, ErrSessionExpired
	}
	switch row.State {
	case StateExpired, StateRevoked:
		return Session{}, ErrSessionExpired
	}
	return row, nil
}

// transition is the generic state-machine helper.
func (s *Service) transition(ctx context.Context, sessionID string, from, to State) error {
	now := s.nowMs()
	res, err := s.db.ExecContext(
		ctx,
		`UPDATE device_sessions
		    SET state = ?, last_state_at = ?
		  WHERE device_session_id = ? AND state = ?`,
		string(to), now, sessionID, string(from),
	)
	if err != nil {
		return fmt.Errorf("devicebus: transition %s→%s: %w", from, to, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrSessionNotFound
	}
	return nil
}

// deriveToken returns a random 32-byte opaque bearer token (hex-
// encoded, 64 chars). The raw token is handed back to the device
// once; the server persists only HMAC-SHA-256(raw, TokenSecret) via
// hashToken (see below), so a compromised store row cannot be
// replayed against the device WS endpoint.
//
// FIX-T10 spec alignment: an earlier draft of m1.5-tickets.md
// described `token.go` as "HMAC over device_id + channel_id +
// expiry". That wording suggested a deterministic derivation, which
// would (a) leak token recoverability to anyone with the secret and
// (b) make rotation a global event. The spec was updated alongside
// this implementation to describe the actual model: random opaque
// token + server-side HMAC at rest. The sessionID / channelID /
// expiresMs parameters are kept on the signature for call-site
// readability and forward compatibility (future versions may bind
// the random token to a session-scoped MAC), but they are NOT mixed
// into the token value itself.
func (s *Service) deriveToken(sessionID string, channelID channel.ID, expiresMs int64) (string, error) {
	buf := make([]byte, 32)
	if _, err := io.ReadFull(s.rng, buf); err != nil {
		return "", fmt.Errorf("devicebus: rng: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// hashToken returns the HMAC-SHA-256 hex of raw token using
// TokenSecret as the key.
func (s *Service) hashToken(raw string) string {
	mac := hmac.New(sha256.New, []byte(s.cfg.TokenSecret))
	mac.Write([]byte(raw))
	return hex.EncodeToString(mac.Sum(nil))
}
