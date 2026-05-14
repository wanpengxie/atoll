// Package identity owns the server-side user identity flow: register
// (email + password + verification code), login (bcrypt verification),
// session (cookie + sqlite token), verification (demo-period log to
// stdout, NO SMTP) and the auth middleware that pulls user_id from the
// cookie for downstream handlers.
//
// Authoritative spec: .dalek/pm/m1.5-tickets.md §T6 (identity 子目录) +
// server-side sqlite schema 精简 (users / verification_codes / sessions).
//
// Demo-period constraints:
//   - bcrypt cost = 4 (configurable; spec says demo)
//   - verification codes are 6-digit numeric, logged to stdout when
//     IssueCode is called; production should plug in SMTP via
//     ServiceOption + Service.notifyFn.
//   - sessions are opaque random tokens; only the HMAC-SHA-256 hex of
//     the raw token lives in sqlite (so DB compromise doesn't leak
//     bearer tokens).
//
// Threading: Service is safe for concurrent use — every operation
// runs against the *sql.DB pool, no mutable in-memory state.
package identity

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

// Default bcrypt cost — keep low for demo + tests so registration is
// snappy. Override via Config.BcryptCost.
const defaultBcryptCost = 4

// Default session TTL — refresh on every Authenticate call within the
// remaining lifetime.
const defaultSessionTTL = 30 * 24 * time.Hour

// Verification code TTL — 15 minutes, in line with typical demo flows.
const defaultVerificationTTL = 15 * time.Minute

// VerificationPurpose enumerates the lifecycle of a code. Demo only
// needs PurposeRegister but the column is wired so adding reset /
// 2fa is a single new value not a schema change.
type VerificationPurpose string

const (
	PurposeRegister VerificationPurpose = "register"
)

// Errors returned by Service. Caller-facing endpoints translate
// these to HTTP 400 / 401 / 409.
var (
	ErrEmailRequired      = errors.New("identity: email required")
	ErrPasswordRequired   = errors.New("identity: password required")
	ErrPasswordTooShort   = errors.New("identity: password must be ≥ 8 chars")
	ErrEmailAlreadyExists = errors.New("identity: email already registered")
	ErrInvalidCredentials = errors.New("identity: invalid email or password")
	ErrCodeRequired       = errors.New("identity: verification code required")
	ErrCodeInvalid        = errors.New("identity: invalid or expired verification code")
	ErrSessionInvalid     = errors.New("identity: invalid or expired session")
	ErrUserNotFound       = errors.New("identity: user not found")
)

// Config carries construction-time options.
type Config struct {
	// SessionSecret seeds the HMAC used to derive the stored token
	// hash. Empty falls back to a dev string — production MUST set
	// this via env / flag.
	SessionSecret string
	// BcryptCost overrides defaultBcryptCost. <= 0 uses the default.
	BcryptCost int
	// SessionTTL overrides defaultSessionTTL. 0 uses default.
	SessionTTL time.Duration
	// VerificationTTL overrides defaultVerificationTTL. 0 uses default.
	VerificationTTL time.Duration
	// Now returns the current wall-clock; tests inject a fixed clock.
	// Nil falls back to time.Now.
	Now func() time.Time
	// NotifyCode is called when a verification code is issued. Demo
	// default logs to stdout; production wires SMTP / SMS. nil keeps
	// the default behaviour.
	NotifyCode func(email, code string, purpose VerificationPurpose)
}

// Service is the identity facade — built once per process and shared
// across HTTP handlers.
type Service struct {
	db   *sql.DB
	cfg  Config
	now  func() time.Time
	rand io.Reader

	bcryptCost      int
	sessionTTL      time.Duration
	verificationTTL time.Duration
	notify          func(email, code string, purpose VerificationPurpose)
}

// NewService constructs a Service rooted at db. The db must already
// have the 0001_identity migration applied (server/store.Apply does
// this transparently).
func NewService(db *sql.DB, cfg Config) *Service {
	if cfg.BcryptCost <= 0 {
		cfg.BcryptCost = defaultBcryptCost
	}
	if cfg.SessionTTL <= 0 {
		cfg.SessionTTL = defaultSessionTTL
	}
	if cfg.VerificationTTL <= 0 {
		cfg.VerificationTTL = defaultVerificationTTL
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	notify := cfg.NotifyCode
	if notify == nil {
		notify = func(email, code string, purpose VerificationPurpose) {
			log.Printf("[identity] verification code email=%s purpose=%s code=%s", email, purpose, code)
		}
	}
	return &Service{
		db:              db,
		cfg:             cfg,
		now:             cfg.Now,
		rand:            rand.Reader,
		bcryptCost:      cfg.BcryptCost,
		sessionTTL:      cfg.SessionTTL,
		verificationTTL: cfg.VerificationTTL,
		notify:          notify,
	}
}

// nowMs returns the current wall-clock as Unix milliseconds.
func (s *Service) nowMs() int64 { return s.now().UnixMilli() }

// hashToken returns the hex-encoded HMAC-SHA-256 of a raw bearer
// token using the configured SessionSecret. Stored in sqlite so DB
// theft never leaks the bearer.
func (s *Service) hashToken(raw string) string {
	mac := hmac.New(sha256.New, []byte(s.cfg.SessionSecret))
	mac.Write([]byte(raw))
	return hex.EncodeToString(mac.Sum(nil))
}

// normalizeEmail folds case + trims whitespace. We do NOT do
// RFC-5321-strict validation — demo uses are local + the verification
// code is the real proof.
func normalizeEmail(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

// generateCode produces a 6-digit numeric verification code.
func (s *Service) generateCode() (string, error) {
	const digits = 6
	var b strings.Builder
	for i := 0; i < digits; i++ {
		n, err := rand.Int(s.rand, big.NewInt(10))
		if err != nil {
			return "", fmt.Errorf("identity: rng: %w", err)
		}
		b.WriteByte(byte('0' + n.Int64()))
	}
	return b.String(), nil
}

// generateSessionToken returns a base16 32-byte random token (256
// bits of entropy) — opaque to clients, never re-derivable.
func (s *Service) generateSessionToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := io.ReadFull(s.rand, buf); err != nil {
		return "", fmt.Errorf("identity: rng: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// hashPassword is a thin wrapper over bcrypt — exposed for tests.
func (s *Service) hashPassword(pw string) (string, error) {
	h, err := bcrypt.GenerateFromPassword([]byte(pw), s.bcryptCost)
	if err != nil {
		return "", fmt.Errorf("identity: bcrypt: %w", err)
	}
	return string(h), nil
}

// checkPassword reports whether pw matches the stored bcrypt hash.
func (s *Service) checkPassword(stored, pw string) error {
	return bcrypt.CompareHashAndPassword([]byte(stored), []byte(pw))
}

// newID generates a UUID v4 — used as the user_id primary key.
func newID() string { return uuid.NewString() }
