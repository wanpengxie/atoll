package identity

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// sessionToken is the output of issueSession — the raw token is given
// to the client cookie; only the hash is stored.
type sessionToken struct {
	Raw       string
	Hash      string
	UserID    string
	CreatedAt int64
	ExpiresAt int64
}

// issueSession creates a fresh session row + raw token bound to
// userID. Raw token is opaque + cryptographically random; the
// database stores only its HMAC hash.
func (s *Service) issueSession(ctx context.Context, userID string) (sessionToken, error) {
	raw, err := s.generateSessionToken()
	if err != nil {
		return sessionToken{}, err
	}
	now := s.nowMs()
	exp := s.now().Add(s.sessionTTL).UnixMilli()

	tok := sessionToken{
		Raw:       raw,
		Hash:      s.hashToken(raw),
		UserID:    userID,
		CreatedAt: now,
		ExpiresAt: exp,
	}

	if _, err := s.db.ExecContext(
		ctx,
		`INSERT INTO sessions (token_hash, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)`,
		tok.Hash, tok.UserID, tok.CreatedAt, tok.ExpiresAt,
	); err != nil {
		return sessionToken{}, fmt.Errorf("identity: insert session: %w", err)
	}
	return tok, nil
}

// Authenticate resolves a raw session token to a User. Returns
// ErrSessionInvalid when the token is expired / unknown / revoked.
func (s *Service) Authenticate(ctx context.Context, rawToken string) (User, error) {
	if rawToken == "" {
		return User{}, ErrSessionInvalid
	}
	now := s.nowMs()
	hashed := s.hashToken(rawToken)

	var u User
	err := s.db.QueryRowContext(
		ctx,
		`SELECT u.id, u.email, u.display_name, u.email_verified, u.created_at
		   FROM sessions s
		   JOIN users u ON u.id = s.user_id
		  WHERE s.token_hash = ?
		    AND s.expires_at >= ?`,
		hashed, now,
	).Scan(&u.ID, &u.Email, &u.DisplayName, &u.EmailVerified, &u.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return User{}, ErrSessionInvalid
		}
		return User{}, fmt.Errorf("identity: load session: %w", err)
	}
	return u, nil
}

// Logout invalidates a single session by raw token.
func (s *Service) Logout(ctx context.Context, rawToken string) error {
	if rawToken == "" {
		return nil
	}
	_, err := s.db.ExecContext(
		ctx,
		`DELETE FROM sessions WHERE token_hash = ?`,
		s.hashToken(rawToken),
	)
	if err != nil {
		return fmt.Errorf("identity: delete session: %w", err)
	}
	return nil
}

// PurgeExpiredSessions removes rows whose expires_at is in the past.
func (s *Service) PurgeExpiredSessions(ctx context.Context) error {
	now := s.nowMs()
	_, err := s.db.ExecContext(ctx, `DELETE FROM sessions WHERE expires_at < ?`, now)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("identity: purge sessions: %w", err)
	}
	return nil
}

// GetUser returns the user row for a userID; used by other services
// (catalog, gateway) that already have a user_id from the auth
// middleware.
func (s *Service) GetUser(ctx context.Context, userID string) (User, error) {
	var u User
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, email, display_name, email_verified, created_at
		   FROM users WHERE id = ?`,
		userID,
	).Scan(&u.ID, &u.Email, &u.DisplayName, &u.EmailVerified, &u.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return User{}, ErrUserNotFound
		}
		return User{}, err
	}
	return u, nil
}
