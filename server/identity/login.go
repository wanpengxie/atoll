package identity

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// LoginInput carries the login-call inputs.
type LoginInput struct {
	Email    string
	Password string
}

// LoginResult is returned on success — Token is the raw bearer to
// hand the client as a cookie.
type LoginResult struct {
	User    User
	Token   string
	Expires int64 // unix ms
}

// Login authenticates a user with email + password and issues a new
// session row + raw token.
func (s *Service) Login(ctx context.Context, in LoginInput) (LoginResult, error) {
	email := normalizeEmail(in.Email)
	if email == "" {
		return LoginResult{}, ErrEmailRequired
	}
	if in.Password == "" {
		return LoginResult{}, ErrPasswordRequired
	}

	var (
		user   User
		hashed string
	)
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, email, password_hash, display_name, email_verified, created_at
		   FROM users
		  WHERE email = ?`,
		email,
	).Scan(&user.ID, &user.Email, &hashed, &user.DisplayName, &user.EmailVerified, &user.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return LoginResult{}, ErrInvalidCredentials
		}
		return LoginResult{}, fmt.Errorf("identity: load user: %w", err)
	}

	if err := s.checkPassword(hashed, in.Password); err != nil {
		return LoginResult{}, ErrInvalidCredentials
	}

	tok, err := s.issueSession(ctx, user.ID)
	if err != nil {
		return LoginResult{}, err
	}

	return LoginResult{
		User:    user,
		Token:   tok.Raw,
		Expires: tok.ExpiresAt,
	}, nil
}
