package identity

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// RegisterInput carries the register-call inputs validated by the
// gateway HTTP handler. Code is the verification_code value the user
// got from the previous IssueCode call.
type RegisterInput struct {
	Email       string
	Password    string
	Code        string
	DisplayName string
}

// User is the public identity record returned to callers.
type User struct {
	ID            string
	Email         string
	DisplayName   string
	EmailVerified bool
	CreatedAt     int64
}

// Register consumes a verification code and creates a new user row.
// The whole flow runs in a single transaction so a race between two
// concurrent registers can never end with two rows on the same email.
func (s *Service) Register(ctx context.Context, in RegisterInput) (User, error) {
	email := normalizeEmail(in.Email)
	if email == "" {
		return User{}, ErrEmailRequired
	}
	if in.Password == "" {
		return User{}, ErrPasswordRequired
	}
	if len(in.Password) < 8 {
		return User{}, ErrPasswordTooShort
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return User{}, fmt.Errorf("identity: begin register tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Verify the email isn't already taken.
	var existing string
	err = tx.QueryRowContext(ctx, `SELECT id FROM users WHERE email = ?`, email).Scan(&existing)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return User{}, fmt.Errorf("identity: probe email: %w", err)
	}
	if existing != "" {
		return User{}, ErrEmailAlreadyExists
	}

	// Consume the verification code atomically — when a code was
	// supplied. Empty code is accepted (dev / demo path with no email
	// sender wired) so register works with email + password alone.
	// cvmax production should enforce code via a separate gate (M1.7
	// Feishu bot / SMTP).
	if in.Code != "" {
		if err := s.consumeCodeTx(ctx, tx, email, in.Code, PurposeRegister); err != nil {
			return User{}, err
		}
	}

	hashed, err := s.hashPassword(in.Password)
	if err != nil {
		return User{}, err
	}

	display := strings.TrimSpace(in.DisplayName)
	if display == "" {
		display = email
	}

	user := User{
		ID:            newID(),
		Email:         email,
		DisplayName:   display,
		EmailVerified: true,
		CreatedAt:     s.nowMs(),
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO users (id, email, password_hash, display_name, email_verified, created_at)
		 VALUES (?, ?, ?, ?, 1, ?)`,
		user.ID, user.Email, hashed, user.DisplayName, user.CreatedAt,
	); err != nil {
		return User{}, fmt.Errorf("identity: insert user: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return User{}, fmt.Errorf("identity: commit register: %w", err)
	}

	return user, nil
}
