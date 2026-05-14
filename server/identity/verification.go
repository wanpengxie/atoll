package identity

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// IssueCode generates a fresh 6-digit verification code for email +
// purpose, stores it (hashed via the verification_codes table —
// stored as raw code; demo only) and invokes the notify function
// (default: log to stdout).
//
// Idempotency: multiple IssueCode calls for the same (email, purpose)
// create distinct rows — any unconsumed code that matches is
// accepted, so issuing twice just gives the user two valid codes
// until both expire.
func (s *Service) IssueCode(ctx context.Context, email string, purpose VerificationPurpose) (string, error) {
	email = normalizeEmail(email)
	if email == "" {
		return "", ErrEmailRequired
	}

	code, err := s.generateCode()
	if err != nil {
		return "", err
	}

	now := s.nowMs()
	expires := s.now().Add(s.verificationTTL).UnixMilli()

	if _, err := s.db.ExecContext(
		ctx,
		`INSERT INTO verification_codes (email, code, purpose, created_at, expires_at)
		 VALUES (?, ?, ?, ?, ?)`,
		email, code, string(purpose), now, expires,
	); err != nil {
		return "", fmt.Errorf("identity: insert verification_code: %w", err)
	}

	s.notify(email, code, purpose)
	return code, nil
}

// consumeCodeTx verifies a (email, purpose, code) triple is valid +
// unconsumed + not expired, then marks it consumed atomically inside
// the supplied transaction. Returns ErrCodeInvalid for any mismatch.
func (s *Service) consumeCodeTx(ctx context.Context, tx *sql.Tx, email, code string, purpose VerificationPurpose) error {
	if code == "" {
		return ErrCodeRequired
	}
	now := s.nowMs()

	res, err := tx.ExecContext(
		ctx,
		`UPDATE verification_codes
		   SET consumed_at = ?
		 WHERE email     = ?
		   AND purpose   = ?
		   AND code      = ?
		   AND consumed_at = 0
		   AND expires_at >= ?`,
		now, email, string(purpose), code, now,
	)
	if err != nil {
		return fmt.Errorf("identity: consume code: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("identity: consume code rows: %w", err)
	}
	if n == 0 {
		return ErrCodeInvalid
	}
	return nil
}

// PurgeExpiredCodes removes consumed or expired rows. Optional
// housekeeping — Service.RunHousekeeping calls this periodically.
func (s *Service) PurgeExpiredCodes(ctx context.Context) error {
	now := s.nowMs()
	_, err := s.db.ExecContext(
		ctx,
		`DELETE FROM verification_codes
		  WHERE consumed_at != 0
		     OR expires_at  < ?`,
		now,
	)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("identity: purge codes: %w", err)
	}
	return nil
}
