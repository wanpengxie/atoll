package app

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

// bootstrapOwnerEmail names the local automation principal minted by --init.
// It is an ordinary user row: same door, same rights, no special casing
// anywhere else (credentials map to a principal — the operator behind it is
// out of scope).
const bootstrapOwnerEmail = "owner@atoll.local"

// bootstrapTokenTTL is a backstop ceiling, not the credential's real
// lifetime: every boot ROTATES the token (see BootstrapOwnerToken), so in
// steady state a session lives only until the next restart. The TTL merely
// bounds a node that never restarts. The file is data-directory local and 0600.
const bootstrapTokenTTL = 365 * 24 * time.Hour

// BootstrapOwnerToken rotates the local automation credential (contract D3 /
// 交接包①): it ensures the local owner principal exists, then — in one
// transaction — mints a fresh session and revokes every other session of that
// principal, and finally publishes the new bearer token to tokenPath (0600).
//
// Restart IS the rotation: at any moment the bootstrap owner has EXACTLY ONE
// live session (a pinned invariant), so the whole response to a suspected
// token leak is "restart the node". Consumers must read the token file per
// use, never copy the value out. The password is random and unrecorded — the
// token file IS the credential; login-by-password for this principal is not a
// supported path.
func BootstrapOwnerToken(ctx context.Context, db *sql.DB, tokenPath string) (string, error) {
	var userID string
	err := db.QueryRowContext(ctx,
		`SELECT id FROM users WHERE email = ?`, bootstrapOwnerEmail,
	).Scan(&userID)
	if err == sql.ErrNoRows {
		hash, herr := bcrypt.GenerateFromPassword([]byte(uuid.NewString()), bcryptCost)
		if herr != nil {
			return "", fmt.Errorf("bootstrap owner: hash: %w", herr)
		}
		userID = uuid.NewString()
		if _, ierr := db.ExecContext(ctx,
			`INSERT INTO users (id, email, password, display_name, created_at) VALUES (?,?,?,?,?)`,
			userID, bootstrapOwnerEmail, string(hash), "Owner", time.Now().UnixMilli(),
		); ierr != nil {
			return "", fmt.Errorf("bootstrap owner: create user: %w", ierr)
		}
	} else if err != nil {
		return "", fmt.Errorf("bootstrap owner: lookup: %w", err)
	}

	// Mint + revoke in one transaction so no interleaving can observe two live
	// sessions (or zero) for the owner.
	token := uuid.NewString()
	now := time.Now().UnixMilli()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("bootstrap owner: rotate: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO sessions (token, user_id, created_at, expires_at) VALUES (?,?,?,?)`,
		token, userID, now, now+bootstrapTokenTTL.Milliseconds(),
	); err != nil {
		_ = tx.Rollback()
		return "", fmt.Errorf("bootstrap owner: mint session: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM sessions WHERE user_id = ? AND token <> ?`, userID, token,
	); err != nil {
		_ = tx.Rollback()
		return "", fmt.Errorf("bootstrap owner: revoke old sessions: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("bootstrap owner: rotate: %w", err)
	}

	// Atomic 0600 publish: write a same-directory temp file then rename. Rename
	// replaces whatever sits at tokenPath (stale file, wrong perms, even a
	// symlink — the link itself is replaced, never followed), and a failed write
	// never leaves a corrupt/partial token. On failure the fresh session is
	// deleted best-effort; the old ones are already revoked, so the node holds
	// ZERO live tokens until a retry succeeds — fail-closed, never two-live.
	if err := writeTokenAtomically(tokenPath, token); err != nil {
		_, _ = db.ExecContext(ctx, `DELETE FROM sessions WHERE token = ?`, token)
		return "", fmt.Errorf("bootstrap owner: publish token file: %w", err)
	}
	return tokenPath, nil
}

func writeTokenAtomically(tokenPath, token string) error {
	tmp, err := os.CreateTemp(filepath.Dir(tokenPath), ".atoll-token-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.WriteString(token + "\n"); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), tokenPath)
}
