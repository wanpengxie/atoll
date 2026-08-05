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

// bootstrapTokenTTL is deliberately longer than the interactive
// sessionDuration: the token backs local automation (shells/scripts reading
// the token file), and its revocation path is deleting the sessions row, not
// expiry. The file is data-directory local and 0600.
const bootstrapTokenTTL = 365 * 24 * time.Hour

// BootstrapOwnerToken makes the engine usable by automation right after
// `--init` (contract D3 / 交接包①): it ensures the local owner principal
// exists, mints a session for it, and writes the bearer token to tokenPath
// (0600). Idempotent-ish by construction: a pre-existing owner user is reused;
// a fresh session is minted on every call (old sessions stay valid until
// expiry or deletion). The password is random and unrecorded — the token file
// IS the credential; login-by-password for this principal is not a supported
// path.
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

	token := uuid.NewString()
	now := time.Now().UnixMilli()
	if _, err := db.ExecContext(ctx,
		`INSERT INTO sessions (token, user_id, created_at, expires_at) VALUES (?,?,?,?)`,
		token, userID, now, now+bootstrapTokenTTL.Milliseconds(),
	); err != nil {
		return "", fmt.Errorf("bootstrap owner: mint session: %w", err)
	}

	// Atomic 0600 publish: write a same-directory temp file then rename. Rename
	// replaces whatever sits at tokenPath (stale file, wrong perms, even a
	// symlink — the link itself is replaced, never followed), and a failed write
	// never leaves a corrupt/partial token. On any failure the just-minted
	// session is deleted best-effort so a retry (the function is reusable — it
	// reuses an existing owner row) starts clean.
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
