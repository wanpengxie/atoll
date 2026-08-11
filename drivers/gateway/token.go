package gateway

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const AutomationSessionTTL = 365 * 24 * time.Hour

// MintAutomationToken publishes one root session atomically. Publication
// failure revokes the in-memory entry, so no live but undiscoverable token is
// left behind.
func MintAutomationToken(store *SessionStore, principal, path string) (string, error) {
	token := store.Mint(principal, AutomationSessionTTL)
	if err := writeTokenAtomically(path, token); err != nil {
		store.Revoke(token)
		return "", err
	}
	return token, nil
}

func writeTokenAtomically(path, token string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".atoll-token-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.WriteString(token + "\n"); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return fmt.Errorf("publish token: %w", err)
	}
	return nil
}
