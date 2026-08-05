package devicehost

// identity.go is the device home's persisted identity — the carrier of the
// home-model rule "身份=持有凭据": a logical device IS whoever holds this
// file, never a hostname, never a display name, never a row looked up by
// name. The daemons row on the server stays the identity AUTHORITY; this file
// only proves possession, so a deleted row wins over a stale file (the reader
// re-mints) and a lost file simply means a fresh device (the old row becomes
// an offline device, reclaimed by the ordinary manual DetachDaemon path).
//
// One file serves both packagings of the carrier: `atoll up` persists the
// provision-minted {daemon_id, api_key}; a standalone `atoll-daemon` persists
// the {server_ws, api_key} it was registered with (--server/--key are
// first-run registration, later runs start bare). Fields a writer does not
// know are simply empty.

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// identityFileName is the device home's identity file, 0600.
const identityFileName = "identity.json"

// Identity is the persisted device identity.
type Identity struct {
	DaemonID string `json:"daemon_id,omitempty"`
	APIKey   string `json:"api_key,omitempty"`
	ServerWS string `json:"server_ws,omitempty"`
}

// LoadIdentity reads the device home's identity. A missing file is a clean
// "no identity yet" (zero value, found=false); a present-but-unreadable or
// corrupt file is an error — silently treating it as absent would mint a new
// device identity while the old one still exists on disk.
func LoadIdentity(home string) (Identity, bool, error) {
	raw, err := os.ReadFile(filepath.Join(home, identityFileName))
	if errors.Is(err, fs.ErrNotExist) {
		return Identity{}, false, nil
	}
	if err != nil {
		return Identity{}, false, fmt.Errorf("devicehost: read identity: %w", err)
	}
	var id Identity
	if err := json.Unmarshal(raw, &id); err != nil {
		return Identity{}, false, fmt.Errorf("devicehost: identity file %s is corrupt: %w", filepath.Join(home, identityFileName), err)
	}
	return id, true, nil
}

// SaveIdentity atomically publishes the identity (same-directory temp file +
// rename, 0600) so a crash mid-write can never leave a corrupt identity — the
// device either is its old self or its new self, never garbage.
func SaveIdentity(home string, id Identity) error {
	raw, err := json.Marshal(id)
	if err != nil {
		return fmt.Errorf("devicehost: encode identity: %w", err)
	}
	path := filepath.Join(home, identityFileName)
	tmp, err := os.CreateTemp(home, ".identity-*")
	if err != nil {
		return fmt.Errorf("devicehost: write identity: %w", err)
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("devicehost: write identity: %w", err)
	}
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return fmt.Errorf("devicehost: write identity: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("devicehost: write identity: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("devicehost: publish identity: %w", err)
	}
	return nil
}
