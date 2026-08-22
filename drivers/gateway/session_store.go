package gateway

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
)

type sessionEntry struct {
	principal string
	expires   time.Time
}

type sessionFileRow struct {
	Token     string `json:"token"`
	Principal string `json:"principal"`
	ExpiresAt int64  `json:"expires_at"`
}

// SessionStore is mechanism state, not a registry row. With a path it survives
// process restarts (the file lives in the install directory, so reinstall —
// not restart — is what revokes everything); without a path it stays
// process-local, for tests and ephemeral hosts.
type SessionStore struct {
	mu      sync.Mutex
	entries map[string]sessionEntry
	now     func() time.Time
	path    string
}

func NewSessionStore() *SessionStore {
	return &SessionStore{entries: map[string]sessionEntry{}, now: time.Now}
}

// OpenSessionStore loads persisted sessions from path (best effort: a missing
// or corrupt file just starts empty) and persists every change back to it.
func OpenSessionStore(path string) *SessionStore {
	s := &SessionStore{entries: map[string]sessionEntry{}, now: time.Now, path: path}
	raw, err := os.ReadFile(path)
	if err != nil {
		return s
	}
	var rows []sessionFileRow
	if json.Unmarshal(raw, &rows) != nil {
		return s
	}
	now := s.now()
	for _, row := range rows {
		expires := time.Unix(0, row.ExpiresAt)
		if row.Token == "" || row.Principal == "" || !now.Before(expires) {
			continue
		}
		s.entries[row.Token] = sessionEntry{principal: row.Principal, expires: expires}
	}
	return s
}

// persistLocked writes the current table atomically. Best effort: sessions are
// a convenience cache — failing to persist must never fail the login itself.
func (s *SessionStore) persistLocked() {
	if s.path == "" {
		return
	}
	rows := make([]sessionFileRow, 0, len(s.entries))
	for token, entry := range s.entries {
		rows = append(rows, sessionFileRow{Token: token, Principal: entry.principal, ExpiresAt: entry.expires.UnixNano()})
	}
	raw, err := json.Marshal(rows)
	if err != nil {
		return
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".atoll-sessions-*")
	if err != nil {
		return
	}
	name := tmp.Name()
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		os.Remove(name)
		return
	}
	if tmp.Close() != nil {
		os.Remove(name)
		return
	}
	if os.Rename(name, s.path) != nil {
		os.Remove(name)
	}
}

func (s *SessionStore) Mint(principal string, ttl time.Duration) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for token, entry := range s.entries {
		if !now.Before(entry.expires) {
			delete(s.entries, token)
		}
	}
	token := uuid.NewString()
	s.entries[token] = sessionEntry{principal: principal, expires: now.Add(ttl)}
	s.persistLocked()
	return token
}
func (s *SessionStore) Verify(token string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.entries[token]
	if !ok {
		return "", false
	}
	if !s.now().Before(entry.expires) {
		delete(s.entries, token)
		return "", false
	}
	return entry.principal, true
}
func (s *SessionStore) Revoke(token string) {
	s.mu.Lock()
	delete(s.entries, token)
	s.persistLocked()
	s.mu.Unlock()
}
