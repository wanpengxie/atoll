package gateway

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

type sessionEntry struct {
	principal string
	expires   time.Time
}

// SessionStore is process-local mechanism state. Restart revokes every token
// by construction; no session row exists in the registry.
type SessionStore struct {
	mu      sync.Mutex
	entries map[string]sessionEntry
	now     func() time.Time
}

func NewSessionStore() *SessionStore {
	return &SessionStore{entries: map[string]sessionEntry{}, now: time.Now}
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
func (s *SessionStore) Revoke(token string) { s.mu.Lock(); delete(s.entries, token); s.mu.Unlock() }
