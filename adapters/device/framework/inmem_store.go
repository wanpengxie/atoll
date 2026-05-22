package framework

import (
	"context"
	"fmt"
	"sync"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
)

// InMemorySessionStore is the launch reference implementation of
// SessionStore backed by a sync.RWMutex + map. It is the test default
// (every framework / adapter / e2e test imports it from this package)
// and is also a defensible production fallback for daemon processes
// that have not yet wired runtime/store (T3) — kept tiny + dependency-
// free so it can live in `adapters/device/framework` without violating
// the go-arch-lint boundary (no sqlite, no runtime imports).
type InMemorySessionStore struct {
	mu   sync.RWMutex
	rows map[devicetransit.DeviceSessionID]DeviceSession
}

// NewInMemorySessionStore constructs an empty store.
func NewInMemorySessionStore() *InMemorySessionStore {
	return &InMemorySessionStore{rows: map[devicetransit.DeviceSessionID]DeviceSession{}}
}

// Upsert implements SessionStore. Returns the row's Validate() error
// without mutating state on failure (atomicity guarantee — callers may
// retry on a corrected row without re-cleaning the map).
func (s *InMemorySessionStore) Upsert(_ context.Context, sess DeviceSession) error {
	if err := sess.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[sess.SessionID] = sess
	return nil
}

// Get implements SessionStore.
func (s *InMemorySessionStore) Get(_ context.Context, sid devicetransit.DeviceSessionID) (DeviceSession, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	row, ok := s.rows[sid]
	return row, ok, nil
}

// SetState implements SessionStore — enforces CanTransitionTo +
// timestamps LastActiveAt with `at`.
func (s *InMemorySessionStore) SetState(_ context.Context, sid devicetransit.DeviceSessionID, next DeviceState, at int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.rows[sid]
	if !ok {
		return fmt.Errorf("framework.InMemorySessionStore.SetState: session %q not found", sid)
	}
	if !row.State.CanTransitionTo(next) {
		return fmt.Errorf("framework.InMemorySessionStore.SetState: illegal transition %s → %s for session %q",
			row.State, next, sid)
	}
	row.State = next
	if at > 0 {
		row.LastActiveAt = at
	}
	s.rows[sid] = row
	return nil
}

// ListByChannel implements SessionStore. Returns a snapshot copy so
// callers can iterate without holding the lock.
func (s *InMemorySessionStore) ListByChannel(_ context.Context, channelID channel.ID) ([]DeviceSession, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]DeviceSession, 0, len(s.rows))
	for _, row := range s.rows {
		if row.ChannelID == channelID {
			out = append(out, row)
		}
	}
	return out, nil
}

// Delete implements SessionStore. Idempotent.
func (s *InMemorySessionStore) Delete(_ context.Context, sid devicetransit.DeviceSessionID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.rows, sid)
	return nil
}

// Len reports the current row count. Test helper.
func (s *InMemorySessionStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.rows)
}
