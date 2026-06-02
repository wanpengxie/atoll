package workerhost

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/wanpengxie/ActOS/kernel/channel"
)

// Lease is one acquired worker slot for one channel.
//
// v2: the worker-lease is VOLATILE — it lives in the compute process memory,
// NOT in a channel-sqlite worker_locks row (that table is gone). It is an
// INSTANCE fence (guards against a zombie/reconnecting worker), not a
// channel-write fence (the channel has a single writer, server harness, by
// construction). LeaseToken is an opaque per-spawn token stamped on every IPC
// frame.
type Lease struct {
	ID         string
	ChannelID  channel.ID
	WorkerID   string
	LeaseToken string
	AcquiredAt int64
	ExpiresAt  int64
}

// LeaseTTL is the per-spawn lease TTL.
const LeaseTTL = 5 * time.Minute

// LeaseStore tracks active worker leases in compute process memory (volatile).
type LeaseStore struct {
	mu     sync.Mutex
	active map[string]Lease // keyed by agentID
}

// NewLeaseStore returns an empty in-memory lease store.
func NewLeaseStore() *LeaseStore { return &LeaseStore{active: make(map[string]Lease)} }

// Acquire takes the worker slot for agentID, minting a fresh opaque lease
// token. Returns (zero, false, nil) when a non-stale lease for the same agent
// is already held; stale leases (expires_at <= now) are overwritten.
func (s *LeaseStore) Acquire(agentID, workerID string, now int64) (Lease, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.active[agentID]; ok && cur.ExpiresAt > now {
		return Lease{}, false, nil
	}
	tok, err := newLeaseToken()
	if err != nil {
		return Lease{}, false, err
	}
	l := Lease{
		ID:         agentID,
		WorkerID:   workerID,
		LeaseToken: tok,
		AcquiredAt: now,
		ExpiresAt:  now + LeaseTTL.Milliseconds(),
	}
	s.active[agentID] = l
	return l, true, nil
}

// Release removes the lease (idempotent).
func (s *LeaseStore) Release(agentID string) {
	s.mu.Lock()
	delete(s.active, agentID)
	s.mu.Unlock()
}

// SweepExpired removes leases whose expires_at <= now; returns the count.
func (s *LeaseStore) SweepExpired(now int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for k, l := range s.active {
		if l.ExpiresAt <= now {
			delete(s.active, k)
			n++
		}
	}
	return n
}

func newLeaseToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
