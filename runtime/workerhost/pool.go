package workerhost

import (
	"errors"
	"sync"
)

// PoolConfig wires a Pool.
type PoolConfig struct {
	// MaxConcurrent caps live worker leases (default 32 per spec).
	MaxConcurrent int
}

// Pool tracks live worker slots (lease quota). The pool itself does NOT
// own sqlite; it consults InMemoryLeaseTable for the active set and
// enforces the quota.
type Pool struct {
	cfg PoolConfig

	mu   sync.Mutex
	live map[string]struct{} // active lease ids
}

// NewPool returns a Pool.
func NewPool(cfg PoolConfig) *Pool {
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 32
	}
	return &Pool{cfg: cfg, live: make(map[string]struct{})}
}

// ErrPoolFull is returned by Reserve when MaxConcurrent is reached.
var ErrPoolFull = errors.New("workerhost: pool full")

// Reserve takes one slot for leaseID. Returns ErrPoolFull when the
// quota is exhausted.
func (p *Pool) Reserve(leaseID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.live[leaseID]; ok {
		return nil // idempotent
	}
	if len(p.live) >= p.cfg.MaxConcurrent {
		return ErrPoolFull
	}
	p.live[leaseID] = struct{}{}
	return nil
}

// Release frees the slot for leaseID. Idempotent.
func (p *Pool) Release(leaseID string) {
	p.mu.Lock()
	delete(p.live, leaseID)
	p.mu.Unlock()
}

// InUse returns the current live count.
func (p *Pool) InUse() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.live)
}

// Capacity returns the configured max.
func (p *Pool) Capacity() int { return p.cfg.MaxConcurrent }
