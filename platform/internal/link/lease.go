package link

import (
	"sync"
	"time"
)

// Default lease parameters: the home judges per-link liveness. last-seen is
// refreshed by ANY inbound frame; the home pings stream 0 every leasePing; if no
// frame arrives within leaseTTL the home declares the link dead (the positive
// observation a frozen/half-open daemon never produces a TCP EOF for).
// Centralised + tunable.
const (
	leasePing = 10 * time.Second
	leaseTTL  = 30 * time.Second
)

// Lease is the authority side's liveness judgment over one physical link — one
// of the substrate's four physical-truth concepts (it answers "is this link's
// far end still alive?"). The home holds a Lease per accepted link: it carries a
// mutex-guarded last-seen instant (refreshed by any inbound frame), the
// ping/TTL window, and a watchdog that tears the link down when last-seen falls
// behind TTL. A frozen daemon (which never produces a TCP EOF) is killed here.
type Lease struct {
	ping time.Duration
	ttl  time.Duration

	mu       sync.Mutex
	lastSeen time.Time
}

// NewLease builds a Lease with the given ping/TTL window; a non-positive value
// falls back to the centralised default (leasePing / leaseTTL). last-seen is
// primed to now so a just-attached link starts inside its window.
func NewLease(ping, ttl time.Duration) *Lease {
	if ping <= 0 {
		ping = leasePing
	}
	if ttl <= 0 {
		ttl = leaseTTL
	}
	return &Lease{
		ping:     ping,
		ttl:      ttl,
		lastSeen: time.Now(),
	}
}

// Refresh stamps last-seen to now. Any inbound frame is liveness — the demux
// loop calls this for every frame it reads.
func (l *Lease) Refresh() {
	l.mu.Lock()
	l.lastSeen = time.Now()
	l.mu.Unlock()
}

// expired reports whether the gap since last-seen has exceeded the TTL.
func (l *Lease) expired() bool {
	l.mu.Lock()
	last := l.lastSeen
	l.mu.Unlock()
	return time.Since(last) > l.ttl
}

// Watch is the liveness judge: every ping it checks last-seen; if the gap
// exceeds TTL it fires expire (the owner tears the whole link down — all actor
// streams EOF = every embodiment on this party falls on the same down
// edge). It returns when expire fires or done closes.
func (l *Lease) Watch(done <-chan struct{}, expire func()) {
	t := time.NewTicker(l.ping)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-t.C:
			if l.expired() {
				expire()
				return
			}
		}
	}
}
