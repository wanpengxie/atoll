package link

import (
	"sync"
	"time"
)

// Default lease parameters: the home judges per-link liveness from
// APPLICATION-layer frames only — never yamux's own keepalive ping/pong, which
// yamux answers inside its own session loop and never surfaces above the
// substream layer (linksession.go's dispatch is where last-seen actually gets
// refreshed, via each peer-opened substream's onFrame hook). The daemon pings
// the control substream every leasePing (dial.go's pingLoop) precisely so a
// live-but-otherwise-idle link still refreshes; if no application frame
// arrives within leaseTTL the home declares the link dead (the positive
// observation a frozen-app-but-live-socket daemon never produces a TCP EOF
// for — it keeps answering yamux keepalive right up to the kill). Centralised
// + tunable.
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

// Refresh stamps last-seen to now. Only an application frame is liveness —
// linksession.go's dispatch calls this (via its per-substream onFrame hook)
// for every application frame it reads, never for yamux's own keepalive
// ping/pong, which never reaches that hook.
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
