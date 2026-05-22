// FIX-T8 — per-channel HumanCaller nonce LRU. Used by WriteMessage
// Handler to reject `control.write_message` frames whose
// (channel_id, nonce) tuple was already observed within one replay
// window. The cache TTL equals the configured ReplayWindow — entries
// older than that window would already have been rejected by the ts
// clock-skew check, so dropping them on read keeps the cache bounded.

package transit

import (
	"sync"
	"time"
)

// nonceCache is a tiny per-channel TTL set. The current implementation
// keeps a map per channel and prunes expired entries lazily on every
// observe call. launch traffic is low enough that a per-call sweep is
// cheaper than a background goroutine; revisit if the cache grows
// beyond ~10k entries / channel.
type nonceCache struct {
	ttlMs int64

	mu       sync.Mutex
	channels map[string]map[string]int64 // channel_id → nonce → expiresAtMs
}

// newNonceCache builds an empty nonce cache with the given TTL.
func newNonceCache(ttl time.Duration) *nonceCache {
	return &nonceCache{
		ttlMs:    ttl.Milliseconds(),
		channels: map[string]map[string]int64{},
	}
}

// observe records a (channelID, nonce) tuple. Returns true on a fresh
// nonce, false when the tuple was already seen within the TTL.
// Concurrent calls are serialized by the cache lock.
func (c *nonceCache) observe(channelID, nonce string, nowMs int64) bool {
	if c == nil || nonce == "" {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := c.channels[channelID]
	if ch == nil {
		ch = map[string]int64{}
		c.channels[channelID] = ch
	}
	expiresAt, present := ch[nonce]
	if present && expiresAt > nowMs {
		return false
	}
	// Lazy prune of this channel's expired entries to keep the map
	// bounded under low-traffic / mostly-unique-nonce workloads.
	for n, exp := range ch {
		if exp <= nowMs {
			delete(ch, n)
		}
	}
	ch[nonce] = nowMs + c.ttlMs
	return true
}
