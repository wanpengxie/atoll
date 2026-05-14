package transit

import (
	"sync"

	"github.com/coagent-ai/coagent/kernel/channel"
	"github.com/coagent-ai/coagent/kernel/viewsync"
)

// CursorTracker holds the in-memory daemon-side view-sync cursors per
// channel: last_pushed_seq + last_acked_seq (L1 §8.6).
//
// The store is the durable source of truth — outbox table holds rows
// with seq <= last_pushed_seq and seq > last_acked_seq. CursorTracker
// is a memory-side view used by the push loop to avoid re-reading the
// table for every decision.
type CursorTracker struct {
	mu      sync.Mutex
	cursors map[channel.ID]channelCursor
}

type channelCursor struct {
	lastPushed viewsync.LastPushedSeq
	lastAcked  viewsync.LastAckedSeq
}

// NewCursorTracker returns an empty tracker.
func NewCursorTracker() *CursorTracker {
	return &CursorTracker{cursors: make(map[channel.ID]channelCursor)}
}

// Get returns the current cursor pair for a channel. Missing channels
// report (0, 0, ok=false).
func (t *CursorTracker) Get(channelID channel.ID) (viewsync.LastPushedSeq, viewsync.LastAckedSeq, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	c, ok := t.cursors[channelID]
	return c.lastPushed, c.lastAcked, ok
}

// AdvancePushed bumps last_pushed_seq to newSeq if newSeq > current.
// Returns ok=true when the advance happened.
func (t *CursorTracker) AdvancePushed(channelID channel.ID, newSeq viewsync.LastPushedSeq) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.cursors[channelID]
	if newSeq <= c.lastPushed {
		return false
	}
	c.lastPushed = newSeq
	t.cursors[channelID] = c
	return true
}

// AdvanceAcked bumps last_acked_seq to newSeq if newSeq > current.
// Returns ok=true when the advance happened.
func (t *CursorTracker) AdvanceAcked(channelID channel.ID, newSeq viewsync.LastAckedSeq) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.cursors[channelID]
	if newSeq <= c.lastAcked {
		return false
	}
	c.lastAcked = newSeq
	t.cursors[channelID] = c
	return true
}

// Reset wipes the cursor for a channel (e.g. channel unload).
func (t *CursorTracker) Reset(channelID channel.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.cursors, channelID)
}
