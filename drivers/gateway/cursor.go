package gateway

import (
	"sync"

	"github.com/wanpengxie/atoll/protocol/channel"
)

// cursor is a lane's read position as a map<channel, seq> vector (design §5.5,
// 裁决7). seq is a PER-CHANNEL local sequence number — cross-channel seqs are NOT
// comparable, so one lane multiplexing several channels down MUST record its
// position per channel component. Today one connection = one channel = a
// single-element map (裁决7 退化). The vector is device-carried, lane-memory-held,
// and evaporates on disconnect (server 零持久化, DoD-6 "server 重启零游标态").
//
// receipt.seq is NEVER folded in here (write位≠读位, 裁决 §S2 契约): a submit
// receipt's harness write-seq must never advance a feed cursor.
type cursor struct {
	mu  sync.Mutex
	pos map[channel.ID]int64
}

// newCursor seeds a cursor from a reconnect's since map (nil → empty). Keys are
// channel ids; values are the last-seen seq per channel.
func newCursor(since map[channel.ID]int64) *cursor {
	pos := make(map[channel.ID]int64, len(since))
	for ch, seq := range since {
		pos[ch] = seq
	}
	return &cursor{pos: pos}
}

// at returns the current read position for ch (0 if unseen — read from the top).
func (c *cursor) at(ch channel.ID) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pos[ch]
}

// advance moves ch's position forward to seq IFF seq is strictly greater (a feed
// frame only ever advances; a receipt never calls this). Cross-channel isolation:
// advancing one channel never touches another's component.
func (c *cursor) advance(ch channel.ID, seq int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if seq > c.pos[ch] {
		c.pos[ch] = seq
	}
}

// snapshot copies the vector (for a reconnect handoff / assertion).
func (c *cursor) snapshot() map[channel.ID]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[channel.ID]int64, len(c.pos))
	for ch, seq := range c.pos {
		out[ch] = seq
	}
	return out
}
