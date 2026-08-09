// Package emit owns the single gated path from the codex worker to the
// Runtime EventSink. The raw sink is captured here at construction and is
// unreachable from the rest of the driver, so the contract rule
// "Publish=false means stop producing immediately" cannot be bypassed:
// there is structurally no other way to publish.
package emit

import (
	"sync"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

// Gate latches shut on the first refused publish and on Close.
type Gate struct {
	mu   sync.Mutex
	shut bool
	sink driverproto.EventSink
}

func New(sink driverproto.EventSink) *Gate { return &Gate{sink: sink} }

// Publish forwards one event while production is allowed. A refused publish
// latches the gate shut.
func (g *Gate) Publish(v driverproto.DriverEvent) bool {
	g.mu.Lock()
	if g.shut {
		g.mu.Unlock()
		return false
	}
	g.mu.Unlock()
	if g.sink.Publish(v) {
		return true
	}
	g.mu.Lock()
	g.shut = true
	g.mu.Unlock()
	return false
}

// Close permanently stops production; used at worker retirement.
func (g *Gate) Close() {
	g.mu.Lock()
	g.shut = true
	g.mu.Unlock()
}
