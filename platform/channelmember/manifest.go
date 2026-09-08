package channelmember

import (
	"context"
	"encoding/json"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/message"
	"sync"
)

// A Seat owns its projection. Hub transports attachment changes but does not
// interpret a word table or participate in actor presence.
type manifestProjection struct {
	mu                 sync.RWMutex
	generation         uint64
	detachedGeneration uint64
	words              map[string]introspect.WordSpec
	err                error
}

func (p *manifestProjection) refresh(ctx context.Context, b HandleBinding) {
	p.mu.Lock()
	if b.Generation < p.generation {
		p.mu.Unlock()
		return
	}
	p.generation = b.Generation
	p.words = nil
	p.err = ErrUnreachable
	if b.Call == nil {
		p.detachedGeneration = b.Generation
	}
	p.mu.Unlock()
	if b.Call == nil {
		return
	}
	response, err := b.Call(ctx, Request{Envelope: wireEnvelope(message.Envelope{Kind: message.KindRequest, Type: introspect.QueryDescribe, Payload: json.RawMessage(`{}`)}), Await: true})
	var described struct {
		Words map[string]introspect.WordSpec
	}
	if err == nil {
		err = json.Unmarshal(response.Payload, &described)
	}
	if err == nil {
		err = introspect.ValidateProjectedWords(nil, described.Words)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// A replacement or detach may have arrived while describe was in flight.
	if b.Generation != p.generation {
		return
	}
	if p.detachedGeneration == b.Generation {
		return
	}
	p.words = described.Words
	p.err = err
}
func (p *manifestProjection) read() (map[string]introspect.WordSpec, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return introspect.CloneWords(p.words), p.err
}
