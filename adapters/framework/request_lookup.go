package framework

import (
	"context"

	"github.com/coagent-ai/coagent/kernel/message"
)

// RequestLookup is the framework-private seam Respond uses to recover
// the original request envelope by id. The daemon composition root (T3)
// wires this to its channel-local store; tests use MemoryRequestLookup.
//
// The lookup MUST be channel-scoped — callers pass an id and trust the
// implementation to refuse cross-channel reads. The Manager validates
// the returned envelope.channel_id matches its bound channel ID.
type RequestLookup interface {
	// FindByID returns the envelope at id. Returns ok=false when the
	// row does not exist or has been deleted.
	FindByID(ctx context.Context, id string) (*message.Envelope, bool, error)
}

// MemoryRequestLookup is the test/default in-memory RequestLookup.
type MemoryRequestLookup struct {
	rows map[string]*message.Envelope
}

// NewMemoryRequestLookup returns a populated MemoryRequestLookup. The
// caller hands in a snapshot of rows keyed by envelope.id.
func NewMemoryRequestLookup(rows map[string]*message.Envelope) *MemoryRequestLookup {
	cp := make(map[string]*message.Envelope, len(rows))
	for k, v := range rows {
		cp[k] = v
	}
	return &MemoryRequestLookup{rows: cp}
}

// Put inserts or overwrites the envelope at env.ID.
func (m *MemoryRequestLookup) Put(env *message.Envelope) {
	if env == nil {
		return
	}
	if m.rows == nil {
		m.rows = map[string]*message.Envelope{}
	}
	m.rows[env.ID] = env
}

// FindByID satisfies RequestLookup.
func (m *MemoryRequestLookup) FindByID(_ context.Context, id string) (*message.Envelope, bool, error) {
	if m.rows == nil {
		return nil, false, nil
	}
	v, ok := m.rows[id]
	return v, ok, nil
}
