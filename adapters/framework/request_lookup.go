package framework

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// RequestLookup re-exports kernel/adapter.RequestLookup. The canonical
// interface lives in kernel/adapter so runtime/store can implement it
// without taking a dependency on adapters/**.
type RequestLookup = adapter.RequestLookup

// MemoryRequestLookup is the test/default in-memory RequestLookup.
type MemoryRequestLookup struct {
	rows map[message.ID]*message.Envelope
}

// NewMemoryRequestLookup returns a populated MemoryRequestLookup. The
// caller hands in a snapshot of rows keyed by envelope.id.
func NewMemoryRequestLookup(rows map[string]*message.Envelope) *MemoryRequestLookup {
	cp := make(map[message.ID]*message.Envelope, len(rows))
	for k, v := range rows {
		cp[message.ID(k)] = v
	}
	return &MemoryRequestLookup{rows: cp}
}

// Put inserts or overwrites the envelope at env.ID.
func (m *MemoryRequestLookup) Put(env *message.Envelope) {
	if env == nil {
		return
	}
	if m.rows == nil {
		m.rows = map[message.ID]*message.Envelope{}
	}
	m.rows[env.ID] = env
}

// FindByID satisfies RequestLookup.
func (m *MemoryRequestLookup) FindByID(_ context.Context, id message.ID) (*message.Envelope, bool, error) {
	if m.rows == nil {
		return nil, false, nil
	}
	v, ok := m.rows[id]
	return v, ok, nil
}

// Compile-time interface check.
var _ RequestLookup = (*MemoryRequestLookup)(nil)
