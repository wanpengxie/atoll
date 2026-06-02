package harness

import (
	"context"
	"sync"

	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// InMemoryTypeRegistry is a test-friendly storespec.TypeViewLookup. It does
// NOT implement install validation — callers feed pre-validated TypeViews
// via Add. Production wiring uses the sqlite-backed runtime/store registry.
type InMemoryTypeRegistry struct {
	mu   sync.RWMutex
	rows map[string]storespec.TypeView
}

// NewInMemoryTypeRegistry returns an empty in-memory registry.
func NewInMemoryTypeRegistry() *InMemoryTypeRegistry {
	return &InMemoryTypeRegistry{rows: map[string]storespec.TypeView{}}
}

// Add upserts a TypeView. Tests use this to seed allowed_kinds /
// handler_actor_id / max_pending_ms.
func (r *InMemoryTypeRegistry) Add(v storespec.TypeView) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rows[v.Type] = v
}

// Lookup implements storespec.TypeViewLookup.
func (r *InMemoryTypeRegistry) Lookup(_ context.Context, typeName string) (storespec.TypeView, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.rows[typeName]
	return v, ok, nil
}
