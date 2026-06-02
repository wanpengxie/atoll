package harness

import (
	"context"
	"sync"

	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// inMemoryTypeRegistry is a test-friendly storespec.TypeViewLookup. It does
// NOT implement install validation — callers feed pre-validated TypeViews
// via Add. Production wiring uses the sqlite-backed runtime/store registry.
type inMemoryTypeRegistry struct {
	mu   sync.RWMutex
	rows map[string]storespec.TypeView
}

// NewinMemoryTypeRegistry returns an empty in-memory registry.
func newinMemoryTypeRegistry() *inMemoryTypeRegistry {
	return &inMemoryTypeRegistry{rows: map[string]storespec.TypeView{}}
}

// Add upserts a TypeView. Tests use this to seed allowed_kinds /
// handler_actor_id / max_pending_ms.
func (r *inMemoryTypeRegistry) Add(v storespec.TypeView) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rows[v.Type] = v
}

// Lookup implements storespec.TypeViewLookup.
func (r *inMemoryTypeRegistry) Lookup(_ context.Context, typeName string) (storespec.TypeView, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.rows[typeName]
	return v, ok, nil
}
