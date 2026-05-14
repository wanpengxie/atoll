package harness

import (
	"context"
	"sync"
)

// InMemoryTypeRegistry is a test-friendly TypeRegistry. It does NOT
// implement install validation — callers feed pre-validated TypeViews
// via Add. Production wiring constructs a sqlite-backed adapter on
// top of adapters/framework.TypeRegistry instead (daemon composition
// root — FIX-T2).
type InMemoryTypeRegistry struct {
	mu   sync.RWMutex
	rows map[string]TypeView
}

// NewInMemoryTypeRegistry returns an empty in-memory TypeRegistry.
func NewInMemoryTypeRegistry() *InMemoryTypeRegistry {
	return &InMemoryTypeRegistry{rows: map[string]TypeView{}}
}

// Add upserts a TypeView. Tests use this to seed allowed_kinds /
// schemas / handler_actor_id / terminal_convention.
func (r *InMemoryTypeRegistry) Add(v TypeView) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rows[v.Type] = v
}

// Lookup implements TypeRegistry.
func (r *InMemoryTypeRegistry) Lookup(_ context.Context, typeName string) (TypeView, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.rows[typeName]
	return v, ok, nil
}
