package framework

import (
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// TerminalConvention re-exports kernel/adapter.TerminalConvention so
// existing framework callers keep their import path. The canonical
// definition lives in kernel/adapter so runtime/store can implement a
// sqlite-backed TypeRegistry without taking a dependency on adapters/**.
type TerminalConvention = adapter.TerminalConvention

// TerminalPayloadStatus / TerminalSingleResponse re-export the kernel
// closed-set values.
const (
	TerminalPayloadStatus  = adapter.TerminalPayloadStatus
	TerminalSingleResponse = adapter.TerminalSingleResponse
)

// TypeRow re-exports kernel/adapter.TypeRow.
type TypeRow = adapter.TypeRow

// TypeRegistry re-exports kernel/adapter.TypeRegistry.
type TypeRegistry = adapter.TypeRegistry

// InMemoryTypeRegistry is the framework-shipped TypeRegistry used by
// tests + the default Manager when no production registry is wired.
type InMemoryTypeRegistry struct {
	mu   sync.RWMutex
	rows map[string]TypeRow
}

// NewInMemoryTypeRegistry returns an empty in-memory TypeRegistry.
func NewInMemoryTypeRegistry() *InMemoryTypeRegistry {
	return &InMemoryTypeRegistry{rows: map[string]TypeRow{}}
}

// Upsert validates row then writes / overwrites.
func (r *InMemoryTypeRegistry) Upsert(_ context.Context, row TypeRow) (TypeRow, error) {
	if err := row.Validate(); err != nil {
		return TypeRow{}, err
	}
	if strings.HasPrefix(row.Type, "system.") {
		return TypeRow{}, &InstallError{Reason: message.InstallTypeRegistryReservedNamespace}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rows[row.Type] = row
	return row, nil
}

// Lookup returns the row for typeName.
func (r *InMemoryTypeRegistry) Lookup(_ context.Context, typeName string) (TypeRow, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	row, ok := r.rows[typeName]
	return row, ok, nil
}

// List returns every row sorted by Type (deterministic for tests).
func (r *InMemoryTypeRegistry) List(_ context.Context) ([]TypeRow, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]TypeRow, 0, len(r.rows))
	for _, row := range r.rows {
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Type < out[j].Type })
	return out, nil
}

// Compile-time interface check.
var _ TypeRegistry = (*InMemoryTypeRegistry)(nil)
