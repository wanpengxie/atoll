package behavior

import (
	"context"
	"errors"
	"sync"
)

// StateStore is the F4 persistent state seam. Adapters that need to
// remember session metadata, cursor positions, or anything else across
// daemon restarts call Get / Put / Delete through this single
// interface; concrete sqlite backends live in runtime/store (T3).
//
// All values are opaque byte sequences — adapters decide their own
// serialization. Keys MUST be scoped by adapter (the framework
// auto-prefixes with the adapter name on the Manager-bound view; see
// NamespacedStateStore).
//
// Implementations MUST be safe for concurrent use.
type StateStore interface {
	// Get returns the value stored at key (ok=false when absent).
	Get(ctx context.Context, key string) (value []byte, ok bool, err error)

	// Put writes value at key, overwriting any prior content.
	Put(ctx context.Context, key string, value []byte) error

	// Delete removes any value at key. No-op when absent.
	Delete(ctx context.Context, key string) error

	// List returns every key matching prefix (in insertion-independent,
	// implementation-defined order — callers sort if they need stability).
	List(ctx context.Context, prefix string) ([]string, error)
}

// ErrStateKeyEmpty is returned by Put / Delete when key is "".
var ErrStateKeyEmpty = errors.New("framework: state key required")

// MemoryStateStore is the framework's default StateStore. Safe for
// concurrent use. Tests inject this; production wires a sqlite-backed
// store from T3.
type MemoryStateStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewMemoryStateStore returns an empty MemoryStateStore.
func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{data: map[string][]byte{}}
}

// Get returns a copy of the value at key.
func (s *MemoryStateStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	if !ok {
		return nil, false, nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true, nil
}

// Put writes a copy of value at key.
func (s *MemoryStateStore) Put(_ context.Context, key string, value []byte) error {
	if key == "" {
		return ErrStateKeyEmpty
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(value))
	copy(cp, value)
	s.data[key] = cp
	return nil
}

// Delete removes the entry at key.
func (s *MemoryStateStore) Delete(_ context.Context, key string) error {
	if key == "" {
		return ErrStateKeyEmpty
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	return nil
}

// List returns every key in the store that starts with prefix.
func (s *MemoryStateStore) List(_ context.Context, prefix string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0)
	for k := range s.data {
		if hasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out, nil
}

func hasPrefix(s, p string) bool {
	if len(p) > len(s) {
		return false
	}
	return s[:len(p)] == p
}

// NamespacedStateStore wraps a StateStore and prefixes every key with
// `<namespace>:`. Used by the framework to scope per-adapter state so
// two adapters cannot collide on the same logical key.
type NamespacedStateStore struct {
	Inner     StateStore
	Namespace string
}

// NewNamespacedStateStore returns a StateStore that scopes every key
// under namespace. Empty namespace is allowed (no prefixing).
func NewNamespacedStateStore(inner StateStore, namespace string) *NamespacedStateStore {
	return &NamespacedStateStore{Inner: inner, Namespace: namespace}
}

// Get prefixes key with the namespace and forwards.
func (n *NamespacedStateStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return n.Inner.Get(ctx, n.scope(key))
}

// Put prefixes key with the namespace and forwards.
func (n *NamespacedStateStore) Put(ctx context.Context, key string, value []byte) error {
	return n.Inner.Put(ctx, n.scope(key), value)
}

// Delete prefixes key with the namespace and forwards.
func (n *NamespacedStateStore) Delete(ctx context.Context, key string) error {
	return n.Inner.Delete(ctx, n.scope(key))
}

// List forwards List with the namespace prefix prepended to prefix and
// strips it from the returned keys so callers observe the namespace as
// invisible.
func (n *NamespacedStateStore) List(ctx context.Context, prefix string) ([]string, error) {
	keys, err := n.Inner.List(ctx, n.scope(prefix))
	if err != nil {
		return nil, err
	}
	if n.Namespace == "" {
		return keys, nil
	}
	cut := len(n.Namespace) + 1 // namespace + ":"
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if len(k) >= cut {
			out = append(out, k[cut:])
		}
	}
	return out, nil
}

func (n *NamespacedStateStore) scope(key string) string {
	if n.Namespace == "" {
		return key
	}
	return n.Namespace + ":" + key
}
