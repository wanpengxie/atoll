package framework

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/wanpengxie/ActOS/kernel/adapter"
)

// correlationKeyPrefix is the StateStore key prefix used by the
// memoryCorrelationTracker when persisting entries through a backing
// StateStore. Format: `adapter.correlation:<adapter_name>:<request_id>`.
const correlationKeyPrefix = "adapter.correlation"

// memoryCorrelationTracker is the framework's default
// kernel/adapter.CorrelationTracker. It keeps every entry in memory
// keyed by request_id and optionally mirrors writes to a StateStore so
// daemon restarts can recover pending requests through
// ListPending.
//
// Concurrency: every method is safe for concurrent use; writes
// serialise on an internal mutex; reads from sqlite (when wired) go
// through the StateStore implementation which has its own
// concurrency rules.
type memoryCorrelationTracker struct {
	adapterName string
	store       StateStore // optional; nil → memory only

	mu      sync.Mutex
	entries map[adapter.CorrelationKey]adapter.CorrelationEntry // request_id → entry
}

// newCorrelationTracker constructs a tracker bound to an adapter name.
// store is optional (nil = pure in-memory).
func newCorrelationTracker(adapterName string, store StateStore) *memoryCorrelationTracker {
	return &memoryCorrelationTracker{
		adapterName: adapterName,
		store:       store,
		entries:     map[adapter.CorrelationKey]adapter.CorrelationEntry{},
	}
}

// Reserve creates a pending entry. Idempotent — second Reserve with the
// same RequestID returns the existing entry unchanged.
func (t *memoryCorrelationTracker) Reserve(ctx context.Context, e adapter.CorrelationEntry) (adapter.CorrelationEntry, error) {
	if e.RequestID == "" {
		return adapter.CorrelationEntry{}, errors.New("framework: Reserve RequestID required")
	}
	t.mu.Lock()
	if existing, ok := t.entries[e.RequestID]; ok {
		t.mu.Unlock()
		return existing, nil
	}
	if e.State == "" {
		e.State = adapter.CorrelationPending
	}
	t.entries[e.RequestID] = e
	t.mu.Unlock()

	if err := t.persist(ctx, e); err != nil {
		return adapter.CorrelationEntry{}, err
	}
	return e, nil
}

// Get returns the entry by request_id.
func (t *memoryCorrelationTracker) Get(_ context.Context, requestID adapter.CorrelationKey) (adapter.CorrelationEntry, bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.entries[requestID]
	return e, ok, nil
}

// MarkDone advances pending → done. Idempotent.
func (t *memoryCorrelationTracker) MarkDone(ctx context.Context, requestID adapter.CorrelationKey) error {
	return t.advance(ctx, requestID, adapter.CorrelationDone)
}

// MarkExpired advances pending → expired. Idempotent.
func (t *memoryCorrelationTracker) MarkExpired(ctx context.Context, requestID adapter.CorrelationKey) error {
	return t.advance(ctx, requestID, adapter.CorrelationExpired)
}

// MarkRejected advances pending → rejected. Stores the reason on the
// existing entry via the AudienceActor field's documentation contract
// (we don't have a Reason slot in CorrelationEntry — log via state).
// Idempotent.
func (t *memoryCorrelationTracker) MarkRejected(ctx context.Context, requestID adapter.CorrelationKey, _ string) error {
	return t.advance(ctx, requestID, adapter.CorrelationRejected)
}

func (t *memoryCorrelationTracker) advance(ctx context.Context, requestID adapter.CorrelationKey, to adapter.CorrelationState) error {
	if requestID == "" {
		return errors.New("framework: advance requestID required")
	}
	t.mu.Lock()
	e, ok := t.entries[requestID]
	if !ok {
		t.mu.Unlock()
		return nil // idempotent — never reserved or already cleared
	}
	if e.State == to {
		t.mu.Unlock()
		return nil
	}
	// Only advance from pending; ignore re-entries.
	if e.State != adapter.CorrelationPending {
		t.mu.Unlock()
		return nil
	}
	e.State = to
	t.entries[requestID] = e
	t.mu.Unlock()
	return t.persist(ctx, e)
}

// ListPending returns every entry currently in pending state. Order is
// deterministic by RequestID so tests can assert exact output.
func (t *memoryCorrelationTracker) ListPending(_ context.Context) ([]adapter.CorrelationEntry, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]adapter.CorrelationEntry, 0)
	for _, e := range t.entries {
		if e.State == adapter.CorrelationPending {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RequestID.String() < out[j].RequestID.String() })
	return out, nil
}

func (t *memoryCorrelationTracker) persist(ctx context.Context, e adapter.CorrelationEntry) error {
	if t.store == nil {
		return nil
	}
	b, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("framework: marshal correlation entry: %w", err)
	}
	key := fmt.Sprintf("%s:%s:%s", correlationKeyPrefix, t.adapterName, e.RequestID.String())
	if err := t.store.Put(ctx, key, b); err != nil {
		return fmt.Errorf("framework: persist correlation: %w", err)
	}
	return nil
}

// recoverFromStore re-hydrates entries from a backing StateStore. Used
// by Manager.BootRecoverTimers when an instance restarts. No-op when
// store is nil.
func (t *memoryCorrelationTracker) recoverFromStore(ctx context.Context) error {
	if t.store == nil {
		return nil
	}
	prefix := fmt.Sprintf("%s:%s:", correlationKeyPrefix, t.adapterName)
	keys, err := t.store.List(ctx, prefix)
	if err != nil {
		return fmt.Errorf("framework: list correlation: %w", err)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, k := range keys {
		v, ok, err := t.store.Get(ctx, k)
		if err != nil {
			return fmt.Errorf("framework: get correlation %s: %w", k, err)
		}
		if !ok {
			continue
		}
		var e adapter.CorrelationEntry
		if err := json.Unmarshal(v, &e); err != nil {
			return fmt.Errorf("framework: unmarshal correlation %s: %w", k, err)
		}
		t.entries[e.RequestID] = e
	}
	return nil
}
