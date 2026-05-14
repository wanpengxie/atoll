package trigger

import (
	"strings"
	"sync"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// SubscriptionFilter expresses which envelopes a subscriber wants the
// trigger gateway to surface to them (L1 §5.4). The protocol baseline
// matches on a 3-tuple:
//
//   - Type (required) — exact envelope.type match. Empty string is
//     invalid and rejected by Register.
//   - Kind (optional) — when empty matches any kind; when set must
//     equal env.Kind.
//   - Visibility (optional) — when empty matches any visibility;
//     when set must equal env.Visibility.
//
// L1 §5.4.2 mentions payload-level constraints (e.g. severity=error)
// as an extension point. The protocol baseline (M1.3) keeps the
// shape limited to the three top-level fields; payload-level matching
// is deferred to L1.1 alongside dynamic subscription. Extending here
// is a non-breaking change — add a payload-constraint field and have
// matches() also check it.
type SubscriptionFilter struct {
	// Type is the envelope type the subscriber cares about. Required.
	// Wildcard semantics (e.g. "system.*") are NOT supported in M1.3
	// baseline — caller registers one entry per concrete type.
	Type string

	// Kind narrows by envelope.kind. Empty value (`v4types.Kind("")`)
	// means "any kind matches".
	Kind v4types.Kind

	// Visibility narrows by envelope.visibility. Empty value means
	// "any visibility matches".
	Visibility v4types.Visibility
}

// matches reports whether env satisfies the filter. The match is
// strictly conjunctive — every non-empty field MUST equal the
// envelope's corresponding field.
func (f SubscriptionFilter) matches(env *v4types.Envelope) bool {
	if env == nil {
		return false
	}
	if f.Type == "" || f.Type != env.Type {
		return false
	}
	if f.Kind != "" && f.Kind != env.Kind {
		return false
	}
	if f.Visibility != "" && f.Visibility != env.Visibility {
		return false
	}
	return true
}

// subscriptionEntry is one row in the in-memory registry — a binding
// of (actor, filter). Multiple rows per actor are legal; rows are
// independent (different filters yield different match sets).
type subscriptionEntry struct {
	actorID string
	filter  SubscriptionFilter
}

// Registry is the protocol-baseline subscription store. It lives in
// daemon process memory and is populated by adapter framework F6
// `init()` at daemon startup (and adapter re-install on subsequent
// boots). There is no persistence layer — L1 §5.4.4 defers
// "运行期动态订阅" to L1.1; M1.3 baseline assumes subscriptions are
// deterministic functions of the installed adapter framework set.
//
// Registry satisfies the gateway's SubscriptionMatcher interface.
// Safe for concurrent reads + writes via an internal RWMutex.
type Registry struct {
	mu      sync.RWMutex
	entries []subscriptionEntry
}

// NewRegistry constructs an empty Registry. Callers register
// subscriptions immediately (typically inside adapter framework F6
// `init()`).
func NewRegistry() *Registry {
	return &Registry{}
}

// Register adds a subscription. Invalid filters (empty Type, blank
// actorID) are silently ignored — this keeps adapter init code from
// having to branch on "did the caller hand me an empty type?". In
// development the daemon logs at the call site; we explicitly do
// NOT panic so a single misconfigured adapter cannot crash the
// daemon. Duplicate (actorID, filter) entries are kept as separate
// rows; Match dedupes the actor list anyway.
//
// The actorID is trimmed of leading/trailing whitespace before storage
// so cosmetic differences ("agent:a" vs "agent:a ") don't produce
// two entries.
func (r *Registry) Register(actorID string, filter SubscriptionFilter) {
	actorID = strings.TrimSpace(actorID)
	if actorID == "" {
		return
	}
	if filter.Type == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, subscriptionEntry{actorID: actorID, filter: filter})
}

// Match returns the deduplicated list of actor ids subscribed to env.
// Result preserves the order in which subscriptions were registered
// — so callers (e.g. trigger.Gateway) see deterministic fan-out
// suitable for log + UI rendering.
//
// Note: Match does NOT consult the actor_registry for liveness; a
// subscriber that has been deregistered will still appear in the
// list. The gateway's downstream consumers are responsible for
// reconciling against actor_registry if they need an active-only
// view. (Protocol baseline assumes adapter framework F6 re-runs
// install on daemon boot — stale subscriptions self-heal via the
// next boot's full re-population.)
func (r *Registry) Match(env *v4types.Envelope) []string {
	if env == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.entries) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(r.entries))
	out := make([]string, 0, len(r.entries))
	for _, e := range r.entries {
		if !e.filter.matches(env) {
			continue
		}
		if _, ok := seen[e.actorID]; ok {
			continue
		}
		seen[e.actorID] = struct{}{}
		out = append(out, e.actorID)
	}
	return out
}

// Size returns the count of registered subscription entries — useful
// for observability / log lines confirming F6 boot-time load count.
func (r *Registry) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries)
}
