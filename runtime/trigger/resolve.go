package trigger

import (
	"context"
	"fmt"
	"sort"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// TypeSystemHeartbeat is the noise-filter type explicitly excluded from
// trigger fan-out per L1 §5.2 (system.heartbeat).
const TypeSystemHeartbeat = "system.heartbeat"

// Options tunes per-call decisions that are NOT carried inside the
// envelope. Today the only switch is BypassSelfTriggerBan — see L1 §5.3
// dispatch-path semantics.
type Options struct {
	// BypassSelfTriggerBan, when true, disables the L1 §5.1 step 3
	// "sender 自己不被 self-trigger" filter. Callers SHOULD set this true
	// when the dispatch-path upstream is NOT the original sender:
	//
	//   - scheduler ticking a future-message back to its own author
	//     (§5.3 explicit case).
	//   - system long-pending fallback writing a terminal response on
	//     behalf of a stuck request (§6.4 — system upstream).
	//
	// Default false matches the harness immediate-write path where the
	// upstream IS the sender.
	BypassSelfTriggerBan bool
}

// Resolve implements the L1 §5.1 decision matrix.
//
// Input is one already-validated, already-appended envelope (the chain
// has run; sender/audience/visibility/type are all post-normalize). The
// registry is the channel-local actor registry the daemon owns.
//
// The returned slice is the set of actor.ActorID values the daemon
// should hand this envelope to via runtime/scheduler.Deliverer. It is
// deduped + sorted by actor id so the caller observes a stable order.
//
// Errors come from the registry (lookup IO); decision-layer rejects
// surface as an empty audience, not an error — fan-out cannot fail.
func Resolve(
	ctx context.Context,
	env *message.Envelope,
	reg actorreg.Registry,
	opts Options,
) ([]actor.ActorID, error) {
	if env == nil {
		return nil, fmt.Errorf("trigger: nil envelope")
	}
	if reg == nil {
		return nil, fmt.Errorf("trigger: nil registry")
	}

	// (1) Noise filter — system.heartbeat never fan-outs (L1 §5.2 explicit
	// row). Evaluated before visibility so that an unusual visibility=public
	// heartbeat (e.g. test fixture) still gets suppressed.
	if env.Type == TypeSystemHeartbeat {
		return nil, nil
	}

	// (2) Visibility filter — L1 §5.1 step 1.
	switch env.Visibility {
	case message.VisibilitySystem:
		// System-internal: no fan-out unless an actor explicitly
		// subscribed (§5.4 — subscription mechanism deferred to L1.1).
		return nil, nil
	case message.VisibilityPrivate:
		// Private = sender-only readable. Combined with the §5.1 step 3
		// self-trigger ban this always reduces to nil (the only candidate
		// would be the sender, who is then filtered out). When
		// BypassSelfTriggerBan is true the upstream is NOT the sender, so
		// even a private envelope can dispatch back to its author (the
		// L1 §5.3 future-message bypass case).
		if !opts.BypassSelfTriggerBan {
			return nil, nil
		}
		// fall through — sender is the only legitimate target.
	}

	// (3) Audience expand — L1 §5.1 step 2.
	candidates, err := expandAudience(ctx, env, reg)
	if err != nil {
		return nil, err
	}

	// (4) Self-trigger ban — L1 §5.1 step 3.
	if !opts.BypassSelfTriggerBan {
		candidates = filterOut(candidates, env.Sender.ID)
	}

	// (5) Dedupe + stable sort. The audience slice may carry the same
	// id twice (caller error) or the wildcard plus an explicit id; ensure
	// a single ordering invariant for downstream Deliverer.
	return dedupeSort(candidates), nil
}

// expandAudience converts envelope.audience into a concrete actor set.
//
// `[*]` (wildcard) or empty → registry.ListActive(). Explicit list →
// per-id Lookup, dropping missing or deregistered rows (matches the L1
// §5.1 "active member" requirement; harness step 5 already validated
// kind=request audience, but for kind=event/response we still trim
// stale ids defensively).
func expandAudience(
	ctx context.Context,
	env *message.Envelope,
	reg actorreg.Registry,
) ([]actor.ActorID, error) {
	if isWildcard(env.Audience) {
		rows, err := reg.ListActive(ctx)
		if err != nil {
			return nil, fmt.Errorf("trigger: list active: %w", err)
		}
		out := make([]actor.ActorID, 0, len(rows))
		for _, rec := range rows {
			out = append(out, rec.ID)
		}
		return out, nil
	}

	out := make([]actor.ActorID, 0, len(env.Audience))
	for _, raw := range env.Audience {
		if raw == "" || raw == message.AudienceWildcard {
			// Mixed-wildcard payloads are caller error; ignore the marker
			// rather than blowing up. The validated harness path never
			// produces this — defense in depth for adapter framework
			// edges that bypass the chain in error.
			continue
		}
		id := actor.ActorID(raw)
		rec, ok, err := reg.Lookup(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("trigger: lookup %q: %w", id, err)
		}
		if !ok || !rec.IsActive() {
			continue
		}
		out = append(out, rec.ID)
	}
	return out, nil
}

// isWildcard reports whether the audience slice means "everyone in the
// channel" per L1 §5.1 step 2. Empty slice is treated as wildcard so
// callers that forget to default `audience=['*']` still get correct
// fan-out (harness step 0 normalize fills it, but in-process callers
// that bypass the chain may not).
func isWildcard(aud message.Audience) bool {
	if len(aud) == 0 {
		return true
	}
	return aud.IsWildcard()
}

// filterOut drops the given id from the slice. Preserves order; callers
// dedupeSort afterwards.
func filterOut(ids []actor.ActorID, drop actor.ActorID) []actor.ActorID {
	if drop == "" {
		return ids
	}
	out := ids[:0]
	for _, id := range ids {
		if id == drop {
			continue
		}
		out = append(out, id)
	}
	return out
}

// dedupeSort removes duplicates and returns a slice sorted by actor id.
// nil-safe: empty input → nil output (not an empty slice — callers can
// switch on `len(audience) == 0`).
func dedupeSort(ids []actor.ActorID) []actor.ActorID {
	if len(ids) == 0 {
		return nil
	}
	seen := make(map[actor.ActorID]struct{}, len(ids))
	out := make([]actor.ActorID, 0, len(ids))
	for _, id := range ids {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
