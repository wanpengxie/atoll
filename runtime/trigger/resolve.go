package trigger

import (
	"context"
	"fmt"
	"sort"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// TypeSystemHeartbeat is the noise-filter type explicitly excluded from
// trigger fan-out per L1 §5.2 (system.heartbeat).
const TypeSystemHeartbeat = "system.heartbeat"

// Options is reserved for future per-call dispatch knobs. Empty today —
// wildcard / self-trigger-ban / bypass machinery were removed after
// owner reframed addressing as Erlang-style explicit `pid ! msg`.
type Options struct{}

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
// Addressing semantics (post wildcard removal):
//
//   - audience MUST be a literal list of actor_ids. Empty → empty
//     fan-out (no receiver, message is observation-only).
//   - sender's own id may legitimately appear in audience — that is the
//     self-schedule entry point and is fan-out unchanged.
//   - non-active / unknown actor_ids are silently dropped.
func Resolve(
	ctx context.Context,
	env *message.Envelope,
	reg storespec.Registry,
	_ Options,
) ([]actor.ActorID, error) {
	if env == nil {
		return nil, fmt.Errorf("trigger: nil envelope")
	}
	if reg == nil {
		return nil, fmt.Errorf("trigger: nil registry")
	}

	// (1) Noise filter — system.heartbeat never fan-outs (L1 §5.2).
	if env.Type == TypeSystemHeartbeat {
		return nil, nil
	}

	// (2) Audience resolve — literal list, drop inactive.
	candidates, err := expandAudience(ctx, env, reg)
	if err != nil {
		return nil, err
	}

	// (3) Dedupe + stable sort.
	return dedupeSort(candidates), nil
}

// expandAudience converts envelope.audience into a concrete actor set.
//
// Each entry must be a literal actor_id. Missing or deregistered rows
// are dropped (matches L1 §5.1 "active member" requirement).
func expandAudience(
	ctx context.Context,
	env *message.Envelope,
	reg storespec.Registry,
) ([]actor.ActorID, error) {
	if len(env.Audience) == 0 {
		return nil, nil
	}
	out := make([]actor.ActorID, 0, len(env.Audience))
	for _, raw := range env.Audience {
		if raw == "" {
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
