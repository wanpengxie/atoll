package trigger

import (
	"context"
	"errors"
	"fmt"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// Sentinel errors callers inspect.
var (
	// ErrNilEnvelope is returned when Dispatch is handed a nil *Envelope.
	// Surfaced as a programming error — callers (harness / scheduler) MUST
	// pass a fully-normalised envelope.
	ErrNilEnvelope = errors.New("trigger: envelope is nil")

	// ErrInvalidVisibility is returned when env.Visibility is not one of
	// the L0 §2.4 closed set {public, private, system}. Step 0 of the
	// harness normalises empty visibility → public, so by the time Dispatch
	// is invoked we expect a populated and valid value; anything else is
	// programmer error.
	ErrInvalidVisibility = errors.New("trigger: invalid visibility")
)

// ActorLookup is the subset of internal/registry/actor.go that the
// trigger gateway needs. The single method powers L1 §5.1 step 2
// audience expansion when `audience=['*']` — it MUST return only
// active actor ids (deregistered rows filtered out).
//
// Real callers wire registry.ListActive (which respects
// `deregistered_at IS NULL` per L1 §12.2). Tests inject canned slices.
type ActorLookup interface {
	// ListActive returns every active actor id in this channel's
	// actor_registry. Order is implementation-defined; the gateway
	// re-sorts during dedupe so callers do not depend on it.
	ListActive(ctx context.Context) ([]string, error)
}

// SubscriptionMatcher returns the actor ids that have explicitly
// subscribed to env (L1 §5.4). The default scope is messages with
// `visibility=system` (system events / cross-channel observation) —
// for `visibility=public` / `private` the audience field already
// expresses routing and subscription is informational. The matcher
// MUST be safe for concurrent reads; the in-memory Registry exposed
// by this package satisfies the contract.
type SubscriptionMatcher interface {
	// Match returns subscribers for env. An empty / nil return means
	// "no subscribers" — the gateway treats it the same way.
	Match(env *v4types.Envelope) []string
}

// noopMatcher is the default SubscriptionMatcher used when caller
// passes nil. It returns no subscribers — the gateway behaves as if
// L1 §5.4 subscription is disabled.
type noopMatcher struct{}

// Match satisfies SubscriptionMatcher.
func (noopMatcher) Match(*v4types.Envelope) []string { return nil }

// Gateway implements the L1 §5 trigger gateway decision algorithm.
//
// It is a pure decision component — given an envelope plus the
// dispatch-path upstream, it returns the list of actor ids that
// should be triggered. It performs no I/O beyond the ActorLookup
// call required by `audience=['*']` expansion (L1 §5.1 step 2), and
// it never mutates the envelope or the channel store.
//
// The component is safe for concurrent use by multiple goroutines as
// long as the injected ActorLookup / SubscriptionMatcher are
// concurrency-safe (the production implementations are).
type Gateway struct {
	actors ActorLookup
	subs   SubscriptionMatcher
}

// NewGateway constructs a Gateway.
//
// `actors` is required (audience expansion calls it; tests may inject
// a stub returning a fixed slice). `subs` is optional — passing nil
// installs a noop matcher equivalent to "no subscriptions registered".
func NewGateway(actors ActorLookup, subs SubscriptionMatcher) (*Gateway, error) {
	if actors == nil {
		return nil, errors.New("trigger: ActorLookup is required")
	}
	if subs == nil {
		subs = noopMatcher{}
	}
	return &Gateway{actors: actors, subs: subs}, nil
}

// Dispatch evaluates which actors should be triggered for env per
// L1 §5.1. The return value preserves the spec's "audience-driven
// fan-out + filter" semantics and is suitable for downstream consumers
// (supervisor wake / view-sync push) to iterate over without further
// inspection.
//
// `upstream` is the dispatch-path direct upstream actor id (L1 §5.3
// "dispatch-path 语义"):
//
//   - For direct harness writes (caller emits, harness commits, gateway
//     fans out) the upstream IS env.Sender.ID — passing it keeps the
//     self-trigger filter in place so the sender does not re-trigger
//     itself.
//   - For scheduler-triggered future messages (this package's
//     FutureScheduler) the upstream is the scheduler, NOT the original
//     sender — callers pass "" (or any sentinel != env.Sender.ID) so
//     the original sender is eligible for trigger.
//   - For long-pending fallback (T9) the system upstream similarly
//     bypasses the sender filter.
//
// The returned slice is freshly allocated; caller may retain or
// modify it. Returns (nil, error) only on programmer error
// (nil envelope, invalid visibility) or backing-store failure
// (ActorLookup returning an error).
func (g *Gateway) Dispatch(
	ctx context.Context,
	env *v4types.Envelope,
	upstream string,
) ([]string, error) {
	if env == nil {
		return nil, ErrNilEnvelope
	}

	// ----- Step 1: visibility filter --------------------------------
	// `system` → no automatic candidates; only subscribers can react.
	// `private` → only the sender's own id is a candidate (which the
	//             self-trigger filter normally removes; result usually
	//             empty — by design, L1 §5.2 example 5).
	// `public`  → expand audience.
	var candidates []string
	switch env.Visibility {
	case v4types.VisibilitySystem:
		// candidates stays empty; subscribers added below.
	case v4types.VisibilityPrivate:
		if env.Sender.ID != "" {
			candidates = []string{env.Sender.ID}
		}
	case v4types.VisibilityPublic:
		expanded, err := g.expandAudience(ctx, env.Audience)
		if err != nil {
			return nil, err
		}
		candidates = expanded
	default:
		return nil, fmt.Errorf("%w: %q", ErrInvalidVisibility, env.Visibility)
	}

	// ----- Subscription augmentation (L1 §5.4) ----------------------
	// Subscribers join the candidate set BEFORE the step-4 filter so a
	// subscriber that happens to match the sender/type drop rule (or is
	// the dispatch-path upstream) is still correctly excluded.
	if subs := g.subs.Match(env); len(subs) > 0 {
		candidates = append(candidates, subs...)
	}

	// ----- Step 4: sender/type + self-trigger filter ----------------
	return filterCandidates(env, candidates, upstream), nil
}

// expandAudience implements L1 §5.1 step 2 audience expansion. The
// broadcast literal `['*']` triggers a ListActive call; explicit
// audience lists are returned verbatim (caller's responsibility to
// have validated them at harness step 5).
func (g *Gateway) expandAudience(ctx context.Context, audience []string) ([]string, error) {
	if len(audience) == 0 {
		// Harness normalize fills missing audience with ['*']; an empty
		// slice here means the envelope skipped normalisation (e.g. a
		// future-message recovery path). Treat as broadcast for parity
		// with normalise's fill-in rule.
		return g.actors.ListActive(ctx)
	}
	if len(audience) == 1 && audience[0] == "*" {
		return g.actors.ListActive(ctx)
	}
	// Explicit audience: copy so caller can keep the result without
	// aliasing the envelope's slice.
	out := make([]string, len(audience))
	copy(out, audience)
	return out, nil
}

// filterCandidates applies L1 §5.1 step 4 filters and dedupes the
// remaining ids in encounter order:
//
//   - sender.kind=system + type=system.heartbeat → drop everyone
//     (system heartbeats are scheduler-only noise; nothing reacts).
//   - sender.kind=agent + type=agent.text + visibility=system → drop
//     everyone (intermediate agent output; not for public consumption).
//   - actor_id == upstream → drop (self-trigger filter, dispatch-path
//     semantics per §5.3). When upstream == "" the check is disabled.
//   - empty string ids are skipped (defensive against caller filling
//     a nil audience).
//
// Encounter order matters because the audience array carries author
// intent ("ping A, then B") and downstream UI may render in the
// supplied order.
func filterCandidates(env *v4types.Envelope, candidates []string, upstream string) []string {
	if dropAll(env) {
		return nil
	}
	seen := make(map[string]struct{}, len(candidates))
	out := make([]string, 0, len(candidates))
	for _, id := range candidates {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}

		// Self-trigger filter (dispatch-path semantics).
		if upstream != "" && id == upstream {
			continue
		}
		out = append(out, id)
	}
	return out
}

// dropAll captures the two L1 §5.1 step 4 rules that remove *all*
// candidates regardless of identity:
//
//  1. `sender.kind=system + type=system.heartbeat` — pure scheduler
//     liveness signal; nothing reacts.
//  2. `sender.kind=agent + type=agent.text + visibility=system` —
//     agent intermediate output (e.g. internal monologue); not
//     visible to anyone outside the channel control plane.
func dropAll(env *v4types.Envelope) bool {
	if env.Sender.Kind == v4types.SenderSystem && env.Type == "system.heartbeat" {
		return true
	}
	if env.Sender.Kind == v4types.SenderAgent &&
		env.Type == "agent.text" &&
		env.Visibility == v4types.VisibilitySystem {
		return true
	}
	return false
}
