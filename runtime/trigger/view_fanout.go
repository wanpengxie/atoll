package trigger

import (
	"context"
	"fmt"
	"sort"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// ViewFanout resolves the set of channel members whose view cache receives
// `env`. Unlike Resolve (which drives audience handler triggers), ViewFanout
// is purely about which members can SEE the envelope:
//
//   - visibility = public  → every channel member view cache (proto-layer1).
//   - visibility = private → audience ∪ {sender} only (proto-layer1).
//   - visibility = system  → no default WS/UI projection in coagent's L3
//     implementation profile; audit/admin/debug readers can still read the
//     persisted message log.
//
// Inputs:
//
//   - env: a harness-accepted envelope.
//   - reg: the channel-local actor registry. View fanout enumerates
//     ListActive when visibility=public.
//   - members (optional): when non-nil it overrides the registry-derived
//     "all channel members" set. Callers that maintain their own member
//     list (server-side viewcache) supply it; daemon-side callers leave
//     nil and let the registry drive the set.
//
// The return value is deduped + sorted by actor id so the caller observes
// a stable order. nil means "no view fanout" (no recipient).
//
// proto-layer1 §4.1.3 notes for public/private view fanout:
//
//   - System actor emit is NOT special-cased. visibility/audience on the
//     envelope alone decides the result.
//   - Wildcard audience has been removed; harness Step 2 rejects any
//     "*" entry before view fanout runs.
func ViewFanout(
	ctx context.Context,
	env *message.Envelope,
	reg actor.Registry,
	members []actor.ActorID,
) ([]actor.ActorID, error) {
	if env == nil {
		return nil, fmt.Errorf("trigger: view fanout nil envelope")
	}
	if reg == nil && members == nil {
		return nil, fmt.Errorf("trigger: view fanout requires registry or explicit member set")
	}

	switch env.Visibility {
	case message.VisibilityPublic, "":
		// Empty visibility defaults to public per proto-layer0 §2.4 and
		// is normalized by harness Step Normalize; this branch tolerates
		// callers that pass a pre-normalize envelope.
		if members != nil {
			return dedupeSort(append([]actor.ActorID(nil), members...)), nil
		}
		return listActiveSorted(ctx, reg)

	case message.VisibilityPrivate:
		// audience ∪ {sender}. After wildcard removal every audience entry
		// is a literal actor_id.
		visible := make([]actor.ActorID, 0, len(env.Audience)+1)
		visible = append(visible, env.Audience...)
		if env.Sender.ID != "" {
			visible = append(visible, env.Sender.ID)
		}
		return dedupeSort(visible), nil

	case message.VisibilitySystem:
		// Coagent implementation profile / impl-layer3 §2.4.1 default
		// subscriber policy: system-visibility messages are not projected
		// to ordinary WS/UI subscribers by default. This is not a
		// proto-layer1 normative delete or invisibility rule; the envelope
		// remains in the channel log for audit/admin/debug read paths.
		return nil, nil
	}

	// Unknown visibility (defensive — Step 2 already rejected).
	return nil, fmt.Errorf("trigger: view fanout: visibility %q outside closed set", env.Visibility)
}

// listActiveSorted enumerates the registry's active members.
func listActiveSorted(ctx context.Context, reg actor.Registry) ([]actor.ActorID, error) {
	rows, err := reg.ListActive(ctx)
	if err != nil {
		return nil, fmt.Errorf("trigger: view fanout list active: %w", err)
	}
	out := make([]actor.ActorID, 0, len(rows))
	for _, rec := range rows {
		out = append(out, rec.ID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}
