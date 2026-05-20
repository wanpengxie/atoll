package trigger

import (
	"context"
	"fmt"
	"sort"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// ViewFanout resolves the set of channel members whose view cache MUST
// receive `env` per proto-layer1 §4.1.3 (view fanout, visibility-driven,
// normative). Unlike Resolve (which drives audience handler triggers),
// ViewFanout is purely about which members can SEE the envelope:
//
//   - visibility = public  → every channel member view cache.
//   - visibility = private → audience ∪ {sender} only.
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
// proto-layer1 §4.1.3 informative notes:
//
//   - System actor emit is NOT special-cased. visibility/audience on the
//     envelope alone decides the result.
//   - The visibility=private + audience=['*'] contradiction is already
//     blocked by harness Step 2 (HarnessVisibilityAudienceInvalid).
func ViewFanout(
	ctx context.Context,
	env *message.Envelope,
	reg actorreg.Registry,
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
		// audience ∪ {sender}.
		visible := make([]actor.ActorID, 0, len(env.Audience)+1)
		for _, a := range env.Audience {
			if a == message.AudienceWildcard {
				// harness Step 2 rejects visibility=private + audience=['*']
				// — this branch only fires for adapter/test callers that
				// bypass the chain. Be conservative: drop the wildcard
				// rather than expanding to "everyone" (which would
				// contradict private semantics).
				continue
			}
			visible = append(visible, a)
		}
		if env.Sender.ID != "" {
			visible = append(visible, env.Sender.ID)
		}
		return dedupeSort(visible), nil
	}

	// Unknown visibility (defensive — Step 2 already rejected).
	return nil, fmt.Errorf("trigger: view fanout: visibility %q outside closed set", env.Visibility)
}

// listActiveSorted enumerates the registry's active members.
func listActiveSorted(ctx context.Context, reg actorreg.Registry) ([]actor.ActorID, error) {
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
