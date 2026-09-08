package channelmember

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
)

// Config is the declared intent: words[type].target names a DECLARATION in the
// body. Which member actually answers is a runtime fact, so it lives in the
// handle's own State under binding/<type>, never in config (design §5.2):
//
//   - a request arrives: a pinned actor that is still an active member answers;
//   - no pin, or the pin is stale: resolve the target declaration once, pin the
//     result, answer; zero members is not_found, several is actor_ambiguous;
//   - admission never looks here, so the handle may be seated before its
//     target, and replacing or restarting the target is invisible to the host.
//
// The pin stores the actor id only (kind:seed). An incarnation is a memory
// pointer and is never persisted; the body's ordinary target resolution turns
// the stored id into whichever incarnation is alive right now.
type wordBinding struct {
	Actor  actor.ActorID `json:"actor"`
	Target string        `json:"target"`
}

func bindingKey(word string) resource.ResourceID { return resource.ResourceID("binding/" + word) }

// targetResolver is the optional face a Members authority may offer for turning
// a persisted kind:seed id into the live member; without it a pin is not
// reusable and every request re-resolves the declaration (still correct).
type targetResolver interface {
	ResolveTarget(string) (actor.ActorID, error)
}

func resolveWordTarget(ctx context.Context, state actorbase.StateHandle, members Members, word, targetDecl string) (actor.ActorID, error) {
	if pinned, ok := pinnedTarget(ctx, state, members, word, targetDecl); ok {
		return pinned, nil
	}
	id, err := members.MemberOfDeclaration(targetDecl)
	if err != nil {
		return "", err
	}
	if state != nil {
		// Best effort: a pin that fails to persist only costs one more
		// declaration lookup on the next request.
		raw, _ := json.Marshal(wordBinding{Actor: persistentID(id), Target: targetDecl})
		_, _ = state.Put(bindingKey(word), raw)
	}
	return id, nil
}

func pinnedTarget(ctx context.Context, state actorbase.StateHandle, members Members, word, targetDecl string) (actor.ActorID, bool) {
	if state == nil {
		return "", false
	}
	out, err := state.Get(bindingKey(word))
	if err != nil || !out.Found || len(out.Value) == 0 {
		return "", false
	}
	var pin wordBinding
	if json.Unmarshal(out.Value, &pin) != nil || pin.Actor == "" || pin.Target != targetDecl {
		// A pin made for another target declaration is stale by definition:
		// the manager changed the config, the runtime fact must follow.
		return "", false
	}
	resolver, ok := members.(targetResolver)
	if !ok {
		return "", false
	}
	live, err := resolver.ResolveTarget(string(pin.Actor))
	if err != nil {
		return "", false
	}
	facts, found, err := members.ActorFacts(ctx, live)
	if err != nil || !found || !facts.Active {
		return "", false
	}
	return live, true
}

// persistentID strips the incarnation: kind:seed:incarnation → kind:seed.
func persistentID(id actor.ActorID) actor.ActorID {
	parts := strings.Split(string(id), ":")
	if len(parts) == 3 {
		return actor.ActorID(parts[0] + ":" + parts[1])
	}
	return id
}
