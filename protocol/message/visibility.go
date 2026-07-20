package message

import "github.com/wanpengxie/atoll/protocol/actor"

// ShouldDeliver answers the delivery half of visibility. Audience is the
// addressing truth for every visibility, including system traffic: internal
// requests intentionally use system visibility while targeting an ordinary
// actor. Audit events remain confined because their audience is the intrinsic
// system actor. Historical reader filtering is intentionally separate.
func ShouldDeliver(target actor.ActorID, env *Envelope) bool {
	if env == nil || target == "" {
		return false
	}
	addressed := false
	for _, id := range env.Audience {
		if id == target {
			addressed = true
			break
		}
	}
	if !addressed {
		return false
	}
	return env.Visibility == VisibilityPublic ||
		env.Visibility == VisibilityPrivate ||
		env.Visibility == VisibilitySystem
}
