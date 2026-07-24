package home

import (
	"context"
	"fmt"
	"sort"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/runtime/actorhost"
)

// planForDaemon is a read adapter over Controller's sole desired projection
// kernel. It makes no placement, liveness, or incarnation decision itself.
func (h *Home) planForDaemon(
	_ context.Context,
	daemonID string,
) ([]platform.PlanActor, error) {
	projected, err := h.actors.PlanFor(actorhost.ExecutionDomain(daemonID))
	if err != nil {
		return nil, err
	}
	out := make([]platform.PlanActor, 0, len(projected))
	for _, desired := range projected {
		body, ok := desired.(actorhost.BodyDesired)
		if !ok {
			return nil, fmt.Errorf("platform: daemon plan contains non-body desired %T", desired)
		}
		out = append(out, platform.PlanActor{
			ActorID:    body.ActorID,
			AttemptKey: body.AttemptKey,
			Kind:       body.ExecutionSpec.Kind,
			Class:      body.ExecutionSpec.Class,
			Config:     append([]byte(nil), body.ExecutionSpec.Config...),
			Idle:       body.ExecutionSpec.IdleTimeout,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ActorID < out[j].ActorID })
	return out, nil
}
