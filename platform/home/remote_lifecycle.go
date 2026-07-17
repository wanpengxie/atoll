package home

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func (h *Home) handleRemoteSpawn(ctx context.Context, inc actorrt.Incarnation, birthVersion int64, nonce string, spec actorrt.ForkSpec) (actor.ActorID, error) {
	if !h.channel.Cells().IsLive(inc) {
		return "", actorrt.ErrParentNotLive
	}
	child, err := h.forkAdmission(ctx, inc.ID(), birthVersion, spec, nonce)
	if err != nil {
		return "", err
	}
	h.pokeReconcile()
	return child, nil
}

func (h *Home) handleRemoteEnd(ctx context.Context, inc actorrt.Incarnation, birthVersion int64, target actor.ActorID, reason string) (func(), error) {
	if target == "" {
		target = inc.ID()
	}
	if reason == "" {
		reason = "ended"
	}
	return (lifecycleEndHandle{home: h, author: storespec.AuthorStamp{ID: inc.ID(), BirthVersion: birthVersion}}).prepare(ctx, target, reason)
}
