package link

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/wanpengxie/atoll/protocol/actor"
)

type targetResolveRequest struct {
	Target string `json:"target"`
}

type targetResolveResponse struct {
	Actor actor.ActorID `json:"actor"`
}

type remoteTargetResolver struct {
	relay *relayClient
}

func (r *remoteTargetResolver) ResolveTarget(ctx context.Context, target string) (actor.ActorID, error) {
	if r == nil || r.relay == nil {
		return "", errors.New("link: target resolver unavailable")
	}
	payload, err := json.Marshal(targetResolveRequest{Target: target})
	if err != nil {
		return "", err
	}
	raw, ackErr, transportErr := r.relay.roundTrip(ctx, payload)
	if transportErr != nil {
		return "", transportErr
	}
	if ackErr != nil {
		return "", ackErr
	}
	var response targetResolveResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		return "", err
	}
	if response.Actor == "" {
		return "", errors.New("link: target resolver returned an empty actor")
	}
	return response.Actor, nil
}

var _ TargetResolver = (*remoteTargetResolver)(nil)
