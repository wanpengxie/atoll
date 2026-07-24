package link

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

var ErrLifecycleTransient = errors.New("link: lifecycle transport unavailable")

type remoteActorLifecycle struct {
	codec *ipc.Codec
	fork  *relayCore[ipc.SpawnAckPayload]
	end   *relayCore[ipc.EndAckPayload]
}

func newRemoteActorLifecycle(codec *ipc.Codec) *remoteActorLifecycle {
	return &remoteActorLifecycle{
		codec: codec,
		fork:  newRelayCore[ipc.SpawnAckPayload](codec, ipc.KindSpawn, ErrLifecycleTransient),
		end:   newRelayCore[ipc.EndAckPayload](codec, ipc.KindEnd, ErrLifecycleTransient),
	}
}

func (h *remoteActorLifecycle) Fork(
	ctx context.Context,
	requestID message.ID,
	spec actorcaps.ForkSpec,
) (actor.ActorID, error) {
	if requestID == "" {
		return "", errors.New("link: fork request id required")
	}
	placementKind, placementHost := "", ""
	if spec.Placement != nil {
		placementKind = string(spec.Placement.Kind)
		placementHost = spec.Placement.DesiredHost
	}
	raw, err := json.Marshal(ipc.SpawnPayload{
		RequestID: requestID, Kind: spec.Kind, Class: spec.Class,
		NameHint: spec.NameHint, Config: append([]byte(nil), spec.Config...),
		PlacementKind: placementKind, PlacementHost: placementHost,
	})
	if err != nil {
		return "", err
	}
	ack, transport, definite := h.fork.roundTrip(ctx, raw)
	if definite != nil {
		return "", definite
	}
	if transport != nil {
		return "", transport
	}
	if err := decodeAckError(ack.ErrorCode, ack.ErrorMessage); err != nil {
		return "", err
	}
	if ack.ChildID == "" {
		return "", errors.New("link: fork result missing child id")
	}
	return ack.ChildID, nil
}

func (h *remoteActorLifecycle) RequestIdle(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := json.Marshal(ipc.IdlePayload{})
	if err != nil {
		return err
	}
	return h.codec.Write(ipc.Frame{Kind: ipc.KindIdle, Payload: raw})
}

func (h *remoteActorLifecycle) EndSelf(
	ctx context.Context,
	request actorcaps.EndSelfRequest,
) error {
	raw, err := json.Marshal(ipc.EndPayload{Reason: request.Reason})
	if err != nil {
		return err
	}
	ack, transport, definite := h.end.roundTrip(ctx, raw)
	if definite != nil {
		return definite
	}
	if transport != nil {
		return transport
	}
	return decodeAckError(ack.ErrorCode, ack.ErrorMessage)
}

func (h *remoteActorLifecycle) close() {
	h.fork.close()
	h.end.close()
}

var _ actorcaps.LifecycleHandle = (*remoteActorLifecycle)(nil)
