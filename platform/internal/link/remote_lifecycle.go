package link

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

var ErrLifecycleTransient = errors.New("link: lifecycle transport unavailable")

type remoteActorLifecycle struct {
	codec *ipc.Codec
	end   *relayCore[ipc.EndAckPayload]
}

func newRemoteActorLifecycle(codec *ipc.Codec) *remoteActorLifecycle {
	return &remoteActorLifecycle{
		codec: codec,
		end:   newRelayCore[ipc.EndAckPayload](codec, ipc.KindEnd, ErrLifecycleTransient),
	}
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
	h.end.close()
}

var _ actorcaps.LifecycleHandle = (*remoteActorLifecycle)(nil)
