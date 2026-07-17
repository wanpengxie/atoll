package link

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

var (
	ErrLifecycleTransient = errors.New("link: lifecycle receipt unconfirmed")
	ErrLifecycleNotLive   = errors.New("link: lifecycle capability no longer the live incarnation")
)

type remoteLifecycleArm struct {
	spawn *relayCore[ipc.SpawnAckPayload]
	end   *relayCore[ipc.EndAckPayload]
}

func newRemoteLifecycleArm(codec *ipc.Codec) *remoteLifecycleArm {
	return &remoteLifecycleArm{
		spawn: newRelayCore[ipc.SpawnAckPayload](codec, ipc.KindSpawn, ErrLifecycleTransient),
		end:   newRelayCore[ipc.EndAckPayload](codec, ipc.KindEnd, ErrLifecycleTransient),
	}
}

func (h *remoteLifecycleArm) Fork(ctx context.Context, spec actorrt.ForkSpec) (actor.ActorID, error) {
	return h.forkWithNonce(ctx, spec, uuid.NewString())
}

func (h *remoteLifecycleArm) forkWithNonce(ctx context.Context, spec actorrt.ForkSpec, nonce string) (actor.ActorID, error) {
	placementKind := ""
	placementHost := ""
	if spec.Placement != nil {
		placementKind = string(spec.Placement.Kind)
		placementHost = spec.Placement.Host
	}
	raw, err := json.Marshal(ipc.SpawnPayload{
		Nonce: nonce, Kind: spec.Kind, Class: spec.Class, NameHint: spec.NameHint,
		Config: append([]byte(nil), spec.Config...), PlacementKind: placementKind, PlacementHost: placementHost,
	})
	if err != nil {
		return "", err
	}
	ack, transport, definite := h.spawn.roundTrip(ctx, raw)
	if definite != nil {
		return "", definite
	}
	if transport != nil {
		return "", fmt.Errorf("%w: %v", ErrLifecycleTransient, transport)
	}
	if err := decodeAckError(ack.ErrorCode, ack.ErrorMessage); err != nil {
		return "", err
	}
	if ack.ChildID == "" {
		return "", errors.New("link: spawn receipt missing child id")
	}
	return ack.ChildID, nil
}

func (h *remoteLifecycleArm) DespawnChild(ctx context.Context, child actor.ActorID, reason string) error {
	return h.endTarget(ctx, child, reason)
}

func (h *remoteLifecycleArm) EndSelf(ctx context.Context) error {
	return h.endTarget(ctx, "", "self_end")
}

func (h *remoteLifecycleArm) endTarget(ctx context.Context, target actor.ActorID, reason string) error {
	raw, err := json.Marshal(ipc.EndPayload{Target: target, Reason: reason})
	if err != nil {
		return err
	}
	ack, transport, definite := h.end.roundTrip(ctx, raw)
	if definite != nil {
		return definite
	}
	if transport != nil {
		return fmt.Errorf("%w: %v", ErrLifecycleTransient, transport)
	}
	return decodeAckError(ack.ErrorCode, ack.ErrorMessage)
}

func (h *remoteLifecycleArm) close() {
	h.spawn.close()
	h.end.close()
}

func retryLifecycle(ctx context.Context, call func(actorrt.LifecycleHandle) error, load func() actorrt.LifecycleHandle) error {
	for {
		raw := load()
		if raw == nil {
			return ErrLifecycleNotLive
		}
		err := call(raw)
		if !errors.Is(err, ErrLifecycleTransient) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

var _ actorrt.LifecycleHandle = (*remoteLifecycleArm)(nil)

type liveLifecycle struct {
	raw  actorrt.LifecycleHandle
	inc  actorrt.Incarnation
	host *actorrt.Runtime
}

func NewLiveLifecycle(raw actorrt.LifecycleHandle, inc actorrt.Incarnation, host *actorrt.Runtime) actorrt.LifecycleHandle {
	return liveLifecycle{raw: raw, inc: inc, host: host}
}

func (h liveLifecycle) Fork(ctx context.Context, spec actorrt.ForkSpec) (actor.ActorID, error) {
	if !h.host.IsLive(h.inc) {
		return "", ErrLifecycleNotLive
	}
	return h.raw.Fork(ctx, spec)
}

func (h liveLifecycle) DespawnChild(ctx context.Context, id actor.ActorID, reason string) error {
	if !h.host.IsLive(h.inc) {
		return ErrLifecycleNotLive
	}
	return h.raw.DespawnChild(ctx, id, reason)
}

func (h liveLifecycle) EndSelf(ctx context.Context) error {
	if !h.host.IsLive(h.inc) {
		return ErrLifecycleNotLive
	}
	return h.raw.EndSelf(ctx)
}

var _ actorrt.LifecycleHandle = liveLifecycle{}
