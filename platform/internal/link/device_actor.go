package link

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

const attachHandshakeTimeout = 10 * time.Second

type laneActorBinding struct {
	endpoint *serverActorEndpoint
	current  func() bool
	close    sync.Once
	done     chan struct{}
}

func (b *laneActorBinding) Deliver(env *message.Envelope) error {
	if b.current == nil || !b.current() {
		return errLinkClosed
	}
	return b.endpoint.Deliver(env)
}
func (b *laneActorBinding) CancelRequest(id message.ID) {
	if b.current != nil && b.current() {
		b.endpoint.CancelRequest(id)
	}
}
func (b *laneActorBinding) Close() error {
	var err error
	b.close.Do(func() { err = b.endpoint.Close() })
	return err
}
func (b *laneActorBinding) Done() <-chan struct{} { return b.done }

// ServeLaneActor performs the immutable lane admission followed by the
// existing inner IPC identity handshake. The returned close function owns
// exactly this actor stream.
func ServeLaneActor(
	ctx context.Context,
	conn net.Conn,
	daemonID string,
	membrane platform.DaemonMembrane,
	current func() bool,
	logger *slog.Logger,
) (func(), <-chan struct{}, error) {
	if conn == nil || !current() {
		if conn != nil {
			_ = conn.Close()
		}
		return nil, nil, errLinkClosed
	}
	_ = conn.SetReadDeadline(time.Now().Add(attachHandshakeTimeout))
	codec := ipc.NewCodec(conn, conn)
	frame, err := codec.Read()
	if err != nil || frame.Kind != ipc.KindHandshake {
		_ = conn.Close()
		return nil, nil, errors.New("link: actor handshake required")
	}
	var handshake ipc.HandshakePayload
	if err := json.Unmarshal(frame.Payload, &handshake); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	id := actor.ActorID(handshake.LeaseID)
	key, err := actorhost.ParseAttemptKey(handshake.AttemptKey)
	if err != nil || membrane.AuthorizeAttach == nil ||
		membrane.AuthorizeAttach(id, key, actorhost.ExecutionDomain(daemonID)) != nil ||
		!current() {
		_ = conn.Close()
		return nil, nil, errors.New("link: actor admission refused")
	}
	_ = conn.SetReadDeadline(time.Time{})
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	handlers := serverActorHandlers{
		emit: func(callCtx context.Context, actorID actor.ActorID, attempt actorhost.AttemptKey, env *message.Envelope) (ipc.EmitResult, error) {
			if !current() {
				return ipc.EmitResult{}, errLinkClosed
			}
			result, err := membrane.Ingress.Emit(callCtx, actorID, attempt, env)
			return ipc.EmitResult{
				MessageID: result.MessageID, Seq: result.Seq,
				RejectReason: string(result.RejectReason), RejectDetail: result.RejectDetail,
			}, err
		},
		access: func(callCtx context.Context, actorID actor.ActorID, attempt actorhost.AttemptKey, payload []byte) ([]byte, error) {
			if !current() {
				return nil, errLinkClosed
			}
			var request accessRequest
			if err := json.Unmarshal(payload, &request); err != nil {
				return nil, fmt.Errorf("link: access payload decode: %w", err)
			}
			call, err := request.decode()
			if err != nil {
				return nil, err
			}
			response, err := membrane.Ingress.Access(callCtx, actorID, attempt, call)
			if err != nil {
				return nil, err
			}
			return json.Marshal(accessResponseOf(call.Kind, response))
		},
		schedule: func(callCtx context.Context, actorID actor.ActorID, payload []byte) ([]byte, error) {
			if !current() {
				return nil, errLinkClosed
			}
			var request scheduleRequest
			if err := json.Unmarshal(payload, &request); err != nil {
				return nil, err
			}
			call, err := request.decode()
			if err != nil {
				return nil, err
			}
			response, err := membrane.Ingress.Schedule(callCtx, actorID, call)
			if err != nil {
				return nil, err
			}
			return json.Marshal(scheduleResponse{ID: response.ID, Timers: response.Timers})
		},
		resolveTarget: func(_ context.Context, target string) (actor.ActorID, error) {
			if !current() {
				return "", errLinkClosed
			}
			if membrane.ResolveTarget == nil {
				return "", errRelayUnavailable
			}
			return membrane.ResolveTarget(target)
		},
		endSelf: func(callCtx context.Context, id actor.ActorID, key actorhost.AttemptKey, request actorcaps.EndSelfRequest) error {
			if !current() {
				return errLinkClosed
			}
			return membrane.Ingress.EndSelf(callCtx, id, key, request)
		},
		cancelRequest: func(id actor.ActorID, request message.ID) {
			if current() && membrane.CancelRequest != nil {
				membrane.CancelRequest(id, request)
			}
		},
		deliverResult: func(id actor.ActorID, request message.ID, outcome, detail string) {
			logger.Warn("platform.delivery.remote_outcome",
				"actor", id, "request", request, "outcome", outcome, "detail", detail)
		},
	}
	var binding actorhost.Binding
	handlers.obs = func(id actor.ActorID, key actorhost.AttemptKey, kind actorrt.ObsKind, value actorrt.ObsValue) {
		if current() && membrane.Observe != nil {
			membrane.Observe(id, key, binding, kind, value)
		}
	}
	endpoint := newServerActorEndpoint(ctx, id, key, conn, codec, handlers)
	resource := &laneActorBinding{endpoint: endpoint, current: current, done: make(chan struct{})}
	binding, err = actorhost.NewBinding(resource)
	if err != nil {
		_ = endpoint.Close()
		return nil, nil, err
	}
	if membrane.AttachBinding == nil {
		_ = endpoint.Close()
		return nil, nil, errors.New("link: actor binding refused")
	}
	if err := membrane.AttachBinding(
		id, key, actorhost.ExecutionDomain(daemonID), binding,
	); err != nil {
		_ = endpoint.Close()
		return nil, nil, errors.New("link: actor binding refused")
	}
	if !current() {
		if membrane.BindingDown != nil {
			membrane.BindingDown(id, binding)
		}
		if membrane.ObserveDown != nil {
			membrane.ObserveDown(id, key, binding)
		}
		_ = endpoint.Close()
		return nil, nil, errLinkClosed
	}
	go func() {
		runErr := endpoint.Run(ctx)
		if membrane.BindingDown != nil {
			membrane.BindingDown(id, binding)
		}
		if membrane.ObserveDown != nil {
			membrane.ObserveDown(id, key, binding)
		}
		if runErr != nil && !errors.Is(runErr, context.Canceled) && !errors.Is(runErr, io.EOF) {
			logger.Debug("link.actor_binding_down", "actor", id, "err", runErr)
		}
		close(resource.done)
	}()
	return func() { _ = resource.Close() }, resource.Done(), nil
}
