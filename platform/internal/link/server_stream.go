package link

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

// serverActorHandlers is the endpoint's outward call table. The identity
// coordinate is NOT in it: (id, key) are fixed on the endpoint at handshake
// time and passed down on every call, so a frame can carry an operation but
// never a claim about who is issuing it.
//
// Which arms carry the attempt key is the permission matrix itself: the pen and
// resource access act AS THE CURRENT TERM (A/G), schedule acts as an IDENTITY
// across terms (A), so its signature has no key to compare.
type serverActorHandlers struct {
	emit          func(context.Context, actor.ActorID, actorhost.AttemptKey, *message.Envelope) (ipc.EmitResult, error)
	access        func(context.Context, actor.ActorID, actorhost.AttemptKey, []byte) ([]byte, error)
	schedule      func(context.Context, actor.ActorID, []byte) ([]byte, error)
	resolveTarget func(context.Context, string) (actor.ActorID, error)
	endSelf       func(context.Context, actor.ActorID, actorhost.AttemptKey, actorcaps.EndSelfRequest) error
	obs           func(actor.ActorID, actorhost.AttemptKey, actorrt.ObsKind, actorrt.ObsValue)
	cancelRequest func(actor.ActorID, message.ID)
	deliverResult func(actor.ActorID, message.ID, string, string)
}

// serverActorEndpoint is the link-owned exact remote endpoint. It contains no
// actor identity/current registry; the surrounding Binding supplies exact
// physical ownership and the injected handlers supply authority.
type serverActorEndpoint struct {
	id       actor.ActorID
	key      actorhost.AttemptKey
	conn     net.Conn
	codec    *ipc.Codec
	handlers serverActorHandlers

	ctx    context.Context
	cancel context.CancelFunc

	closeOnce sync.Once
	done      chan struct{}
}

func newServerActorEndpoint(
	parent context.Context,
	id actor.ActorID,
	key actorhost.AttemptKey,
	conn net.Conn,
	codec *ipc.Codec,
	handlers serverActorHandlers,
) *serverActorEndpoint {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	if codec == nil {
		codec = ipc.NewCodec(conn, conn)
	}
	return &serverActorEndpoint{
		id: id, key: key, conn: conn, codec: codec,
		handlers: handlers, ctx: ctx, cancel: cancel,
		done: make(chan struct{}),
	}
}

func (s *serverActorEndpoint) Run(context.Context) error {
	err := s.readLoop()
	_ = s.Close()
	close(s.done)
	return err
}

func (s *serverActorEndpoint) Done() <-chan struct{} { return s.done }

func (s *serverActorEndpoint) Close() error {
	if s == nil {
		return nil
	}
	var closeErr error
	s.closeOnce.Do(func() {
		s.cancel()
		closeErr = s.conn.Close()
	})
	return closeErr
}

func (s *serverActorEndpoint) Deliver(env *message.Envelope) error {
	if s == nil || env == nil {
		return actorrt.ErrUnitStopped
	}
	raw, err := json.Marshal(ipc.DeliverPayload{Envelope: *env})
	if err != nil {
		return err
	}
	return s.send(ipc.Frame{Kind: ipc.KindDeliver, Payload: raw})
}

func (s *serverActorEndpoint) CancelRequest(id message.ID) {
	if s == nil {
		return
	}
	raw, err := json.Marshal(ipc.CancelPayload{RequestID: id})
	if err != nil {
		return
	}
	_ = s.send(ipc.Frame{Kind: ipc.KindCancel, Payload: raw})
}

// send is the endpoint's only outbound path. It writes directly through the
// actor stream owner: no queue, writer goroutine, capacity, or overflow policy.
func (s *serverActorEndpoint) send(frame ipc.Frame) error {
	if s == nil {
		return actorrt.ErrUnitStopped
	}
	select {
	case <-s.ctx.Done():
		return actorrt.ErrUnitStopped
	default:
	}
	if err := s.codec.Write(frame); err != nil {
		// actorStreamConn has already published the write failure and woken this
		// endpoint's reader. The reader owns physical Close so this caller is not
		// held behind yamux's connection write timeout.
		failActorStream(s.conn)
		return err
	}
	return nil
}

func (s *serverActorEndpoint) readLoop() error {
	for {
		frame, err := s.codec.Read()
		if err != nil {
			return err
		}
		switch frame.Kind {
		case ipc.KindEmit:
			if err := s.handleEmit(frame.Payload); err != nil {
				return err
			}
		case ipc.KindAccess:
			if err := s.handleRelay(ipc.KindAccessAck, frame.Payload, func(
				ctx context.Context, payload []byte,
			) ([]byte, error) {
				if s.handlers.access == nil {
					return nil, errRelayUnavailable
				}
				return s.handlers.access(ctx, s.id, s.key, payload)
			}); err != nil {
				return err
			}
		case ipc.KindSchedule:
			if err := s.handleRelay(ipc.KindScheduleAck, frame.Payload, func(
				ctx context.Context, payload []byte,
			) ([]byte, error) {
				if s.handlers.schedule == nil {
					return nil, errRelayUnavailable
				}
				return s.handlers.schedule(ctx, s.id, payload)
			}); err != nil {
				return err
			}
		case ipc.KindResolveTarget:
			if err := s.handleRelay(ipc.KindResolveTargetAck, frame.Payload, func(
				ctx context.Context, payload []byte,
			) ([]byte, error) {
				if s.handlers.resolveTarget == nil {
					return nil, errRelayUnavailable
				}
				var request targetResolveRequest
				if err := json.Unmarshal(payload, &request); err != nil {
					return nil, err
				}
				resolved, err := s.handlers.resolveTarget(ctx, request.Target)
				if err != nil {
					return nil, err
				}
				return json.Marshal(targetResolveResponse{Actor: resolved})
			}); err != nil {
				return err
			}
		case ipc.KindEnd:
			if err := s.handleEnd(frame.Payload); err != nil {
				return err
			}
		case ipc.KindObs:
			var value ipc.ObsPayload
			if err := json.Unmarshal(frame.Payload, &value); err != nil {
				return err
			}
			if s.handlers.obs != nil {
				s.handlers.obs(s.id, s.key, actorrt.ObsKind(value.Kind), actorrt.ObsValue(value.Value))
			}
		case ipc.KindCancelRequest:
			var value ipc.CancelPayload
			if err := json.Unmarshal(frame.Payload, &value); err != nil {
				return err
			}
			if s.handlers.cancelRequest != nil {
				s.handlers.cancelRequest(s.id, value.RequestID)
			}
		case ipc.KindDeliverResult:
			var value ipc.DeliverResultPayload
			if err := json.Unmarshal(frame.Payload, &value); err != nil {
				return err
			}
			if s.handlers.deliverResult != nil {
				s.handlers.deliverResult(s.id, value.EnvelopeID, value.Outcome, value.Detail)
			}
		case ipc.KindDown:
			return errors.New("link: remote actor down")
		case ipc.KindDetach:
			return nil
		default:
			return fmt.Errorf("link: actor %s unknown frame kind %q", s.id, frame.Kind)
		}
	}
}

func (s *serverActorEndpoint) handleEmit(payload []byte) error {
	if s.handlers.emit == nil {
		return errors.New("link: emit handler unavailable")
	}
	var request ipc.EmitPayload
	if err := json.Unmarshal(payload, &request); err != nil {
		return err
	}
	result, callErr := s.handlers.emit(s.ctx, s.id, s.key, &request.Envelope)
	ack := ipc.EmitAckPayload{EmitResult: result}
	if callErr != nil {
		ack.ErrorCode, ack.ErrorMessage = ipc.EncodeError(callErr)
	}
	raw, err := json.Marshal(ack)
	if err != nil {
		return err
	}
	return s.send(ipc.Frame{Kind: ipc.KindEmitAck, Payload: raw})
}

var errRelayUnavailable = errors.New("link: relay handler unavailable")

func (s *serverActorEndpoint) handleRelay(
	ackKind ipc.Kind,
	payload []byte,
	call func(context.Context, []byte) ([]byte, error),
) error {
	result, callErr := call(s.ctx, payload)
	ack := ipc.RelayAckPayload{Payload: result}
	if callErr != nil {
		ack.ErrorCode, ack.ErrorMessage = ipc.EncodeError(callErr)
	}
	raw, err := json.Marshal(ack)
	if err != nil {
		return err
	}
	return s.send(ipc.Frame{Kind: ackKind, Payload: raw})
}

func (s *serverActorEndpoint) handleEnd(payload []byte) error {
	var request ipc.EndPayload
	if err := json.Unmarshal(payload, &request); err != nil {
		return err
	}
	ack := ipc.EndAckPayload{}
	if request.Target != "" && request.Target != s.id {
		ack.ErrorCode, ack.ErrorMessage = ipc.EncodeError(errors.New("link: lifecycle target must be self"))
	} else if s.handlers.endSelf == nil {
		ack.ErrorCode, ack.ErrorMessage = ipc.EncodeError(errors.New("link: end handler unavailable"))
	} else if err := s.handlers.endSelf(
		s.ctx,
		s.id,
		s.key,
		actorcaps.EndSelfRequest{Reason: request.Reason},
	); err != nil {
		ack.ErrorCode, ack.ErrorMessage = ipc.EncodeError(err)
	}
	raw, err := json.Marshal(ack)
	if err != nil {
		return err
	}
	return s.send(ipc.Frame{Kind: ipc.KindEndAck, Payload: raw})
}

var _ actorhost.ActorEndpoint = (*serverActorEndpoint)(nil)
