package link

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/ipc"
	"github.com/wanpengxie/atoll/runtime/remoteingress"
)

const serverStreamQueue = 64

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
	fork          func(context.Context, actor.ActorID, actorhost.AttemptKey, remoteingress.ForkRequest) (actor.ActorID, error)
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
	conn     io.ReadWriteCloser
	codec    *ipc.Codec
	handlers serverActorHandlers

	ctx    context.Context
	cancel context.CancelFunc
	sendq  chan ipc.Frame

	closeOnce sync.Once
	done      chan struct{}
}

func newServerActorEndpoint(
	parent context.Context,
	id actor.ActorID,
	key actorhost.AttemptKey,
	conn io.ReadWriteCloser,
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
		sendq: make(chan ipc.Frame, serverStreamQueue), done: make(chan struct{}),
	}
}

func (s *serverActorEndpoint) Run(context.Context) error {
	errs := make(chan error, 2)
	go func() { errs <- s.writeLoop() }()
	go func() { errs <- s.readLoop() }()
	err := <-errs
	_ = s.Close()
	<-errs
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
	select {
	case <-s.ctx.Done():
		return actorrt.ErrUnitStopped
	default:
	}
	select {
	case s.sendq <- ipc.Frame{Kind: ipc.KindDeliver, Payload: raw}:
		return nil
	default:
		return actorrt.ErrMailboxFull
	}
}

func (s *serverActorEndpoint) CancelRequest(id message.ID) {
	if s == nil {
		return
	}
	raw, err := json.Marshal(ipc.CancelPayload{RequestID: id})
	if err != nil {
		return
	}
	select {
	case s.sendq <- ipc.Frame{Kind: ipc.KindCancel, Payload: raw}:
	default:
	}
}

func (s *serverActorEndpoint) writeLoop() error {
	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case frame := <-s.sendq:
			if err := s.codec.Write(frame); err != nil {
				return err
			}
		}
	}
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
		case ipc.KindSpawn:
			if err := s.handleFork(frame.Payload); err != nil {
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
	return s.codec.Write(ipc.Frame{Kind: ipc.KindEmitAck, Payload: raw})
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
	return s.codec.Write(ipc.Frame{Kind: ackKind, Payload: raw})
}

func (s *serverActorEndpoint) handleFork(payload []byte) error {
	var request ipc.SpawnPayload
	if err := json.Unmarshal(payload, &request); err != nil {
		return err
	}
	ack := ipc.SpawnAckPayload{}
	if s.handlers.fork == nil {
		ack.ErrorCode, ack.ErrorMessage = ipc.EncodeError(errors.New("link: fork handler unavailable"))
	} else {
		var placement *channel.Placement
		if request.PlacementKind != "" || request.PlacementHost != "" {
			placement = &channel.Placement{
				Kind:        channel.PlacementKind(request.PlacementKind),
				DesiredHost: request.PlacementHost,
			}
		}
		child, err := s.handlers.fork(s.ctx, s.id, s.key, remoteingress.ForkRequest{
			RequestID: request.RequestID,
			Spec: actorcaps.ForkSpec{
				Kind: request.Kind, Class: request.Class, NameHint: request.NameHint,
				Config: append([]byte(nil), request.Config...), Placement: placement,
			},
		})
		if err != nil {
			ack.ErrorCode, ack.ErrorMessage = ipc.EncodeError(err)
		} else {
			ack.ChildID = child
		}
	}
	raw, err := json.Marshal(ack)
	if err != nil {
		return err
	}
	return s.codec.Write(ipc.Frame{Kind: ipc.KindSpawnAck, Payload: raw})
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
	return s.codec.Write(ipc.Frame{Kind: ipc.KindEndAck, Payload: raw})
}

var _ actorhost.ActorEndpoint = (*serverActorEndpoint)(nil)
