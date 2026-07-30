package link

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

// ClientActorLane opens actor streams stamped with one exact lane generation.
type ClientActorLane struct {
	Carrier *ClientCarrier
	Lane    *LaneStream
	Host    *actorhost.HostSupervisor
	Control DeviceLaneControl
	Files   LocalFileOpener
	Logger  *slog.Logger
}

type actorStream struct {
	id          actor.ActorID
	stream      io.ReadWriteCloser
	codec       *ipc.Codec
	writer      *RemoteWriter
	access      *relayClient
	sched       *relayClient
	lifecycleV2 *remoteActorLifecycle
	dispatch    func(*message.Envelope) error
	cancel      func(message.ID)
	doneOnce    sync.Once
	done        chan struct{}
}

func (l *ClientActorLane) IsCurrent() bool {
	return l != nil && l.Carrier != nil && l.Lane != nil && !l.Lane.Retired()
}

func (l *ClientActorLane) Done() <-chan struct{} {
	if l == nil || l.Lane == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	return l.Lane.Done()
}

func (l *ClientActorLane) OpenActorStream(
	ctx context.Context,
	id actor.ActorID,
	key actorhost.AttemptKey,
) (*DeviceActorStream, error) {
	if !l.IsCurrent() || l.Host == nil || id == "" {
		return nil, ErrInvalidPhysicalChild
	}
	stream, err := l.Carrier.OpenActor(ctx, l.Lane.Channel, l.Lane.Gen)
	if err != nil {
		return nil, err
	}
	codec := ipc.NewCodec(stream, stream)
	raw, err := json.Marshal(ipc.HandshakePayload{LeaseID: string(id), AttemptKey: string(key)})
	if err != nil {
		_ = stream.Close()
		return nil, err
	}
	if err := codec.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: raw}); err != nil {
		_ = stream.Close()
		return nil, err
	}
	writer := NewRemoteWriter(codec)
	accessRelay := newRelayClient(codec, ipc.KindAccess)
	scheduleRelay := newRelayClient(codec, ipc.KindSchedule)
	lifecycle := newRemoteActorLifecycle(codec)
	actorStream := &actorStream{
		id: id, stream: stream, codec: codec, writer: writer,
		access: accessRelay, sched: scheduleRelay, lifecycleV2: lifecycle,
		dispatch: func(env *message.Envelope) error { return l.Host.Deliver(id, env) },
		cancel:   func(requestID message.ID) { l.Host.CancelRequest(id, requestID) },
		done:     make(chan struct{}),
	}
	go readDeviceActorStream(actorStream, l.Logger)
	resource := ActorStreamResource{
		Arms: RawActorArms{
			Pen: writer, Access: &remoteResourceHandle{
				relay:    accessRelay,
				redeemer: &deviceFileRedeemer{control: l.Control, files: l.Files},
			},
			State:    &remoteAccessHandle{relay: accessRelay, scope: accessScopeState},
			Schedule: &remoteScheduleHandle{relay: scheduleRelay}, Lifecycle: lifecycle,
		},
		Close: stream.Close, Done: actorStream.done,
		CancelRequest: writer.sendCancel, PublishObs: writer.publishObs,
	}
	return newDeviceActorStream(resource), nil
}

type DeviceLaneControl interface {
	ResolveCoord(context.Context, string) (ResolveCoordReply, error)
	SendCommitted(context.Context, string) (CommittedReply, error)
}

type deviceFileRedeemer struct {
	control DeviceLaneControl
	files   LocalFileOpener
}

func (r *deviceFileRedeemer) redeemFileRoute(
	ctx context.Context,
	route accessdoor.FileRoute,
) (accessdoor.FileAccess, error) {
	if r.control == nil || r.files == nil {
		return accessdoor.FileAccess{}, errors.New("link: file route unavailable")
	}
	reply, err := r.control.ResolveCoord(ctx, route.Token)
	if err != nil {
		return accessdoor.FileAccess{}, err
	}
	if !reply.OK {
		return accessdoor.FileAccess{}, fileRouteErr("resolve coord: %s", reply.Reason)
	}
	if route.Dir {
		root, err := r.files.OpenDir(reply.Coord)
		if err != nil {
			return accessdoor.FileAccess{}, err
		}
		return accessdoor.FileAccess{Local: &accessdoor.LocalFile{Dir: root}}, nil
	}
	switch route.Mode {
	case access.OpRead:
		handle, err := r.files.OpenRead(reply.Coord)
		if err != nil {
			return accessdoor.FileAccess{}, err
		}
		return accessdoor.FileAccess{Local: &accessdoor.LocalFile{Read: handle}}, nil
	case access.OpWrite:
		handle, err := r.files.OpenWrite(reply.Coord)
		if err != nil {
			return accessdoor.FileAccess{}, err
		}
		if reply.ReservationID != "" {
			handle = &deviceCommittingWrite{
				LocalWriteHandle: handle, control: r.control, files: r.files,
				reservationID: reply.ReservationID, coord: reply.Coord,
			}
		}
		return accessdoor.FileAccess{Local: &accessdoor.LocalFile{Write: handle}}, nil
	default:
		return accessdoor.FileAccess{}, fileRouteErr("unknown mode %q", route.Mode)
	}
}

type deviceCommittingWrite struct {
	accessdoor.LocalWriteHandle
	control       DeviceLaneControl
	files         LocalFileOpener
	reservationID string
	coord         string
}

func (h *deviceCommittingWrite) Commit() error {
	if err := h.LocalWriteHandle.Commit(); err != nil {
		return err
	}
	reply, err := h.control.SendCommitted(context.Background(), h.reservationID)
	if err != nil {
		return fmt.Errorf("link: committed outcome unknown: %w", err)
	}
	if reply.Reason != "" && !reply.Lost {
		return errors.New(reply.Reason)
	}
	if reply.Lost {
		_ = h.files.ReclaimCoord(h.coord)
		return errors.New("link: create reservation lost")
	}
	return nil
}

type DeviceActorStream struct {
	resource ActorStreamResource
	done     chan struct{}
	once     sync.Once
}

func newDeviceActorStream(resource ActorStreamResource) *DeviceActorStream {
	stream := &DeviceActorStream{resource: resource, done: make(chan struct{})}
	go func() {
		if resource.Done != nil {
			<-resource.Done
		}
		stream.once.Do(func() { close(stream.done) })
	}()
	return stream
}

func (s *DeviceActorStream) Arms() RawActorArms    { return s.resource.Arms }
func (s *DeviceActorStream) Done() <-chan struct{} { return s.done }
func (s *DeviceActorStream) Close() error {
	err := s.resource.Close()
	s.once.Do(func() { close(s.done) })
	return err
}
func (s *DeviceActorStream) SendCancelRequest(id message.ID) error {
	return s.resource.CancelRequest(id)
}
func (s *DeviceActorStream) PublishObs(kind string, value []byte) error {
	return s.resource.PublishObs(kind, value)
}

func readDeviceActorStream(stream *actorStream, logger *slog.Logger) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	defer func() {
		stream.writer.Close()
		stream.access.close()
		stream.sched.close()
		stream.lifecycleV2.close()
		stream.doneOnce.Do(func() { close(stream.done) })
		_ = stream.stream.Close()
	}()
	for {
		frame, err := stream.codec.Read()
		if err != nil {
			return
		}
		switch frame.Kind {
		case ipc.KindDeliver:
			var payload ipc.DeliverPayload
			if json.Unmarshal(frame.Payload, &payload) != nil ||
				stream.dispatch(&payload.Envelope) != nil {
				return
			}
		case ipc.KindEmitAck:
			var payload ipc.EmitAckPayload
			if json.Unmarshal(frame.Payload, &payload) != nil {
				return
			}
			stream.writer.DeliverAck(payload)
		case ipc.KindAccessAck:
			var payload ipc.RelayAckPayload
			if json.Unmarshal(frame.Payload, &payload) != nil {
				return
			}
			stream.access.deliverAck(payload)
		case ipc.KindScheduleAck:
			var payload ipc.RelayAckPayload
			if json.Unmarshal(frame.Payload, &payload) != nil {
				return
			}
			stream.sched.deliverAck(payload)
		case ipc.KindSpawnAck:
			var payload ipc.SpawnAckPayload
			if json.Unmarshal(frame.Payload, &payload) != nil {
				return
			}
			stream.lifecycleV2.fork.deliverAck(payload)
		case ipc.KindEndAck:
			var payload ipc.EndAckPayload
			if json.Unmarshal(frame.Payload, &payload) != nil {
				return
			}
			stream.lifecycleV2.end.deliverAck(payload)
		case ipc.KindCancel:
			var payload ipc.CancelPayload
			if json.Unmarshal(frame.Payload, &payload) != nil {
				return
			}
			stream.cancel(payload.RequestID)
		default:
			return
		}
	}
}
