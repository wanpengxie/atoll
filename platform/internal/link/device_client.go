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
	// The reader is also the actor stream's physical supervisor. Start it before
	// the handshake write so every write-failure path can wake an existing
	// collector instead of synchronously waiting in Close.
	go readDeviceActorStream(actorStream, l.Logger)
	raw, err := json.Marshal(ipc.HandshakePayload{LeaseID: string(id), AttemptKey: string(key)})
	if err != nil {
		failActorStream(stream)
		return nil, err
	}
	if err := codec.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: raw}); err != nil {
		failActorStream(stream)
		return nil, err
	}
	resource := ActorStreamResource{
		Arms: RawActorArms{
			Pen: writer, Access: &remoteResourceHandle{
				relay: accessRelay,
				redeemer: &deviceFileRedeemer{control: l.Control, files: l.Files, dial: func(ctx context.Context) (io.ReadWriteCloser, error) {
					return l.Carrier.OpenExchange(ctx, l.Lane.Channel, l.Lane.Gen)
				}},
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
	SendCommitted(context.Context, string, string) (CommittedReply, error)
}

type deviceFileRedeemer struct {
	control DeviceLaneControl
	files   LocalFileOpener
	dial    func(context.Context) (io.ReadWriteCloser, error)
}

func (r *deviceFileRedeemer) redeemFileRoute(
	ctx context.Context,
	route accessdoor.FileRoute,
) (accessdoor.FileAccess, error) {
	if route.Redeem == accessdoor.FileRedeemRemote {
		if route.Dir || r.dial == nil {
			return accessdoor.FileAccess{}, errors.New("link: remote file route unavailable")
		}
		conn, err := r.dial(ctx)
		if err != nil {
			return accessdoor.FileAccess{}, err
		}
		if err := WriteExchangeControl(conn, ExchangeTicketHeader{Ticket: route.Token}); err != nil {
			_ = conn.Close()
			return accessdoor.FileAccess{}, err
		}
		switch route.Mode {
		case access.OpRead:
			return accessdoor.FileAccess{Remote: &accessdoor.RemoteFile{Read: &remoteExchangeReader{reader: NewExchangeReader(conn)}}}, nil
		case access.OpWrite:
			return accessdoor.FileAccess{Remote: &accessdoor.RemoteFile{Write: &remoteExchangeWriter{handle: NewExchangeWriteHandle(conn)}}}, nil
		default:
			_ = conn.Close()
			return accessdoor.FileAccess{}, fileRouteErr("unknown mode %q", route.Mode)
		}
	}
	if route.Redeem != accessdoor.FileRedeemLocal {
		return accessdoor.FileAccess{}, errors.New("link: unknown file redemption route")
	}
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
	if reply.Dir {
		root, err := r.files.OpenDir(reply.Coord)
		if err != nil {
			return accessdoor.FileAccess{}, err
		}
		return accessdoor.FileAccess{Local: &accessdoor.LocalFile{Dir: root}}, nil
	}
	switch reply.Mode {
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
				WriteHandle: handle, control: r.control, files: r.files,
				reservationID: reply.ReservationID, ticket: route.Token, coord: reply.Coord,
			}
		}
		return accessdoor.FileAccess{Local: &accessdoor.LocalFile{Write: handle}}, nil
	default:
		return accessdoor.FileAccess{}, fileRouteErr("unknown mode %q", reply.Mode)
	}
}

type remoteExchangeReader struct{ reader *ExchangeReader }

func (r *remoteExchangeReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	return n, mapExchangeError(err)
}
func (r *remoteExchangeReader) Close() error { return r.reader.Close() }

type remoteExchangeWriter struct{ handle *ExchangeWriteHandle }

func (w *remoteExchangeWriter) Write(p []byte) (int, error) { return w.handle.Write(p) }
func (w *remoteExchangeWriter) Commit() error               { return mapExchangeError(w.handle.Commit()) }
func (w *remoteExchangeWriter) Abort() error                { return w.handle.Abort() }

func mapExchangeError(err error) error {
	var terminal *ExchangeTerminalError
	if errors.As(err, &terminal) && terminal.Code == "host_offline" {
		return accessdoor.NewHostOfflineError(terminal.Detail)
	}
	return err
}

type deviceCommittingWrite struct {
	accessdoor.WriteHandle
	control       DeviceLaneControl
	files         LocalFileOpener
	reservationID string
	ticket        string
	coord         string
}

func (h *deviceCommittingWrite) Commit() error {
	if err := h.WriteHandle.Commit(); err != nil {
		return err
	}
	reply, err := h.control.SendCommitted(context.Background(), h.reservationID, h.ticket)
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
