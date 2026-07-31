package daemonhost

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/platform/internal/link"
)

var laneRPCTimeout = link.LaneRPCTimeout

type serverLane struct {
	carrier  *carrierRow
	stream   *link.LaneStream
	membrane membraneRow

	retire    sync.Once
	mu        sync.Mutex
	retired   bool
	pending   map[string]chan link.LaneFrame
	actors    map[uint64]func()
	nextActor uint64
}

func newServerLane(carrier *carrierRow, stream *link.LaneStream, membrane membraneRow) *serverLane {
	return &serverLane{
		carrier: carrier, stream: stream, membrane: membrane,
		pending: make(map[string]chan link.LaneFrame), actors: make(map[uint64]func()),
	}
}

// start takes the carrier's physical ticket for this lane. The reader is what
// eventually closes this lane's actor streams and its stream, so the ticket is
// returned only once that has happened — taking it as a parameter keeps the
// pairing at a single call site instead of two bookkeeping halves.
func (l *serverLane) start(physical *sync.WaitGroup) {
	go func() {
		defer physical.Done()
		l.readLoop()
	}()
}

func (l *serverLane) current() bool {
	if l == nil || l.stream.Retired() || l.carrier.sealed.Load() {
		return false
	}
	l.carrier.mu.Lock()
	defer l.carrier.mu.Unlock()
	return l.carrier.lanes[l.stream.Channel] == l
}

func (l *serverLane) readLoop() {
	defer l.collectPhysical()
	defer l.retireLogical()
	for {
		var frame link.LaneFrame
		if err := l.stream.Decode(&frame); err != nil {
			return
		}
		if err := frame.Validate(); err != nil {
			return
		}
		if !l.current() {
			return
		}
		switch frame.Kind {
		case link.LanePlanPull:
			actors, err := l.membrane.bundle.Plan(l.carrier.host.ctx, l.carrier.daemonID)
			if l.stream.Send(link.PlanLaneReply(frame.RequestID, actors, err)) != nil {
				return
			}
		case link.LaneCommitted:
			request := frame.Committed
			if request == nil || l.membrane.bundle.Storage == nil {
				return
			}
			found, lost, err := l.membrane.bundle.Storage.Committed(
				l.carrier.host.ctx, l.carrier.daemonID, request.ReservationID)
			reply := &link.CommittedReply{RequestID: request.RequestID, Found: found, Lost: lost}
			if err != nil {
				reply.Reason = err.Error()
			}
			if l.stream.Send(link.LaneFrame{
				Kind: link.LaneCommittedReply, RequestID: request.RequestID, CommittedReply: reply,
			}) != nil {
				return
			}
		case link.LaneReclaimAck:
			request := frame.ReclaimAck
			if request == nil || l.membrane.bundle.Storage == nil {
				return
			}
			found, err := l.membrane.bundle.Storage.ReclaimAck(
				l.carrier.host.ctx, l.carrier.daemonID, request.TombstoneID)
			reply := &link.ReclaimAckReply{RequestID: request.RequestID, Found: found}
			if err != nil {
				reply.Reason = err.Error()
			}
			if l.stream.Send(link.LaneFrame{
				Kind: link.LaneReclaimAckReply, RequestID: request.RequestID, ReclaimAckReply: reply,
			}) != nil {
				return
			}
		case link.LaneReconcilePull:
			request := frame.ReconcilePull
			if request == nil || l.membrane.bundle.Storage == nil {
				return
			}
			resources, reservations, tombstones, err := l.membrane.bundle.Storage.ReconcilePull(
				l.carrier.host.ctx, l.carrier.daemonID, request.ActiveCoords)
			reply := &link.ReconcilePullReply{RequestID: request.RequestID}
			for _, row := range resources {
				reply.Resources = append(reply.Resources, link.ReconcileResource{Coord: row.Coord})
			}
			for _, row := range reservations {
				reply.PendingReservations = append(reply.PendingReservations, link.ReconcileReservation{
					ReservationID: row.ReservationID, Coord: row.Coord,
				})
			}
			for _, row := range tombstones {
				reply.PendingTombstones = append(reply.PendingTombstones, link.ReconcileTombstone{
					TombstoneID: row.TombstoneID, Coord: row.Coord,
				})
			}
			if err != nil {
				reply.Reason = err.Error()
			}
			if l.stream.Send(link.LaneFrame{
				Kind: link.LaneReconcilePullReply, RequestID: request.RequestID, ReconcilePullReply: reply,
			}) != nil {
				return
			}
		case link.LaneResolveCoord:
			request := frame.ResolveCoord
			if request == nil {
				return
			}
			ticket, ok := l.carrier.host.resolveTransfer(
				l.carrier.daemonID, string(l.stream.Channel), request.Token)
			reply := &link.ResolveCoordReply{
				RequestID: request.RequestID, OK: ok,
			}
			if ok {
				reply.Coord, reply.Mode = ticket.coord, ticket.mode
				reply.ReservationID = ticket.reservationID
			} else {
				reply.Reason = "unknown or unauthorized transfer token"
			}
			if l.stream.Send(link.LaneFrame{
				Kind: link.LaneResolveCoordReply, RequestID: request.RequestID,
				ResolveCoordReply: reply,
			}) != nil {
				return
			}
		case link.LaneAllocReply, link.LaneReclaimReply, link.LaneResolveCoordReply:
			l.deliver(frame.RequestID, frame)
		default:
			return
		}
	}
}

func (l *serverLane) acceptActor(conn net.Conn) {
	closeFn, done, err := link.ServeLaneActor(
		l.carrier.host.ctx, conn, l.carrier.daemonID, l.membrane.bundle,
		l.current, l.carrier.host.logger,
	)
	if err != nil {
		return
	}
	if !l.current() {
		closeFn()
		return
	}
	l.mu.Lock()
	if l.stream.Retired() {
		l.mu.Unlock()
		closeFn()
		return
	}
	l.nextActor++
	id := l.nextActor
	l.actors[id] = closeFn
	l.mu.Unlock()
	go func() {
		<-done
		l.mu.Lock()
		delete(l.actors, id)
		l.mu.Unlock()
	}()
}

func (l *serverLane) retireLogical() {
	if l == nil {
		return
	}
	l.retire.Do(func() {
		l.mu.Lock()
		l.retired = true
		pending := l.pending
		l.pending = make(map[string]chan link.LaneFrame)
		l.mu.Unlock()
		l.carrier.mu.Lock()
		l.carrier.retirements[l.stream.Channel]++
		l.carrier.mu.Unlock()
		l.stream.RetireLogical()
		for _, waiter := range pending {
			close(waiter)
		}
	})
}

func (l *serverLane) markStreamRetired() {
	l.mu.Lock()
	l.retired = true
	pending := l.pending
	l.pending = make(map[string]chan link.LaneFrame)
	l.mu.Unlock()
	for _, waiter := range pending {
		close(waiter)
	}
}

func (l *serverLane) collectPhysical() {
	l.mu.Lock()
	actors := make([]func(), 0, len(l.actors))
	for _, closeFn := range l.actors {
		actors = append(actors, closeFn)
	}
	l.actors = make(map[uint64]func())
	l.mu.Unlock()
	for _, closeFn := range actors {
		closeFn()
	}
	l.stream.CollectPhysical()
}

func (l *serverLane) deliver(id string, frame link.LaneFrame) {
	l.mu.Lock()
	if l.retired {
		l.mu.Unlock()
		return
	}
	waiter := l.pending[id]
	delete(l.pending, id)
	l.mu.Unlock()
	if waiter != nil {
		waiter <- frame
	}
}

func (l *serverLane) roundTrip(ctx context.Context, frame link.LaneFrame) (link.LaneFrame, error) {
	if !l.current() {
		return link.LaneFrame{}, ErrLaneUnavailable
	}
	id := uuid.NewString()
	frame.RequestID = id
	switch {
	case frame.AllocRequest != nil:
		frame.AllocRequest.RequestID = id
	case frame.ReclaimRequest != nil:
		frame.ReclaimRequest.RequestID = id
	}
	waiter := make(chan link.LaneFrame, 1)
	l.mu.Lock()
	if l.retired {
		l.mu.Unlock()
		return link.LaneFrame{}, ErrLaneUnavailable
	}
	l.pending[id] = waiter
	l.mu.Unlock()
	if err := l.stream.Send(frame); err != nil {
		l.mu.Lock()
		delete(l.pending, id)
		l.mu.Unlock()
		return link.LaneFrame{}, err
	}
	timer := time.NewTimer(laneRPCTimeout)
	defer timer.Stop()
	select {
	case reply, ok := <-waiter:
		if !ok {
			return link.LaneFrame{}, ErrLaneUnavailable
		}
		return reply, nil
	case <-ctx.Done():
		l.mu.Lock()
		delete(l.pending, id)
		l.mu.Unlock()
		return link.LaneFrame{}, ctx.Err()
	case <-timer.C:
		l.mu.Lock()
		delete(l.pending, id)
		l.mu.Unlock()
		return link.LaneFrame{}, fmt.Errorf("daemonhost: lane RPC %s: %w", id, link.ErrLaneRPCTimeout)
	}
}

func (l *serverLane) alloc(ctx context.Context, coord string, dir bool) error {
	reply, err := l.roundTrip(ctx, link.LaneFrame{
		Kind:         link.LaneAllocRequest,
		AllocRequest: &link.AllocRequest{Coord: coord, Dir: dir},
	})
	if err != nil {
		return err
	}
	if reply.AllocReply == nil || !reply.AllocReply.OK {
		if reply.AllocReply != nil {
			return errors.New(reply.AllocReply.Reason)
		}
		return errors.New("daemonhost: malformed alloc reply")
	}
	return nil
}

func (l *serverLane) reclaim(ctx context.Context, coord string) error {
	reply, err := l.roundTrip(ctx, link.LaneFrame{
		Kind: link.LaneReclaimRequest, ReclaimRequest: &link.ReclaimRequest{Coord: coord},
	})
	if err != nil {
		return err
	}
	if reply.ReclaimReply == nil || !reply.ReclaimReply.OK {
		if reply.ReclaimReply != nil {
			return errors.New(reply.ReclaimReply.Reason)
		}
		return errors.New("daemonhost: malformed reclaim reply")
	}
	return nil
}
