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

// laneRPCTimeout is a test seam. Production always leaves it at the protocol
// budget.
var laneRPCTimeout = link.LaneRPCTimeout

type serverLane struct {
	carrier  *carrierRow
	stream   *link.LaneStream
	membrane membraneRow

	retire  sync.Once
	mu      sync.Mutex
	retired bool
	// storage is the device-opened storage sibling of this lane. Alloc and
	// reclaim ride it instead of the lane so that the device's storage
	// executor — the one reader that legitimately blocks in a filesystem
	// syscall — can freeze without freezing plan traffic. It shares this
	// lane's generation and has no lifecycle of its own: admitted only against
	// this exact lane, retired with it, and its death retires the lane.
	storage *link.LaneStream
	// workspaceRoot caches this lane's channel directory on the device, as the
	// device reported it. It is lane-scoped: constant while the lane lives and
	// gone with it, so it is not the stale copy LaneAttached refuses — that one
	// tracks compartment readiness, which varies under a live lane.
	workspaceRoot string
	pending       map[string]chan link.LaneFrame
	actors       map[uint64]func()
	nextActor    uint64
	exchanges    map[uint64]net.Conn
	nextExchange uint64
	exchangeWG   sync.WaitGroup
}

func newServerLane(carrier *carrierRow, stream *link.LaneStream, membrane membraneRow) *serverLane {
	return &serverLane{
		carrier: carrier, stream: stream, membrane: membrane,
		pending: make(map[string]chan link.LaneFrame), actors: make(map[uint64]func()),
		exchanges: make(map[uint64]net.Conn),
	}
}

// start consumes the physical ticket ensureLane took for this lane. The
// reader is what eventually closes this lane's actor streams and its stream,
// so the ticket is returned only once that has happened.
func (l *serverLane) start() {
	go func() {
		defer l.carrier.returnReader()
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
		case link.LaneFileReply:
			l.deliver(frame.RequestID, frame)
		default:
			// Alloc and reclaim replies belong on the storage sibling now; one
			// arriving here is a protocol violation like any other unknown kind.
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

func (l *serverLane) trackExchange(conn net.Conn) (func(), bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.retired || l.stream.Retired() {
		return nil, false
	}
	l.nextExchange++
	id := l.nextExchange
	l.exchanges[id] = conn
	l.exchangeWG.Add(1)
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			delete(l.exchanges, id)
			l.mu.Unlock()
			l.exchangeWG.Done()
		})
	}, true
}

func (l *serverLane) acceptExchange(conn net.Conn) {
	cleanup, ok := l.trackExchange(conn)
	if !ok {
		_ = conn.Close()
		return
	}
	defer cleanup()
	if l.carrier.host.dataPlane == nil {
		_ = conn.Close()
		return
	}
	l.carrier.host.dataPlane.ServeExchange(l.carrier.host.ctx, l.stream.Channel, conn)
}

func (l *serverLane) retireLogical() {
	if l == nil {
		return
	}
	l.retire.Do(func() {
		l.mu.Lock()
		l.retired = true
		pending := l.pending
		exchanges := l.exchanges
		l.pending = make(map[string]chan link.LaneFrame)
		l.exchanges = make(map[uint64]net.Conn)
		l.mu.Unlock()
		l.carrier.mu.Lock()
		l.carrier.retirements[l.stream.Channel]++
		l.carrier.mu.Unlock()
		for _, waiter := range pending {
			close(waiter)
		}
		for _, conn := range exchanges {
			_ = conn.Close()
		}
		l.exchangeWG.Wait()
		// Retiring the physical lane invokes markStreamRetired synchronously.
		// Do it only after children have joined, so that callback cannot wait
		// on work this same stack still needs to close.
		l.stream.RetireLogical()
	})
}

func (l *serverLane) markStreamRetired() {
	l.mu.Lock()
	l.retired = true
	storage := l.storage
	exchanges := l.exchanges
	pending := l.pending
	l.pending = make(map[string]chan link.LaneFrame)
	l.exchanges = make(map[uint64]net.Conn)
	l.mu.Unlock()
	// The pair shares one generation: half a pair routes storage into nowhere,
	// so the lane's retirement takes the storage sibling with it. Its reader
	// wakes on the retire and collects the physical end itself.
	if storage != nil {
		storage.RetireLogical()
	}
	for _, waiter := range pending {
		close(waiter)
	}
	for _, conn := range exchanges {
		_ = conn.Close()
	}
	l.exchangeWG.Wait()
}

// attachStorage installs the device-opened storage sibling. One per lane: a
// generation names exactly one pair, so a duplicate is refused, and a retired
// lane refuses too — its generation is spent.
func (l *serverLane) attachStorage(stream *link.LaneStream) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.retired || l.storage != nil {
		return false
	}
	l.storage = stream
	return true
}

// storageLoop reads the storage sibling. It only dispatches: every frame here
// is a reply to an RPC this host sent, so nothing in this loop blocks on
// anything but the wire. Exit retires the whole pair.
func (l *serverLane) storageLoop(stream *link.LaneStream) {
	defer stream.CollectPhysical()
	defer l.retireLogical()
	for {
		var frame link.LaneFrame
		if err := stream.Decode(&frame); err != nil {
			return
		}
		if err := frame.Validate(); err != nil {
			return
		}
		if !l.current() {
			return
		}
		switch frame.Kind {
		case link.LaneFileReply:
			l.deliver(frame.RequestID, frame)
		default:
			return
		}
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

func (l *serverLane) storageRoundTrip(ctx context.Context, frame link.LaneFrame) (link.LaneFrame, error) {
	if !l.current() {
		return link.LaneFrame{}, ErrLaneUnavailable
	}
	id := uuid.NewString()
	frame.RequestID = id
	if frame.FileRequest != nil {
		frame.FileRequest.RequestID = id
	}
	waiter := make(chan link.LaneFrame, 1)
	l.mu.Lock()
	if l.retired {
		l.mu.Unlock()
		return link.LaneFrame{}, ErrLaneUnavailable
	}
	storage := l.storage
	if storage == nil {
		l.mu.Unlock()
		// Half-open pair: the lane is admitted but the device has not opened
		// its storage sibling yet. Nothing was attempted, so the verdict is
		// the same as an unbuilt compartment — retry later, not a refusal.
		return link.LaneFrame{}, ErrLaneUnavailable
	}
	l.pending[id] = waiter
	l.mu.Unlock()
	if err := storage.Send(frame); err != nil {
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

// workspace answers where this lane's channel directory lives on the device,
// asking once and remembering the answer for the lane's life. Only the device
// knows $ATOLL_HOME, and the server needs the root to turn a device-local
// absolute path into the channel-relative one the access plane addresses by.
func (l *serverLane) workspace(ctx context.Context) (string, error) {
	l.mu.Lock()
	cached, retired := l.workspaceRoot, l.retired
	l.mu.Unlock()
	if retired {
		return "", ErrLaneUnavailable
	}
	if cached != "" {
		return cached, nil
	}
	reply, err := l.file(ctx, link.FileRoot, "")
	if err != nil {
		return "", err
	}
	if reply.Root == "" {
		return "", errors.New("daemonhost: device reported an empty channel workspace")
	}
	l.mu.Lock()
	l.workspaceRoot = reply.Root
	l.mu.Unlock()
	return reply.Root, nil
}

func (l *serverLane) file(ctx context.Context, op, path string) (link.FileReply, error) {
	reply, err := l.storageRoundTrip(ctx, link.LaneFrame{
		Kind: link.LaneFileRequest, FileRequest: &link.FileRequest{Op: op, Path: path},
	})
	if err != nil {
		return link.FileReply{}, err
	}
	if reply.FileReply == nil {
		return link.FileReply{}, errors.New("daemonhost: malformed file reply")
	}
	if !reply.FileReply.OK {
		return *reply.FileReply, errors.New(reply.FileReply.Reason)
	}
	return *reply.FileReply, nil
}
