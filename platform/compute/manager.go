package compute

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorhost"
)

var laneRPCTimeout = link.LaneRPCTimeout

type compartmentManager struct {
	ctx    context.Context
	cfg    Config
	logger *slog.Logger

	mu         sync.Mutex
	closed     bool
	carrier    *link.ClientCarrier
	carrierGen link.CarrierGeneration
	daemonID   string
	root       string
	cells      map[string]*compartment
	terminal   chan error
	wg         sync.WaitGroup
}

type compartment struct {
	manager *compartmentManager
	chID    string

	mu           sync.Mutex
	state        string
	reason       string
	closing      bool
	closeStarted bool
	condemned    bool
	lane         *clientLane
	pending      *clientLane
	resources    CompartmentResources
	host         *actorhost.HostSupervisor
	outbound     *DaemonOutbound
	storage      *storageHostForwarder
	runtimeCtx   context.Context
	cancel       context.CancelFunc
	storageDone  chan struct{}
	buildDone    chan struct{}
	stopBuild    chan struct{}
	stopOnce     sync.Once
}

func newCompartmentManager(ctx context.Context, cfg Config, logger *slog.Logger) *compartmentManager {
	return &compartmentManager{
		ctx: ctx, cfg: cfg, logger: logger, cells: make(map[string]*compartment),
		terminal: make(chan error, 1),
	}
}

func (m *compartmentManager) bindCarrier(
	carrier *link.ClientCarrier,
	accepted link.SpineFrame,
	root string,
) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		_ = carrier.Close()
		return
	}
	m.carrier = carrier
	m.carrierGen = accepted.CarrierGen
	m.daemonID = accepted.DaemonID
	m.root = root
	cells := make([]*compartment, 0, len(m.cells))
	for _, cell := range m.cells {
		cells = append(cells, cell)
	}
	m.wg.Add(2)
	m.mu.Unlock()
	for _, cell := range cells {
		cell.redeclare(carrier)
	}
	go func() {
		defer m.wg.Done()
		m.readSpine(carrier)
	}()
	go func() {
		defer m.wg.Done()
		m.acceptLanes(carrier)
	}()
}

func (m *compartmentManager) readSpine(carrier *link.ClientCarrier) {
	for {
		var frame link.SpineFrame
		if err := carrier.ReadSpine(&frame); err != nil {
			_ = carrier.Close()
			return
		}
		if err := frame.Validate(); err != nil {
			_ = carrier.Close()
			return
		}
		switch frame.Kind {
		case link.SpineCompartmentClose:
			m.closeCompartment(string(frame.Channel))
		case link.SpineProbe:
			if carrier.SendSpine(link.SpineFrame{Kind: link.SpineProbeReply, Nonce: frame.Nonce}) != nil {
				return
			}
		case link.SpineCarrierReject:
			if frame.Class == link.CarrierTerminal {
				m.mu.Lock()
				current := m.carrier == carrier
				m.mu.Unlock()
				if current {
					select {
					case m.terminal <- terminalCarrierError{
						err: errors.New("compute: carrier rejected: " + frame.Reason),
					}:
					default:
					}
				}
			}
			_ = carrier.Close()
			return
		default:
			_ = carrier.Close()
			return
		}
	}
}

func (m *compartmentManager) acceptLanes(carrier *link.ClientCarrier) {
	_ = carrier.ServeStreams(func(conn net.Conn, header link.DeviceStreamHeader) {
		if header.Kind == link.DeviceStreamCarrier {
			_ = conn.Close()
			_ = carrier.Close()
			return
		}
		laneStream, err := link.AdoptLane(carrier, header, conn)
		if err != nil {
			return
		}
		lane := newClientLane(m, carrier, laneStream)
		m.acceptLane(lane)
	})
}

func (m *compartmentManager) acceptLane(lane *clientLane) {
	chID := string(lane.stream.Channel)
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		lane.stream.RetireLogical()
		lane.stream.CollectPhysical()
		return
	}
	cell := m.cells[chID]
	if cell == nil {
		cell = &compartment{
			manager: m, chID: chID, state: "building", stopBuild: make(chan struct{}),
		}
		m.cells[chID] = cell
	}
	lane.setRetire(func(exact *clientLane) { cell.laneDown(exact) })
	lane.mu.Lock()
	alreadyRetired := lane.retired
	lane.mu.Unlock()
	if alreadyRetired || lane.stream.Retired() {
		lane.start()
		m.mu.Unlock()
		lane.retireLogical()
		return
	}
	cell.mu.Lock()
	if cell.condemned {
		cell.mu.Unlock()
		lane.start()
		m.mu.Unlock()
		lane.retireLogical()
		cell.declare("fault", "condemned")
		return
	}
	if cell.closing {
		old := cell.pending
		cell.pending = lane
		cell.mu.Unlock()
		lane.start()
		m.mu.Unlock()
		if old != nil {
			old.retireLogical()
		}
		return
	}
	old := cell.lane
	cell.lane = lane
	needsBuild := cell.host == nil
	startBuild := needsBuild && cell.buildDone == nil
	if startBuild {
		cell.buildDone = make(chan struct{})
	}
	if !needsBuild {
		m.wg.Add(1)
	}
	cell.mu.Unlock()
	lane.start()
	m.mu.Unlock()
	if old != nil {
		old.retireLogical()
	}
	if startBuild {
		cell.declare("building", "")
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			lane.retireLogical()
			return
		}
		m.mu.Unlock()
		go func() {
			defer func() {
				cell.mu.Lock()
				if cell.buildDone != nil {
					close(cell.buildDone)
					cell.buildDone = nil
				}
				cell.mu.Unlock()
			}()
			cell.buildLoop()
		}()
		return
	}
	if needsBuild {
		return
	}
	go func() {
		defer m.wg.Done()
		cell.bindLane(lane)
	}()
}

func (m *compartmentManager) currentCarrier() (*link.ClientCarrier, link.CarrierGeneration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.carrier, m.carrierGen
}

func (m *compartmentManager) carrierDown(exact *link.ClientCarrier) {
	m.mu.Lock()
	if m.carrier != exact {
		m.mu.Unlock()
		return
	}
	m.carrier = nil
	m.carrierGen = ""
	cells := make([]*compartment, 0, len(m.cells))
	for _, cell := range m.cells {
		cells = append(cells, cell)
	}
	m.mu.Unlock()
	lanes := make([]*clientLane, 0, len(cells)*2)
	for _, cell := range cells {
		cell.mu.Lock()
		lane := cell.lane
		cell.lane = nil
		pending := cell.pending
		cell.pending = nil
		cell.mu.Unlock()
		if lane != nil {
			lanes = append(lanes, lane)
			lane.retireLogical()
		}
		if pending != nil {
			lanes = append(lanes, pending)
			pending.retireLogical()
		}
	}
	_ = exact.Close()
	for _, lane := range lanes {
		<-lane.stream.PhysicalDone()
	}
}

func (m *compartmentManager) closeCompartment(chID string) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	cell := m.cells[chID]
	m.mu.Unlock()
	if cell == nil {
		if carrier, _ := m.currentCarrier(); carrier != nil {
			_ = carrier.SendSpine(link.SpineFrame{
				Kind: link.SpineCompartmentState, Channel: linkChannel(chID), State: "gone",
			})
		}
		return
	}
	cell.mu.Lock()
	if cell.closing {
		pending := cell.pending
		cell.pending = nil
		cell.mu.Unlock()
		if pending != nil {
			pending.retireLogical()
		}
		return
	}
	cell.closing = true
	pending := cell.pending
	cell.pending = nil
	cell.mu.Unlock()
	cell.stopBuilding()
	if pending != nil {
		pending.retireLogical()
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	cell.mu.Lock()
	if cell.closeStarted {
		cell.mu.Unlock()
		m.mu.Unlock()
		return
	}
	cell.closeStarted = true
	cell.mu.Unlock()
	m.wg.Add(1)
	m.mu.Unlock()
	go func() {
		defer m.wg.Done()
		cell.close()
	}()
}

func (m *compartmentManager) close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		m.wg.Wait()
		return
	}
	m.closed = true
	carrier := m.carrier
	cells := make([]*compartment, 0, len(m.cells))
	for _, cell := range m.cells {
		cells = append(cells, cell)
	}
	m.mu.Unlock()
	if carrier != nil {
		_ = carrier.Close()
	}
	for _, cell := range cells {
		cell.mu.Lock()
		cell.closing = true
		startClose := !cell.closeStarted
		cell.closeStarted = true
		cell.mu.Unlock()
		cell.stopBuilding()
		if startClose {
			cell.close()
		}
	}
	m.wg.Wait()
}

func (c *compartment) buildLoop() {
	backoff := time.Second
	for {
		c.mu.Lock()
		if c.closing || c.condemned || c.host != nil {
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()
		if err := c.build(); err == nil {
			return
		} else {
			c.mu.Lock()
			stopping := c.closing || c.condemned
			c.mu.Unlock()
			if stopping {
				return
			}
			c.declare("fault", err.Error())
		}
		timer := time.NewTimer(jitterBackoff(backoff))
		select {
		case <-c.manager.ctx.Done():
			timer.Stop()
			return
		case <-c.stopBuild:
			timer.Stop()
			return
		case <-timer.C:
		}
		if backoff < time.Minute {
			backoff *= 2
			if backoff > time.Minute {
				backoff = time.Minute
			}
		}
		c.declare("building", "")
	}
}

func (c *compartment) stopBuilding() {
	if c.stopBuild == nil {
		return
	}
	c.stopOnce.Do(func() { close(c.stopBuild) })
}

func (c *compartment) build() (retErr error) {
	c.manager.mu.Lock()
	root := c.manager.root
	daemonID := c.manager.daemonID
	c.manager.mu.Unlock()
	workspaceRoot := filepath.Join(root, "workspace")
	if err := ensureDirectory(workspaceRoot); err != nil {
		return err
	}
	workspace, err := coordinatePath(workspaceRoot, c.chID)
	if err != nil {
		return err
	}
	if err := ensureDirectory(workspace); err != nil {
		return err
	}
	resources, err := c.manager.cfg.BuildCompartment(c.chID, workspace)
	if err != nil {
		return err
	}
	if resources.Factories == nil {
		var closeErr error
		if resources.Close != nil {
			closeErr = resources.Close()
		}
		if closeErr != nil {
			c.condemn("compartment resource rollback failed: " + closeErr.Error())
			return errors.Join(errors.New("compute: compartment factories required"), closeErr)
		}
		return errors.New("compute: compartment factories required")
	}
	runtimeCtx, cancel := context.WithCancel(context.Background())
	outbound := NewDaemonOutbound(DaemonOutboundConfig{Parent: runtimeCtx})
	host, err := actorhost.New(actorhost.Config{
		Parent: runtimeCtx, Domain: actorhost.ExecutionDomain(daemonID),
		Logger:      c.manager.logger.With("channel", c.chID),
		Events:      &daemonHostEvents{outbound: outbound},
		BodyBuilder: daemonBodyBuilder(outbound, resources.Factories, c.manager.logger),
	})
	if err != nil {
		rollbackErr := rollbackCompartment(nil, outbound, cancel, resources)
		if rollbackErr != nil {
			c.condemn("compartment rollback failed: " + rollbackErr.Error())
		}
		return errors.Join(err, rollbackErr)
	}
	storage := newStorageHostForwarder(
		resources.StorageHost, c.manager.logger, c.manager.cfg.ScrubberInterval)
	c.mu.Lock()
	if c.closing || c.condemned {
		c.mu.Unlock()
		rollbackErr := rollbackCompartment(host, outbound, cancel, resources)
		if rollbackErr != nil {
			c.condemn("compartment rollback failed: " + rollbackErr.Error())
		}
		return errors.Join(errors.New("compute: compartment closed during build"), rollbackErr)
	}
	c.resources, c.runtimeCtx, c.cancel = resources, runtimeCtx, cancel
	c.host, c.outbound, c.storage = host, outbound, storage
	lane := c.lane
	c.state, c.reason = "ready", ""
	storageDone := make(chan struct{})
	c.storageDone = storageDone
	c.mu.Unlock()
	go func() {
		defer close(storageDone)
		storage.pump(runtimeCtx)
	}()
	c.declare("ready", "")
	if lane != nil {
		c.bindLane(lane)
	}
	return nil
}

func ensureDirectory(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("compute: %q is not a real directory", path)
	}
	return nil
}

func rollbackCompartment(
	host *actorhost.HostSupervisor,
	outbound *DaemonOutbound,
	cancel context.CancelFunc,
	resources CompartmentResources,
) error {
	ctx, stop := context.WithTimeout(context.Background(), compartmentJoinTimeout)
	defer stop()
	var rollbackErr error
	if outbound != nil {
		rollbackErr = errors.Join(rollbackErr, outbound.Seal(ctx))
	}
	if host != nil {
		rollbackErr = errors.Join(rollbackErr, host.Close(ctx))
	}
	if outbound != nil {
		rollbackErr = errors.Join(rollbackErr, outbound.CloseResidual())
	}
	if cancel != nil {
		cancel()
	}
	if resources.Close != nil {
		rollbackErr = errors.Join(rollbackErr, resources.Close())
	}
	return rollbackErr
}

func (c *compartment) condemn(reason string) {
	c.mu.Lock()
	c.condemned = true
	lane := c.lane
	c.lane = nil
	pending := c.pending
	c.pending = nil
	c.mu.Unlock()
	if lane != nil {
		lane.retireLogical()
	}
	if pending != nil {
		pending.retireLogical()
	}
	c.declare("fault", "condemned: "+reason)
}

func (c *compartment) bindLane(lane *clientLane) {
	c.mu.Lock()
	if c.closing || c.condemned || c.lane != lane || c.host == nil {
		c.mu.Unlock()
		return
	}
	host, outbound, storage := c.host, c.outbound, c.storage
	resources := c.resources
	c.mu.Unlock()
	lane.setHost(host)
	lane.mu.Lock()
	lane.local = resources.LocalFileOpener
	lane.storage = resources.StorageHost
	lane.outbound = outbound
	lane.mu.Unlock()
	desired, err := lane.pullPlan(c.manager.ctx)
	if err != nil {
		lane.retireLogical()
		return
	}
	c.mu.Lock()
	if c.closing || c.condemned || c.lane != lane || c.host != host {
		c.mu.Unlock()
		return
	}
	err = host.AcceptFullDesired(desired)
	c.mu.Unlock()
	if err != nil {
		lane.retireLogical()
		return
	}
	if err := outbound.SetLane(lane.actorSession); err != nil {
		lane.retireLogical()
		return
	}
	storage.Rebind(lane)
	host.Wake()
	outbound.Wake()
}

func (c *compartment) laneDown(exact *clientLane) {
	c.mu.Lock()
	if c.pending == exact {
		c.pending = nil
		c.mu.Unlock()
		return
	}
	if c.lane != exact {
		c.mu.Unlock()
		return
	}
	c.lane = nil
	c.mu.Unlock()
}

func (c *compartment) declare(state, reason string) {
	carrier, _ := c.manager.currentCarrier()
	c.mu.Lock()
	c.state, c.reason = state, reason
	if carrier != nil {
		_ = carrier.SendSpine(link.SpineFrame{
			Kind: link.SpineCompartmentState, Channel: linkChannel(c.chID),
			State: state, Reason: reason,
		})
	}
	c.mu.Unlock()
}

func (c *compartment) redeclare(carrier *link.ClientCarrier) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing || c.condemned {
		return
	}
	_ = carrier.SendSpine(link.SpineFrame{
		Kind: link.SpineCompartmentState, Channel: linkChannel(c.chID),
		State: c.state, Reason: c.reason,
	})
}

func (c *compartment) close() {
	ctx, stop := context.WithTimeout(context.Background(), compartmentJoinTimeout)
	defer stop()
	c.mu.Lock()
	buildDone := c.buildDone
	c.mu.Unlock()
	if buildDone != nil {
		select {
		case <-buildDone:
		case <-ctx.Done():
			c.condemn("compartment build join timeout")
			return
		}
	}
	c.mu.Lock()
	outbound, host, cancel := c.outbound, c.host, c.cancel
	resources := c.resources
	storageDone := c.storageDone
	lane := c.lane
	c.lane = nil
	c.mu.Unlock()
	var closeErr error
	if outbound != nil {
		closeErr = errors.Join(closeErr, outbound.Seal(ctx))
	}
	if host != nil {
		closeErr = errors.Join(closeErr, host.Close(ctx))
	}
	if outbound != nil {
		closeErr = errors.Join(closeErr, outbound.CloseResidual())
	}
	if cancel != nil {
		cancel()
	}
	if storageDone != nil {
		select {
		case <-storageDone:
		case <-ctx.Done():
			closeErr = errors.Join(closeErr, errors.New("compute: storage pump join timeout"))
		}
	}
	if resources.Close != nil {
		closeErr = errors.Join(closeErr, resources.Close())
	}
	if lane != nil {
		lane.retireLogical()
	}
	if closeErr != nil {
		c.condemn(closeErr.Error())
		return
	}
	carrier, _ := c.manager.currentCarrier()
	if carrier != nil {
		_ = carrier.SendSpine(link.SpineFrame{
			Kind: link.SpineCompartmentState, Channel: linkChannel(c.chID), State: "gone",
		})
	}
	c.manager.mu.Lock()
	if c.manager.cells[c.chID] == c {
		delete(c.manager.cells, c.chID)
	}
	c.manager.mu.Unlock()
	c.mu.Lock()
	pending := c.pending
	c.pending = nil
	c.mu.Unlock()
	if pending != nil && !pending.stream.Retired() {
		c.manager.acceptLane(pending)
	}
}

func linkChannel(value string) channel.ID { return channel.ID(value) }

type clientLane struct {
	manager *compartmentManager
	carrier *link.ClientCarrier
	stream  *link.LaneStream

	mu           sync.Mutex
	startOnce    sync.Once
	retire       sync.Once
	retired      bool
	onRetire     func(*clientLane)
	pending      map[string]chan link.LaneFrame
	replan       chan struct{}
	host         *actorhost.HostSupervisor
	actorSession *laneSession
	local        LocalFileOpener
	storage      StorageHost
	outbound     *DaemonOutbound
}

type laneSession struct{ lane *clientLane }

func (s *laneSession) IsCurrent() bool       { return s != nil && s.lane.current() }
func (s *laneSession) Done() <-chan struct{} { return s.lane.stream.Done() }
func (s *laneSession) OpenActorStream(
	ctx context.Context, id actor.ActorID, key actorhost.AttemptKey,
) (laneActorStream, error) {
	s.lane.mu.Lock()
	host := s.lane.host
	s.lane.mu.Unlock()
	client := &link.ClientActorLane{
		Carrier: s.lane.carrier, Lane: s.lane.stream, Host: host,
		Control: s.lane, Files: s.lane.local, Logger: s.lane.manager.logger,
	}
	stream, err := client.OpenActorStream(ctx, id, key)
	if err != nil {
		// Converting a nil *DeviceActorStream directly to laneActorStream would
		// create a non-nil interface and make the retry cleanup call Close on a
		// nil receiver.
		return nil, err
	}
	return stream, nil
}

func newClientLane(
	manager *compartmentManager,
	carrier *link.ClientCarrier,
	stream *link.LaneStream,
) *clientLane {
	lane := &clientLane{
		manager: manager, carrier: carrier, stream: stream,
		pending: make(map[string]chan link.LaneFrame), replan: make(chan struct{}, 1),
	}
	stream.SetRetire(func(*link.LaneStream) { lane.markStreamRetired() })
	lane.actorSession = &laneSession{lane: lane}
	return lane
}

func (l *clientLane) setHost(host *actorhost.HostSupervisor) {
	l.mu.Lock()
	l.host = host
	l.mu.Unlock()
}
func (l *clientLane) setRetire(fn func(*clientLane)) {
	l.mu.Lock()
	l.onRetire = fn
	retired := l.retired
	l.mu.Unlock()
	if retired && fn != nil {
		fn(l)
	}
}
func (l *clientLane) current() bool {
	if l == nil || l.stream.Retired() {
		return false
	}
	l.manager.mu.Lock()
	current := l.manager.carrier == l.carrier
	cell := l.manager.cells[string(l.stream.Channel)]
	l.manager.mu.Unlock()
	if !current || cell == nil {
		return false
	}
	cell.mu.Lock()
	defer cell.mu.Unlock()
	return cell.lane == l
}
func (l *clientLane) start() {
	l.startOnce.Do(func() {
		l.manager.wg.Add(2)
		go func() {
			defer l.manager.wg.Done()
			l.readLoop()
		}()
		go func() {
			defer l.manager.wg.Done()
			l.replanLoop()
		}()
	})
}
func (l *clientLane) retireLogical() {
	l.markStreamRetired()
	l.stream.RetireLogical()
}

func (l *clientLane) markStreamRetired() {
	l.retire.Do(func() {
		l.mu.Lock()
		l.retired = true
		pending := l.pending
		l.pending = make(map[string]chan link.LaneFrame)
		onRetire := l.onRetire
		l.mu.Unlock()
		for _, waiter := range pending {
			close(waiter)
		}
		if onRetire != nil {
			onRetire(l)
		}
	})
}

func (l *clientLane) readLoop() {
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
		case link.LanePlanReply, link.LaneCommittedReply,
			link.LaneReclaimAckReply, link.LaneReconcilePullReply,
			link.LaneResolveCoordReply:
			l.deliver(frame.RequestID, frame)
		case link.LanePlanPoke:
			select {
			case l.replan <- struct{}{}:
			default:
			}
		case link.LaneAllocRequest:
			request := frame.AllocRequest
			reply := &link.AllocReply{RequestID: frame.RequestID}
			if request == nil || l.storage == nil {
				reply.Reason = "compute: storage unavailable"
			} else if err := l.storage.Alloc(request.Coord, request.Dir); err != nil {
				reply.Reason = err.Error()
			} else {
				reply.OK = true
			}
			if l.stream.Send(link.LaneFrame{
				Kind: link.LaneAllocReply, RequestID: frame.RequestID, AllocReply: reply,
			}) != nil {
				return
			}
		case link.LaneReclaimRequest:
			request := frame.ReclaimRequest
			reply := &link.ReclaimReply{RequestID: frame.RequestID}
			if request == nil || l.local == nil {
				reply.Reason = "compute: storage unavailable"
			} else if err := l.local.ReclaimCoord(request.Coord); err != nil {
				reply.Reason = err.Error()
			} else {
				reply.OK = true
			}
			if l.stream.Send(link.LaneFrame{
				Kind: link.LaneReclaimReply, RequestID: frame.RequestID, ReclaimReply: reply,
			}) != nil {
				return
			}
		default:
			return
		}
	}
}

func (l *clientLane) collectPhysical() {
	l.mu.Lock()
	outbound := l.outbound
	l.mu.Unlock()
	if outbound != nil {
		outbound.LaneDown(l.actorSession)
	}
	l.stream.CollectPhysical()
}

func (l *clientLane) replanLoop() {
	for {
		select {
		case <-l.stream.Done():
			return
		case <-l.replan:
			l.mu.Lock()
			host := l.host
			l.mu.Unlock()
			if host == nil || !l.current() {
				continue
			}
			desired, err := l.pullPlan(l.manager.ctx)
			if err == nil && host.AcceptFullDesired(desired) == nil {
				host.Wake()
			}
		}
	}
}

func (l *clientLane) deliver(id string, frame link.LaneFrame) {
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

func (l *clientLane) roundTrip(ctx context.Context, frame link.LaneFrame) (link.LaneFrame, error) {
	if !l.current() {
		return link.LaneFrame{}, ErrOutboundDisconnected
	}
	id := uuid.NewString()
	frame.RequestID = id
	switch {
	case frame.Committed != nil:
		frame.Committed.RequestID = id
	case frame.ReclaimAck != nil:
		frame.ReclaimAck.RequestID = id
	case frame.ReconcilePull != nil:
		frame.ReconcilePull.RequestID = id
	case frame.ResolveCoord != nil:
		frame.ResolveCoord.RequestID = id
	}
	waiter := make(chan link.LaneFrame, 1)
	l.mu.Lock()
	if l.retired {
		l.mu.Unlock()
		return link.LaneFrame{}, ErrOutboundDisconnected
	}
	l.pending[id] = waiter
	l.mu.Unlock()
	if err := l.stream.Send(frame); err != nil {
		l.mu.Lock()
		delete(l.pending, id)
		l.mu.Unlock()
		return link.LaneFrame{}, err
	}
	reply, err := waitClientLaneReply(ctx, waiter, l.stream.Done())
	if err != nil {
		l.mu.Lock()
		delete(l.pending, id)
		l.mu.Unlock()
	}
	return reply, err
}

func waitClientLaneReply(
	ctx context.Context,
	waiter <-chan link.LaneFrame,
	streamDone <-chan struct{},
) (link.LaneFrame, error) {
	timer := time.NewTimer(laneRPCTimeout)
	defer timer.Stop()
	select {
	case reply, ok := <-waiter:
		if !ok {
			return link.LaneFrame{}, ErrOutboundDisconnected
		}
		return reply, nil
	case <-ctx.Done():
		return link.LaneFrame{}, ctx.Err()
	case <-timer.C:
		return link.LaneFrame{}, link.ErrLaneRPCTimeout
	case <-streamDone:
		return link.LaneFrame{}, ErrOutboundDisconnected
	}
}

func (l *clientLane) pullPlan(ctx context.Context) ([]actorhost.Desired, error) {
	reply, err := l.roundTrip(ctx, link.LaneFrame{Kind: link.LanePlanPull})
	if err != nil {
		return nil, err
	}
	if reply.PlanReply == nil {
		return nil, errors.New("compute: malformed plan reply")
	}
	if reply.PlanReply.Error != "" {
		return nil, errors.New(reply.PlanReply.Error)
	}
	desired := make([]actorhost.Desired, 0, len(reply.PlanReply.Actors))
	for _, row := range reply.PlanReply.Actors {
		desired = append(desired, actorhost.BodyDesired{
			ActorID: row.ActorID, AttemptKey: row.AttemptKey,
			ExecutionSpec: actorhost.ExecutionSpec{
				Kind: row.Kind, Class: row.Class, Config: append([]byte(nil), row.Config...),
			},
		})
	}
	return desired, nil
}

func (l *clientLane) SendCommitted(ctx context.Context, reservationID string) (link.CommittedReply, error) {
	reply, err := l.roundTrip(ctx, link.LaneFrame{
		Kind: link.LaneCommitted, Committed: &link.Committed{ReservationID: reservationID},
	})
	if err != nil || reply.CommittedReply == nil {
		return link.CommittedReply{}, errors.Join(err, errors.New("compute: outcome unknown"))
	}
	return *reply.CommittedReply, nil
}

func (l *clientLane) ResolveCoord(ctx context.Context, token string) (link.ResolveCoordReply, error) {
	reply, err := l.roundTrip(ctx, link.LaneFrame{
		Kind:         link.LaneResolveCoord,
		ResolveCoord: &link.ResolveCoordRequest{Token: token},
	})
	if err != nil || reply.ResolveCoordReply == nil {
		return link.ResolveCoordReply{}, err
	}
	return *reply.ResolveCoordReply, nil
}

func (l *clientLane) SendReclaimAck(ctx context.Context, tombstoneID string) (link.ReclaimAckReply, error) {
	reply, err := l.roundTrip(ctx, link.LaneFrame{
		Kind: link.LaneReclaimAck, ReclaimAck: &link.ReclaimAck{TombstoneID: tombstoneID},
	})
	if err != nil || reply.ReclaimAckReply == nil {
		return link.ReclaimAckReply{}, err
	}
	return *reply.ReclaimAckReply, nil
}

func (l *clientLane) SendReconcilePull(ctx context.Context, active []string) (link.ReconcilePullReply, error) {
	reply, err := l.roundTrip(ctx, link.LaneFrame{
		Kind: link.LaneReconcilePull, ReconcilePull: &link.ReconcilePull{ActiveCoords: active},
	})
	if err != nil || reply.ReconcilePullReply == nil {
		return link.ReconcilePullReply{}, err
	}
	return *reply.ReconcilePullReply, nil
}

var _ storageControlClient = (*clientLane)(nil)
var _ link.DeviceLaneControl = (*clientLane)(nil)
