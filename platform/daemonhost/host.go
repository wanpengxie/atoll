// Package daemonhost owns realm-wide daemon carriers and their per-channel
// lanes. Channel Homes publish value capabilities here and never hold sockets.
package daemonhost

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/channel"
)

const (
	defaultScanInterval    = 30 * time.Second
	defaultLeaseTTL        = 30 * time.Second
	defaultProbeInterval   = 10 * time.Second
	defaultFactTimeout     = 2 * time.Second
	defaultGoneTimeout     = 30 * time.Second
	defaultLaneOpenTimeout = 10 * time.Second
	defaultDiagnosticTTL   = 10 * time.Minute
	transferTicketTTL      = 10 * time.Minute
	diagnosticCapacity     = 64
)

var (
	ErrClosed           = errors.New("daemonhost: closed")
	ErrDuplicateCurrent = errors.New("daemonhost: duplicate current carrier")
	ErrLaneUnavailable  = errors.New("daemonhost: lane unavailable")
)

type DaemonFact uint8

const (
	DaemonAlive DaemonFact = iota + 1
	DaemonDeleted
	DaemonUnavailable
)

type Config struct {
	Logger       *slog.Logger
	ScanInterval time.Duration
	LeaseTTL     time.Duration
	Present      func(context.Context) ([]channel.ID, error)
	DaemonFact   func(context.Context, string) DaemonFact
	Now          func() time.Time
}

type CompartmentState string

const (
	CompartmentBuilding CompartmentState = "building"
	CompartmentReady    CompartmentState = "ready"
	CompartmentFault    CompartmentState = "fault"
	CompartmentGone     CompartmentState = "gone"
)

type LaneView struct {
	ChID        channel.ID
	State       string
	LaneGen     link.LaneGeneration
	Retirements uint64
}

type Diagnostic struct {
	CarrierGen link.CarrierGeneration `json:"carrier_gen,omitempty"`
	ChID       channel.ID             `json:"channel_id,omitempty"`
	LaneGen    link.LaneGeneration    `json:"lane_gen,omitempty"`
	Kind       string                 `json:"kind"`
	Time       time.Time              `json:"time"`
}

type membraneRow struct {
	generation uint64
	bundle     platform.DaemonMembrane
}

type compartmentView struct {
	state     CompartmentState
	reason    string
	closeSent bool
	closeAt   time.Time
	goneTimed bool
}

type carrierRow struct {
	host     *Host
	daemonID string
	gen      link.CarrierGeneration
	wire     *link.ServerCarrier
	sealed   atomic.Bool

	mu           sync.Mutex
	lanes        map[channel.ID]*serverLane
	retirements  map[channel.ID]uint64
	compartments map[channel.ID]compartmentView
	coordLocks   map[channel.ID]*sync.Mutex
	coordTasks   map[channel.ID]*coordTask
	lastSeen     time.Time
}

type coordTask struct {
	dirty   bool
	running bool
}

type daemonRow struct {
	tombstone  bool
	current    *carrierRow
	diagnostic []Diagnostic
}

type Host struct {
	logger *slog.Logger
	cfg    Config
	ctx    context.Context
	cancel context.CancelFunc

	mu               sync.RWMutex
	closed           bool
	daemons          map[string]*daemonRow
	membranes        map[channel.ID]membraneRow
	retiredMembranes map[channel.ID]uint64
	transfers        map[string]transferTicket
	wg               sync.WaitGroup
	upgrader         websocket.Upgrader
}

func New(cfg Config) *Host {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.ScanInterval <= 0 {
		cfg.ScanInterval = defaultScanInterval
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = defaultLeaseTTL
	}
	ctx, cancel := context.WithCancel(context.Background())
	h := &Host{
		logger: cfg.Logger, cfg: cfg, ctx: ctx, cancel: cancel,
		daemons: make(map[string]*daemonRow), membranes: make(map[channel.ID]membraneRow),
		retiredMembranes: make(map[channel.ID]uint64),
		transfers:        make(map[string]transferTicket),
		upgrader:         websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }},
	}
	h.wg.Add(1)
	go h.run()
	return h
}

func (h *Host) run() {
	defer h.wg.Done()
	scanTicker := time.NewTicker(h.cfg.ScanInterval)
	probeTicker := time.NewTicker(defaultProbeInterval)
	defer scanTicker.Stop()
	defer probeTicker.Stop()
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-scanTicker.C:
			h.Scan()
		case <-probeTicker.C:
			h.probeCarriers()
		}
	}
}

func (h *Host) now() time.Time {
	if h.cfg.Now != nil {
		return h.cfg.Now()
	}
	return time.Now()
}

func (h *Host) probeCarriers() {
	now := h.now()
	h.mu.Lock()
	var carriers []*carrierRow
	var expired []*carrierRow
	for _, row := range h.daemons {
		carrier := row.current
		if carrier == nil || carrier.sealed.Load() {
			continue
		}
		if now.Sub(carrier.lastSeen) > h.cfg.LeaseTTL {
			if h.sealCarrierLocked(carrier) {
				expired = append(expired, carrier)
			}
			continue
		}
		carriers = append(carriers, carrier)
	}
	for daemonID, row := range h.daemons {
		if row.current == nil && !row.tombstone && len(row.diagnostic) > 0 &&
			now.Sub(row.diagnostic[len(row.diagnostic)-1].Time) >= defaultDiagnosticTTL {
			delete(h.daemons, daemonID)
		}
	}
	h.mu.Unlock()
	for _, carrier := range expired {
		h.cleanupCarrier(carrier)
	}
	for _, carrier := range carriers {
		if err := carrier.wire.SendSpine(link.SpineFrame{
			Kind: link.SpineProbe, Nonce: uuid.NewString(),
		}); err != nil {
			h.carrierDown(carrier)
		}
	}
}

func (h *Host) Register(chID channel.ID, generation uint64, bundle platform.DaemonMembrane) {
	if chID == "" || generation == 0 {
		return
	}
	bundle.Generation = generation
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	if generation <= h.retiredMembranes[chID] {
		h.mu.Unlock()
		return
	}
	old, exists := h.membranes[chID]
	if exists && old.generation > generation {
		h.mu.Unlock()
		return
	}
	h.membranes[chID] = membraneRow{generation: generation, bundle: bundle}
	carriers := h.currentCarriersLocked()
	h.mu.Unlock()
	for _, carrier := range carriers {
		if exists && old.generation != generation {
			h.replaceMembrane(carrier, chID, generation)
			continue
		}
		h.reconcileCoord(carrier, chID)
	}
}

func (h *Host) replaceMembrane(carrier *carrierRow, chID channel.ID, generation uint64) {
	if carrier == nil || carrier.sealed.Load() {
		return
	}
	lock := carrier.coordLock(chID)
	lock.Lock()
	defer lock.Unlock()
	h.mu.RLock()
	membrane, known := h.membranes[chID]
	h.mu.RUnlock()
	if !known || membrane.generation != generation {
		return
	}
	carrier.retireLane(chID)
	h.reconcileCoordLocked(carrier, chID, membrane)
}

func (h *Host) Unregister(chID channel.ID, generation uint64) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	if generation > h.retiredMembranes[chID] {
		h.retiredMembranes[chID] = generation
	}
	row, ok := h.membranes[chID]
	if !ok || row.generation != generation {
		h.mu.Unlock()
		return
	}
	delete(h.membranes, chID)
	carriers := h.currentCarriersLocked()
	h.mu.Unlock()
	for _, carrier := range carriers {
		lock := carrier.coordLock(chID)
		lock.Lock()
		h.mu.RLock()
		_, stillRegistered := h.membranes[chID]
		h.mu.RUnlock()
		if !stillRegistered {
			carrier.retireLane(chID)
		}
		lock.Unlock()
	}
}

func (h *Host) currentCarriersLocked() []*carrierRow {
	out := make([]*carrierRow, 0, len(h.daemons))
	for _, row := range h.daemons {
		if row.current != nil && !row.current.sealed.Load() {
			out = append(out, row.current)
		}
	}
	return out
}

func (h *Host) Serve(w http.ResponseWriter, r *http.Request, daemonID string) {
	if daemonID == "" {
		http.Error(w, "authenticated daemon id required", http.StatusUnauthorized)
		return
	}
	ws, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	wire, err := link.AcceptDeviceCarrier(ws, h.logger.With("daemon", daemonID))
	if err != nil {
		if wire != nil {
			class := link.CarrierRetryable
			if errors.Is(err, link.ErrProtocolVersion) {
				class = link.CarrierTerminal
			}
			_ = wire.SendSpine(link.SpineFrame{
				Kind: link.SpineCarrierReject, Class: class, Reason: err.Error(),
			})
			_ = wire.Close()
		} else {
			_ = ws.Close()
		}
		return
	}
	carrier := &carrierRow{
		host: h, daemonID: daemonID, wire: wire,
		lanes:        make(map[channel.ID]*serverLane),
		retirements:  make(map[channel.ID]uint64),
		compartments: make(map[channel.ID]compartmentView),
		coordLocks:   make(map[channel.ID]*sync.Mutex),
		coordTasks:   make(map[channel.ID]*coordTask),
		lastSeen:     h.now(),
	}
	if err := h.admit(carrier); err != nil {
		class := link.CarrierRetryable
		if !errors.Is(err, ErrDuplicateCurrent) {
			class = link.CarrierTerminal
		}
		_ = wire.SendSpine(link.SpineFrame{
			Kind: link.SpineCarrierReject, Class: class, Reason: err.Error(),
		})
		_ = wire.Close()
		return
	}
	if err := wire.SendSpine(link.SpineFrame{
		Kind: link.SpineCarrierAccept, DaemonID: daemonID, CarrierGen: carrier.gen,
	}); err != nil {
		h.carrierDown(carrier)
		return
	}
	h.mu.Lock()
	row := h.daemons[daemonID]
	if h.closed || row == nil || row.current != carrier || carrier.sealed.Load() {
		h.mu.Unlock()
		h.carrierDown(carrier)
		return
	}
	h.wg.Add(1)
	h.mu.Unlock()
	go func() {
		defer h.wg.Done()
		h.runCarrier(carrier)
	}()
	h.Scan()
	<-wire.Done()
}

func (h *Host) admit(carrier *carrierRow) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return ErrClosed
	}
	row := h.daemons[carrier.daemonID]
	if row == nil {
		row = &daemonRow{}
		h.daemons[carrier.daemonID] = row
	}
	if row.tombstone {
		return errors.New("daemonhost: daemon revoked")
	}
	if row.current != nil && !row.current.sealed.Load() {
		return ErrDuplicateCurrent
	}
	carrier.gen = link.NewCarrierGeneration()
	row.current = carrier
	return nil
}

func (h *Host) runCarrier(carrier *carrierRow) {
	defer h.carrierDown(carrier)
	var readers sync.WaitGroup
	readers.Add(2)
	go func() {
		defer readers.Done()
		h.readSpine(carrier)
	}()
	go func() {
		defer readers.Done()
		h.acceptStreams(carrier)
	}()
	readers.Wait()
}

func (h *Host) readSpine(carrier *carrierRow) {
	for {
		var frame link.SpineFrame
		if err := carrier.wire.ReadSpine(&frame); err != nil {
			return
		}
		if err := frame.Validate(); err != nil {
			return
		}
		h.mu.Lock()
		if row := h.daemons[carrier.daemonID]; row != nil && row.current == carrier {
			carrier.lastSeen = h.now()
		}
		h.mu.Unlock()
		carrier.mu.Lock()
		switch frame.Kind {
		case link.SpineCompartmentState:
			switch CompartmentState(frame.State) {
			case CompartmentGone:
				delete(carrier.compartments, frame.Channel)
			case CompartmentBuilding, CompartmentReady, CompartmentFault:
				current := carrier.compartments[frame.Channel]
				current.state = CompartmentState(frame.State)
				current.reason = frame.Reason
				carrier.compartments[frame.Channel] = current
			}
		case link.SpineProbe:
			carrier.mu.Unlock()
			if carrier.wire.SendSpine(link.SpineFrame{Kind: link.SpineProbeReply, Nonce: frame.Nonce}) != nil {
				return
			}
			continue
		case link.SpineProbeReply:
		default:
			carrier.mu.Unlock()
			return
		}
		carrier.mu.Unlock()
	}
}

func (h *Host) acceptStreams(carrier *carrierRow) {
	_ = carrier.wire.ServeStreams(func(conn net.Conn, header link.DeviceStreamHeader) {
		if header.Kind == link.DeviceStreamCarrier {
			_ = conn.Close()
			_ = carrier.wire.Close()
			return
		}
		if header.Kind != link.DeviceStreamActor {
			_ = conn.Close()
			return
		}
		if err := header.Validate(); err != nil {
			_ = conn.Close()
			return
		}
		carrier.mu.Lock()
		lane := carrier.lanes[header.Channel]
		current := lane != nil && lane.stream.Gen == header.LaneGen && !lane.stream.Retired()
		carrier.mu.Unlock()
		if !current {
			_ = conn.Close()
			return
		}
		lane.acceptActor(conn)
	})
}

func (h *Host) carrierDown(carrier *carrierRow) {
	if carrier == nil {
		return
	}
	h.mu.Lock()
	if !h.sealCarrierLocked(carrier) {
		h.mu.Unlock()
		return
	}
	h.mu.Unlock()
	h.cleanupCarrier(carrier)
}

// sealCarrierLocked is the carrier registry's single decision write. h.mu
// must be held; all physical collection happens after it returns.
func (h *Host) sealCarrierLocked(carrier *carrierRow) bool {
	if carrier.sealed.Load() {
		return false
	}
	carrier.sealed.Store(true)
	if row := h.daemons[carrier.daemonID]; row != nil && row.current == carrier {
		row.current = nil
		if !row.tombstone && len(row.diagnostic) == 0 {
			delete(h.daemons, carrier.daemonID)
		}
	}
	return true
}

func (h *Host) cleanupCarrier(carrier *carrierRow) {
	carrier.mu.Lock()
	lanes := make([]*serverLane, 0, len(carrier.lanes))
	for _, lane := range carrier.lanes {
		lanes = append(lanes, lane)
	}
	carrier.lanes = make(map[channel.ID]*serverLane)
	carrier.compartments = make(map[channel.ID]compartmentView)
	carrier.mu.Unlock()
	for _, lane := range lanes {
		lane.retireLogical()
	}
	_ = carrier.wire.Close()
	for _, lane := range lanes {
		<-lane.stream.PhysicalDone()
	}
}

func (h *Host) Scan() {
	h.mu.RLock()
	carriers := h.currentCarriersLocked()
	membranes := make(map[channel.ID]membraneRow, len(h.membranes))
	for id, row := range h.membranes {
		membranes[id] = row
	}
	h.mu.RUnlock()
	if h.cfg.Present != nil {
		present, err := h.presentChannels()
		if err == nil {
			for _, id := range present {
				if _, ok := membranes[id]; !ok {
					// Present-but-closed is intentionally unknown. Keeping it
					// in the scan domain prevents absence from being treated
					// as an unbind command.
					membranes[id] = membraneRow{}
				}
			}
		}
	}
	for _, carrier := range carriers {
		if h.cfg.DaemonFact != nil {
			fact := h.daemonFact(carrier.daemonID)
			if fact == DaemonDeleted {
				h.RevokeDaemon(carrier.daemonID)
				continue
			}
		}
		coords := make(map[channel.ID]struct{}, len(membranes))
		for id := range membranes {
			coords[id] = struct{}{}
		}
		carrier.mu.Lock()
		for id := range carrier.lanes {
			coords[id] = struct{}{}
		}
		for id := range carrier.compartments {
			coords[id] = struct{}{}
		}
		carrier.mu.Unlock()
		for chID := range coords {
			h.markCoordDirty(carrier, chID)
		}
		h.observeGoneTimeouts(carrier)
	}
}

func (h *Host) markCoordDirty(carrier *carrierRow, chID channel.ID) {
	h.mu.Lock()
	if h.closed || carrier == nil || carrier.sealed.Load() {
		h.mu.Unlock()
		return
	}
	carrier.mu.Lock()
	task := carrier.coordTasks[chID]
	if task == nil {
		task = &coordTask{}
		carrier.coordTasks[chID] = task
	}
	task.dirty = true
	start := !task.running
	if start {
		task.running = true
		h.wg.Add(1)
	}
	carrier.mu.Unlock()
	h.mu.Unlock()
	if !start {
		return
	}
	go func() {
		defer h.wg.Done()
		for {
			carrier.mu.Lock()
			task := carrier.coordTasks[chID]
			if task == nil || !task.dirty {
				if task != nil {
					task.running = false
				}
				carrier.mu.Unlock()
				return
			}
			task.dirty = false
			carrier.mu.Unlock()
			h.reconcileCoord(carrier, chID)
		}
	}()
}

func (h *Host) presentChannels() ([]channel.ID, error) {
	ctx, cancel := context.WithTimeout(h.ctx, defaultFactTimeout)
	defer cancel()
	type result struct {
		ids []channel.ID
		err error
	}
	done := make(chan result, 1)
	go func() {
		ids, err := h.cfg.Present(ctx)
		done <- result{ids: ids, err: err}
	}()
	select {
	case result := <-done:
		return result.ids, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (h *Host) daemonFact(daemonID string) DaemonFact {
	ctx, cancel := context.WithTimeout(h.ctx, defaultFactTimeout)
	defer cancel()
	done := make(chan DaemonFact, 1)
	go func() { done <- h.cfg.DaemonFact(ctx, daemonID) }()
	select {
	case fact := <-done:
		return fact
	case <-ctx.Done():
		return DaemonUnavailable
	}
}

func (h *Host) observeGoneTimeouts(carrier *carrierRow) {
	now := h.now()
	var diagnostics []Diagnostic
	carrier.mu.Lock()
	for chID, view := range carrier.compartments {
		if !view.closeSent || view.goneTimed || now.Sub(view.closeAt) < defaultGoneTimeout {
			continue
		}
		view.goneTimed = true
		carrier.compartments[chID] = view
		laneGen := link.LaneGeneration("")
		if lane := carrier.lanes[chID]; lane != nil {
			laneGen = lane.stream.Gen
		}
		diagnostics = append(diagnostics, Diagnostic{
			CarrierGen: carrier.gen, ChID: chID, LaneGen: laneGen,
			Kind: "gone_timeout", Time: now,
		})
	}
	carrier.mu.Unlock()
	for _, diagnostic := range diagnostics {
		h.recordDiagnostic(carrier.daemonID, diagnostic)
	}
}

func (h *Host) reconcileCoord(carrier *carrierRow, chID channel.ID) {
	if carrier == nil || carrier.sealed.Load() {
		return
	}
	lock := carrier.coordLock(chID)
	lock.Lock()
	defer lock.Unlock()
	h.mu.RLock()
	membrane, known := h.membranes[chID]
	h.mu.RUnlock()
	if !known || membrane.bundle.IsBound == nil {
		return
	}
	h.reconcileCoordLocked(carrier, chID, membrane)
}

func (h *Host) reconcileCoordLocked(
	carrier *carrierRow,
	chID channel.ID,
	membrane membraneRow,
) {
	bound, err := h.isBound(carrier.daemonID, membrane.bundle.IsBound)
	if err != nil {
		return
	}
	if bound {
		carrier.ensureLane(chID, membrane)
		return
	}
	carrier.retireLane(chID)
	carrier.mu.Lock()
	_, hasCompartment := carrier.compartments[chID]
	carrier.mu.Unlock()
	if hasCompartment && carrier.wire.SendSpine(link.SpineFrame{
		Kind: link.SpineCompartmentClose, Channel: chID,
	}) == nil {
		carrier.mu.Lock()
		view := carrier.compartments[chID]
		if !view.closeSent {
			view.closeAt = h.now()
			view.goneTimed = false
		}
		view.closeSent = true
		carrier.compartments[chID] = view
		carrier.mu.Unlock()
	}
}

func (h *Host) isBound(
	daemonID string,
	resolve func(context.Context, string) (bool, error),
) (bool, error) {
	ctx, cancel := context.WithTimeout(h.ctx, defaultFactTimeout)
	defer cancel()
	type result struct {
		bound bool
		err   error
	}
	done := make(chan result, 1)
	go func() {
		bound, err := resolve(ctx, daemonID)
		done <- result{bound: bound, err: err}
	}()
	select {
	case result := <-done:
		return result.bound, result.err
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (c *carrierRow) coordLock(chID channel.ID) *sync.Mutex {
	c.mu.Lock()
	defer c.mu.Unlock()
	lock := c.coordLocks[chID]
	if lock == nil {
		lock = &sync.Mutex{}
		c.coordLocks[chID] = lock
	}
	return lock
}

func (c *carrierRow) ensureLane(chID channel.ID, membrane membraneRow) {
	c.mu.Lock()
	if current := c.lanes[chID]; current != nil && !current.stream.Retired() &&
		current.membrane.generation == membrane.generation {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	generation := link.NewLaneGeneration()
	ctx, cancel := context.WithTimeout(c.host.ctx, defaultLaneOpenTimeout)
	stream, err := c.wire.OpenLane(ctx, chID, generation)
	cancel()
	if err != nil {
		return
	}
	lane := newServerLane(c, stream, membrane)
	stream.SetRetire(func(exact *link.LaneStream) {
		lane.markStreamRetired()
		c.mu.Lock()
		if c.lanes[chID] == lane {
			delete(c.lanes, chID)
		}
		c.mu.Unlock()
	})
	c.mu.Lock()
	if c.sealed.Load() || stream.Retired() {
		c.mu.Unlock()
		lane.start()
		stream.RetireLogical()
		return
	}
	old := c.lanes[chID]
	c.lanes[chID] = lane
	c.mu.Unlock()
	lane.start()
	if old != nil {
		old.retireLogical()
	}
}

func (c *carrierRow) retireLane(chID channel.ID) {
	c.mu.Lock()
	lane := c.lanes[chID]
	if lane != nil {
		delete(c.lanes, chID)
	}
	c.mu.Unlock()
	if lane != nil {
		lane.retireLogical()
	}
}

func (h *Host) DaemonOnline(daemonID string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	row := h.daemons[daemonID]
	return row != nil && row.current != nil && !row.current.sealed.Load()
}

func (h *Host) LaneAttached(daemonID, chID string) bool {
	h.mu.RLock()
	row := h.daemons[daemonID]
	var carrier *carrierRow
	if row != nil {
		carrier = row.current
	}
	h.mu.RUnlock()
	if carrier == nil || carrier.sealed.Load() {
		return false
	}
	id := channel.ID(chID)
	carrier.mu.Lock()
	defer carrier.mu.Unlock()
	lane := carrier.lanes[id]
	view, ok := carrier.compartments[id]
	return lane != nil && !lane.stream.Retired() && ok &&
		view.state == CompartmentReady && !view.closeSent
}

func (h *Host) AttachedDaemons(chID string) []string {
	h.mu.RLock()
	ids := make([]string, 0, len(h.daemons))
	for id := range h.daemons {
		ids = append(ids, id)
	}
	h.mu.RUnlock()
	out := ids[:0]
	for _, id := range ids {
		if h.LaneAttached(id, chID) {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

func (h *Host) LaneView(daemonID string) []LaneView {
	h.mu.RLock()
	row := h.daemons[daemonID]
	var carrier *carrierRow
	if row != nil {
		carrier = row.current
	}
	h.mu.RUnlock()
	if carrier == nil {
		return nil
	}
	carrier.mu.Lock()
	defer carrier.mu.Unlock()
	out := make([]LaneView, 0, len(carrier.lanes))
	for id, lane := range carrier.lanes {
		out = append(out, LaneView{
			ChID: id, State: "active", LaneGen: lane.stream.Gen,
			Retirements: carrier.retirements[id],
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ChID < out[j].ChID })
	return out
}

func (h *Host) RetirementCount(daemonID, chID string) uint64 {
	h.mu.RLock()
	row := h.daemons[daemonID]
	var carrier *carrierRow
	if row != nil {
		carrier = row.current
	}
	h.mu.RUnlock()
	if carrier == nil {
		return 0
	}
	carrier.mu.Lock()
	defer carrier.mu.Unlock()
	return carrier.retirements[channel.ID(chID)]
}

func (h *Host) Diagnostics(daemonID string) []Diagnostic {
	h.mu.RLock()
	defer h.mu.RUnlock()
	row := h.daemons[daemonID]
	if row == nil {
		return nil
	}
	return append([]Diagnostic(nil), row.diagnostic...)
}

func (h *Host) recordDiagnostic(daemonID string, diagnostic Diagnostic) {
	h.mu.Lock()
	defer h.mu.Unlock()
	row := h.daemons[daemonID]
	if row == nil {
		row = &daemonRow{}
		h.daemons[daemonID] = row
	}
	if len(row.diagnostic) == diagnosticCapacity {
		copy(row.diagnostic, row.diagnostic[1:])
		row.diagnostic = row.diagnostic[:diagnosticCapacity-1]
	}
	row.diagnostic = append(row.diagnostic, diagnostic)
}

func (h *Host) RevokeDaemon(daemonID string) {
	h.mu.Lock()
	row := h.daemons[daemonID]
	if row == nil {
		row = &daemonRow{}
		h.daemons[daemonID] = row
	}
	row.tombstone = true
	carrier := row.current
	row.current = nil
	if carrier != nil {
		carrier.sealed.Store(true)
	}
	h.mu.Unlock()
	if carrier != nil {
		carrier.mu.Lock()
		lanes := make([]*serverLane, 0, len(carrier.lanes))
		for _, lane := range carrier.lanes {
			lanes = append(lanes, lane)
		}
		compartments := make(map[channel.ID]compartmentView, len(carrier.compartments))
		for chID, view := range carrier.compartments {
			compartments[chID] = view
		}
		carrier.lanes = make(map[channel.ID]*serverLane)
		carrier.mu.Unlock()
		for _, lane := range lanes {
			lane.retireLogical()
		}
		_ = carrier.wire.SendSpine(link.SpineFrame{
			Kind: link.SpineCarrierReject, Class: link.CarrierTerminal, Reason: "daemon revoked",
		})
		h.recordDiagnostic(daemonID, Diagnostic{
			CarrierGen: carrier.gen, Kind: "revoke", Time: h.now(),
		})
		for chID, view := range compartments {
			if view.state != CompartmentGone {
				h.recordDiagnostic(daemonID, Diagnostic{
					CarrierGen: carrier.gen, ChID: chID,
					Kind: "gone_unobserved_terminal", Time: h.now(),
				})
			}
		}
		_ = carrier.wire.Close()
		for _, lane := range lanes {
			<-lane.stream.PhysicalDone()
		}
	}
}

func (h *Host) RetireLane(daemonID, chID string) {
	h.mu.RLock()
	row := h.daemons[daemonID]
	var carrier *carrierRow
	if row != nil {
		carrier = row.current
	}
	h.mu.RUnlock()
	if carrier != nil {
		id := channel.ID(chID)
		lock := carrier.coordLock(id)
		lock.Lock()
		carrier.retireLane(id)
		lock.Unlock()
	}
}

func (h *Host) PokePlan(daemonID, chID string) {
	if lane := h.currentLane(daemonID, channel.ID(chID)); lane != nil {
		_ = lane.stream.Send(link.LaneFrame{Kind: link.LanePlanPoke})
	}
}

func (h *Host) currentLane(daemonID string, chID channel.ID) *serverLane {
	h.mu.RLock()
	row := h.daemons[daemonID]
	var carrier *carrierRow
	if row != nil {
		carrier = row.current
	}
	h.mu.RUnlock()
	if carrier == nil {
		return nil
	}
	carrier.mu.Lock()
	defer carrier.mu.Unlock()
	return carrier.lanes[chID]
}

func (h *Host) SendAlloc(ctx context.Context, daemonID, chID, coord string, dir bool) error {
	lane := h.currentLane(daemonID, channel.ID(chID))
	if lane == nil {
		return ErrLaneUnavailable
	}
	return lane.alloc(ctx, coord, dir)
}

func (h *Host) SendReclaim(ctx context.Context, daemonID, chID, coord string) error {
	lane := h.currentLane(daemonID, channel.ID(chID))
	if lane == nil {
		return ErrLaneUnavailable
	}
	return lane.reclaim(ctx, coord)
}

type transferTicket struct {
	daemonID, chID, coord, reservationID string
	mode                                 access.Operation
	expires                              time.Time
}

func (h *Host) OpenTransfer(
	_ context.Context,
	daemonID, chID, coord string,
	mode access.Operation,
	reservationID string,
) (string, error) {
	if coord == "" || (mode != access.OpRead && mode != access.OpWrite) ||
		h.currentLane(daemonID, channel.ID(chID)) == nil {
		return "", ErrLaneUnavailable
	}
	token := uuid.NewString()
	now := h.now()
	h.mu.Lock()
	h.sweepExpiredTransfersLocked(now)
	h.transfers[token] = transferTicket{
		daemonID: daemonID, chID: chID, coord: coord, mode: mode,
		reservationID: reservationID, expires: now.Add(transferTicketTTL),
	}
	h.mu.Unlock()
	return token, nil
}

func (h *Host) resolveTransfer(daemonID, chID, token string) (transferTicket, bool) {
	now := h.now()
	h.mu.Lock()
	defer h.mu.Unlock()
	ticket, ok := h.transfers[token]
	if !ok || ticket.daemonID != daemonID || ticket.chID != chID ||
		transferTicketExpired(ticket, now) {
		if ok && transferTicketExpired(ticket, now) {
			delete(h.transfers, token)
		}
		return transferTicket{}, false
	}
	return ticket, true
}

func transferTicketExpired(ticket transferTicket, now time.Time) bool {
	return !now.Before(ticket.expires)
}

// sweepExpiredTransfersLocked bounds the table to tickets minted within one
// TTL window. Mint-time GC needs no extra goroutine and reclaims tickets that
// were opened but never redeemed.
func (h *Host) sweepExpiredTransfersLocked(now time.Time) {
	for token, ticket := range h.transfers {
		if transferTicketExpired(ticket, now) {
			delete(h.transfers, token)
		}
	}
}

func (h *Host) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	carriers := h.currentCarriersLocked()
	for _, row := range h.daemons {
		if row.current != nil {
			row.current.sealed.Store(true)
		}
		row.current = nil
	}
	h.mu.Unlock()
	h.cancel()
	for _, carrier := range carriers {
		h.cleanupCarrier(carrier)
	}
	h.wg.Wait()
	return nil
}

var _ platform.DaemonRoutes = (*Host)(nil)
