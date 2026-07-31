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

// factTimeout is a test seam for non-cooperative platform callbacks. Production
// always leaves it at the named construction constant above.
var factTimeout = defaultFactTimeout

// sendCarrierAccept is the narrow fault-injection seam for the post-admission
// accept write. Production always uses the direct spine send below.
var sendCarrierAccept = func(wire *link.ServerCarrier, frame link.SpineFrame) error {
	return wire.SendSpine(frame)
}

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

// Compartment existence is the device's own physical fact and this host keeps
// no projection of it. The device converges its compartment set onto the
// complete snapshot it pulls (compartment_plan_pull), so there is no teardown
// command to track, no declaration to cache, and no acknowledgement to await.

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

type carrierRow struct {
	host     *Host
	daemonID string
	gen      link.CarrierGeneration
	wire     *link.ServerCarrier
	sealed   atomic.Bool

	mu          sync.Mutex
	lanes       map[channel.ID]*serverLane
	retirements map[channel.ID]uint64
	coordLocks  map[channel.ID]*coordGate
	coordTasks  map[channel.ID]*coordTask

	// lastSeen and probeNonce are guarded by Host.mu, not by the mu above:
	// the lease decision is the host's, and it is taken while walking every
	// carrier at once. probeNonce is the probe this host has sent and not yet
	// had answered; empty means there is nothing outstanding to match.
	lastSeen   time.Time
	probeNonce string

	// physical counts this carrier's physical sub-lifetimes: one ticket per lane
	// open, held from before the open starts until that lane's reader has
	// collected. It is not a ledger and takes part in no routing or liveness
	// decision — it exists so this carrier's supervisor, the single owner of
	// those goroutines, can join them at the end.
	//
	// Issuing a ticket is serialised with sealed under mu, so no ticket can
	// appear after the supervisor has begun waiting.
	physical sync.WaitGroup
	stopOnce sync.Once
}

type coordGate struct {
	mu   sync.Mutex
	refs int
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
	type outstandingProbe struct {
		carrier *carrierRow
		nonce   string
	}
	var carriers []outstandingProbe
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
		// A fresh probe replaces whatever is outstanding. The previous one went
		// unanswered for a whole interval, so a reply to it no longer attests
		// anything about the round trip this host is asking about now.
		nonce := uuid.NewString()
		carrier.probeNonce = nonce
		carriers = append(carriers, outstandingProbe{carrier: carrier, nonce: nonce})
	}
	for daemonID, row := range h.daemons {
		if row.current == nil && !row.tombstone && len(row.diagnostic) > 0 &&
			now.Sub(row.diagnostic[len(row.diagnostic)-1].Time) >= defaultDiagnosticTTL {
			delete(h.daemons, daemonID)
		}
	}
	h.mu.Unlock()
	for _, carrier := range expired {
		h.beginCarrierShutdown(carrier)
	}
	for _, probe := range carriers {
		if err := probe.carrier.wire.SendSpine(link.SpineFrame{
			Kind: link.SpineProbe, Nonce: probe.nonce,
		}); err != nil {
			h.beginCarrierShutdown(probe.carrier)
		}
	}
}

// renewLeaseOnProbeReply renews only when the reply answers the probe this host
// actually sent.
//
// The lease attests a round trip. Anything the device sends proves only that
// its own send path works, so renewing on inbound traffic would keep a carrier
// whose downstream direction is dead online forever: it would go on pulling and
// talking while every frame this host sends is lost, and the daemon could never
// be replaced because a current carrier is already recorded.
func (h *Host) renewLeaseOnProbeReply(carrier *carrierRow, nonce string) {
	if carrier == nil || nonce == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	row := h.daemons[carrier.daemonID]
	if row == nil || row.current != carrier || carrier.probeNonce != nonce {
		return
	}
	carrier.probeNonce = ""
	carrier.lastSeen = h.now()
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
	unlock := carrier.lockCoord(chID)
	defer unlock()
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
		unlock := carrier.lockCoord(chID)
		h.mu.RLock()
		_, stillRegistered := h.membranes[chID]
		h.mu.RUnlock()
		if !stillRegistered {
			carrier.retireLane(chID)
		}
		unlock()
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
	// Admission is refused before the handshake. Upgrading first and rejecting
	// afterwards spends a websocket on a host that will never serve it, and
	// hands the caller a carrier-level reject in place of an HTTP answer it can
	// attribute. admit remains the authority — this gate only keeps a closed
	// host from taking connections it has already stopped serving.
	h.mu.RLock()
	closed := h.closed
	h.mu.RUnlock()
	if closed {
		http.Error(w, "daemon host closed", http.StatusServiceUnavailable)
		return
	}
	ws, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	wire, err := link.AcceptDeviceCarrier(ws, h.logger.With("daemon", daemonID))
	if err != nil {
		if wire != nil {
			_ = wire.SendSpine(link.SpineFrame{
				Kind: link.SpineCarrierReject, Class: handshakeRejectClass(err), Reason: err.Error(),
			})
			_ = wire.Close()
		} else {
			_ = ws.Close()
		}
		return
	}
	carrier := &carrierRow{
		host: h, daemonID: daemonID, wire: wire,
		lanes:       make(map[channel.ID]*serverLane),
		retirements: make(map[channel.ID]uint64),
		coordLocks:  make(map[channel.ID]*coordGate),
		coordTasks:  make(map[channel.ID]*coordTask),
		lastSeen:    h.now(),
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
	// The supervisor starts the moment this carrier is in the ledger, before the
	// accept frame goes out. Admission publishes it as current, so a concurrent
	// scan can already open lanes on it; starting the supervisor afterwards left
	// a window where a failed accept write produced a carrier with lanes and no
	// owner to join them. h.wg is registered under h.mu, the same lock Close
	// writes h.closed under, so no supervisor can be added after Close waits.
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		h.beginCarrierShutdown(carrier)
		return
	}
	h.wg.Add(1)
	h.mu.Unlock()
	go func() {
		defer h.wg.Done()
		h.runCarrier(carrier)
	}()
	if err := sendCarrierAccept(wire, link.SpineFrame{
		Kind: link.SpineCarrierAccept, DaemonID: daemonID, CarrierGen: carrier.gen,
	}); err != nil {
		h.beginCarrierShutdown(carrier)
		return
	}
	h.Scan()
	<-wire.Done()
}

func handshakeRejectClass(err error) link.CarrierClass {
	if errors.Is(err, link.ErrProtocolVersion) {
		return link.CarrierTerminal
	}
	return link.CarrierRetryable
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

// runCarrier is this carrier's supervisor and the only place that joins its
// physical children. Every other shutdown caller decides and leaves; this one
// stays until the lanes it owned have collected.
func (h *Host) runCarrier(carrier *carrierRow) {
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
	// A natural disconnect reaches shutdown here; a shutdown decided elsewhere
	// already ran and this is a no-op. Either way the join below happens once.
	h.beginCarrierShutdown(carrier)
	carrier.physical.Wait()
}

func (h *Host) readSpine(carrier *carrierRow) {
	for {
		var frame link.SpineFrame
		if err := carrier.wire.ReadSpine(&frame); err != nil {
			_ = carrier.wire.Close()
			return
		}
		if err := frame.Validate(); err != nil {
			_ = carrier.wire.Close()
			return
		}
		switch frame.Kind {
		case link.SpineCompartmentPlanPull:
			h.answerCompartmentPlan(carrier, frame.Nonce)
		case link.SpineProbe:
			if carrier.wire.SendSpine(link.SpineFrame{Kind: link.SpineProbeReply, Nonce: frame.Nonce}) != nil {
				return
			}
			continue
		case link.SpineProbeReply:
			h.renewLeaseOnProbeReply(carrier, frame.Nonce)
		default:
			_ = carrier.wire.Close()
			return
		}
	}
}

// answerCompartmentPlan replies with the complete channel snapshot this daemon
// must converge onto, or stays silent.
//
// Silence is the only safe partial answer: the device retires every compartment
// whose channel the snapshot did not name, so a snapshot missing a channel the
// realm still has would destroy a compartment that must live. A channel this
// host cannot judge right now (its Home is not open, or the binding store is
// unreachable) is therefore named in Unknown rather than omitted, and a
// directory enumeration that fails at all suppresses the reply entirely.
func (h *Host) answerCompartmentPlan(carrier *carrierRow, nonce string) {
	if carrier == nil || carrier.sealed.Load() || h.cfg.Present == nil {
		return
	}
	h.mu.RLock()
	row := h.daemons[carrier.daemonID]
	currentCarrier := row != nil && row.current == carrier
	h.mu.RUnlock()
	if !currentCarrier {
		return
	}
	ctx, cancel := context.WithTimeout(h.ctx, factTimeout)
	present, err := h.cfg.Present(ctx)
	cancel()
	if err != nil {
		return
	}
	serve := make([]channel.ID, 0, len(present))
	unknown := make([]channel.ID, 0)
	for _, chID := range present {
		h.mu.RLock()
		membrane, known := h.membranes[chID]
		h.mu.RUnlock()
		if !known {
			unknown = append(unknown, chID)
			continue
		}
		bound, err := h.isBound(carrier.daemonID, membrane.bundle.IsBound)
		if err != nil {
			unknown = append(unknown, chID)
			continue
		}
		if bound {
			serve = append(serve, chID)
		}
	}
	sortChannels(serve)
	sortChannels(unknown)
	_ = carrier.wire.SendSpine(link.SpineFrame{
		Kind: link.SpineCompartmentPlanReply, Nonce: nonce, Serve: serve, Unknown: unknown,
	})
}

func sortChannels(ids []channel.ID) {
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
}

// PokeCompartmentPlan tells one daemon its channel snapshot moved. It carries
// no payload and needs no delivery guarantee: the device also pulls on carrier
// establishment and on its own period, so a lost poke costs latency only.
func (h *Host) PokeCompartmentPlan(daemonID string) {
	h.mu.RLock()
	row := h.daemons[daemonID]
	var carrier *carrierRow
	if row != nil {
		carrier = row.current
	}
	h.mu.RUnlock()
	if carrier == nil || carrier.sealed.Load() {
		return
	}
	_ = carrier.wire.SendSpine(link.SpineFrame{Kind: link.SpineCompartmentPlanPoke})
}

// pokeAllCompartmentPlans fans the poke to every live carrier. Channel-level
// truth changes (a binding flip, a channel gone) move the snapshot of every
// daemon that could be serving it, and this host does not track which ones do.
func (h *Host) pokeAllCompartmentPlans() {
	h.mu.RLock()
	carriers := h.currentCarriersLocked()
	h.mu.RUnlock()
	for _, carrier := range carriers {
		if carrier == nil || carrier.sealed.Load() {
			continue
		}
		_ = carrier.wire.SendSpine(link.SpineFrame{Kind: link.SpineCompartmentPlanPoke})
	}
}

func (h *Host) acceptStreams(carrier *carrierRow) {
	_ = carrier.wire.ServeStreams(func(conn net.Conn, header link.DeviceStreamHeader) {
		h.acceptStream(carrier, conn, header)
	})
	_ = carrier.wire.Close()
}

func (h *Host) acceptStream(
	carrier *carrierRow,
	conn net.Conn,
	header link.DeviceStreamHeader,
) {
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
}

// sealCarrierLocked is the carrier registry's single decision write. h.mu must
// be held. It also takes carrier.mu for the sealed write, so that sealing and
// issuing a physical ticket cannot interleave: whichever wins, the supervisor's
// wait either never sees a ticket or is guaranteed to see it.
func (h *Host) sealCarrierLocked(carrier *carrierRow) bool {
	carrier.mu.Lock()
	if carrier.sealed.Load() {
		carrier.mu.Unlock()
		return false
	}
	carrier.sealed.Store(true)
	carrier.mu.Unlock()
	if row := h.daemons[carrier.daemonID]; row != nil && row.current == carrier {
		row.current = nil
		if !row.tombstone && len(row.diagnostic) == 0 {
			delete(h.daemons, carrier.daemonID)
		}
	}
	return true
}

// beginCarrierShutdown is the one shutdown entry, and it is a decision only.
// When it returns, this carrier and its lanes are gone from every ledger this
// host answers from and the wire is closing — nothing here waits for a lane's
// physical collection.
//
// That collection has an owner: the carrier's supervisor, which is the single
// place that joins these goroutines. Waiting for it here instead would put a
// device's stuck handler in front of revocation, scanning, lease sweeping and
// the accept path, which is how one wedged daemon used to stall the realm.
//
// Idempotent: a second call finds no lanes and a wire already closing.
func (h *Host) beginCarrierShutdown(carrier *carrierRow) {
	if carrier == nil {
		return
	}
	h.mu.Lock()
	h.sealCarrierLocked(carrier)
	h.mu.Unlock()
	carrier.mu.Lock()
	lanes := make([]*serverLane, 0, len(carrier.lanes))
	for _, lane := range carrier.lanes {
		lanes = append(lanes, lane)
	}
	carrier.lanes = make(map[channel.ID]*serverLane)
	carrier.mu.Unlock()
	for _, lane := range lanes {
		lane.retireLogical()
	}
	carrier.stopOnce.Do(func() { _ = carrier.wire.Close() })
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
		// The scan domain is this host's own truth only: registered membranes and
		// the lanes it opened. Compartments the device holds are not in it — the
		// device converges those itself against the snapshot it pulls, so this
		// host never needs to name a coordinate it no longer has a record of.
		carrier.mu.Lock()
		for id := range carrier.lanes {
			coords[id] = struct{}{}
		}
		carrier.mu.Unlock()
		for chID := range coords {
			h.markCoordDirty(carrier, chID)
		}
	}
	h.pokeAllCompartmentPlans()
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
					if carrier.coordTasks[chID] == task {
						delete(carrier.coordTasks, chID)
					}
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
	ctx, cancel := context.WithTimeout(h.ctx, factTimeout)
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
	ctx, cancel := context.WithTimeout(h.ctx, factTimeout)
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

func (h *Host) reconcileCoord(carrier *carrierRow, chID channel.ID) {
	if carrier == nil || carrier.sealed.Load() {
		return
	}
	unlock := carrier.lockCoord(chID)
	defer unlock()
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
	// Unbinding retires the route and nothing else. Whether the device still
	// holds a compartment here is the device's own business: it will see this
	// channel drop out of its next snapshot and retire it itself. The poke that
	// shortens that wait is issued once per scan, not once per coordinate.
	carrier.retireLane(chID)
}

func (h *Host) isBound(
	daemonID string,
	resolve func(context.Context, string) (bool, error),
) (bool, error) {
	ctx, cancel := context.WithTimeout(h.ctx, factTimeout)
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

func (c *carrierRow) lockCoord(chID channel.ID) func() {
	c.mu.Lock()
	gate := c.coordLocks[chID]
	if gate == nil {
		gate = &coordGate{}
		c.coordLocks[chID] = gate
	}
	gate.refs++
	c.mu.Unlock()

	gate.mu.Lock()
	return func() {
		gate.mu.Unlock()
		c.mu.Lock()
		gate.refs--
		if gate.refs == 0 && c.coordLocks[chID] == gate {
			delete(c.coordLocks, chID)
		}
		c.mu.Unlock()
	}
}

func (c *carrierRow) ensureLane(chID channel.ID, membrane membraneRow) {
	c.mu.Lock()
	if c.sealed.Load() {
		c.mu.Unlock()
		return
	}
	if current := c.lanes[chID]; current != nil && !current.stream.Retired() &&
		current.membrane.generation == membrane.generation {
		c.mu.Unlock()
		return
	}
	// The ticket is taken before the open, under the same lock that seals the
	// carrier: a shutdown racing this either wins, and no ticket is issued, or
	// loses, and the supervisor's wait is guaranteed to see this one. It covers
	// the whole span — a failed open returns it below, a successful one hands
	// it to the lane's reader, which is what eventually collects.
	c.physical.Add(1)
	c.mu.Unlock()
	generation := link.NewLaneGeneration()
	ctx, cancel := context.WithTimeout(c.host.ctx, defaultLaneOpenTimeout)
	stream, err := c.wire.OpenLane(ctx, chID, generation)
	cancel()
	if err != nil {
		c.physical.Done()
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
		lane.start(&c.physical)
		stream.RetireLogical()
		return
	}
	old := c.lanes[chID]
	c.lanes[chID] = lane
	c.mu.Unlock()
	lane.start(&c.physical)
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

// LaneAttached answers whether this host has routed this channel to this
// daemon. It is a conjunction of this host's own ledger — a live current
// carrier and a live lane row — and consults nothing the device reported.
//
// Whether the device's compartment finished building is the device's fact, and
// a cached copy of it could only ever be stale: it says "ready" for a
// compartment that may have faulted a millisecond later. Answering from a
// stale copy bought a narrow false positive and cost a permanent false
// negative every time the copy got stuck, so the copy is gone.
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
	carrier.mu.Lock()
	defer carrier.mu.Unlock()
	lane := carrier.lanes[channel.ID(chID)]
	return lane != nil && !lane.stream.Retired()
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
	if carrier != nil {
		h.sealCarrierLocked(carrier)
	}
	h.mu.Unlock()
	if carrier == nil {
		return
	}
	// Best effort, and before the wire closes. The tombstone above is the
	// revocation; whether this frame lands changes nothing about it.
	_ = carrier.wire.SendSpine(link.SpineFrame{
		Kind: link.SpineCarrierReject, Class: link.CarrierTerminal, Reason: "daemon revoked",
	})
	h.recordDiagnostic(daemonID, Diagnostic{
		CarrierGen: carrier.gen, Kind: "revoke", Time: h.now(),
	})
	h.beginCarrierShutdown(carrier)
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
		unlock := carrier.lockCoord(id)
		carrier.retireLane(id)
		unlock()
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
	h.mu.Unlock()
	// Cancelling first is what lets a well-behaved handler leave: every host
	// implementation a lane reader calls receives this context. Close still has
	// no deadline of its own — its whole value is the guarantee that everything
	// this host owns has finished, and a Close that returned without it would
	// be indistinguishable from one that kept it.
	h.cancel()
	for _, carrier := range carriers {
		h.beginCarrierShutdown(carrier)
	}
	h.wg.Wait()
	return nil
}

var _ platform.DaemonRoutes = (*Host)(nil)
