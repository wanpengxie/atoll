package compute

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
)

// laneRPCTimeout is a test seam. Production always leaves it at the protocol
// budget.
var laneRPCTimeout = link.LaneRPCTimeout

func applyFileReplyError(reply *link.FileReply, err error) bool {
	reply.OK = err == nil
	if err == nil {
		return false
	}
	reply.Reason = err.Error()
	if errors.Is(err, accessdoor.ErrMalformedFileCursor) {
		reply.Code = link.FileErrorBadCursor
	}
	return true
}

// compartmentJoinTimeout bounds one compartment's teardown. Every step of that
// teardown runs under it, including the ones that accept no context of their
// own — teardown holds the coordinate out of service while it runs. It is a
// var as a test seam only; production always leaves it at the named budget.
var compartmentJoinTimeout = 30 * time.Second

// planReplyTimeout is a test seam for a peer that remains connected but never
// answers a snapshot pull. Production leaves it at §2.8's named budget.
var planReplyTimeout = compartmentPlanTimeout

// storageOpenTimeout bounds opening a lane's storage sibling. A failed open
// retires the lane, so this is a retry cadence, not a correctness bound.
const storageOpenTimeout = 10 * time.Second

// openStorageStream is the narrow seam through which a lane opens its storage
// sibling. Production always uses the carrier's real open; admission fixtures
// that fake the carrier at the pipe level replace it.
var openStorageStream = func(
	ctx context.Context, carrier *link.ClientCarrier,
	chID channel.ID, gen link.LaneGeneration,
) (*link.LaneStream, error) {
	return carrier.OpenStorage(ctx, chID, gen)
}

// storageStallThreshold is how long a storage call may sit inside its syscall
// before the stall sweep names it. Detection only: the call cannot be
// recalled, and its stream already confines the freeze to storage traffic.
var storageStallThreshold = 30 * time.Second

// computeCloseBudget bounds Run's whole teardown. It is the library's own
// budget — Run has no caller deadline for teardown — and sits under
// cmd/daemon's process watchdog so the library reports and returns before the
// backstop shoots the process.
var computeCloseBudget = 45 * time.Second

// ErrCloseAbandoned is the teardown's honest expiry verdict: the budget ran
// out with workers still uncollected, and the error names them from the stall
// ledger. Waiting longer would collect nothing a wait can collect — the only
// workers that miss this deadline are parked in storage syscalls no
// cancellation reaches, and process death is what actually reclaims them.
var ErrCloseAbandoned = errors.New("compute: close abandoned uncollected workers")

type compartmentManager struct {
	ctx    context.Context
	cfg    Config
	logger *slog.Logger

	mu       sync.Mutex
	closed   bool
	carrier  *link.ClientCarrier
	daemonID string
	root     string
	cells    map[string]*compartment
	terminal chan error
	wg       sync.WaitGroup

	// planWake is a level, not a queue: a poke that arrives while a pull is in
	// flight just means "pull again", and coalescing pokes is correct.
	planWake    chan struct{}
	planReplyMu sync.Mutex
	planWaiter  map[string]chan link.SpineFrame

	// stallMu guards the ledger of storage calls currently inside their
	// syscall. It is a leaf lock: taken around map writes only, never while
	// holding or taking any other lock.
	stallMu sync.Mutex
	stalls  map[*storageStall]struct{}

	// liveWorkers mirrors m.wg for accounting only: a bounded close that gives
	// up must report how many it abandoned, and a WaitGroup cannot be read.
	liveWorkers atomic.Int64
}

func (m *compartmentManager) addWorkers(n int) {
	m.liveWorkers.Add(int64(n))
	m.wg.Add(n)
}

func (m *compartmentManager) workerDone() {
	m.wg.Done()
	m.liveWorkers.Add(-1)
}

// storageStall is one storage call in flight: which channel, which operation,
// which coordinate, since when. The ledger exists because these calls are the
// one thing in the device that cancellation cannot reach — a frozen one must
// be named by the sweep, never hang silently.
type storageStall struct {
	chID  string
	op    string
	coord string
	since time.Time
}

func (m *compartmentManager) beginStorageOp(chID, op, coord string) *storageStall {
	mark := &storageStall{chID: chID, op: op, coord: coord, since: time.Now()}
	m.stallMu.Lock()
	m.stalls[mark] = struct{}{}
	m.stallMu.Unlock()
	return mark
}

func (m *compartmentManager) endStorageOp(mark *storageStall) {
	m.stallMu.Lock()
	delete(m.stalls, mark)
	m.stallMu.Unlock()
}

// sweepStorageStalls names every storage call that has been inside its syscall
// past the threshold. It runs on the plan loop's cadence and repeats while the
// stall lasts — a frozen executor stays visible for as long as it is frozen.
func (m *compartmentManager) sweepStorageStalls() {
	now := time.Now()
	m.stallMu.Lock()
	for mark := range m.stalls {
		if stalled := now.Sub(mark.since); stalled >= storageStallThreshold {
			m.logger.Error("platform.compute.storage_call_stalled",
				"channel", mark.chID, "op", mark.op, "coord", mark.coord,
				"stalled_for", stalled.Round(time.Second).String())
		}
	}
	m.stallMu.Unlock()
}

type compartment struct {
	manager *compartmentManager
	chID    string
	chName  string
	// workspace is the channel directory this coordinate built, kept so the
	// lane can answer link.FileRoot with the value the storage host was
	// actually opened on. The server needs it to turn a device-local absolute
	// path into a channel-relative one, and only this side knows $ATOLL_HOME.
	workspace string

	mu           sync.Mutex
	state        string
	reason       string
	closing      bool
	closeStarted bool
	condemned    bool
	// leaked marks resources a failed rollback or teardown left behind. A
	// leaked coordinate never returns to service: building a second resource
	// set over one that may still be alive is the one thing condemnation
	// exists to prevent. Without the mark, condemnation alone cannot say
	// whether the coordinate is merely retired or actually unsafe.
	leaked  bool
	lane    *clientLane
	pending *clientLane
	// latestLaneGen is the highest generation this coordinate has admitted on
	// the carrier the manager currently holds. It is its own field and never
	// read back out of the slots: a slot empties when a lane retires and while
	// the compartment tears down, and an empty slot must not readmit a
	// generation this coordinate already moved past. It is cleared only when
	// the carrier goes down, because generation identity is really
	// (carrier, lane) and generations minted by different carriers do not
	// order against each other.
	latestLaneGen link.LaneGeneration
	resources     CompartmentResources
	host          *actorhost.HostSupervisor
	outbound      *DaemonOutbound
	runtimeCtx    context.Context
	cancel        context.CancelFunc
	buildDone     chan struct{}
	stopBuild     chan struct{}
	stopOnce      sync.Once
}

func newCompartmentManager(ctx context.Context, cfg Config, logger *slog.Logger) *compartmentManager {
	return &compartmentManager{
		ctx: ctx, cfg: cfg, logger: logger, cells: make(map[string]*compartment),
		terminal:   make(chan error, 1),
		planWake:   make(chan struct{}, 1),
		planWaiter: make(map[string]chan link.SpineFrame),
		stalls:     make(map[*storageStall]struct{}),
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
	m.daemonID = accepted.DaemonID
	m.root = root
	// Surviving compartments announce nothing on a new carrier. The reconcile
	// loop below pulls the snapshot and converges them, which is the same path
	// that runs every period — reconnection is not a special case.
	m.addWorkers(3)
	m.mu.Unlock()
	go func() {
		defer m.workerDone()
		m.readSpine(carrier)
	}()
	go func() {
		defer m.workerDone()
		m.acceptLanes(carrier)
	}()
	go func() {
		defer m.workerDone()
		m.reconcilePlan(carrier)
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
		case link.SpineCompartmentPlanPoke:
			m.wakePlan()
		case link.SpineCompartmentPlanReply:
			m.deliverPlan(frame)
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

// wakePlan raises the pull level. It never blocks and never queues: N pokes
// between two pulls mean exactly what one poke means.
func (m *compartmentManager) wakePlan() {
	select {
	case m.planWake <- struct{}{}:
	default:
	}
}

func (m *compartmentManager) deliverPlan(frame link.SpineFrame) {
	m.planReplyMu.Lock()
	waiter := m.planWaiter[frame.Nonce]
	delete(m.planWaiter, frame.Nonce)
	m.planReplyMu.Unlock()
	if waiter != nil {
		waiter <- frame
	}
}

// reconcilePlan is this device's compartment reconcile loop. It pulls the whole
// authoritative snapshot and converges the local compartment set onto it — the
// same shape actorhost uses for bodies (pull full desired, retire whatever the
// snapshot did not name). The server issues no teardown command, so nothing
// here waits on one, and a snapshot that never arrives leaves every compartment
// exactly where it is.
func (m *compartmentManager) reconcilePlan(carrier *link.ClientCarrier) {
	m.wakePlan()
	ticker := time.NewTicker(compartmentPlanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-carrier.Done():
			return
		case <-ticker.C:
		case <-m.planWake:
		}
		m.sweepStorageStalls()
		plan, ok := m.pullPlanSnapshot(carrier)
		if !ok {
			continue
		}
		m.convergeToPlan(plan)
	}
}

func (m *compartmentManager) pullPlanSnapshot(carrier *link.ClientCarrier) (link.SpineFrame, bool) {
	nonce := uuid.NewString()
	waiter := make(chan link.SpineFrame, 1)
	m.planReplyMu.Lock()
	m.planWaiter[nonce] = waiter
	m.planReplyMu.Unlock()
	drop := func() {
		m.planReplyMu.Lock()
		delete(m.planWaiter, nonce)
		m.planReplyMu.Unlock()
	}
	if err := carrier.SendSpine(link.SpineFrame{
		Kind: link.SpineCompartmentPlanPull, Nonce: nonce,
	}); err != nil {
		drop()
		return link.SpineFrame{}, false
	}
	timer := time.NewTimer(planReplyTimeout)
	defer timer.Stop()
	select {
	case reply := <-waiter:
		return reply, true
	case <-timer.C:
		drop()
		return link.SpineFrame{}, false
	case <-carrier.Done():
		drop()
		return link.SpineFrame{}, false
	case <-m.ctx.Done():
		drop()
		return link.SpineFrame{}, false
	}
}

// convergeToPlan retires every compartment the snapshot did not name.
//
// A channel in Unknown is one the server could not judge this round; it is
// named precisely so that "absent" keeps its single meaning — the channel no
// longer exists — and a compartment is only ever destroyed by that.
func (m *compartmentManager) convergeToPlan(plan link.SpineFrame) {
	named := make(map[string]struct{}, len(plan.Serve)+len(plan.Unknown))
	for _, id := range plan.Serve {
		named[string(id)] = struct{}{}
	}
	for _, id := range plan.Unknown {
		named[string(id)] = struct{}{}
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	// Capture the exact compartments this snapshot condemns. Re-looking them up
	// by coordinate at teardown time would let a rebind that happened in between
	// hand us a newer compartment the snapshot never spoke about.
	stale := make([]*compartment, 0)
	for chID, cell := range m.cells {
		if _, keep := named[chID]; !keep {
			stale = append(stale, cell)
		}
	}
	m.mu.Unlock()
	for _, cell := range stale {
		m.closeExactCompartment(cell)
	}
}

func (m *compartmentManager) acceptLanes(carrier *link.ClientCarrier) {
	_ = carrier.ServeStreams(func(conn net.Conn, header link.DeviceStreamHeader) {
		if header.Kind == link.DeviceStreamCarrier {
			_ = conn.Close()
			_ = carrier.Close()
			return
		}
		if header.Kind == link.DeviceStreamExchange {
			m.acceptExchange(carrier, conn, header)
			return
		}
		if header.Kind == link.DeviceStreamPTY {
			m.acceptPTY(carrier, conn, header)
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

func (m *compartmentManager) acceptExchange(carrier *link.ClientCarrier, conn net.Conn, header link.DeviceStreamHeader) {
	if err := header.Validate(); err != nil {
		_ = conn.Close()
		return
	}
	m.mu.Lock()
	cell := m.cells[string(header.Channel)]
	currentCarrier := m.carrier == carrier
	m.mu.Unlock()
	if !currentCarrier || cell == nil {
		_ = conn.Close()
		return
	}
	cell.mu.Lock()
	lane := cell.lane
	current := lane != nil && lane.stream.Gen == header.LaneGen
	cell.mu.Unlock()
	if !current {
		_ = conn.Close()
		return
	}
	cleanup, ok := lane.trackExchange(conn)
	if !ok {
		_ = conn.Close()
		return
	}
	defer cleanup()
	defer conn.Close()
	serveHostExchange(conn, lane)
}

func serveHostExchange(conn io.ReadWriteCloser, lane *clientLane) {
	fail := func(code string, err error) {
		detail := ""
		if err != nil {
			detail = err.Error()
		}
		_ = link.WriteExchangeControl(conn, link.ExchangeStatus{OK: false, Code: code, Detail: detail})
	}
	var head link.ExchangeHostHeader
	if err := link.ReadExchangeControl(conn, &head); err != nil || head.Path == "" {
		fail("protocol_error", err)
		return
	}
	files, bound := lane.boundResources()
	if !bound || files == nil {
		fail("storage_unavailable", errors.New("compute: storage unavailable"))
		return
	}
	switch head.Mode {
	case access.OpRead:
		handle, err := files.OpenRead(head.Path)
		if err != nil {
			fail("open_failed", err)
			return
		}
		defer handle.Close()
		if err := link.WriteExchangeBytes(conn, handle); err != nil {
			return
		}
		_ = link.WriteExchangeControl(conn, link.ExchangeStatus{OK: true})
	case access.OpWrite:
		handle, err := files.OpenWrite(head.Path)
		if err != nil {
			fail("open_failed", err)
			return
		}
		if err := link.ReadExchangeBytes(handle, conn); err != nil {
			_ = handle.Abort()
			fail("transfer_failed", err)
			return
		}
		if err := handle.Commit(); err != nil {
			fail("commit_failed", err)
			return
		}
		_ = link.WriteExchangeControl(conn, link.ExchangeStatus{OK: true})
	default:
		fail("protocol_error", fmt.Errorf("compute: unsupported exchange mode %q", head.Mode))
	}
}

func (m *compartmentManager) acceptLane(lane *clientLane) {
	chID := string(lane.stream.Channel)
	m.mu.Lock()
	// A lane is admissible only on the carrier this manager currently holds.
	// The redial loop does not join the dead carrier's stream workers, so one
	// that already parsed its header can land here after the next carrier is
	// bound. Turning it away before the compartment lookup also keeps it from
	// conjuring a compartment at a coordinate this device does not serve.
	if m.closed || m.carrier != lane.carrier {
		m.mu.Unlock()
		lane.stream.RetireLogical()
		lane.stream.CollectPhysical()
		return
	}
	cell := m.cells[chID]
	if cell == nil {
		cell = &compartment{
			manager: m, chID: chID, chName: lane.stream.ChannelName, state: "building", stopBuild: make(chan struct{}),
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
	// Streams are accepted in the order the server opened them, but each one's
	// header is parsed by its own worker, so two generations opened close
	// together can arrive here in either order. Installing the older one would
	// retire the generation the server is actually routing on, leaving both
	// ends without a lane until the next authority scan.
	//
	// Lane generations are canonical lowercase UUIDv7 values minted through
	// NewLaneGeneration by the one server process. Their lexical order follows
	// the UUIDv7 timestamp and its monotonic sub-millisecond sequence, so
	// comparing the strings compares the mint order. Admission depends on that
	// ordering, and only within one carrier. An equal generation is refused
	// too: a generation names exactly one stream.
	gen := lane.stream.Gen
	if cell.latestLaneGen != "" && gen <= cell.latestLaneGen {
		cell.mu.Unlock()
		lane.start()
		m.mu.Unlock()
		lane.retireLogical()
		return
	}
	if cell.closing {
		old := cell.pending
		cell.latestLaneGen = gen
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
	cell.latestLaneGen = gen
	cell.lane = lane
	needsBuild := cell.host == nil
	startBuild := needsBuild && cell.buildDone == nil
	if startBuild {
		cell.buildDone = make(chan struct{})
	}
	if !needsBuild {
		m.addWorkers(1)
	}
	// The storage sibling's opener doubles as its reader; its ticket is taken
	// here, under the same m.mu the manager's close checks, so no worker can
	// appear after the final join began.
	m.addWorkers(1)
	cell.mu.Unlock()
	lane.start()
	m.mu.Unlock()
	if old != nil {
		old.retireLogical()
	}
	go func() {
		defer m.workerDone()
		lane.openStorage()
	}()
	if startBuild {
		cell.declare("building", "")
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			lane.retireLogical()
			return
		}
		// The build's ledger ticket is taken under the same m.mu the final
		// join checks: a BuildCompartment that ignores every stop signal is
		// counted and named by the abandonment account instead of outliving a
		// close that answered nil.
		m.addWorkers(1)
		m.mu.Unlock()
		go func() {
			defer m.workerDone()
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
		defer m.workerDone()
		cell.bindLane(lane)
	}()
}

func (m *compartmentManager) carrierDown(exact *link.ClientCarrier) {
	m.mu.Lock()
	if m.carrier != exact {
		m.mu.Unlock()
		return
	}
	m.carrier = nil
	cells := make([]*compartment, 0, len(m.cells))
	for _, cell := range m.cells {
		cells = append(cells, cell)
	}
	m.mu.Unlock()
	for _, cell := range cells {
		cell.mu.Lock()
		lane := cell.lane
		cell.lane = nil
		pending := cell.pending
		cell.pending = nil
		// The watermark went down with the carrier that gave it meaning. The
		// next carrier's generations are minted by a different lane series and
		// are not comparable with this one, so the coordinate starts over.
		cell.latestLaneGen = ""
		cell.mu.Unlock()
		// Same ownership transfer as condemn: nulling cell.lane disarms
		// laneDown's exact-lane guard, so the forwarder unbind happens here —
		// the disconnect window skips quietly instead of failing every pump
		// pass through the dead lane until the next carrier binds.
		if lane != nil {
			lane.retireLogical()
		}
		if pending != nil {
			pending.retireLogical()
		}
	}
	// Closing the carrier closes its session, and with it every stream on it —
	// the physical end is reclaimed right here. What remains is each lane
	// reader noticing, and this is the redial loop: making reconnection wait on
	// a reader that may be parked inside a local storage call would put one
	// stuck compartment in front of the whole device coming back. Those readers
	// already have an owner in m.wg, which the manager joins when it shuts down.
	_ = exact.Close()
}

// closeExactCompartment retires this exact compartment, and only while it is
// still the one installed at its coordinate.
func (m *compartmentManager) closeExactCompartment(cell *compartment) {
	if cell == nil {
		return
	}
	m.mu.Lock()
	current := !m.closed && m.cells[cell.chID] == cell
	m.mu.Unlock()
	if !current {
		return
	}
	m.closeCompartment(cell.chID)
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
		// Nothing here to retire. Convergence is idempotent by construction, so
		// an absent compartment needs no answer and produces no send — which is
		// why no one has to speak for a compartment that does not exist.
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
	m.addWorkers(1)
	m.mu.Unlock()
	go func() {
		defer m.workerDone()
		cell.close()
	}()
}

// close tears the manager down under computeCloseBudget and answers with what
// it could not collect. The budget covers the whole teardown from this call:
// per-cell dismantling spends from its own per-cell budgets as before, and
// whatever remains bounds the final join. Expiry names the abandoned workers
// from the stall ledger instead of pretending completion — the account is the
// guarantee, and process death is what actually reclaims a reader parked in a
// syscall cancellation cannot reach.
func (m *compartmentManager) close() error {
	deadline := time.Now().Add(computeCloseBudget)
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return m.joinWorkers(deadline)
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
	return m.joinWorkers(deadline)
}

func (m *compartmentManager) joinWorkers(deadline time.Time) error {
	// The watcher outlives an expired budget: it is the one waiter the
	// WaitGroup API requires, and it parks harmlessly until the wedged worker
	// — or the process — goes away.
	joined := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(joined)
	}()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-joined:
		return nil
	case <-timer.C:
	}
	stalled := m.describeStalls()
	m.logger.Error("platform.compute.close_abandoned",
		"workers", m.liveWorkers.Load(), "stalled_storage_calls", stalled)
	return fmt.Errorf("%w: %d workers still out; stalled storage calls: %s",
		ErrCloseAbandoned, m.liveWorkers.Load(), stalled)
}

// describeStalls renders the stall ledger for the abandonment account: every
// storage call still inside its syscall, by channel, operation, coordinate,
// and age.
func (m *compartmentManager) describeStalls() string {
	now := time.Now()
	m.stallMu.Lock()
	defer m.stallMu.Unlock()
	if len(m.stalls) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(m.stalls))
	for mark := range m.stalls {
		parts = append(parts, fmt.Sprintf("%s %s %s for %s",
			mark.chID, mark.op, mark.coord, now.Sub(mark.since).Round(time.Second)))
	}
	sort.Strings(parts)
	return strings.Join(parts, "; ")
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
			// The declared state has no reader in production; the log is what
			// makes a compartment that fails to build every round visible.
			c.manager.logger.Warn("platform.compute.compartment_build_failed",
				"channel", c.chID, "err", err,
				"retry_in", backoff.Round(time.Second).String())
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
	channelsRoot := filepath.Join(root, "channels")
	if err := ensureDirectory(channelsRoot); err != nil {
		return err
	}
	workspace, err := coordinatePath(channelsRoot, c.chName)
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
	c.mu.Lock()
	c.workspace = workspace
	c.mu.Unlock()
	if resources.Factories == nil {
		var closeErr error
		if resources.Close != nil {
			closeErr = resources.Close()
		}
		if closeErr != nil {
			c.condemnLeaked("compartment resource rollback failed: " + closeErr.Error())
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
			c.condemnLeaked("compartment rollback failed: " + rollbackErr.Error())
		}
		return errors.Join(err, rollbackErr)
	}
	c.mu.Lock()
	if c.closing || c.condemned {
		c.mu.Unlock()
		rollbackErr := rollbackCompartment(host, outbound, cancel, resources)
		if rollbackErr != nil {
			c.condemnLeaked("compartment rollback failed: " + rollbackErr.Error())
		}
		return errors.Join(errors.New("compute: compartment closed during build"), rollbackErr)
	}
	c.resources, c.runtimeCtx, c.cancel = resources, runtimeCtx, cancel
	c.host, c.outbound = host, outbound
	lane := c.lane
	c.state, c.reason = "ready", ""
	c.mu.Unlock()
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

// withinJoinBudget runs a teardown step that takes no context under the
// compartment's join budget.
//
// The step keeps running if the budget expires — nothing here can interrupt a
// call that does not accept cancellation — but the teardown stops waiting and
// says which step it gave up on. Teardown holds this coordinate out of service
// while it runs, so a step outside the budget stalls the coordinate for as long
// as it takes, which is exactly what the budget exists to prevent.
func withinJoinBudget(ctx context.Context, step string, run func() error) error {
	done := make(chan error, 1)
	go func() { done <- run() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("compute: %s exceeded the compartment join budget", step)
	}
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
		rollbackErr = errors.Join(rollbackErr,
			withinJoinBudget(ctx, "outbound residual close", outbound.CloseResidual))
	}
	if cancel != nil {
		cancel()
	}
	if resources.Close != nil {
		rollbackErr = errors.Join(rollbackErr,
			withinJoinBudget(ctx, "compartment resource close", resources.Close))
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
	// Clearing c.lane above is exactly what makes laneDown's exact-lane guard
	// skip, so the forwarder unbind laneDown owns falls to this path: a
	// condemned coordinate's forwarder must not keep pulling through a dead
	// lane it also keeps alive.
	if lane != nil {
		lane.retireLogical()
	}
	if pending != nil {
		pending.retireLogical()
	}
	c.manager.logger.Error("platform.compute.compartment_condemned",
		"channel", c.chID, "reason", reason)
	c.declare("fault", "condemned: "+reason)
}

// condemnLeaked condemns this coordinate and records that the resources it
// held were not provably released. reclaimAfterBuild consults the mark: a
// coordinate whose build settled clean is freed, a leaked one stays out of
// service until the process restarts.
func (c *compartment) condemnLeaked(reason string) {
	c.mu.Lock()
	c.leaked = true
	c.mu.Unlock()
	c.condemn(reason)
}

func (c *compartment) bindLane(lane *clientLane) {
	c.mu.Lock()
	if c.closing || c.condemned || c.lane != lane || c.host == nil {
		c.mu.Unlock()
		return
	}
	host, outbound := c.host, c.outbound
	resources := c.resources
	workspace := c.workspace
	c.mu.Unlock()
	lane.setHost(host)
	lane.mu.Lock()
	lane.local = resources.LocalFileOpener
	lane.workspace = workspace
	lane.outbound = outbound
	lane.bound = true
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
	// The forwarder must not keep pulling through the retired lane: unbinding
	// lets the pump skip quietly until the next lane binds, instead of failing
	// every pass through a dead reference it also keeps alive. The exact-lane
	// guard above is what makes this safe against a replacement: a newer
	// lane's binding never gets overwritten, because its predecessor's
	// laneDown already returned at the guard.
}

// declare records this compartment's state locally and sends nothing. The
// state is this device's own physical fact; the server routes by its own
// ledger and never consults it, so there is no report, no acknowledgement, and
// no ordering obligation between compartments.
func (c *compartment) declare(state, reason string) {
	c.mu.Lock()
	c.state, c.reason = state, reason
	c.mu.Unlock()
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
			c.manager.mu.Lock()
			if !c.manager.closed {
				// The reclaimer is the runtime arm only — during shutdown the
				// manager owns every remaining coordinate and process exit is
				// what reclaims the build — and it holds a ledger ticket for
				// the same reason the build does: whoever waits on the wedged
				// build is a live worker the close account must see.
				c.manager.addWorkers(1)
				go func() {
					defer c.manager.workerDone()
					c.reclaimAfterBuild(buildDone)
				}()
			}
			c.manager.mu.Unlock()
			return
		}
	}
	c.mu.Lock()
	outbound, host, cancel := c.outbound, c.host, c.cancel
	resources := c.resources
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
		closeErr = errors.Join(closeErr,
			withinJoinBudget(ctx, "outbound residual close", outbound.CloseResidual))
	}
	if cancel != nil {
		cancel()
	}
	if resources.Close != nil {
		closeErr = errors.Join(closeErr,
			withinJoinBudget(ctx, "compartment resource close", resources.Close))
	}
	if lane != nil {
		lane.retireLogical()
	}
	if closeErr != nil {
		c.condemnLeaked(closeErr.Error())
		return
	}
	c.forget()
}

// forget releases this coordinate once nothing this compartment held can still
// be alive. Nothing is reported: the server holds no projection of this
// compartment, so there is no row to delete and no flag to clear, and this
// removal races with nothing on the other end. Only the exact object leaves
// the table: a replacement installed at the same coordinate meanwhile is
// someone else's to retire.
func (c *compartment) forget() {
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

// reclaimAfterBuild frees a coordinate that close gave up on because its build
// overran the join budget. Condemnation already holds new lanes off the
// coordinate, so all that remains is deciding what the overrunning build left
// behind — and only the build settling can answer that. Once it does, the
// build has either installed nothing, rolled back cleanly, installed a
// resource set nobody has torn down, or recorded a leak. Rerunning close under
// a fresh budget covers the first three — every teardown step is nil-guarded,
// so a build that left nothing behind falls straight through to the release —
// and the leak mark keeps the last one out of service: a coordinate that could
// not release its resources must never build a second set over them.
func (c *compartment) reclaimAfterBuild(buildDone <-chan struct{}) {
	select {
	case <-buildDone:
	case <-c.manager.ctx.Done():
		return
	}
	c.mu.Lock()
	leaked := c.leaked
	c.mu.Unlock()
	if leaked {
		return
	}
	c.manager.mu.Lock()
	closed := c.manager.closed
	c.manager.mu.Unlock()
	if closed {
		// Shutdown owns every remaining coordinate; a late reclaim would race
		// its sweep for no benefit.
		return
	}
	c.close()
}

type clientLane struct {
	manager *compartmentManager
	carrier *link.ClientCarrier
	stream  *link.LaneStream

	mu          sync.Mutex
	startOnce   sync.Once
	storageOnce sync.Once
	retire      sync.Once
	retired     bool
	onRetire    func(*clientLane)
	// storageStream is the storage sibling this device opened for this lane's
	// generation. It has no lifecycle of its own: it dies with the lane and
	// its death retires the lane.
	storageStream *link.LaneStream
	exchanges     map[uint64]net.Conn
	nextExchange  uint64
	exchangeWG    sync.WaitGroup
	pending       map[string]chan link.LaneFrame
	replan        chan struct{}
	host          *actorhost.HostSupervisor
	actorSession  *laneSession
	// bound records that bindLane has installed this lane's resources. It is
	// its own field rather than a nil check on the two below, because a bound
	// compartment may legitimately have no storage host configured — that is a
	// standing verdict, while not-yet-bound is the absence of one.
	bound bool
	local LocalFileOpener
	// workspace answers link.FileRoot. It is installed with local, by the same
	// bind, so a lane never reports a root for a storage host it does not hold.
	workspace string
	outbound  *DaemonOutbound
}

type laneSession struct{ lane *clientLane }

func (s *laneSession) IsCurrent() bool       { return s != nil && s.lane.current() }
func (s *laneSession) Done() <-chan struct{} { return s.lane.stream.Done() }
func (s *laneSession) OpenActorStream(
	ctx context.Context, id actor.ActorID, key actorhost.AttemptKey,
) (laneActorStream, error) {
	s.lane.mu.Lock()
	host, files := s.lane.host, s.lane.local
	s.lane.mu.Unlock()
	client := &link.ClientActorLane{
		Carrier: s.lane.carrier, Lane: s.lane.stream, Host: host,
		Files: files, DialExchange: s.lane.openExchange,
		Logger: s.lane.manager.logger,
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
		exchanges: make(map[uint64]net.Conn),
	}
	stream.SetRetire(func(*link.LaneStream) { lane.markStreamRetired() })
	lane.actorSession = &laneSession{lane: lane}
	return lane
}

func (l *clientLane) trackExchange(conn net.Conn) (func(), bool) {
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

var openClientExchange = func(
	ctx context.Context, carrier *link.ClientCarrier, chID channel.ID, generation link.LaneGeneration,
) (net.Conn, error) {
	return carrier.OpenExchange(ctx, chID, generation)
}

func (l *clientLane) openExchange(ctx context.Context) (io.ReadWriteCloser, error) {
	if l == nil || l.carrier == nil || l.stream == nil {
		return nil, link.ErrInvalidPhysicalChild
	}
	conn, err := openClientExchange(ctx, l.carrier, l.stream.Channel, l.stream.Gen)
	if err != nil {
		return nil, err
	}
	cleanup, ok := l.trackExchange(conn)
	if !ok {
		_ = conn.Close()
		return nil, link.ErrInvalidPhysicalChild
	}
	return &trackedClientExchange{ReadWriteCloser: conn, cleanup: cleanup}, nil
}

type trackedClientExchange struct {
	io.ReadWriteCloser
	once    sync.Once
	cleanup func()
}

func (c *trackedClientExchange) Close() error {
	err := c.ReadWriteCloser.Close()
	c.once.Do(c.cleanup)
	return err
}

func (l *clientLane) setHost(host *actorhost.HostSupervisor) {
	l.mu.Lock()
	l.host = host
	l.mu.Unlock()
}

// boundResources reads the resources bindLane installs on this lane, and
// whether it has run at all. The reader is running before they are installed —
// a lane starts reading the moment it is admitted, while bindLane runs
// afterwards (immediately for an already-built compartment, only after the
// build succeeds for a new one) — so every read from the reader's side is
// concurrent with that write and must take the lane's lock.
func (l *clientLane) boundResources() (LocalFileOpener, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.local, l.bound
}

func (l *clientLane) boundWorkspace() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.workspace
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
		l.manager.addWorkers(2)
		go func() {
			defer l.manager.workerDone()
			l.readLoop()
		}()
		go func() {
			defer l.manager.workerDone()
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
		storage := l.storageStream
		exchanges := l.exchanges
		pending := l.pending
		l.pending = make(map[string]chan link.LaneFrame)
		l.exchanges = make(map[uint64]net.Conn)
		onRetire := l.onRetire
		l.mu.Unlock()
		// The pair shares one generation: the lane's retirement takes its
		// storage sibling with it, and the sibling's reader collects the
		// physical end after it wakes.
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
		case link.LanePlanReply, link.LaneFileReply:
			l.deliver(frame.RequestID, frame)
		case link.LanePlanPoke:
			select {
			case l.replan <- struct{}{}:
			default:
			}
		default:
			// Storage instructions belong on the storage sibling now; one
			// arriving here is a protocol violation like any other unknown
			// kind. This reader only dispatches and pokes — it never executes,
			// so nothing on the lane can block behind a filesystem call.
			return
		}
	}
}

// openStorage opens this lane's storage sibling and becomes its reader. A
// failed open retires the lane: half a pair is not a lane — without its
// sibling the server's storage face answers NotReady forever while the lane
// looks healthy, and retiring lets the server's next scan reopen a whole pair.
func (l *clientLane) openStorage() {
	l.storageOnce.Do(func() {
		ctx, cancel := context.WithTimeout(l.manager.ctx, storageOpenTimeout)
		stream, err := openStorageStream(ctx, l.carrier, l.stream.Channel, l.stream.Gen)
		cancel()
		if err != nil {
			l.retireLogical()
			return
		}
		l.mu.Lock()
		if l.retired {
			l.mu.Unlock()
			stream.RetireLogical()
			stream.CollectPhysical()
			return
		}
		l.storageStream = stream
		l.mu.Unlock()
		l.storageLoop(stream)
	})
}

// storageLoop executes the server's storage instructions. This is the one
// reader in the device that legitimately blocks: file requests end in
// filesystem syscalls no context can recall. That is exactly why they ride
// their own stream — a dead disk freezes this loop and nothing else; plan
// replies and pokes keep flowing on the lane — and why every operation is
// marked in the manager's stall ledger while it runs: a frozen executor must
// be named, never silent. Exit retires the whole pair.
func (l *clientLane) storageLoop(stream *link.LaneStream) {
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
		case link.LaneFileRequest:
			request := frame.FileRequest
			reply := &link.FileReply{RequestID: frame.RequestID}
			files, bound := l.boundResources()
			if request == nil {
				reply.Reason = "compute: malformed file request"
			} else if !bound || files == nil {
				reply.Reason = "compute: channel files unavailable"
			} else {
				mark := l.manager.beginStorageOp(string(stream.Channel), request.Op, request.Path)
				switch request.Op {
				case link.FileCreate:
					reply.OK = files.Create(request.Path, request.NodeType) == nil
					if !reply.OK {
						reply.Reason = "compute: create failed"
					}
				case link.FileDelete:
					err := files.Delete(request.Path)
					applyFileReplyError(reply, err)
				case link.FileStat:
					info, found, err := files.Stat(request.Path)
					reply.OK, reply.Found = err == nil, found
					if err != nil {
						reply.Reason = err.Error()
					} else if found {
						reply.Entries = []link.FileEntry{{Path: info.Path, NodeType: info.NodeType, Size: info.Size, ModifiedAt: info.ModifiedAt}}
					}
				case link.FileRoot:
					// Answered from the value bindLane installed, not from a
					// re-derivation of $ATOLL_HOME/daemons/<id>/channels/<name>:
					// a second copy of that rule drifts, and the drift shows up
					// as an authorization decision quietly using the wrong root.
					root := l.boundWorkspace()
					reply.OK = root != ""
					reply.Root = root
					if !reply.OK {
						reply.Reason = "compute: channel workspace unavailable"
					}
				case link.FileList:
					rows, next, err := files.List(request.Path, request.Limit, request.Cursor)
					if !applyFileReplyError(reply, err) {
						reply.Next = next
						for _, row := range rows {
							reply.Entries = append(reply.Entries, link.FileEntry{Path: row.Path, NodeType: row.NodeType, Size: row.Size, ModifiedAt: row.ModifiedAt})
						}
					}
				}
				l.manager.endStorageOp(mark)
			}
			if stream.Send(link.LaneFrame{Kind: link.LaneFileReply, RequestID: frame.RequestID, FileReply: reply}) != nil {
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
	if frame.FileRequest != nil {
		frame.FileRequest.RequestID = id
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
