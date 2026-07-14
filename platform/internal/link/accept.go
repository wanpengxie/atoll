package link

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/ipc"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// errUndeclaredActor is the resolve verdict for an actor stream whose lease id
// is not in the link's attach declaration set (an actor the daemon never
// declared may not bind an embodiment).
var errUndeclaredActor = errors.New("link: actor not in attach declarations")

type declarationSnapshotEntry struct {
	Kind     actor.Kind
	Binding  actor.Binding
	Epoch    int64
	DaemonID string
}

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

// attachHandshakeTimeout bounds one actor stream's connect-in handshake. A
// peer that opens a stream but never sends the ipc handshake must not pin the
// Attach goroutine forever; the substrate self-guards this step, the host only
// supplies the deadline.
const attachHandshakeTimeout = 10 * time.Second

// attachRejectDrain gives the already-written attach_reply one scheduler turn
// to reach the peer's control dispatcher before the rejected session is killed.
// The write itself is synchronous, but yamux session teardown can otherwise win
// the peer's read-loop select and erase the precise rejection reason.
const attachRejectDrain = 25 * time.Millisecond

// Acceptor is the home end of the link: it upgrades attaching daemon
// connections, registers declared actors into membership, and binds each
// actor stream through the runtime prepare/commit port admission (the stream runs native ipc, so a remote cell
// is indistinguishable from a local one — zero translation). It judges liveness
// via the per-link lease. It owns NO business logic — Writer/Runtime/Membership
// are injected capabilities of the home.
type Acceptor struct {
	minter       harness.Minter
	access       accessdoor.AccessMinter
	sched        schedule.Minter
	runtime      *actorrt.Runtime
	registry     storespec.Registry
	composition  storespec.CompositionReader
	declarations DeclarationCoordinator
	channelID    channel.ID
	logger       *slog.Logger
	leasePing    time.Duration
	leaseTTL     time.Duration

	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	admissionMu sync.Mutex
	closed      bool
	closeOnce   sync.Once
	closeDone   chan struct{}
	leaked      atomic.Int64

	// attached is the live attach refcount per compute id (daemon). A daemon is
	// "online" (attached) iff its count > 0. Refcount, not bool, so an
	// overlapping reconnect (old link tearing down after the new one attached)
	// does not flap the daemon offline. Volatile runtime state — never persisted.
	attachedMu sync.Mutex
	attached   map[string]int

	// cancelReq is the home's injected KindCancelRequest handler (the caller-side
	// upstream cancel: a daemon-hosted caller abandoning its own outbound request).
	// Passed into runtime port preparation as onCancelRequest — the link
	// layer holds no request-lookup logic (the closure is Home's).
	cancelReq func(actor.ActorID, message.ID)

	// storageControl is the home-side handler for the daemon-INITIATED half of
	// the §4.7 control-RPC plane (Committed/ReclaimAck/ReconcilePull) — nil
	// means those three frames get an honest error reply (no storage host
	// wired on this channel yet), never a silent drop (§4.7's own frames are
	// request/response; a daemon retrying forever against a home that just
	// swallows the frame would be a worse failure mode than an explicit nak).
	storageControl  StorageHostControl
	planProvider    PlanProvider
	daemonAuthority DaemonAuthority
	actorLock       func(actor.ActorID) func()
	portIndex       PortIndex
	nextPortOwner   atomic.Uint64

	// pendingAlloc correlates AllocRequest (home→daemon) with its AllocReply —
	// a home-initiated leg of the control-RPC plane (the daemon-initiated
	// frames — Committed/ReclaimAck/ReconcilePull — are correlated on the
	// Dialer side instead).
	pendingAlloc *pendingReplies[AllocReply]

	// pendingReclaim correlates ReclaimRequest (home→daemon) with its
	// ReclaimReply — the second home-initiated leg (期11 review §2.5 #B, the
	// content-less create loser's synchronous coord reclaim), same shape as
	// pendingAlloc.
	pendingReclaim *pendingReplies[ReclaimReply]

	// lane is §5's resource lane bookkeeping (per-daemon yamux sessions +
	// the Token-keyed transfer registry) — see lanecontrol.go.
	lane *laneState

	// links is the per-compute revocation handle table (§8.3 KickDaemon): every
	// link whose daemonID is known is registered here at runLink ENTRY — before
	// the attach frame is even read, not only after a successful attach — so a
	// Kick reaches a connection still mid-handshake (the T2→T3 half-attach
	// window) too. Multiple entries under the same id are normal during an
	// overlapping reconnect (old link tearing down as the new one comes up);
	// Kick tears down every one of them. Every key is an authenticated daemon id.
	linksMu sync.Mutex
	links   map[string][]*linkHandle

	// slots is the one-incumbent state machine per compute. A link must reserve
	// candidate before declaration writes and is promoted after all declaration
	// writers have exited, immediately before its accepted reply is put on the
	// wire. A failed reply write kills the promoted incumbent.
	slotMu sync.Mutex
	slots  map[string]*incumbentSlot
}

type incumbentState uint8

const (
	incumbentCandidate incumbentState = iota + 1
	incumbentActive
)

type incumbentSlot struct {
	link     *linkSession
	state    incumbentState
	writers  int
	accepted bool
	dead     bool
	deadFlag atomic.Bool
}

type incumbentPin struct {
	a       *Acceptor
	compute string
	link    *linkSession
	slot    *incumbentSlot
	once    sync.Once
}

// linkHandle is the complete graceful-close capability for one link. Keeping
// this as a pointer gives the registry one stable identity and, more
// importantly, makes every graceful initiator share the same closeOnce and the
// same ordered pipeline.
type linkHandle struct {
	closeOnce    sync.Once
	gate         *actorGate
	invalidate   func()
	waitWorkers  func()
	takePorts    func() []actorrt.Incarnation
	quietPort    func(actorrt.Incarnation)
	closeCarrier func()
	sendControl  func([]byte) error
}

func (h *linkHandle) closeQuietly() {
	if h == nil {
		return
	}
	h.closeOnce.Do(func() {
		// Publication fence: after seal returns no actor handshake can newly
		// enter the set whose successful commits publish into the port index.
		h.gate.seal()
		h.invalidate()
		h.waitWorkers()
		for _, inc := range h.takePorts() {
			h.quietPort(inc)
		}
		h.closeCarrier()
	})
}

func (h *linkHandle) send(raw []byte) error {
	if h == nil || h.sendControl == nil {
		return errors.New("link: connection is closed")
	}
	return h.sendControl(raw)
}

// Config configures an Acceptor. Auth is the app layer's concern — Serve
// receives a pre-authenticated daemonID. LeasePing/LeaseTTL default to the
// centralised constants (10s / 30s); zero means default (tests may shorten).
type Config struct {
	Minter harness.Minter
	// Access / Schedule are the plane-2 and time-axis minters the home welds a
	// remote port's incarnation onto (the wire arms of Caps.Access/State/Schedule).
	// Same source the cell path draws from (cs.Access, the schedule engine Minter),
	// so a daemon-hosted cell's off-log / timer capability is behaviourally
	// identical to a local one (transport neutrality).
	Access   accessdoor.AccessMinter
	Schedule schedule.Minter
	Runtime  *actorrt.Runtime
	// Registry and Composition are required read authorities used to revalidate
	// every actor handshake against the just-committed declaration snapshot.
	Registry     storespec.Registry
	Composition  storespec.CompositionReader
	Declarations DeclarationCoordinator
	ChannelID    channel.ID
	Logger       *slog.Logger
	LeasePing    time.Duration
	LeaseTTL     time.Duration
	// CancelRequest (optional) is the home's injected handler for a KindCancelRequest
	// frame (a daemon-hosted caller abandoning one of its OWN outbound requests). It
	// is passed the connection's authenticated bound id + the request id; the home
	// closure reverse-resolves the request's target from the log and validates the
	// sender before firing Home.CancelRequest (non-self-report; the four failure
	// branches — not found / non-request / empty audience / sender mismatch — all
	// silently drop + log, best-effort no-ack semantics). nil → inbound
	// cancel_request is dropped (no consumer).
	CancelRequest func(actor.ActorID, message.ID)
	// StorageHostControl (optional) handles the daemon-initiated half of the
	// §4.7 storage control-RPC plane (Committed/ReclaimAck/ReconcilePull).
	// nil → those three frames get an honest error reply.
	StorageHostControl StorageHostControl
	PlanProvider       PlanProvider
	DaemonAuthority    DaemonAuthority
	ActorLock          func(actor.ActorID) func()
	PortIndex          PortIndex
}

type PlanProvider interface {
	Plan(context.Context, string) ([]platform.PlanActor, error)
}

// DeclarationCoordinator is the Home-owned S2 seam. It brackets the channel
// transaction and body actions with actor lifecycle gates, returning only the
// declarations that may be published to the link snapshot.
type DeclarationCoordinator interface {
	ApplyComputeDeclaration(context.Context, PortOwner, string, []storespec.ComputeDeclaration) ([]storespec.ComputeDeclaration, error)
}

// DaemonAuthority holds the app-owned per-daemon keyed lock while it freshly
// validates the daemon and its binding to this channel.
type DaemonAuthority interface {
	LockAndValidate(context.Context, string, channel.ID) (release func(), err error)
}

type PortOwner uint64

// PortIndex is Home's authoritative cross-link remote-port index. Every method
// is pointer-conditional and performs only an index pointer swap while holding
// its own leaf lock.
type PortIndex interface {
	Register(PortOwner, actorrt.Incarnation)
	Remove(PortOwner, actorrt.Incarnation)
	Take(PortOwner, actor.ActorID) (actorrt.Incarnation, bool)
	TakeOwner(PortOwner) []actorrt.Incarnation
}

// NewAcceptor builds an Acceptor only from a complete authority set.
func NewAcceptor(cfg Config) (*Acceptor, error) {
	switch {
	case cfg.Runtime == nil:
		return nil, errors.New("link: runtime is required")
	case cfg.Declarations == nil:
		return nil, errors.New("link: declaration coordinator is required")
	case cfg.Composition == nil:
		return nil, errors.New("link: composition reader is required")
	case cfg.Registry == nil:
		return nil, errors.New("link: registry is required")
	case cfg.DaemonAuthority == nil:
		return nil, errors.New("link: daemon authority is required")
	case cfg.ActorLock == nil:
		return nil, errors.New("link: actor lock is required")
	case cfg.PortIndex == nil:
		return nil, errors.New("link: port index is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Acceptor{
		minter:          cfg.Minter,
		access:          cfg.Access,
		sched:           cfg.Schedule,
		runtime:         cfg.Runtime,
		registry:        cfg.Registry,
		composition:     cfg.Composition,
		declarations:    cfg.Declarations,
		channelID:       cfg.ChannelID,
		logger:          logger,
		leasePing:       cfg.LeasePing,
		leaseTTL:        cfg.LeaseTTL,
		ctx:             ctx,
		cancel:          cancel,
		closeDone:       make(chan struct{}),
		attached:        map[string]int{},
		cancelReq:       cfg.CancelRequest,
		storageControl:  cfg.StorageHostControl,
		planProvider:    cfg.PlanProvider,
		daemonAuthority: cfg.DaemonAuthority,
		actorLock:       cfg.ActorLock,
		portIndex:       cfg.PortIndex,
		pendingAlloc:    newPendingReplies[AllocReply](),
		pendingReclaim:  newPendingReplies[ReclaimReply](),
		lane:            newLaneState(),
		links:           map[string][]*linkHandle{},
		slots:           map[string]*incumbentSlot{},
	}, nil
}

func (a *Acceptor) enterDeclarationWriter(compute string, lc *linkSession) (*incumbentPin, string) {
	a.slotMu.Lock()
	defer a.slotMu.Unlock()
	s := a.slots[compute]
	if s == nil {
		s = &incumbentSlot{link: lc, state: incumbentCandidate, writers: 1}
		a.slots[compute] = s
		return &incumbentPin{a: a, compute: compute, link: lc, slot: s}, ""
	}
	if s.link != lc || s.dead {
		return nil, "compute_busy"
	}
	if s.state == incumbentCandidate {
		return nil, "duplicate_attach"
	}
	s.writers++
	return &incumbentPin{a: a, compute: compute, link: lc, slot: s}, ""
}

func (a *Acceptor) enterPortWriter(compute string, lc *linkSession) (*incumbentPin, error) {
	a.slotMu.Lock()
	defer a.slotMu.Unlock()
	s := a.slots[compute]
	if s == nil || s.link != lc || s.state != incumbentActive || !s.accepted || s.dead {
		return nil, errors.New("link: actor attach is not on the active incumbent")
	}
	s.writers++
	return &incumbentPin{a: a, compute: compute, link: lc, slot: s}, nil
}

func (p *incumbentPin) valid() bool {
	return p != nil && p.slot != nil && !p.slot.deadFlag.Load()
}

func (p *incumbentPin) finish(ok bool) {
	if p == nil {
		return
	}
	p.once.Do(func() {
		p.a.slotMu.Lock()
		s := p.a.slots[p.compute]
		kill := false
		if s != nil && s.link == p.link {
			if s.writers > 0 {
				s.writers--
			}
			if !ok {
				s.dead = true
				s.deadFlag.Store(true)
				kill = true
			}
			if s.dead && s.writers == 0 {
				delete(p.a.slots, p.compute)
			}
		}
		p.a.slotMu.Unlock()
		if kill {
			p.a.afterOwned(attachRejectDrain, func() { p.link.kill("attach_rejected", nil) })
		}
	})
}

func (a *Acceptor) acceptIncumbent(compute string, lc *linkSession) bool {
	a.slotMu.Lock()
	defer a.slotMu.Unlock()
	s := a.slots[compute]
	if s == nil || s.link != lc || s.dead {
		return false
	}
	s.accepted = true
	if s.state == incumbentCandidate && s.writers == 0 {
		s.state = incumbentActive
		return true
	}
	return s.state == incumbentActive
}

// publishAcceptedAttach linearizes local incumbent activation before the
// success reply is externally observable. The callback is the actual wire
// write; keeping that boundary explicit gives the ordering invariant a direct,
// deterministic test instead of relying on scheduler luck in an end-to-end
// attach race.
func (a *Acceptor) publishAcceptedAttach(compute string, lc *linkSession, sendAccepted func() error) bool {
	if !a.acceptIncumbent(compute, lc) {
		a.failIncumbent(compute, lc, "attach_candidate_deposed")
		return false
	}
	if err := sendAccepted(); err != nil {
		a.logger.Info("link.attach_reply_failed", "compute", compute, "err", err)
		a.failIncumbent(compute, lc, "attach_reply_failed")
		return false
	}
	return true
}

func (a *Acceptor) failIncumbent(compute string, lc *linkSession, cause string) {
	a.slotMu.Lock()
	s := a.slots[compute]
	if s != nil && s.link == lc {
		s.dead = true
		s.deadFlag.Store(true)
		if s.writers == 0 {
			delete(a.slots, compute)
		}
	}
	a.slotMu.Unlock()
	lc.kill(cause, nil)
}

func (a *Acceptor) markIncumbentDead(lc *linkSession) {
	a.slotMu.Lock()
	for id, s := range a.slots {
		if s.link != lc {
			continue
		}
		s.dead = true
		s.deadFlag.Store(true)
		if s.writers == 0 {
			delete(a.slots, id)
		}
	}
	a.slotMu.Unlock()
}

// registerLink adds this link's handle under id — called at runLink entry
// (daemonID known, attach not yet processed), not at markAttached time.
func (a *Acceptor) registerLink(id string, h *linkHandle) {
	if id == "" {
		return
	}
	a.linksMu.Lock()
	a.links[id] = append(a.links[id], h)
	a.linksMu.Unlock()
}

// deregisterLink removes exactly this link's handle (pointer identity, not
// a wholesale clear) — an overlapping reconnect leaves the other entry intact.
func (a *Acceptor) deregisterLink(id string, target *linkHandle) {
	if id == "" {
		return
	}
	a.linksMu.Lock()
	hs := a.links[id]
	for i, h := range hs {
		if h == target {
			hs = append(hs[:i], hs[i+1:]...)
			break
		}
	}
	if len(hs) == 0 {
		delete(a.links, id)
	} else {
		a.links[id] = hs
	}
	a.linksMu.Unlock()
}

// KickDaemon closes every link currently registered under computeID — the
// substrate half of a revocation (S3 §8.3). It is a snapshot-then-act: the
// handle slice is copied under the lock, then each handle is torn down OUTSIDE
// it (seal/join/quiet first — kick is a voluntary teardown, not a death edge —
// then Close through the existing funnel), so a concurrent runLink deregistration
// never deadlocks against it. Returns the number of links closed. No
// generation/tombstone bookkeeping (S-P21 拍定 A): a residual pre-auth
// connection that has not registered yet is closed by the app-side convergence
// loop (`while IsAttached { Kick }`, §6), not by a shadow revoked-set here.
func (a *Acceptor) KickDaemon(computeID string) int {
	a.linksMu.Lock()
	hs := append([]*linkHandle(nil), a.links[computeID]...)
	a.linksMu.Unlock()
	for _, h := range hs {
		h.closeQuietly()
	}
	// Loud on every call (not edge-gated): each call is itself a discrete
	// revocation act, not a repeating steady-state poll — the only way to
	// reconstruct "who revoked which daemon and how many links it actually
	// closed" from slog. count==0 (nothing was registered under this id) is
	// still worth a line — it says the revocation found no live link, not
	// that nothing happened.
	a.logger.Info("link.kick_daemon", "compute", computeID, "closed", len(hs))
	return len(hs)
}

// markAttached / markDetached / IsAttached are the L0 link-attachment read
// seam: the Acceptor authoritatively holds which computes have a live attach
// right now (it owns the connections + lease). markAttached is called once
// per accepted link (after attach success); markDetached once when that link
// tears down (peer gone / lease expiry / Close).
func (a *Acceptor) markAttached(id string) {
	a.attachedMu.Lock()
	a.attached[id]++
	a.attachedMu.Unlock()
}

func (a *Acceptor) markDetached(id string) {
	a.attachedMu.Lock()
	a.attached[id]--
	zeroed := a.attached[id] <= 0
	if zeroed {
		delete(a.attached, id)
	}
	a.attachedMu.Unlock()
	// Only the refcount's TRUE zero edge is loud (an overlapping reconnect's
	// stale link tearing down while a newer one is still live decrements
	// without reaching zero — that must stay silent, not flap a false
	// "detached" signal) — symmetric with markAttached's link.attached.
	if zeroed {
		a.logger.Info("link.detached", "compute", id)
	}
}

// IsAttached reports whether compute id has a live attach right now (L0).
func (a *Acceptor) IsAttached(id string) bool {
	a.attachedMu.Lock()
	defer a.attachedMu.Unlock()
	return a.attached[id] > 0
}

// AttachedDaemonIDs returns every authenticated daemon id with a live attach right now
// (L0, same authority as IsAttached) — the platform-layer StorageMounts
// implementation's ONLY data source (期11 spec §4.3): a snapshot, not a
// subscription, so a caller building a placement candidate list re-reads it
// per call rather than caching (an attach/detach between two calls is not
// this method's problem to paper over, matching Lookup's own read-time
// discipline). Order is unspecified.
func (a *Acceptor) AttachedDaemonIDs() []string {
	a.attachedMu.Lock()
	defer a.attachedMu.Unlock()
	out := make([]string, 0, len(a.attached))
	for id := range a.attached {
		out = append(out, id)
	}
	return out
}

// Serve upgrades an attaching daemon connection and runs its link for the
// connection's lifetime. daemonID is the required pre-authenticated identifier
// from the app layer. It blocks until the link tears down.
func (a *Acceptor) Serve(w http.ResponseWriter, r *http.Request, daemonID string) {
	if daemonID == "" {
		http.Error(w, "authenticated daemon id required", http.StatusUnauthorized)
		return
	}
	if !a.beginServe() {
		http.Error(w, "link acceptor closed", http.StatusServiceUnavailable)
		return
	}
	defer a.endServe()

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	a.runLink(r.Context(), ws, daemonID)
}

func (a *Acceptor) beginServe() bool {
	a.admissionMu.Lock()
	defer a.admissionMu.Unlock()
	if a.closed {
		return false
	}
	a.wg.Add(1)
	return true
}

func (a *Acceptor) endServe() { a.wg.Done() }

// afterOwned schedules a short Acceptor-owned callback. Its admission and wg.Add
// share admissionMu with Close's seal, so Close either joins the callback or the
// callback loses admission and runs synchronously — no timer goroutine can touch
// Acceptor/link state after Close returns.
func (a *Acceptor) afterOwned(delay time.Duration, fn func()) {
	a.admissionMu.Lock()
	if a.closed {
		a.admissionMu.Unlock()
		fn()
		return
	}
	a.wg.Add(1)
	a.admissionMu.Unlock()
	time.AfterFunc(delay, func() {
		defer a.wg.Done()
		fn()
	})
}

// runLink drives one accepted link: build the mux, handle the stream-0 attach,
// then demux actor streams to the runtime port admission while the lease watchdog judges
// liveness.
func (a *Acceptor) runLink(reqCtx context.Context, ws *websocket.Conn, daemonID string) {
	defer func() { _ = ws.Close() }()
	var gate actorGate
	var lc *linkSession
	portOwner := PortOwner(a.nextPortOwner.Add(1))

	// One pointer-shaped declaration snapshot carries every freshness field. Each
	// successful declaration transaction publishes it wholesale; readers can
	// never combine allowed from one generation with kind/epoch from another.
	var (
		mu       sync.Mutex
		declared = map[actor.ActorID]declarationSnapshotEntry{}
	)
	var portMu sync.Mutex
	removePort := func(inc actorrt.Incarnation) {
		a.portIndex.Remove(portOwner, inc)
	}
	defer func() {
		// Hard-death safety net. The graceful pipeline has already consumed this
		// owner snapshot before it closes the carrier.
		portMu.Lock()
		_ = a.portIndex.TakeOwner(portOwner)
		portMu.Unlock()
	}()
	// boundID is the compute id this link counts as online under, set once on the
	// first accepted attach and torn down when runLink returns. It is WRITTEN only
	// by onControl (the single control-substream read goroutine), but READ across
	// goroutines now — onLane runs on a per-substream accept-dispatch goroutine
	// (the retired mux processed control + stream-open in ONE demux goroutine; the
	// yamux accept loop and the control read loop are separate), and the deferred
	// markDetached runs on the runLink goroutine — so boundMu guards it.
	var (
		boundMu sync.Mutex
		boundID string
	)
	defer func() {
		boundMu.Lock()
		b := boundID
		boundMu.Unlock()
		if b != "" {
			a.markDetached(b)
		}
	}()
	lookupDeclared := func(hp ipc.HandshakePayload) (actor.ActorID, declarationSnapshotEntry, error) {
		id := actor.ActorID(hp.LeaseID)
		mu.Lock()
		meta, ok := declared[id]
		mu.Unlock()
		if !ok {
			return "", declarationSnapshotEntry{}, errUndeclaredActor
		}
		return id, meta, nil
	}
	// kindOf reads the cached declaration Kind for an attached actor (under the
	// same mu as allowed). ok=false only when no attach ever declared the id —
	// which resolve already excludes, so a live port's emit is never a miss.
	kindOf := func(id actor.ActorID) (actor.Kind, bool) {
		mu.Lock()
		meta, ok := declared[id]
		mu.Unlock()
		return meta.Kind, ok
	}

	// onActor: each peer-opened tag=actor substream runs native ipc — hand it
	// straight to runtime port preparation. The substrate does the ipc handshake on the
	// stream, resolves the actor (checks it is in the declared set), and registers
	// it as a port embodiment. EOF on the substream (its own Close or session
	// teardown) = the port reads EOF = down edge. The emitSink is the home write
	// gate (the same notify pen a local cell writes with); the authoritative
	// WriteResult flows back as the ipc EmitAck (writer contract not downgraded
	// across the wire). Runs off the accept-dispatch goroutine (its own goroutine)
	// so the bounded handshake never stalls the accept loop.
	onActor := func(conn net.Conn) {
		// Admission gate first (公理 7 的通用款, per-link instance): a dispatch
		// goroutine can still be delivering a substream while teardown has begun
		// its bounded join — admitting through the gate makes a late Add against
		// an in-progress Wait impossible (WaitGroup reuse panic); a stream
		// arriving after the seal is closed on the spot.
		if !gate.admit() {
			_ = conn.Close()
			return
		}
		go func() {
			defer gate.done()
			// The handshake is bounded by attachHandshakeTimeout (substrate self-
			// guards the time limit). The port LIFETIME stays the runtime's, not
			// this bounded ctx — Attach only uses hsCtx for the handshake read.
			hsCtx, cancel := context.WithTimeout(reqCtx, attachHandshakeTimeout)
			defer cancel()
			// Attach (substrate) owns the conn from here: on failure it closes the
			// stream itself (single owner), so we never double-close here.
			sinks := actorrt.Sinks{
				Emit:     a.emitSink(kindOf),
				Access:   a.accessSink(),
				Schedule: a.scheduleSink(),
			}
			var (
				writerPin        *incumbentPin
				authorityRelease func()
				actorRelease     func()
			)
			releaseOuter := func() {
				if actorRelease != nil {
					actorRelease()
					actorRelease = nil
				}
				if writerPin != nil {
					writerPin.finish(true)
					writerPin = nil
				}
				if authorityRelease != nil {
					authorityRelease()
					authorityRelease = nil
				}
			}
			resolve := func(hp ipc.HandshakePayload) (actor.ActorID, error) {
				id, meta, err := lookupDeclared(hp)
				if err != nil {
					return "", err
				}
				authorityRelease, err = a.daemonAuthority.LockAndValidate(reqCtx, meta.DaemonID, a.channelID)
				if err != nil {
					return "", err
				}
				pin, err := a.enterPortWriter(meta.DaemonID, lc)
				if err != nil {
					releaseOuter()
					return "", err
				}
				writerPin = pin
				actorRelease = a.actorLock(id)
				id, err = a.resolveFreshHandshake(reqCtx, hp, meta)
				if err != nil {
					releaseOuter()
					return "", err
				}
				return id, nil
			}
			// Parse and authenticate before taking portMu. This is the ordering seam
			// for the outer daemon/slot/actor gates: those higher locks are acquired
			// after the actor id is known, while portMu spans only prepared insertion
			// + ACK + commit.
			prepared, err := a.runtime.PrepareHandshake(hsCtx, conn, sinks, resolve, actorrt.KindOf(kindOf), a.cancelReq, removePort)
			if err != nil {
				releaseOuter()
				a.logger.Info("link.attach_stream_failed", "err", err)
				return
			}
			defer releaseOuter()
			portMu.Lock()
			inc, err := prepared.Commit(writerPin.valid)
			if err != nil {
				portMu.Unlock()
				a.logger.Info("link.attach_stream_failed", "err", err)
				return
			}
			// Commit and Home-index publication are one ordered barrier. Close/Kick
			// takes the same barrier before its snapshot, so a live port is never
			// outside the only ownership index it can snapshot.
			a.portIndex.Register(portOwner, inc)
			portMu.Unlock()
			if !a.runtime.IsLive(inc) {
				removePort(inc)
			}
		}()
	}

	// onLane: §5's flattened resource lane (tag=lane). Every lane substream a
	// daemon opens toward the home is a redeem attempt (§5 item 0) — dispatched
	// straight to handleLaneRedeem, which relays it to the transfer's target
	// daemon's own link. Runs on the per-substream accept-dispatch goroutine (its
	// own goroutine), so blocking on the byte pump never stalls the accept loop.
	// boundID (the requester's confirmed id) is read under boundMu — it is
	// written by the onControl goroutine, and the daemon only opens a lane
	// substream after its own attach succeeded, so it is set by the time any
	// redeem arrives.
	onLane := func(conn net.Conn) {
		boundMu.Lock()
		id := boundID
		boundMu.Unlock()
		a.handleLaneRedeem(id, conn)
	}

	// the per-link lease, refreshed by any inbound frame.
	lease := NewLease(a.leasePing, a.leaseTTL)

	onControl := func(payload []byte) {
		switch peekControlKind(payload) {
		case ctrlAttach:
			cf, err := decodeControl(payload)
			if err != nil || cf.Attach == nil {
				return
			}
			isFirstAttachOnLink := boundID == ""
			id, accepted := a.handleAttach(reqCtx, lc, cf.RequestID, cf.Attach, daemonID, isFirstAttachOnLink, &mu, &declared, portOwner)
			// Count the daemon online only after a SUCCESSFUL attach (membership
			// applied, Accepted reply sent) — a rejected/half attach must not show
			// online. Once per link (first accepted frame). boundID is read unlocked
			// here (this is its only writer goroutine) but WRITTEN under boundMu so
			// the cross-goroutine readers (onLane, the deferred markDetached) see it.
			if accepted && boundID == "" {
				boundMu.Lock()
				boundID = id
				boundMu.Unlock()
				a.markAttached(id)
				// Register this link for lane relay under its confirmed id, so a
				// redeem on ANOTHER daemon's link whose transfer targets THIS one
				// can open a lane substream toward it (handleLaneRedeem → laneLink).
				a.registerLaneLink(id, lc)
			}
		case ctrlAllocReply:
			sf, err := decodeStorageControl(payload)
			if err != nil || sf.AllocReply == nil {
				return
			}
			a.pendingAlloc.deliver(sf.AllocReply.RequestID, *sf.AllocReply)
		case ctrlReclaimReply:
			sf, err := decodeStorageControl(payload)
			if err != nil || sf.ReclaimReply == nil {
				return
			}
			a.pendingReclaim.deliver(sf.ReclaimReply.RequestID, *sf.ReclaimReply)
		case ctrlCommitted:
			sf, err := decodeStorageControl(payload)
			if err != nil || sf.Committed == nil {
				return
			}
			a.handleCommitted(reqCtx, lc, boundID, sf.Committed)
		case ctrlReclaimAck:
			sf, err := decodeStorageControl(payload)
			if err != nil || sf.ReclaimAck == nil {
				return
			}
			a.handleReclaimAck(reqCtx, lc, boundID, sf.ReclaimAck)
		case ctrlReconcilePull:
			sf, err := decodeStorageControl(payload)
			if err != nil || sf.ReconcilePull == nil {
				return
			}
			a.handleReconcilePull(reqCtx, lc, boundID, sf.ReconcilePull)
		case ctrlResolveCoord:
			lf, err := decodeLaneControl(payload)
			if err != nil || lf.ResolveCoord == nil {
				return
			}
			reply := a.handleResolveCoord(boundID, lf.ResolveCoord)
			raw, eerr := encodeLaneControl(laneControlFrame{Kind: ctrlResolveCoordReply, ResolveCoordReply: &reply})
			if eerr != nil {
				return
			}
			_ = lc.sendControl(raw)
		case ctrlPlanPull:
			cf, err := decodeControl(payload)
			if err != nil || cf.PlanPull == nil {
				return
			}
			boundMu.Lock()
			confirmed := boundID
			boundMu.Unlock()
			reply := PlanReply{}
			switch {
			case confirmed == "":
				reply.Error = "link: plan pull before accepted attach"
			case a.planProvider == nil:
				reply.Error = "link: no plan provider"
			default:
				reply.Actors, err = a.planProvider.Plan(reqCtx, confirmed)
				if err != nil {
					reply.Error = err.Error()
				}
			}
			raw, eerr := encodeControl(controlFrame{RequestID: cf.RequestID, Kind: ctrlPlanReply, PlanReply: &reply})
			if eerr == nil {
				_ = lc.sendControl(raw)
			}
		}
	}

	// Build the top-level yamux server session over the raw WS byte stream.
	// lease.Refresh is passed as onFrame: linksession.go's dispatch fires it per
	// application frame on whichever peer-opened substream carries it (the
	// daemon's app-level idle ping on the control substream, or an actor's ipc
	// frame) — deliberately NOT on the raw carrier, where yamux's own keepalive
	// ping/pong also flows and would otherwise mask a frozen app.
	var lerr error
	lc, lerr = acceptLinkSession(ws, onControl, onActor, onLane, lease.Refresh, a.logger)
	if lerr != nil {
		a.logger.Info("link.accept_session_failed", "err", lerr)
		return
	}
	defer a.markIncumbentDead(lc)

	// One pointer-shaped handle owns the complete graceful-close protocol. The
	// port snapshot is taken only after admission is sealed and every worker that
	// crossed the gate has either published its port or finished unsuccessfully.
	handle := &linkHandle{
		gate:       &gate,
		invalidate: func() { a.markIncumbentDead(lc) },
		waitWorkers: func() {
			a.joinActorWorkers(&gate, daemonID, attachHandshakeTimeout)
		},
		takePorts: func() []actorrt.Incarnation {
			portMu.Lock()
			defer portMu.Unlock()
			return a.portIndex.TakeOwner(portOwner)
		},
		quietPort: a.runtime.DespawnQuiet,
		closeCarrier: func() {
			_ = lc.Close()
		},
		sendControl: lc.sendControl,
	}

	// Register this link's Kick handle BEFORE the attach frame is even read (not
	// only after a successful attach, unlike markAttached/boundID above) — this
	// closes the half-attach window (§S-P21): a daemon that is pre-authenticated
	// but has not yet completed its attach handshake is still reachable by
	// KickDaemon. Deregistered on runLink exit regardless of how it ends.
	a.registerLink(daemonID, handle)
	defer a.deregisterLink(daemonID, handle)
	// Evict this link from the lane-relay table on teardown (keyed by boundID,
	// read at defer-run time — pointer-guarded so an overlapping reconnect's
	// newer registration survives this stale link's exit).
	defer func() {
		boundMu.Lock()
		b := boundID
		boundMu.Unlock()
		a.deregisterLaneLink(b, lc)
	}()

	// Lease watchdog: tears the link down when last-seen falls behind TTL.
	done := make(chan struct{})
	go lease.Watch(done, func() {
		a.logger.Info("link.lease_expired", "channel", string(a.channelID))
		lc.kill("lease_expired", nil)
	})

	// Acceptor Close tears the link down; quiet-stop this link's ports FIRST so the
	// ensuing stream EOFs are silent. A request-context cancel or peer-gone (the
	// session dying) stays LOUD — those are the positively-observed death edges.
	go func() {
		select {
		case <-a.ctx.Done():
			handle.closeQuietly()
		case <-reqCtx.Done():
			lc.kill("accept_request_cancelled", reqCtx.Err())
		case <-done:
		}
	}()

	// Launch the session's accept/read loops now that lc is assigned + registered,
	// then block for the link's whole life — the session dying (peer gone, carrier
	// error, lease-expiry Close) fires closed(), the direct replacement for the
	// retired demux loop returning. Every open substream is errored by yamux on
	// session death, so each port's ipc read loop fails and publishes its down edge
	// (the same death funnel the old teardown() drove).
	lc.start()
	<-lc.closed()
	// Restore a total order between the last control mutation and teardown's
	// deferred online/lane cleanup: kill closes the worker stop signal first, so
	// queued frames cannot START after session death, and joining the worker here
	// fences whatever was ALREADY running before boundID is inspected by the
	// defers. The join is BOUNDED (H-1): a control handler wedged in a long-lived
	// storage call in the declaration coordinator running on the attach reqCtx
	// (which has no deadline) must not
	// pin teardown — and, through Serve's WaitGroup, Home shutdown — on a stalled
	// store. Bound = 2× the out-network write budget (streamWriteBudget = 10s):
	// generous for any live handler, decisive against a hung one. On timeout we
	// abandon the join and proceed with teardown (a stuck store is never allowed
	// to become an un-closeable station), leaving a loud, attributed trace.
	boundMu.Lock()
	b := boundID
	boundMu.Unlock()
	a.joinLinkWorkers(lc, &gate, b, 2*streamWriteBudget, attachHandshakeTimeout)
	close(done)
}

func (a *Acceptor) resolveFreshHandshake(ctx context.Context, hp ipc.HandshakePayload, meta declarationSnapshotEntry) (actor.ActorID, error) {
	id := actor.ActorID(hp.LeaseID)
	if hp.Epoch != meta.Epoch {
		return "", fmt.Errorf("link: stale handshake epoch: handshake=%d declaration=%d", hp.Epoch, meta.Epoch)
	}
	row, found, err := a.composition.LookupComposition(ctx, id)
	if err != nil {
		return "", fmt.Errorf("link: composition lookup: %w", err)
	}
	canonical := actor.BindingRuntimeInboundViaRelay
	if !found || row.Placement != storespec.PlacementDaemon || row.DesiredHost != meta.DaemonID ||
		row.Epoch != meta.Epoch || meta.Binding != canonical {
		return "", errors.New("link: declaration/handshake no longer matches composition")
	}
	rec, found, err := a.registry.Lookup(ctx, id)
	if err != nil || !found || !rec.IsActive() || rec.Host != meta.DaemonID || rec.Kind != meta.Kind || rec.Binding != canonical {
		return "", errors.New("link: declaration/handshake no longer matches active registry")
	}
	return id, nil
}

// handleAttach applies the complete declaration through Home's single decision
// table, then publishes one immutable allow snapshot for subsequent handshakes.
func (a *Acceptor) handleAttach(ctx context.Context, lc *linkSession, requestID string, att *AttachRequest, daemonID string, isFirstAttachOnLink bool, mu *sync.Mutex, declared *map[actor.ActorID]declarationSnapshotEntry, portOwner PortOwner) (string, bool) {
	if reason := validateAttachEnvelope(att); reason != "" {
		a.rejectAttach(lc, requestID, reason, "attach_"+reason, nil)
		return "", false
	}
	computeID := daemonID

	// Reserved-id guard: no daemon may declare the system actor id. `system` is
	// the substrate's OWN authority (it authors actor.* mirror events + substrate-
	// death terminals), not a tenant actor — a half-trusted daemon (it runs on a
	// user host) must never get a pen welded to it, or it could forge mirror events
	// and force-close any open request as fake substrate-death. This closes only the
	// system-impersonation sub-case (the largest blast radius); full per-daemon
	// actor-ownership validation (A6) stays deferred under single-tenancy.
	for _, d := range att.Declarations {
		if d.ActorID == actor.SystemActorID {
			a.rejectAttach(lc, requestID, "declared actor id is reserved: "+string(d.ActorID), "attach_reserved_actor", nil)
			return "", false
		}
	}
	authorityRelease, err := a.daemonAuthority.LockAndValidate(ctx, computeID, a.channelID)
	if err != nil {
		a.rejectAttach(lc, requestID, "daemon_binding_stale", "attach_daemon_binding_stale", err)
		return "", false
	}
	defer authorityRelease()

	pin, reason := a.enterDeclarationWriter(computeID, lc)
	if reason != "" {
		a.rejectAttach(lc, requestID, reason, "attach_"+reason, nil)
		return "", false
	}
	pinFinished := false
	defer func() {
		if !pinFinished {
			pin.finish(false)
		}
	}()

	input := make([]storespec.ComputeDeclaration, 0, len(att.Declarations))
	for _, d := range att.Declarations {
		input = append(input, storespec.ComputeDeclaration{ActorID: d.ActorID, Kind: d.Kind, Binding: d.Binding, Epoch: d.Epoch})
	}
	allowed, err := a.declarations.ApplyComputeDeclaration(ctx, portOwner, computeID, input)
	if err != nil {
		a.sendReply(lc, requestID, AttachReply{Accepted: false, Reason: "apply_declaration: " + err.Error()})
		return "", false
	}
	admitted := make([]Declaration, 0, len(allowed))
	for _, d := range allowed {
		admitted = append(admitted, Declaration{ActorID: d.ActorID, Kind: d.Kind, Binding: d.Binding, Epoch: d.Epoch})
	}

	// Build the FULL admitted set fresh, then swap it in under mu in one step — a
	// Reattach carries every actor this compute currently hosts (never an
	// increment), so this IS the re-diff: an id absent from the admitted set that
	// was allowed before is simply not in newAllowed after. Orphans were already
	// dropped above, so they never enter the allow-set (the 问① OpenStream gate).
	newDeclared := make(map[actor.ActorID]declarationSnapshotEntry, len(admitted))
	for _, d := range admitted {
		newDeclared[d.ActorID] = declarationSnapshotEntry{Kind: d.Kind, Binding: d.Binding, Epoch: d.Epoch, DaemonID: computeID}
	}
	mu.Lock()
	*declared = newDeclared
	mu.Unlock()

	// The store transaction/diff and the single-pointer declaration publication
	// are complete. Exit the writer, then commit the candidate locally before the
	// success reply becomes externally observable. Once the peer sees Accepted it
	// may immediately open an actor stream, so promotion after the write creates a
	// real wire-order race. A failed write rolls the promoted incumbent back by
	// marking it dead and closing the session.
	pin.finish(true)
	pinFinished = true

	if !a.publishAcceptedAttach(computeID, lc, func() error {
		return a.sendReply(lc, requestID, AttachReply{ChannelID: a.channelID, Accepted: true, DaemonID: computeID})
	}) {
		return computeID, false
	}
	// "接上了" and "重申了一遍" are different events — the daemon re-declares its
	// full set every reconcile tick, and logging both as link.attached makes the
	// attach/detach ledger permanently unbalanced (12 attach vs 2 detach over one
	// soak run) besides growing linearly with uptime. First frame on the link is
	// the attach; every later Reattach is a redeclare, on the record at Debug.
	if isFirstAttachOnLink {
		a.logger.Info("link.attached", "compute", computeID, "actors", len(att.Declarations))
	} else {
		a.logger.Debug("link.redeclared", "compute", computeID, "actors", len(att.Declarations))
	}
	return computeID, true
}

// validateAttachEnvelope is the parse-level gate: it runs before writer
// admission and before any registry/store action. Duplicate ids make the whole
// declaration ambiguous (kind/binding/epoch would otherwise be last-wins), so
// the frame is rejected atomically rather than partially normalized.
func validateAttachEnvelope(att *AttachRequest) string {
	if att == nil || att.Proto < 2 {
		return "protocol_too_old"
	}
	seen := make(map[actor.ActorID]struct{}, len(att.Declarations))
	for _, decl := range att.Declarations {
		if _, exists := seen[decl.ActorID]; exists {
			return "duplicate_declaration"
		}
		seen[decl.ActorID] = struct{}{}
	}
	return ""
}

func (a *Acceptor) sendReply(lc *linkSession, requestID string, reply AttachReply) error {
	raw, err := encodeControl(controlFrame{RequestID: requestID, Kind: ctrlAttachReply, AttachReply: &reply})
	if err != nil {
		return err
	}
	return lc.sendControl(raw)
}

// rejectAttach delivers the terminal reject when possible, then closes the
// session after a short bounded drain so a rejected peer cannot keep a
// half-attached link alive by sending frames that refresh its lease. A failed
// reply write closes immediately because there is nothing left to drain.
func (a *Acceptor) rejectAttach(lc *linkSession, requestID, reason, killReason string, cause error) {
	if err := a.sendReply(lc, requestID, AttachReply{Accepted: false, Reason: reason}); err != nil {
		lc.kill(killReason, errors.Join(cause, err))
		return
	}
	a.afterOwned(attachRejectDrain, func() { lc.kill(killReason, cause) })
}

// peekControlKind reads ONLY the "kind" field of a stream-0 control payload —
// the dispatch key onControl uses to decide which of the two control
// vocabularies (attach's controlFrame vs the storage plane's
// storageControlFrame, §4.7) to fully decode the payload as. Both share the
// same wire discipline (one JSON object, a "kind" string field), so a bad/
// truncated payload simply peeks as the zero controlKind (dispatches to no
// case, silently dropped — the same discipline decodeControl's own error
// path already had before this dispatch existed).
func peekControlKind(payload []byte) controlKind {
	var probe struct {
		Kind controlKind `json:"kind"`
	}
	_ = json.Unmarshal(payload, &probe)
	return probe.Kind
}

// sendStorageControl is the storage control-RPC plane's single guarded send
// path (mirrors sendReply for attach) — a marshal failure is dropped (same
// discipline sendReply already has: an encode failure here means a Go bug in
// this package, not a wire condition worth propagating to the caller, who
// has no slot to receive an error from an async control send anyway).
func (a *Acceptor) sendStorageControl(lc *linkSession, f storageControlFrame) {
	raw, err := encodeStorageControl(f)
	if err != nil {
		return
	}
	_ = lc.sendControl(raw)
}

// SendAllocRequest is the door's (accessdoor.StorageControl, via a platform
// adapter) send-half of §4.7's first frame: routes to daemonID's most
// recently registered live link (a.links, the same table KickDaemon reads),
// waits for the correlated AllocReply. Returns a Go error on "no live
// connection for daemonID" (the caller's placement chain chose a daemon that
// is no longer attached — a StorageMounts snapshot can go stale between
// ListStorageDaemons and this call, §4.3's own read-time-not-cached
// discipline), a send failure, or a controlRPCTimeout/ctx-cancel/link-close
// while waiting; ok=false on the daemon's own AllocReply{OK:false} (a
// non-nil error with the reply's Reason, e.g. mkdir failed on the daemon's
// side — a genuine Allocator failure, not a transport one).
func (a *Acceptor) SendAllocRequest(ctx context.Context, daemonID string, req AllocRequest) error {
	a.linksMu.Lock()
	hs := a.links[daemonID]
	var handle *linkHandle
	if len(hs) > 0 {
		handle = hs[len(hs)-1] // most recently registered connection for this compute
	}
	a.linksMu.Unlock()
	if handle == nil {
		return fmt.Errorf("link: no live connection for daemon %q", daemonID)
	}

	if req.RequestID == "" {
		req.RequestID = newRequestID()
	}
	ch := a.pendingAlloc.register(req.RequestID)
	raw, err := encodeStorageControl(storageControlFrame{Kind: ctrlAllocRequest, AllocRequest: &req})
	if err != nil {
		a.pendingAlloc.cancel(req.RequestID)
		return err
	}
	if err := handle.send(raw); err != nil {
		a.pendingAlloc.cancel(req.RequestID)
		return err
	}
	reply, err := a.pendingAlloc.wait(ctx, req.RequestID, ch, a.ctx.Done())
	if err != nil {
		return err
	}
	if !reply.OK {
		return fmt.Errorf("link: alloc request denied by daemon %q: %s", daemonID, reply.Reason)
	}
	return nil
}

// SendReclaimRequest is the door's (accessdoor.StorageControl, via the
// platform adapter) send-half of 期11 review §2.5 #B's synchronous coord
// reclaim: routes to daemonID's most recent live link, waits for the
// correlated ReclaimReply. Exact structural mirror of SendAllocRequest (same
// home-initiated request/reply discipline, same pending table shape) — used
// by the content-less create loser path to collect the orphaned empty coord
// the with-content path's CommittedReply.Lost→ReclaimCoord signal would
// otherwise have handled). A "no live connection" is a non-fatal Go error
// this package itself does NOT log — the failure is surfaced at the
// accessdoor door side instead (runtime/accessdoor/query.go's ReclaimRequest
// caller WARNs it, A5), the same seam that owns every other content-less-
// create-loser cleanup decision (the reservation is already gone; a missed
// reclaim is at worst a leftover empty dir the next resource-delete-scale
// reclaim never revisits — the same best-effort posture every other
// daemon-side cleanup in this plane documents).
func (a *Acceptor) SendReclaimRequest(ctx context.Context, daemonID, coord string) error {
	a.linksMu.Lock()
	hs := a.links[daemonID]
	var handle *linkHandle
	if len(hs) > 0 {
		handle = hs[len(hs)-1]
	}
	a.linksMu.Unlock()
	if handle == nil {
		return fmt.Errorf("link: no live connection for daemon %q", daemonID)
	}

	req := ReclaimRequest{RequestID: newRequestID(), Coord: coord}
	ch := a.pendingReclaim.register(req.RequestID)
	raw, err := encodeStorageControl(storageControlFrame{Kind: ctrlReclaimRequest, ReclaimRequest: &req})
	if err != nil {
		a.pendingReclaim.cancel(req.RequestID)
		return err
	}
	if err := handle.send(raw); err != nil {
		a.pendingReclaim.cancel(req.RequestID)
		return err
	}
	reply, err := a.pendingReclaim.wait(ctx, req.RequestID, ch, a.ctx.Done())
	if err != nil {
		return err
	}
	if !reply.OK {
		return fmt.Errorf("link: reclaim request denied by daemon %q: %s", daemonID, reply.Reason)
	}
	return nil
}

// handleCommitted answers a daemon-initiated Committed(reservation_id) frame
// (§4.7's second frame): sender-auth (senderDaemonID must equal the
// reservation's OWN placement_daemon_id, read via ResourceOutbox before
// acting — §4.7's mechanical "sender==placement_daemon_id" assertion),
// then CommitReservation, then reply. senderDaemonID=="" (not yet attached —
// should be unreachable in practice, since only an attached link is even
// reading frames off this connection, but checked defensively) and a nil
// storageControl both reply with an honest Reason, never a silent drop (this
// RPC plane is request/response — a daemon retrying forever against silence
// is a worse failure mode than an explicit nak).
func (a *Acceptor) handleCommitted(ctx context.Context, lc *linkSession, senderDaemonID string, msg *Committed) {
	reply := CommittedReply{RequestID: msg.RequestID}
	switch {
	case a.storageControl == nil:
		reply.Reason = "link: no storage host control wired on this channel"
	case senderDaemonID == "":
		reply.Reason = "link: committed frame from an unattached sender"
	default:
		found, lost, err := a.storageControl.Committed(ctx, senderDaemonID, msg.ReservationID)
		if err != nil {
			reply.Reason = err.Error()
		} else {
			reply.Found, reply.Lost = found, lost
		}
	}
	a.sendStorageControl(lc, storageControlFrame{Kind: ctrlCommittedReply, CommittedReply: &reply})
}

// handleReclaimAck is handleCommitted's delete-side mirror (§4.7's third
// frame).
func (a *Acceptor) handleReclaimAck(ctx context.Context, lc *linkSession, senderDaemonID string, msg *ReclaimAck) {
	reply := ReclaimAckReply{RequestID: msg.RequestID}
	switch {
	case a.storageControl == nil:
		reply.Reason = "link: no storage host control wired on this channel"
	case senderDaemonID == "":
		reply.Reason = "link: reclaim_ack frame from an unattached sender"
	default:
		found, err := a.storageControl.ReclaimAck(ctx, senderDaemonID, msg.TombstoneID)
		if err != nil {
			reply.Reason = err.Error()
		} else {
			reply.Found = found
		}
	}
	a.sendStorageControl(lc, storageControlFrame{Kind: ctrlReclaimAckReply, ReclaimAckReply: &reply})
}

// handleReconcilePull answers the Scrubber's periodic pull (§4.7's fourth
// frame): senderDaemonID is trusted DIRECTLY as the sole filter (unlike
// Committed/ReclaimAck, there is no separate id to cross-check it against —
// ReconcilePull carries no target id of its own, it simply asks "what is
// mine"), so StorageHostControl.ReconcilePull's OWN implementation is
// responsible for the "只返回该 sender 名下" confinement (§4.7) via the
// ResourceOutbox's already-per-daemon-filtered List*ByDaemon methods.
func (a *Acceptor) handleReconcilePull(ctx context.Context, lc *linkSession, senderDaemonID string, msg *ReconcilePull) {
	reply := ReconcilePullReply{RequestID: msg.RequestID}
	switch {
	case a.storageControl == nil:
		reply.Reason = "link: no storage host control wired on this channel"
	case senderDaemonID == "":
		reply.Reason = "link: reconcile_pull frame from an unattached sender"
	default:
		resources, pendingReservations, pendingTombstones, err := a.storageControl.ReconcilePull(ctx, senderDaemonID, msg.ActiveCoords)
		if err != nil {
			reply.Reason = err.Error()
		} else {
			reply.Resources = resources
			reply.PendingReservations = pendingReservations
			reply.PendingTombstones = pendingTombstones
		}
	}
	a.sendStorageControl(lc, storageControlFrame{Kind: ctrlReconcilePullReply, ReconcilePullReply: &reply})
}

// StorageHostControl is the home-side handler for the daemon-initiated half
// of the §4.7 control-RPC plane — platform assembly implements it (over
// runtime.ChannelStores.Outbox, the resourcespec.ResourceOutbox slice) and
// injects it via Config.StorageHostControl; this package only defines the
// contract and drives the sender-auth + reply-envelope mechanics around it.
type StorageHostControl interface {
	// Committed lands a content-bearing file create's reservation
	// (resourcespec.Registry.CommitReservation) — see that method's doc for
	// the found/err(ErrReservationLost) contract this must preserve.
	Committed(ctx context.Context, senderDaemonID, reservationID string) (found, lost bool, err error)
	// ReclaimAck clears a collected tombstone
	// (resourcespec.Registry.ClearTombstone).
	ReclaimAck(ctx context.Context, senderDaemonID, tombstoneID string) (found bool, err error)
	// ReconcilePull answers senderDaemonID's own recovery picture — landed
	// resources placed on it, its pending reservations, its pending
	// tombstones (resourcespec.Registry's three List*ByDaemon methods).
	// activeCoords is the sender's own ReconcilePull.ActiveCoords (期11
	// review) — the implementor's liveness-touch narrows to exactly these
	// coords, never a blanket "every reservation this daemon owns".
	ReconcilePull(ctx context.Context, senderDaemonID string, activeCoords []string) (resources []ReconcileResource, pendingReservations []ReconcileReservation, pendingTombstones []ReconcileTombstone, err error)
}

// emitSink builds the per-link EmitSink: a remote cell's emit is written through
// the home write gate, and the authoritative WriteResult returns as the ipc
// EmitAck. The author identity is welded HERE by Minting a Pen for the
// connection's authenticated bound id (with its cached declaration kind) — never
// read from the envelope's self-reported sender (the daemon's relay-only proxy
// pen leaves Sender.ID/ChannelID empty; identity does not downgrade across the
// wire). A stream authenticated as one actor whose envelope self-reports a
// foreign sender is rejected fail-fast (HarnessIdentityNotCallerSettable) by the
// host pen — the substrate-injected identity fields are not caller-settable, so a
// forged self-report never reaches step 4.
//
// The minted pen is wrapped in a livePen welded to the emitting port's
// Incarnation and freshly minted per emit (the port death-write gate): every
// emit first checks the port is STILL the live embodiment (by pointer, ABA-safe) —
// message-plane parity with the cell path, so a replaced/torn-down port's
// in-flight emit is fenced with ErrWriterNotLive instead of authoring truth on a
// dead incarnation's behalf.
func (a *Acceptor) emitSink(kindOf func(actor.ActorID) (actor.Kind, bool)) actorrt.EmitSink {
	return func(ctx context.Context, inc actorrt.Incarnation, env *message.Envelope) (ipc.EmitResult, error) {
		id := inc.ID()
		kind, ok := kindOf(id)
		if !ok {
			// The stream-0 attach always precedes any actor stream opening (resolve
			// gates the stream on the same declaration set), so a live port's emit
			// whose bound id has no cached kind is a protocol violation. Fail-fast
			// rather than Mint with an empty kind (a silent empty kind would blunt
			// the harness sender gate); the error relays back as the emit ack's Err.
			return ipc.EmitResult{}, fmt.Errorf("link: emit from %q has no cached declaration kind (attach missing)", id)
		}
		res, err := NewLivePen(a.minter.Mint(id, kind, a.channelID), inc, a.runtime).Write(ctx, env)
		// Mirror EVERY verdict field of the harness WriteResult onto the wire — the
		// writer contract must not downgrade across the link (a remote cell's
		// Respond observes the same verdict a local cell's would).
		return ipc.EmitResult{
			MessageID:    res.MessageID,
			Seq:          res.Seq,
			RejectReason: string(res.RejectReason),
			RejectDetail: res.RejectDetail,
		}, err
	}
}

// accessSink builds the per-link access RelaySink: a remote cell's plane-2
// invocation is resolved through the home's access door under the connection's
// authenticated bound id, and the authoritative verdict returns as the
// KindAccess ack. The caller identity is welded HERE (the door minter binds
// inc.ID()) — every arm door-welds caller structurally (the Invocation arm's
// wire-level Invocation.Caller is rejected fail-fast if self-reported; the
// Create/Query arms carry no caller field at ALL, a stronger structural
// version of the same rule). Each handle is wrapped in the matching liveness
// membrane over the emitting port's OWN Incarnation (same source as
// emitSink — never a cross-stream lookup), freshly per invocation (the port
// death gate on the access plane): a replaced/torn-down port's in-flight
// invoke is fenced instead of acting on a dead incarnation's behalf.
//
// Three arms (期11 spec §3.3's sum): Invocation (state OR channel-scoped, by
// Scope — pre-existing), Create (channel-scoped only), Query (channel-scoped
// only, Stat or List by QueryKind). All three share this ONE ipc KindAccess
// frame kind — the sum lives inside the payload, the frame closed set is
// untouched.
func (a *Acceptor) accessSink() actorrt.RelaySink {
	return func(ctx context.Context, inc actorrt.Incarnation, payload []byte) ([]byte, error) {
		if a.access == nil {
			return nil, errors.New("link: access plane not wired on this home")
		}
		var req accessRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			return nil, fmt.Errorf("link: access payload decode: %w", err)
		}
		id := inc.ID()

		switch req.Kind {
		case accessKindInvocation:
			return a.accessInvocation(ctx, inc, id, req)
		case accessKindCreate:
			return a.accessCreate(ctx, inc, id, req)
		case accessKindQuery:
			return a.accessQuery(ctx, inc, id, req)
		default:
			return nil, fmt.Errorf("link: access unknown request kind %q", req.Kind)
		}
	}
}

// accessInvocation handles the pre-existing Invoke arm — state OR
// channel-scoped, selected by Scope.
func (a *Acceptor) accessInvocation(ctx context.Context, inc actorrt.Incarnation, id actor.ActorID, req accessRequest) ([]byte, error) {
	if req.Inv == nil {
		return nil, errors.New("link: access invocation request missing its inv payload")
	}
	if req.Inv.Caller != "" {
		// Identity is not caller-settable across the wire (mirrors the pen
		// rejecting a pre-filled Sender): fail-fast, never silently overwrite.
		return nil, fmt.Errorf("link: access invocation self-reported caller %q — identity is home-welded, not wire-settable", req.Inv.Caller)
	}
	var raw accessdoor.AccessHandle
	switch req.Scope {
	case accessScopeChannel:
		raw = a.access.Mint(id)
	case accessScopeState:
		raw = a.access.MintState(id)
	default:
		return nil, fmt.Errorf("link: access unknown scope %q", req.Scope)
	}
	outcome, err := NewLiveAccess(raw, inc, a.runtime).Invoke(ctx, req.Inv.Operation, req.Inv.Resource, req.Inv.Args, req.Inv.Grant)
	if err != nil {
		return nil, err
	}
	return json.Marshal(accessResponse{Kind: accessKindInvocation, Value: outcome.Value, Found: outcome.Found, RejectReason: outcome.RejectReason, Route: outcome.Route})
}

// accessCreate handles the Create arm — structurally channel-scoped only
// (期11 spec §3.3: "Create/Query 天然只属资源面"), so it always Mints (never
// MintState) and carries no self-reportable caller field at all.
func (a *Acceptor) accessCreate(ctx context.Context, inc actorrt.Incarnation, id actor.ActorID, req accessRequest) ([]byte, error) {
	if req.Create == nil {
		return nil, errors.New("link: access create request missing its create payload")
	}
	rh := NewLiveResourceAccess(a.access.Mint(id), inc, a.runtime)
	outcome, err := rh.Create(ctx, req.Create.Resource, req.Create.Spec, req.Create.Initial)
	if err != nil {
		return nil, err
	}
	return json.Marshal(accessResponse{Kind: accessKindCreate, Value: outcome.Value, Found: outcome.Found, RejectReason: outcome.RejectReason, Route: outcome.Route})
}

// accessQuery handles the Query arm (Stat or List, by QueryKind) — same
// channel-scoped-only structural rule as Create.
func (a *Acceptor) accessQuery(ctx context.Context, inc actorrt.Incarnation, id actor.ActorID, req accessRequest) ([]byte, error) {
	if req.Query == nil {
		return nil, errors.New("link: access query request missing its query payload")
	}
	rh := NewLiveResourceAccess(a.access.Mint(id), inc, a.runtime)
	switch req.Query.QueryKind {
	case accessQueryStat:
		res, err := rh.Stat(ctx, req.Query.Resource)
		if err != nil {
			return nil, err
		}
		return json.Marshal(accessResponse{Kind: accessKindQuery, Stat: &accessStatRespFields{Meta: res.Meta, Ops: res.Ops, Reject: res.Reject}})
	case accessQueryList:
		if req.Query.List == nil {
			return nil, errors.New("link: access list query missing its list payload")
		}
		page, err := rh.List(ctx, accessdoor.ListQuery{Prefix: req.Query.List.Prefix, Limit: req.Query.List.Limit, Cursor: req.Query.List.Cursor})
		if err != nil {
			return nil, err
		}
		return json.Marshal(accessResponse{Kind: accessKindQuery, List: &accessListRespFields{Entries: page.Entries, Next: page.Next, Reject: page.Reject}})
	default:
		return nil, fmt.Errorf("link: access unknown query kind %q", req.Query.QueryKind)
	}
}

// scheduleSink builds the per-link schedule RelaySink: a remote cell's time-axis
// call is welded to the connection's authenticated bound id (the engine Minter
// binds inc.ID() as author — the wire never self-reports it) and wrapped in a
// liveSchedule over the port's OWN Incarnation (the time-plane port death gate).
// The CorrelationID inside ScheduleReq crosses the wire intact.
func (a *Acceptor) scheduleSink() actorrt.RelaySink {
	return func(ctx context.Context, inc actorrt.Incarnation, payload []byte) ([]byte, error) {
		if a.sched == nil {
			return nil, errors.New("link: schedule plane not wired on this home")
		}
		var req scheduleRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			return nil, fmt.Errorf("link: schedule payload decode: %w", err)
		}
		h := NewLiveSchedule(a.sched.Mint(inc.ID()), inc, a.runtime)
		switch req.Method {
		case scheduleMethodSchedule:
			tid, err := h.Schedule(ctx, req.Req)
			if err != nil {
				return nil, err
			}
			return json.Marshal(scheduleResponse{ID: tid})
		case scheduleMethodCancel:
			return nil, h.Cancel(ctx, req.ID)
		default:
			return nil, fmt.Errorf("link: schedule unknown method %q", req.Method)
		}
	}
}

// Close stops accepting new links and tears down active ones, waiting for all
// Serve goroutines and admitted delayed-reject callbacks to exit. The outer
// bound is DERIVED from the inner joins a
// runLink teardown can legitimately spend back to back — the control-worker
// join (2×streamWriteBudget), the actor-gate join (attachHandshakeTimeout),
// and the graceful carrier close (streamWriteBudget)
// — plus margin, so the backstop never abandons (and double-counts) a link
// whose inner waits are still within their own ratified budgets.
func (a *Acceptor) Close() error {
	return a.closeWithin(acceptorCloseBudget())
}

func acceptorCloseBudget() time.Duration {
	return 3*streamWriteBudget + attachHandshakeTimeout + 5*time.Second
}

func (a *Acceptor) closeWithin(timeout time.Duration) error {
	a.closeOnce.Do(func() {
		defer close(a.closeDone)
		a.admissionMu.Lock()
		a.closed = true
		a.admissionMu.Unlock()
		a.cancel()
		if !waitGroupWithin(&a.wg, timeout) {
			// Bounded abandon proof: late link writes hit the closed store, while
			// every Attach production attempt is rejected by Runtime.Seal.
			a.leaked.Add(1)
			a.logger.Error("link.acceptor_join_timeout", "timeout", timeout,
				"safety", "runtime admission is sealed and stores reject late writes")
		}
		a.logger.Info("link.acceptor_closed")
	})
	<-a.closeDone
	return nil
}

func (a *Acceptor) Leaked() int64 { return a.leaked.Load() }

// actorGate fences one link's actor-stream worker admission against teardown's
// bounded join — the same closed+mutex+WaitGroup shape as the Acceptor's own
// Serve admission (beginServe/closeWithin), instantiated per link. The seal
// and the Add live under one mutex (公理 7 通用款: 关门判断与登记同一临界区),
// so once seal has taken the lock no later admit can Add against the
// in-progress Wait (which would both leave an unjoined worker and violate
// WaitGroup's reuse precondition).
type actorGate struct {
	mu        sync.Mutex
	sealed    bool
	abandoned bool
	wg        sync.WaitGroup
	waitOnce  sync.Once
	waitDone  chan struct{}
}

// admit registers one actor worker; false = the gate is sealed (teardown has
// begun) and the stream must be refused.
func (g *actorGate) admit() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.sealed {
		return false
	}
	g.wg.Add(1)
	return true
}

func (g *actorGate) done() { g.wg.Done() }

// seal closes admission immediately and starts the one shared waiter. It is a
// separate phase so graceful close can establish the publication fence before
// performing any other teardown action.
func (g *actorGate) seal() {
	g.mu.Lock()
	g.sealed = true
	if g.waitDone == nil {
		g.waitDone = make(chan struct{})
	}
	done := g.waitDone
	g.mu.Unlock()
	// All callers observe one waiter and one terminal channel. A caller whose
	// budget expires abandons only its own observation; it never leaves another
	// Wait goroutine behind. Once the admitted workers finish, the shared done
	// edge settles permanently and every later observer returns immediately.
	g.waitOnce.Do(func() {
		go func() {
			g.wg.Wait()
			close(done)
		}()
	})
}

// waitWithin joins admitted workers within timeout. account is true for
// exactly one timeout observer; later teardown stages sample the same terminal
// channel and never spend a second actor wait budget.
func (g *actorGate) waitWithin(timeout time.Duration) (joined, account bool) {
	g.seal()
	g.mu.Lock()
	done := g.waitDone
	abandoned := g.abandoned
	g.mu.Unlock()
	if abandoned {
		select {
		case <-done:
			return true, false
		default:
			return false, false
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true, false
	case <-timer.C:
		g.mu.Lock()
		if !g.abandoned {
			g.abandoned = true
			account = true
		}
		g.mu.Unlock()
		return false, account
	}
}

func (a *Acceptor) joinActorWorkers(gate *actorGate, daemonID string, budget time.Duration) {
	joined, account := gate.waitWithin(budget)
	if joined || !account {
		return
	}
	a.leaked.Add(1)
	a.logger.Error("link.actor_worker_join_timeout", "timeout", budget,
		"daemon", daemonID,
		"safety", "actor admission is sealed and incumbent publication is fenced; teardown proceeds without a second wait budget")
}

// joinLinkWorkers is runLink's teardown join half — control workers first
// (they can be wedged in a store call), then the actor-stream workers behind
// their admission gate. Budgets are parameters so a test can drive this exact
// production path (accounting included) with compressed budgets; runLink
// passes 2×streamWriteBudget / attachHandshakeTimeout.
func (a *Acceptor) joinLinkWorkers(lc *linkSession, gate *actorGate, boundID string, controlBudget, actorBudget time.Duration) {
	if !lc.waitControlWorkers(controlBudget) {
		a.recordControlWorkerLeak()
		a.logger.Error("link.control_worker_join_timeout",
			"msg", "control worker join timeout, abandoning — teardown proceeding",
			"daemon", boundID, "channel", string(a.channelID))
	}
	a.joinActorWorkers(gate, boundID, actorBudget)
}

func (a *Acceptor) recordControlWorkerLeak() {
	// One incident owns both residues: the stuck worker and the waiter goroutine
	// that remains until that worker eventually returns.
	a.leaked.Add(1)
}

func waitGroupWithin(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}
