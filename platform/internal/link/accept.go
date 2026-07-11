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
	"time"

	"github.com/gorilla/websocket"

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

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

// attachHandshakeTimeout bounds one actor stream's connect-in handshake. A
// peer that opens a stream but never sends the ipc handshake must not pin the
// Attach goroutine forever; the substrate self-guards this step, the host only
// supplies the deadline.
const attachHandshakeTimeout = 10 * time.Second

// Acceptor is the home end of the link: it upgrades attaching daemon
// connections, registers declared actors into membership, and binds each
// actor stream to runtime.Attach (the stream runs native ipc, so a remote cell
// is indistinguishable from a local one — zero translation). It judges liveness
// via the per-link lease. It owns NO business logic — Writer/Runtime/Membership
// are injected capabilities of the home.
type Acceptor struct {
	minter     harness.Minter
	access     accessdoor.AccessMinter
	sched      schedule.Minter
	runtime    *actorrt.Runtime
	membership storespec.MembershipControlPlane
	registry   storespec.Registry
	channelID  channel.ID
	logger     *slog.Logger
	leasePing  time.Duration
	leaseTTL   time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// attached is the live attach refcount per compute id (daemon). A daemon is
	// "online" (attached) iff its count > 0. Refcount, not bool, so an
	// overlapping reconnect (old link tearing down after the new one attached)
	// does not flap the daemon offline. Volatile runtime state — never persisted.
	attachedMu sync.Mutex
	attached   map[string]int

	// obsWatcher folds each attached actor's obs PUSH (L3 device presence) into the
	// home device-presence fold; registered per declared actor at attach (the home-side
	// arm of the actor-source obs axis). nil → no folding. obsReg dedups so a
	// daemon reconnect does not re-append the same watcher.
	obsWatcher actorrt.ObsWatcher
	obsMu      sync.Mutex
	obsReg     map[actor.ActorID]bool

	// cancelReq is the home's injected KindCancelRequest handler (the caller-side
	// upstream cancel: a daemon-hosted caller abandoning its own outbound request).
	// Passed straight to runtime.Attach as the port's onCancelRequest — the link
	// layer holds no request-lookup logic (the closure is Home's).
	cancelReq func(actor.ActorID, message.ID)

	// storageControl is the home-side handler for the daemon-INITIATED half of
	// the §4.7 control-RPC plane (Committed/ReclaimAck/ReconcilePull) — nil
	// means those three frames get an honest error reply (no storage host
	// wired on this channel yet), never a silent drop (§4.7's own frames are
	// request/response; a daemon retrying forever against a home that just
	// swallows the frame would be a worse failure mode than an explicit nak).
	storageControl StorageHostControl

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
	// Kick tears down every one of them. Empty id (dev self-declared mode) is
	// never registered — Kick cannot target what auth never named.
	linksMu sync.Mutex
	links   map[string][]linkHandle
}

// linkHandle is what Kick needs to voluntarily tear one link down: quietStop
// silences this link's ports FIRST (kick is a revocation, not a death — the
// same silent edge as a graceful detach), then lc.Close() drives it through the
// existing teardown funnel (frame.go).
type linkHandle struct {
	lc        *linkSession
	quietStop func()
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
	Access     accessdoor.AccessMinter
	Schedule   schedule.Minter
	Runtime    *actorrt.Runtime
	Membership storespec.MembershipControlPlane
	// Registry is the membership READ face (storespec.Registry, segregated from
	// the write-only MembershipControlPlane) a Reattach reconciles against: it
	// lists which actors this compute currently owns (Host==computeID) so a
	// shrunk declaration set can detect what fell out. Optional — nil skips
	// attach reconciliation (existing single-declare callers unaffected).
	Registry  storespec.Registry
	ChannelID channel.ID
	Logger    *slog.Logger
	LeasePing time.Duration
	LeaseTTL  time.Duration
	// ObsWatcher (optional) receives each attached actor's obs PUSH via per-actor
	// WatchObs registration at attach — the home-side arm of the L3 device-presence fold.
	ObsWatcher actorrt.ObsWatcher
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
}

// NewAcceptor builds an Acceptor.
func NewAcceptor(cfg Config) *Acceptor {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Acceptor{
		minter:         cfg.Minter,
		access:         cfg.Access,
		sched:          cfg.Schedule,
		runtime:        cfg.Runtime,
		membership:     cfg.Membership,
		registry:       cfg.Registry,
		channelID:      cfg.ChannelID,
		logger:         logger,
		leasePing:      cfg.LeasePing,
		leaseTTL:       cfg.LeaseTTL,
		ctx:            ctx,
		cancel:         cancel,
		attached:       map[string]int{},
		obsWatcher:     cfg.ObsWatcher,
		obsReg:         map[actor.ActorID]bool{},
		cancelReq:      cfg.CancelRequest,
		storageControl: cfg.StorageHostControl,
		pendingAlloc:   newPendingReplies[AllocReply](),
		pendingReclaim: newPendingReplies[ReclaimReply](),
		lane:           newLaneState(),
		links:          map[string][]linkHandle{},
	}
}

// registerLink adds this link's handle under id — called at runLink entry
// (daemonID known, attach not yet processed), not at markAttached time.
func (a *Acceptor) registerLink(id string, h linkHandle) {
	if id == "" {
		return
	}
	a.linksMu.Lock()
	a.links[id] = append(a.links[id], h)
	a.linksMu.Unlock()
}

// deregisterLink removes exactly this link's handle (identity match on lc, not
// a wholesale clear) — an overlapping reconnect leaves the other entry intact.
func (a *Acceptor) deregisterLink(id string, lc *linkSession) {
	if id == "" {
		return
	}
	a.linksMu.Lock()
	hs := a.links[id]
	for i, h := range hs {
		if h.lc == lc {
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
// it (quietStop first — kick is a voluntary teardown, not a death edge — then
// Close through the existing funnel), so a concurrent runLink deregistration
// never deadlocks against it. Returns the number of links closed. No
// generation/tombstone bookkeeping (S-P21 拍定 A): a residual pre-auth
// connection that has not registered yet is closed by the app-side convergence
// loop (`while IsAttached { Kick }`, §6), not by a shadow revoked-set here.
func (a *Acceptor) KickDaemon(computeID string) int {
	a.linksMu.Lock()
	hs := append([]linkHandle(nil), a.links[computeID]...)
	a.linksMu.Unlock()
	for _, h := range hs {
		h.quietStop()
		_ = h.lc.Close()
	}
	return len(hs)
}

// markAttached / markDetached / IsAttached are the L0 link-attachment read
// seam: the Acceptor authoritatively holds which computes have a live attach
// right now (it owns the connections + lease). markAttached is called once
// per accepted link (after attach success); markDetached once when that link
// tears down (peer gone / lease expiry / Close). Empty id (dev self-declared
// mode) is not tracked.
func (a *Acceptor) markAttached(id string) {
	if id == "" {
		return
	}
	a.attachedMu.Lock()
	a.attached[id]++
	a.attachedMu.Unlock()
}

func (a *Acceptor) markDetached(id string) {
	if id == "" {
		return
	}
	a.attachedMu.Lock()
	if a.attached[id]--; a.attached[id] <= 0 {
		delete(a.attached, id)
	}
	a.attachedMu.Unlock()
}

// IsAttached reports whether compute id has a live attach right now (L0).
func (a *Acceptor) IsAttached(id string) bool {
	a.attachedMu.Lock()
	defer a.attachedMu.Unlock()
	return a.attached[id] > 0
}

// AttachedComputeIDs returns every compute id with a live attach right now
// (L0, same authority as IsAttached) — the platform-layer StorageMounts
// implementation's ONLY data source (期11 spec §4.3): a snapshot, not a
// subscription, so a caller building a placement candidate list re-reads it
// per call rather than caching (an attach/detach between two calls is not
// this method's problem to paper over, matching Lookup's own read-time
// discipline). Order is unspecified.
func (a *Acceptor) AttachedComputeIDs() []string {
	a.attachedMu.Lock()
	defer a.attachedMu.Unlock()
	out := make([]string, 0, len(a.attached))
	for id := range a.attached {
		out = append(out, id)
	}
	return out
}

// Serve upgrades an attaching daemon connection and runs its link for the
// connection's lifetime. daemonID is the pre-authenticated identifier from the
// app layer (empty → the daemon's self-declared id, dev mode). It blocks until
// the link tears down (peer gone, lease expiry, or acceptor Close).
func (a *Acceptor) Serve(w http.ResponseWriter, r *http.Request, daemonID string) {
	if a.ctx.Err() != nil {
		http.Error(w, "link acceptor closed", http.StatusServiceUnavailable)
		return
	}
	a.wg.Add(1)
	defer a.wg.Done()

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	a.runLink(r.Context(), ws, daemonID)
}

// runLink drives one accepted link: build the mux, handle the stream-0 attach,
// then demux actor streams to runtime.Attach while the lease watchdog judges
// liveness.
func (a *Acceptor) runLink(reqCtx context.Context, ws *websocket.Conn, daemonID string) {
	defer func() { _ = ws.Close() }()

	// allowed is the attach declaration set: the resolve seam checks that an
	// opening actor stream is one the daemon actually declared (membership-backed).
	// kinds caches each declared actor's Kind alongside allowed (populated in the
	// SAME critical section at attach): emitSink needs it to Mint a pen welded to
	// the actor's kind — without it a daemon-attached actor's Sender.Kind would be
	// a silent empty value and blunt the harness sender gate. Volatile, per-link.
	var (
		mu      sync.Mutex
		allowed = map[actor.ActorID]bool{}
		kinds   = map[actor.ActorID]actor.Kind{}
	)
	// Both maps are wholesale-replaced (never merged) by each successful
	// handleAttach — see the &allowed/&kinds pointer pass below.
	// ports holds the live port Incarnation per attached actor on THIS link (stored
	// at onOpen). It is the graceful-teardown handle: on a home-side Close the link
	// quiet-stops each port (pointer-guarded, no down edge) so in-flight requests do
	// not materialise receiver_unavailable. A stale entry (the port already died /
	// was replaced) is a safe no-op — DespawnQuiet guards by pointer identity — so no
	// explicit stream-death eviction is needed (the map lives only for this link).
	var (
		portMu sync.Mutex
		ports  = map[actor.ActorID]actorrt.Incarnation{}
	)
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
	resolve := func(leaseID string) (actor.ActorID, error) {
		id := actor.ActorID(leaseID)
		mu.Lock()
		ok := allowed[id]
		mu.Unlock()
		if !ok {
			return "", errUndeclaredActor
		}
		return id, nil
	}
	// kindOf reads the cached declaration Kind for an attached actor (under the
	// same mu as allowed). ok=false only when no attach ever declared the id —
	// which resolve already excludes, so a live port's emit is never a miss.
	kindOf := func(id actor.ActorID) (actor.Kind, bool) {
		mu.Lock()
		k, ok := kinds[id]
		mu.Unlock()
		return k, ok
	}

	// onActor: each peer-opened tag=actor substream runs native ipc — hand it
	// straight to runtime.Attach. The substrate does the ipc handshake on the
	// stream, resolves the actor (checks it is in the declared set), and registers
	// it as a port embodiment. EOF on the substream (its own Close or session
	// teardown) = the port reads EOF = down edge. The emitSink is the home write
	// gate (the same notify pen a local cell writes with); the authoritative
	// WriteResult flows back as the ipc EmitAck (writer contract not downgraded
	// across the wire). Runs off the accept-dispatch goroutine (its own goroutine)
	// so the bounded handshake never stalls the accept loop.
	onActor := func(conn net.Conn) {
		go func() {
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
			inc, err := a.runtime.Attach(hsCtx, conn, sinks, resolve, actorrt.KindOf(kindOf), a.cancelReq)
			if err != nil {
				a.logger.Info("link.attach_stream_failed", "err", err)
				return
			}
			// Retain the Incarnation so a home-side Close can quiet-stop this port
			// (see the ports map above). A same-id reattach overwrites the entry.
			portMu.Lock()
			ports[inc.ID()] = inc
			portMu.Unlock()
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

	var lc *linkSession
	onControl := func(payload []byte) {
		switch peekControlKind(payload) {
		case ctrlAttach:
			cf, err := decodeControl(payload)
			if err != nil || cf.Attach == nil {
				return
			}
			isFirstAttachOnLink := boundID == ""
			id, accepted := a.handleAttach(reqCtx, lc, cf.RequestID, cf.Attach, daemonID, isFirstAttachOnLink, &mu, &allowed, &kinds, &portMu, ports)
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

	// quietStopPorts tears down every port on this link WITHOUT a down edge — the
	// home is going away gracefully, so its port-hosted actors must fall silent (no
	// receiver_unavailable for in-flight requests) rather than surfacing a spurious
	// death. Pointer-guarded per incarnation (a since-replaced/dead port is a no-op).
	quietStopPorts := func() {
		portMu.Lock()
		incs := make([]actorrt.Incarnation, 0, len(ports))
		for _, inc := range ports {
			incs = append(incs, inc)
		}
		portMu.Unlock()
		for _, inc := range incs {
			a.runtime.DespawnQuiet(inc)
		}
	}

	// Register this link's Kick handle BEFORE the attach frame is even read (not
	// only after a successful attach, unlike markAttached/boundID above) — this
	// closes the half-attach window (§S-P21): a daemon that is pre-authenticated
	// but has not yet completed its attach handshake is still reachable by
	// KickDaemon. Deregistered on runLink exit regardless of how it ends.
	a.registerLink(daemonID, linkHandle{lc: lc, quietStop: quietStopPorts})
	defer a.deregisterLink(daemonID, lc)
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
		_ = lc.Close()
	})

	// Acceptor Close tears the link down; quiet-stop this link's ports FIRST so the
	// ensuing stream EOFs are silent. A request-context cancel or peer-gone (the
	// session dying) stays LOUD — those are the positively-observed death edges.
	go func() {
		select {
		case <-a.ctx.Done():
			quietStopPorts()
			_ = lc.Close()
		case <-reqCtx.Done():
			_ = lc.Close()
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
	close(done)
}

// handleAttach processes a stream-0 attach — the first on a link, or a later
// Reattach (§S-P8): register declared actors into membership (register/
// reactivate — detach never deregisters), WHOLESALE-REPLACE the link's allowed/
// kinds sets to exactly att.Declarations (idempotent — an unchanged re-declare
// swaps in an identical set; self-correcting — a dropped declaration falls out),
// reconcile the compute's Host-owned membership rows against the fresh
// declaration set (see reconcileHost), and reply. The replace is a single
// atomic swap under mu (never a partial merge), so a concurrent
// resolve()/kindOf() sees either the old full set or the new one, never a mix.
// Membership semantics are unchanged by this: a member row is durable; a daemon
// detaching does NOT remove it (membership ≠ attachment) — reconcileHost only
// removes rows the compute itself no longer declares. It reports (computeID,
// accepted) so the caller can count link attachment only on success.
func (a *Acceptor) handleAttach(ctx context.Context, lc *linkSession, requestID string, att *AttachRequest, daemonID string, isFirstAttachOnLink bool, mu *sync.Mutex, allowed *map[actor.ActorID]bool, kinds *map[actor.ActorID]actor.Kind, portMu *sync.Mutex, ports map[actor.ActorID]actorrt.Incarnation) (string, bool) {
	computeID := att.ComputeID
	if daemonID != "" {
		computeID = daemonID
	}

	// Reserved-id guard: no daemon may declare the system actor id. `system` is
	// the substrate's OWN authority (it authors actor.* mirror events + substrate-
	// death terminals), not a tenant actor — a half-trusted daemon (it runs on a
	// user host) must never get a pen welded to it, or it could forge mirror events
	// and force-close any open request as fake substrate-death. This closes only the
	// system-impersonation sub-case (the largest blast radius); full per-daemon
	// actor-ownership validation (A6) stays deferred under single-tenancy.
	for _, d := range att.Declarations {
		if d.ActorID == actor.SystemActorID {
			a.sendReply(lc, requestID, AttachReply{Accepted: false, Reason: "declared actor id is reserved: " + string(d.ActorID)})
			return "", false
		}
	}

	// 膜律 (问①, v1.8): a declaration is admitted ONLY if it names an id with an
	// active户籍 — one active-census Lookup per declaration, and that ONE verdict
	// gates EVERY downstream use of the declaration: the membership host-stamp,
	// the allowed/kinds allow-set (so OpenStream can bind a welded pen), and the
	// obs fold. An orphan (never Admitted, or already deregistered) is dropped
	// from ALL of them — not merely skipped from the membership write — so a
	// half-trusted daemon cannot OpenStream a welded pen for an id it never had
	// 户籍 for and forge truth ("a daemon may attach" is not "a daemon may任命
	// members"). registry-less rigs (nil) admit every declaration (no truth store
	// to consult). 问②③ (placement / desired_host authority) is enforced at plan
	// generation (app), not here.
	admitted := make([]Declaration, 0, len(att.Declarations))
	for _, d := range att.Declarations {
		if a.registry != nil {
			rec, ok, err := a.registry.Lookup(ctx, d.ActorID)
			if err != nil {
				a.sendReply(lc, requestID, AttachReply{Accepted: false, Reason: "membership lookup: " + err.Error()})
				return "", false
			}
			if !ok || !rec.IsActive() {
				a.logger.Warn("link.attach.declaration_no_membership",
					"compute", computeID, "actor", string(d.ActorID))
				continue
			}
			// Ontological gate (期12 S3.5, 主题A A2): a human is恒 home-hosted
			// (三层律 — the person is a DEVICE behind a gateway link; the cell
			// lives on home). A daemon declaring a KindHuman id is claiming to
			// host what cannot run on a daemon — an invalid declaration by
			// ontology, not a bad-daemon defence (A6 stays deferred). Judged on
			// the REGISTRY's kind (rec.Kind), never the daemon's self-report:
			// the declaration is dropped from host-stamp/allow-set/obs alike,
			// same as an orphan. Sibling of the reserved-system-id guard above.
			if rec.Kind == actor.KindHuman {
				a.logger.Warn("link.attach.declaration_human_rejected",
					"compute", computeID, "actor", string(d.ActorID))
				continue
			}
		}
		admitted = append(admitted, d)
	}

	if a.membership != nil && len(admitted) > 0 {
		nowMs := time.Now().UnixMilli()
		adds := make([]storespec.MemberActorAdd, 0, len(admitted))
		for _, d := range admitted {
			// 有户籍 → applyMemberAddTx only UPDATEs host for an active row (只盖 Host).
			adds = append(adds, storespec.MemberActorAdd{ID: d.ActorID, Kind: d.Kind, Binding: d.Binding, Host: computeID, At: nowMs})
		}
		if err := a.membership.ApplyMemberTransitions(ctx, adds, nil); err != nil {
			a.sendReply(lc, requestID, AttachReply{Accepted: false, Reason: "register: " + err.Error()})
			return "", false
		}
	}

	// Build the FULL admitted set fresh, then swap it in under mu in one step — a
	// Reattach carries every actor this compute currently hosts (never an
	// increment), so this IS the re-diff: an id absent from the admitted set that
	// was allowed before is simply not in newAllowed after. Orphans were already
	// dropped above, so they never enter the allow-set (the 问① OpenStream gate).
	newAllowed := make(map[actor.ActorID]bool, len(admitted))
	newKinds := make(map[actor.ActorID]actor.Kind, len(admitted))
	for _, d := range admitted {
		newAllowed[d.ActorID] = true
		newKinds[d.ActorID] = d.Kind
	}
	mu.Lock()
	*allowed = newAllowed
	*kinds = newKinds
	mu.Unlock()

	// reconcileHost is a DESTRUCTIVE full-set diff (§S-P8's "kubelet
	// node-status idiom, never an increment" — an id absent from THIS
	// declaration is deregistered outright) — it must run ONLY against a
	// caller's genuinely-authoritative declared set, never against
	// RunCompute's own bootstrap attach. That FIRST attach on every (re)dial
	// deliberately carries att.Declarations==nil (compute.go's own doc:
	// "Dial declares NOTHING yet: every actor this compute hosts is declared
	// by the ring's own Reattach... inside the first reconcile pass") — a
	// PLACEHOLDER meaning "not yet known", not "authoritatively zero actors".
	// Found+fixed during 期11 S6's platform-level crash-recovery walk
	// verification: running reconcileHost against this nil/bootstrap
	// declaration was DEREGISTERING every actor this compute had previously
	// declared (Host==computeID) milliseconds before the ring's OWN Reattach
	// (always a non-nil, even if zero-length, slice — computeRing.reconcile's
	// `make([]link.Declaration, 0, len(current))`) arrived with the real set
	// — so EVERY reconnect of an already-attached daemon raced its own
	// membership out from under itself, permanently locking every one of its
	// actors out (膜律's "问①" active-membership gate then refuses every
	// later OpenStream for them, forever, since nothing ever re-registers a
	// deregistered row).
	//
	// The skip condition is narrower than "declarations==nil" alone: a
	// caller MAY legitimately Reattach(ctx, nil) on an ALREADY-established
	// link to explicitly shrink its declared set to zero (this package's own
	// TestReattach_HostReconcile_UnwatchesObsOnDereg does exactly that) — nil
	// is ambiguous by itself; "is this the very FIRST attach frame this
	// specific link has ever sent" is the real bootstrap signal (isFirstAttachOnLink,
	// evaluated by the caller BEFORE boundID is set). Only nil-AND-first is
	// the placeholder case; nil-on-a-later-Reattach keeps its full
	// authoritative shrink-to-zero meaning unchanged, and a non-nil (even
	// empty) declaration ALWAYS reconciles regardless of position.
	if att.Declarations != nil || !isFirstAttachOnLink {
		a.reconcileHost(ctx, computeID, newAllowed, portMu, ports)
	}

	// Fold each ADMITTED actor's obs PUSH (L3 device presence) into the home fold.
	// Registered here (before the actor's stream opens / port publishes) so no
	// early edge is missed; deduped so a reconnect does not re-append the watcher.
	if a.obsWatcher != nil {
		a.obsMu.Lock()
		for _, d := range admitted {
			if !a.obsReg[d.ActorID] {
				a.runtime.WatchObs(d.ActorID, a.obsWatcher)
				a.obsReg[d.ActorID] = true
			}
		}
		a.obsMu.Unlock()
	}

	a.sendReply(lc, requestID, AttachReply{ChannelID: a.channelID, Accepted: true, DaemonID: computeID})
	a.logger.Info("link.attached", "compute", computeID, "actors", len(att.Declarations))
	return computeID, true
}

// reconcileHost is the attach-time membership reconciliation (§10.13 推导7 /
// spec S5): a Reattach's declaration set is the compute's FULL current
// placement (kubelet node-status idiom, never an increment), so any actor row
// still marked Host==computeID but absent from newAllowed has fallen out — the
// compute stopped hosting it and must be deregistered. nil Registry (not every
// caller wires one — e.g. dev/test single-declare rigs) skips reconciliation
// entirely; a Lookup/ListActive error is logged and skipped for this round
// (best-effort — the attach itself already succeeded, the next Reattach
// retries the diff).
//
// Despawn-first order (§10.12 mechanical coupling): each falling-out id is
// despawned BEFORE the membership row is removed, so the removal transaction
// never reports the row gone while the embodiment is still live. The despawn
// is GUARDED by the per-link Incarnation pointer retained in ports (stored at
// onOpen) — Despawn only acts if the runtime's live embodiment for that id is
// STILL this very pointer. This defeats the TOCTOU where the id already
// migrated to a successor compute B between this reconcile snapshot and the
// despawn call: a mismatched pointer makes Despawn a no-op (the successor's
// embodiment survives), and — doubly safe — that row's Host has already
// flipped to B, so it would not have surfaced in ListActive(Host==computeID)
// on B's own next reconcile either. An id with no retained pointer on THIS
// link (e.g. it was declared on a since-dead prior connection and its
// embodiment already reaped with that connection) has nothing local to
// despawn — the row removal alone is safe.
func (a *Acceptor) reconcileHost(ctx context.Context, computeID string, newAllowed map[actor.ActorID]bool, portMu *sync.Mutex, ports map[actor.ActorID]actorrt.Incarnation) {
	if a.registry == nil || a.membership == nil || computeID == "" {
		return
	}
	active, err := a.registry.ListActive(ctx)
	if err != nil {
		a.logger.Warn("link.reconcile_host_list_failed", "compute", computeID, "err", err)
		return
	}
	nowMs := time.Now().UnixMilli()
	var removes []storespec.MemberActorRemove
	for _, rec := range active {
		if rec.Host != computeID || newAllowed[rec.ID] {
			continue
		}
		portMu.Lock()
		inc, ok := ports[rec.ID]
		portMu.Unlock()
		if ok {
			a.runtime.Despawn(inc)
		}
		// ExpectedHost closes the remaining write-side TOCTOU: between this
		// ListActive snapshot and the transition tx, the row's Host may flip to a
		// successor compute B (B attaches while A's stale reconcile is in flight).
		// The guard makes the deregistration conditional on host==computeID inside
		// the tx itself — a flipped row is a 0-rows-affected no-op (no cascade of
		// state/timers, no dereg mirror), so B's active row survives untouched.
		removes = append(removes, storespec.MemberActorRemove{ID: rec.ID, ExpectedHost: computeID, At: nowMs})
	}
	if len(removes) == 0 {
		return
	}
	if err := a.membership.ApplyMemberTransitions(ctx, nil, removes); err != nil {
		a.logger.Warn("link.reconcile_host_dereg_failed", "compute", computeID, "err", err)
		return
	}
	// H5 obs cleanup runs AFTER the remove tx commits, and confirms EACH row is
	// actually gone before touching obsReg — never eagerly on the pre-tx
	// candidate list (the bug this closes). Race: a successor compute B can take
	// over rec.ID (attach, re-register under Host==B) between the ListActive
	// snapshot above and this tx running; ApplyMemberTransitions' ExpectedHost
	// guard already makes THAT row's removal a 0-rows-affected no-op, but obs
	// cleanup has no guard of its own — clearing it unconditionally on the
	// candidate list (as this method used to, inline in the loop above, BEFORE
	// the tx even ran) would rip out B's now-live obs registration on the
	// strength of a stale snapshot, even though B's own handleAttach correctly
	// skipped re-registering it (obsReg dedup saw it already true). There is no
	// per-row changed signal from ApplyMemberTransitions, so re-Lookup NOW, per
	// id, and clean obs ONLY for a row confirmed gone/inactive; a row that moved
	// to a successor (still active, whatever its Host now is) keeps its
	// registration untouched — it is owned by whoever hosts it now.
	if a.obsWatcher == nil {
		return
	}
	for _, rm := range removes {
		rec2, ok2, err2 := a.registry.Lookup(ctx, rm.ID)
		if err2 != nil {
			a.logger.Warn("link.reconcile_host_obs_lookup_failed", "compute", computeID, "actor", string(rm.ID), "err", err2)
			continue
		}
		if ok2 && rec2.IsActive() {
			continue // confirmed NOT gone — leave its obs registration alone
		}
		a.obsMu.Lock()
		if a.obsReg[rm.ID] {
			a.runtime.UnwatchObs(rm.ID, a.obsWatcher)
			delete(a.obsReg, rm.ID)
		}
		a.obsMu.Unlock()
	}
}

func (a *Acceptor) sendReply(lc *linkSession, requestID string, reply AttachReply) {
	raw, err := encodeControl(controlFrame{RequestID: requestID, Kind: ctrlAttachReply, AttachReply: &reply})
	if err != nil {
		return
	}
	_ = lc.sendControl(raw)
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
	var lc *linkSession
	if len(hs) > 0 {
		lc = hs[len(hs)-1].lc // most recently registered connection for this compute
	}
	a.linksMu.Unlock()
	if lc == nil {
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
	if err := lc.sendControl(raw); err != nil {
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
// otherwise have handled. A "no live connection" is a non-fatal Go error the
// caller logs (the reservation is already gone; a missed reclaim is at worst a
// leftover empty dir the next resource-delete-scale reclaim never revisits —
// the same best-effort posture every other daemon-side cleanup in this plane
// documents).
func (a *Acceptor) SendReclaimRequest(ctx context.Context, daemonID, coord string) error {
	a.linksMu.Lock()
	hs := a.links[daemonID]
	var lc *linkSession
	if len(hs) > 0 {
		lc = hs[len(hs)-1].lc
	}
	a.linksMu.Unlock()
	if lc == nil {
		return fmt.Errorf("link: no live connection for daemon %q", daemonID)
	}

	req := ReclaimRequest{RequestID: newRequestID(), Coord: coord}
	ch := a.pendingReclaim.register(req.RequestID)
	raw, err := encodeStorageControl(storageControlFrame{Kind: ctrlReclaimRequest, ReclaimRequest: &req})
	if err != nil {
		a.pendingReclaim.cancel(req.RequestID)
		return err
	}
	if err := lc.sendControl(raw); err != nil {
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
// Serve goroutines to exit.
func (a *Acceptor) Close() error {
	a.cancel()
	a.wg.Wait()
	a.logger.Info("link.acceptor_closed")
	return nil
}
