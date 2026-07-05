package link

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
	lc        *linkConn
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
}

// NewAcceptor builds an Acceptor.
func NewAcceptor(cfg Config) *Acceptor {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Acceptor{
		minter:     cfg.Minter,
		access:     cfg.Access,
		sched:      cfg.Schedule,
		runtime:    cfg.Runtime,
		membership: cfg.Membership,
		registry:   cfg.Registry,
		channelID:  cfg.ChannelID,
		logger:     logger,
		leasePing:  cfg.LeasePing,
		leaseTTL:   cfg.LeaseTTL,
		ctx:        ctx,
		cancel:     cancel,
		attached:   map[string]int{},
		obsWatcher: cfg.ObsWatcher,
		obsReg:     map[actor.ActorID]bool{},
		links:      map[string][]linkHandle{},
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
func (a *Acceptor) deregisterLink(id string, lc *linkConn) {
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
	// first accepted attach and torn down when runLink returns. Single-goroutine
	// (onControl runs on this run loop), so a plain guard suffices.
	var boundID string
	defer func() {
		if boundID != "" {
			a.markDetached(boundID)
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

	// onOpen: each peer-opened actor stream runs native ipc — hand it straight to
	// runtime.Attach. The substrate does the ipc handshake on the stream, resolves
	// the actor (checks it is in the declared set), and registers it as a port
	// embodiment. EOF on the stream (OpClose or link teardown) = the port reads EOF
	// = down edge. The emitSink is the home write gate (the same notify pen
	// a local cell writes with); the authoritative WriteResult flows back as the
	// ipc EmitAck (writer contract not downgraded across the wire).
	onOpen := func(s *stream) {
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
			inc, err := a.runtime.Attach(hsCtx, s, sinks, resolve, actorrt.KindOf(kindOf))
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

	// the per-link lease, refreshed by any inbound frame.
	lease := NewLease(a.leasePing, a.leaseTTL)

	var lc *linkConn
	onControl := func(payload []byte) {
		cf, err := decodeControl(payload)
		if err != nil || cf.Kind != ctrlAttach || cf.Attach == nil {
			return
		}
		id, accepted := a.handleAttach(reqCtx, lc, cf.Attach, daemonID, &mu, &allowed, &kinds, &portMu, ports)
		// Count the daemon online only after a SUCCESSFUL attach (membership
		// applied, Accepted reply sent) — a rejected/half attach must not show
		// online. Once per link (first accepted frame).
		if accepted && boundID == "" {
			boundID = id
			a.markAttached(id)
		}
	}

	lc = newLinkConn(&wsConn{ws: ws}, onControl, onOpen)

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

	// Lease watchdog: tears the link down when last-seen falls behind TTL.
	done := make(chan struct{})
	go lease.Watch(done, func() {
		a.logger.Info("link.lease_expired", "channel", string(a.channelID))
		_ = lc.Close()
	})

	// Acceptor Close tears the link down; quiet-stop this link's ports FIRST so the
	// ensuing stream EOFs are silent. A request-context cancel or peer-gone (the run
	// loop ending) stays LOUD — those are the positively-observed death edges.
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

	lc.run(lease.Refresh)
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
func (a *Acceptor) handleAttach(ctx context.Context, lc *linkConn, att *AttachRequest, daemonID string, mu *sync.Mutex, allowed *map[actor.ActorID]bool, kinds *map[actor.ActorID]actor.Kind, portMu *sync.Mutex, ports map[actor.ActorID]actorrt.Incarnation) (string, bool) {
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
			a.sendReply(lc, AttachReply{Accepted: false, Reason: "declared actor id is reserved: " + string(d.ActorID)})
			return "", false
		}
	}

	if a.membership != nil {
		nowMs := time.Now().UnixMilli()
		adds := make([]storespec.MemberActorAdd, 0, len(att.Declarations))
		for _, d := range att.Declarations {
			// 膜律 (问①, v1.8): a declaration only STAMPS Host onto an EXISTING
			// active member — it never mints membership. An id with no active户籍
			// (never Admitted, or already deregistered) is refused: "a daemon may
			// attach" must not silently升级 to "a daemon may任命 members" (it runs on
			// a half-trusted user host). 有户籍 → applyMemberAddTx only UPDATEs host
			// for an active row (只盖 Host). registry-less rigs (nil) keep the old
			// unconditional write (no truth store to consult). 问②③ (placement /
			// desired_host authority) is enforced at plan generation (app), not here.
			if a.registry != nil {
				rec, ok, err := a.registry.Lookup(ctx, d.ActorID)
				if err != nil {
					a.sendReply(lc, AttachReply{Accepted: false, Reason: "membership lookup: " + err.Error()})
					return "", false
				}
				if !ok || !rec.IsActive() {
					a.logger.Warn("link.attach.declaration_no_membership",
						"compute", computeID, "actor", string(d.ActorID))
					continue
				}
			}
			adds = append(adds, storespec.MemberActorAdd{ID: d.ActorID, Kind: d.Kind, Binding: d.Binding, Host: computeID, At: nowMs})
		}
		if len(adds) > 0 {
			if err := a.membership.ApplyMemberTransitions(ctx, adds, nil); err != nil {
				a.sendReply(lc, AttachReply{Accepted: false, Reason: "register: " + err.Error()})
				return "", false
			}
		}
	}

	// Build the FULL declared set fresh, then swap it in under mu in one step — a
	// Reattach carries every actor this compute currently hosts (never an
	// increment), so this IS the re-diff: an id absent from att.Declarations that
	// was allowed before is simply not in newAllowed after.
	newAllowed := make(map[actor.ActorID]bool, len(att.Declarations))
	newKinds := make(map[actor.ActorID]actor.Kind, len(att.Declarations))
	for _, d := range att.Declarations {
		newAllowed[d.ActorID] = true
		newKinds[d.ActorID] = d.Kind
	}
	mu.Lock()
	*allowed = newAllowed
	*kinds = newKinds
	mu.Unlock()

	a.reconcileHost(ctx, computeID, newAllowed, portMu, ports)

	// Fold each declared actor's obs PUSH (L3 device presence) into the home fold.
	// Registered here (before the actor's stream opens / port publishes) so no
	// early edge is missed; deduped so a reconnect does not re-append the watcher.
	if a.obsWatcher != nil {
		a.obsMu.Lock()
		for _, d := range att.Declarations {
			if !a.obsReg[d.ActorID] {
				a.runtime.WatchObs(d.ActorID, a.obsWatcher)
				a.obsReg[d.ActorID] = true
			}
		}
		a.obsMu.Unlock()
	}

	a.sendReply(lc, AttachReply{ChannelID: a.channelID, Accepted: true})
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

func (a *Acceptor) sendReply(lc *linkConn, reply AttachReply) {
	raw, err := encodeControl(controlFrame{Kind: ctrlAttachReply, AttachReply: &reply})
	if err != nil {
		return
	}
	_ = lc.sendControl(raw)
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
// authenticated bound id, and the authoritative Outcome returns as the KindAccess
// ack. The caller identity is welded HERE (the door minter binds inc.ID()) — the
// wire's self-reported Invocation.Caller is rejected fail-fast, never trusted.
// The minted handle is wrapped in a liveAccess over the emitting port's OWN
// Incarnation (same source as emitSink — never a cross-stream lookup) and freshly
// per invocation (the port death gate on the access plane): a replaced/torn-down
// port's in-flight invoke is fenced with ErrAccessNotLive instead of acting on a
// dead incarnation's behalf. State rides this same arm — the scope field selects
// MintState (actor-scoped) over Mint (channel-scoped).
func (a *Acceptor) accessSink() actorrt.RelaySink {
	return func(ctx context.Context, inc actorrt.Incarnation, payload []byte) ([]byte, error) {
		if a.access == nil {
			return nil, errors.New("link: access plane not wired on this home")
		}
		var req accessRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			return nil, fmt.Errorf("link: access payload decode: %w", err)
		}
		if req.Inv.Caller != "" {
			// Identity is not caller-settable across the wire (mirrors the pen
			// rejecting a pre-filled Sender): fail-fast, never silently overwrite.
			return nil, fmt.Errorf("link: access invocation self-reported caller %q — identity is home-welded, not wire-settable", req.Inv.Caller)
		}
		id := inc.ID()
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
		return json.Marshal(accessResponse{Value: outcome.Value, Found: outcome.Found, RejectReason: outcome.RejectReason})
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
