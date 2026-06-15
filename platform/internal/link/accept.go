package link

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/ipc"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// errUndeclaredActor is the resolve verdict for an actor stream whose lease id
// is not in the link's attach declaration set (an actor the daemon never
// declared may not bind a presence).
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
	writer     harness.Writer
	runtime    *actorrt.Runtime
	membership storespec.MembershipControlPlane
	channelID  channel.ID
	logger     *slog.Logger
	leasePing  time.Duration
	leaseTTL   time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// attached is the live attach refcount per compute id (daemon). A daemon is
	// "online" (L1 link presence) iff its count > 0. Refcount, not bool, so an
	// overlapping reconnect (old link tearing down after the new one attached)
	// does not flap the daemon offline. Volatile runtime state — never persisted.
	attachedMu sync.Mutex
	attached   map[string]int

	// obsWatcher folds each attached actor's obs PUSH (L3 device presence) into the
	// home presence fold; registered per declared actor at attach (the home-side
	// arm of the actor-source obs axis). nil → no folding. obsReg dedups so a
	// daemon reconnect does not re-append the same watcher.
	obsWatcher actorrt.ObsWatcher
	obsMu      sync.Mutex
	obsReg     map[actor.ActorID]bool
}

// Config configures an Acceptor. Auth is the app layer's concern — Serve
// receives a pre-authenticated daemonID. LeasePing/LeaseTTL default to the
// centralised constants (10s / 30s); zero means default (tests may shorten).
type Config struct {
	Writer     harness.Writer
	Runtime    *actorrt.Runtime
	Membership storespec.MembershipControlPlane
	ChannelID  channel.ID
	Logger     *slog.Logger
	LeasePing  time.Duration
	LeaseTTL   time.Duration
	// ObsWatcher (optional) receives each attached actor's obs PUSH via per-actor
	// WatchObs registration at attach — the home-side arm of the L3 presence fold.
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
		writer:     cfg.Writer,
		runtime:    cfg.Runtime,
		membership: cfg.Membership,
		channelID:  cfg.ChannelID,
		logger:     logger,
		leasePing:  cfg.LeasePing,
		leaseTTL:   cfg.LeaseTTL,
		ctx:        ctx,
		cancel:     cancel,
		attached:   map[string]int{},
		obsWatcher: cfg.ObsWatcher,
		obsReg:     map[actor.ActorID]bool{},
	}
}

// markAttached / markDetached / IsAttached / AttachedDaemons are the L1 link-
// presence read seam: the Acceptor authoritatively holds which computes have a
// live attach right now (it owns the connections + lease). markAttached is
// called once per accepted link (after attach success); markDetached once when
// that link tears down (peer gone / lease expiry / Close). Empty id (dev self-
// declared mode) is not tracked.
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

// IsAttached reports whether compute id has a live attach right now (L1).
func (a *Acceptor) IsAttached(id string) bool {
	a.attachedMu.Lock()
	defer a.attachedMu.Unlock()
	return a.attached[id] > 0
}

// AttachedDaemons returns a snapshot of the currently-attached compute ids.
func (a *Acceptor) AttachedDaemons() []string {
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

	// allowed is the attach declaration set: the resolve seam校验 an opening
	// actor stream is one the daemon actually declared (membership-backed).
	var (
		mu      sync.Mutex
		allowed = map[actor.ActorID]bool{}
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

	// onOpen: each peer-opened actor stream runs native ipc — hand it straight to
	// runtime.Attach. The substrate does the ipc handshake on the stream, resolves
	// the actor (校验 it is in the declared set), and registers it as a port
	// presence. EOF on the stream (OpClose or link teardown) = the port reads EOF
	// = presence-down edge. The emitSink is the home write门 (the same notify pen
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
			if _, err := a.runtime.Attach(hsCtx, s, a.emitSink(), resolve); err != nil {
				a.logger.Info("link.attach_stream_failed", "err", err)
			}
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
		id, accepted := a.handleAttach(reqCtx, lc, cf.Attach, daemonID, &mu, allowed)
		// Count the daemon online only after a SUCCESSFUL attach (membership
		// applied, Accepted reply sent) — a rejected/half attach must not show
		// online. Once per link (first accepted frame).
		if accepted && boundID == "" {
			boundID = id
			a.markAttached(id)
		}
	}

	lc = newLinkConn(&wsConn{ws: ws}, onControl, onOpen)

	// Lease watchdog: tears the link down when last-seen falls behind TTL.
	done := make(chan struct{})
	go lease.Watch(done, func() {
		a.logger.Info("link.lease_expired", "channel", string(a.channelID))
		_ = lc.Close()
	})
	// Acceptor Close / request cancellation also tears the link down.
	go func() {
		select {
		case <-a.ctx.Done():
			_ = lc.Close()
		case <-reqCtx.Done():
			_ = lc.Close()
		case <-done:
		}
	}()

	lc.run(lease.Refresh)
	close(done)
}

// handleAttach processes the stream-0 attach: register declared actors into
// membership (register/reactivate — detach never deregisters), record the
// allowed set, and reply. Membership semantics照旧: a member row is durable; a
// daemon detaching does NOT remove it (membership ≠ presence).
// handleAttach processes the stream-0 attach and reports (computeID, accepted)
// so the caller can count L1 link presence only on success.
func (a *Acceptor) handleAttach(ctx context.Context, lc *linkConn, att *AttachRequest, daemonID string, mu *sync.Mutex, allowed map[actor.ActorID]bool) (string, bool) {
	computeID := att.ComputeID
	if daemonID != "" {
		computeID = daemonID
	}

	if a.membership != nil {
		nowMs := time.Now().UnixMilli()
		adds := make([]storespec.MemberActorAdd, len(att.Declarations))
		for i, d := range att.Declarations {
			adds[i] = storespec.MemberActorAdd{ID: d.ActorID, Kind: d.Kind, Binding: d.Binding, At: nowMs}
		}
		if err := a.membership.ApplyMemberTransitions(ctx, adds, nil); err != nil {
			a.sendReply(lc, AttachReply{Accepted: false, Reason: "register: " + err.Error()})
			return "", false
		}
	}

	mu.Lock()
	for _, d := range att.Declarations {
		allowed[d.ActorID] = true
	}
	mu.Unlock()

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

func (a *Acceptor) sendReply(lc *linkConn, reply AttachReply) {
	raw, err := encodeControl(controlFrame{Kind: ctrlAttachReply, AttachReply: &reply})
	if err != nil {
		return
	}
	_ = lc.sendControl(raw)
}

// emitSink builds the per-link EmitSink: a remote cell's emit is written through
// the home write门 with the source actor stamped as caller, and the
// authoritative WriteResult returns as the ipc EmitAck. The caller identity is
// the connection's authenticated bound id (the basis stamps the author), NOT the
// envelope's self-reported sender — the identity axiom does not downgrade across
// the wire, so a stream authenticated as one actor emitting on behalf of another
// falls on sender_mismatch exactly as a local cell would.
func (a *Acceptor) emitSink() actorrt.EmitSink {
	return func(ctx context.Context, id actor.ActorID, env *message.Envelope) (ipc.EmitResult, error) {
		cctx := harness.CtxWithCaller(ctx, harness.CallerContext{
			ActorID:   id,
			ChannelID: a.channelID,
		})
		res, err := a.writer.Write(cctx, env)
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

// CancelRequest reaches the request-scope of cancel(scope) across the wire: the
// home (where a request's caller lives) tells the daemon hosting `actor` to
// cancel the reqCtx its cell is running `requestID` under. The bound port
// presence writes a KindCancel frame down that actor's stream; the daemon fires
// the matching reqCtx off its cell goroutine. No-op if the actor is not a hosted
// port here or the request already closed — cancel is a best-effort hint, the
// caller's closure owns the terminal. The real producer (a caller actively
// abandoning a request) is the app/domain trigger above the substrate; this is
// the substrate mechanism it drives.
func (a *Acceptor) CancelRequest(target actor.ActorID, requestID message.ID) {
	a.runtime.CancelRequest(target, requestID)
}

// Close stops accepting new links and tears down active ones, waiting for all
// Serve goroutines to exit.
func (a *Acceptor) Close() error {
	a.cancel()
	a.wg.Wait()
	a.logger.Info("link.acceptor_closed")
	return nil
}
