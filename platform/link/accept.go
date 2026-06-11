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

// Lease parameters: the home judges per-link liveness. last-seen is refreshed by
// ANY inbound frame; the daemon pings stream 0 every leasePing; if no frame
// arrives within leaseTTL the home declares the link dead (the正面观察 a frozen
// /half-open daemon never produces a TCP EOF for). Centralised + tunable.
const (
	leasePing = 10 * time.Second
	leaseTTL  = 30 * time.Second
)

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
}

// NewAcceptor builds an Acceptor.
func NewAcceptor(cfg Config) *Acceptor {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ping := cfg.LeasePing
	if ping <= 0 {
		ping = leasePing
	}
	ttl := cfg.LeaseTTL
	if ttl <= 0 {
		ttl = leaseTTL
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Acceptor{
		writer:     cfg.Writer,
		runtime:    cfg.Runtime,
		membership: cfg.Membership,
		channelID:  cfg.ChannelID,
		logger:     logger,
		leasePing:  ping,
		leaseTTL:   ttl,
		ctx:        ctx,
		cancel:     cancel,
	}
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
			if _, err := a.runtime.Attach(s, a.emitSink(reqCtx), resolve); err != nil {
				a.logger.Info("link.attach_stream_failed", "err", err)
				_ = s.Close()
			}
		}()
	}

	// last-seen lease state, refreshed by any inbound frame.
	var lastSeen atomicTime
	lastSeen.set(time.Now())

	var lc *linkConn
	onControl := func(payload []byte) {
		cf, err := decodeControl(payload)
		if err != nil || cf.Kind != ctrlAttach || cf.Attach == nil {
			return
		}
		a.handleAttach(reqCtx, lc, cf.Attach, daemonID, &mu, allowed)
	}

	lc = newLinkConn(&wsConn{ws: ws}, onControl, onOpen)

	// Lease watchdog: tears the link down when last-seen falls behind TTL.
	done := make(chan struct{})
	go a.leaseWatch(lc, &lastSeen, done)
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

	lc.run(func() { lastSeen.set(time.Now()) })
	close(done)
}

// handleAttach processes the stream-0 attach: register declared actors into
// membership (register/reactivate — detach never deregisters), record the
// allowed set, and reply. Membership semantics照旧: a member row is durable; a
// daemon detaching does NOT remove it (membership ≠ presence).
func (a *Acceptor) handleAttach(ctx context.Context, lc *linkConn, att *AttachRequest, daemonID string, mu *sync.Mutex, allowed map[actor.ActorID]bool) {
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
		if err := a.membership.ApplyMemberTransitions(ctx, a.channelID, adds, nil); err != nil {
			a.sendReply(lc, AttachReply{Accepted: false, Reason: "register: " + err.Error()})
			return
		}
	}

	mu.Lock()
	for _, d := range att.Declarations {
		allowed[d.ActorID] = true
	}
	mu.Unlock()

	a.sendReply(lc, AttachReply{ChannelID: a.channelID, Accepted: true})
	a.logger.Info("link.attached", "compute", computeID, "actors", len(att.Declarations))
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
// authoritative WriteResult returns as the ipc EmitAck.
func (a *Acceptor) emitSink(reqCtx context.Context) actorrt.EmitSink {
	return func(ctx context.Context, env *message.Envelope) (ipc.EmitResult, error) {
		cctx := harness.CtxWithCaller(ctx, harness.CallerContext{
			ActorID:   env.Sender.ID,
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

// leaseWatch is the liveness法官: every leasePing it checks last-seen; if the
// gap exceeds leaseTTL it tears the whole link down (all actor streams EOF =
// every presence on this party falls on the same presence-down edge — the
// closure materialises receiver_unavailable). A frozen daemon (no TCP EOF) is
// killed here.
func (a *Acceptor) leaseWatch(lc *linkConn, lastSeen *atomicTime, done <-chan struct{}) {
	t := time.NewTicker(a.leasePing)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-t.C:
			if time.Since(lastSeen.get()) > a.leaseTTL {
				a.logger.Info("link.lease_expired", "channel", string(a.channelID))
				_ = lc.Close()
				return
			}
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
