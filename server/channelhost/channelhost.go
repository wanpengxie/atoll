package channelhost

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	khrn "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/adapterhost"
	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/lib/channelkit"
	"github.com/wanpengxie/ActOS/lib/sysactor"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/store"
	"github.com/wanpengxie/ActOS/wire/computebus"
)

// ChannelHome is one channel's truth-holding home (v2). It composes the
// deployment-agnostic core — runtime/store (append-only truth) + runtime/harness
// (9-step write chain) + lib/channelkit (system cell + supervision) — into a
// process that owns channel truth. (Port of framework/multiuser/runtime/
// daemon.go channel runtime, minus the v1 viewsync/channel-lock/observer cruft.)
type ChannelHome struct {
	channelID channel.ID

	db       *sql.DB
	messages *store.Messages
	registry *store.ActorRegistry
	typeReg  *store.TypeRegistry
	lookup   *store.RequestLookup

	// chain is the fanout-wrapping write path: every committed envelope fans out
	// to its audience (local cells / remote computes). All cell-originated writes
	// (responses, events, closure terminals) and ingress go through it, so a
	// write reaching truth always reaches its audience.
	chain   *fanoutChain
	channel *channelkit.Channel
	system  *sysactor.SystemActor
	nowMs   func() int64

	logger  behavior.Logger
	metrics behavior.Metrics

	hub *pushHub // client-push fan-out signal
}

// RunClosureScan runs the caller-scoped closure loop (closure author #2): it
// periodically scans for pending requests past their expires_at with no final
// response and writes a caller-authored unanswered_timeout terminal for each
// (sender = the request's own sender; harness Step 8 callerSelfClose authorises
// it). This is the home-side materialisation of "caller 到点不等了" — the only
// timeout author (no global-guess). Blocks until ctx is done.
func (h *ChannelHome) RunClosureScan(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h.ClosurePass(ctx)
		}
	}
}

// ClosurePass does one caller-scoped closure sweep (exposed for tests). For each
// expired pending request it writes an unanswered_timeout terminal authored by
// the request's own sender (caller-scoped).
func (h *ChannelHome) ClosurePass(ctx context.Context) {
	reqs, err := h.messages.LongPendingRequests(ctx, h.nowMs(), 0)
	if err != nil {
		return
	}
	clock := func() time.Time { return time.UnixMilli(h.nowMs()) }
	for i := range reqs {
		req := reqs[i]
		term, berr := behavior.BuildResponseFromRequest(&req, clock, req.Sender,
			behavior.CorrelationKey(req.ID),
			behavior.ResponseSpec{
				Status: "failed",
				Reason: string(message.TerminalUnansweredTimeout),
			})
		if berr != nil {
			continue
		}
		cctx := harness.CtxWithCaller(ctx, harness.CallerContext{ActorID: req.Sender.ID, ChannelID: h.channelID, AllowProvidedSenderKind: true})
		_, _ = h.chain.Write(cctx, term)
	}
}

// InstallEmbeddedAdapter hosts an embedded-binding adapter as a real cell ON THE
// HOME (co-located with truth — binding=embedded means no relay/compute hop). It
// wires every home-side InstallDeps seam DIRECTLY (Registry/TypeReg/Lookup/Chain
// all point at home truth, no uplink), registers the actor, publishes its type
// rows, and spawns the cell into channelkit. This is the P0 "hosts something"
// path: a request dispatched here reaches the cell's Handle and its Respond
// writes a real terminal back into truth — the full happy path, end to end.
func (h *ChannelHome) InstallEmbeddedAdapter(ctx context.Context, mod behavior.Module) (actor.ActorID, error) {
	decl := mod.Declares()
	// The embedded actor must exist in truth before Install validates it (and so
	// its responses sender-authenticate through the harness).
	if err := h.registry.Insert(ctx, actor.Record{ID: decl.ActorID, Kind: actor.KindTool, Binding: decl.Binding}); err != nil {
		return "", fmt.Errorf("channelhost: register embedded actor %s: %w", decl.ActorID, err)
	}
	res, err := adapterhost.Install(ctx, mod, adapterhost.InstallDeps{
		ChannelID:     h.channelID,
		Chain:         h.chain,
		Lookup:        h.lookup,
		Registry:      h.registry,
		TypeReg:       h.typeReg,
		ReadinessSink: h.registry, // store.ActorRegistry implements ReadinessUpdater
		Logger:        h.logger,
		Metrics:       h.metrics,
		Clock:         func() time.Time { return time.UnixMilli(h.nowMs()) },
	})
	if err != nil {
		return "", err
	}
	h.channel.Cells().Spawn(res.ActorID, res.Actor)
	// An embedded cell is co-located with truth — always present (no lease to
	// re-arm); a very long TTL keeps it present without a heartbeat.
	h.MarkPresence(ctx, res.ActorID, true, embeddedPresenceTTLMs)
	return res.ActorID, nil
}

// embeddedPresenceTTLMs is the (effectively non-expiring) presence lease for an
// embedded cell — it lives with the home, so there is no compute heartbeat.
const embeddedPresenceTTLMs int64 = 100 * 365 * 24 * 60 * 60 * 1000

// MarkPresence emits a SYSTEM-authored actor.presence.changed event (fanned out
// to the sysactor cell) recording an actor's physical presence — true on
// attach/heartbeat (lease re-armed for leaseTTLMs), false on detach. This is the
// compute→fleet→home→sysactor presence link (the gap: fleet was a no-op and the
// sysactor presence projection stayed empty). Advisory only — never a dispatch
// gate.
func (h *ChannelHome) MarkPresence(ctx context.Context, id actor.ActorID, present bool, leaseTTLMs int64) {
	now := h.nowMs()
	payload, _ := json.Marshal(map[string]any{"actor": string(id), "present": present, "lease_ttl_ms": leaseTTLMs})
	env := &message.Envelope{
		// ts in the id keeps each heartbeat re-arm a distinct append-only event.
		ID:        message.ID(fmt.Sprintf("event:actor.presence.changed:%s:%d", id, now)),
		TS:        now,
		ChannelID: h.channelID,
		// SystemOnly type → sender MUST be the channel system actor; the subject
		// actor travels in the payload ("actor").
		Sender:   message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:     message.KindEvent,
		Type:     "actor.presence.changed",
		Payload:  payload,
		Audience: message.Audience{actor.SystemActorID},
	}
	cctx := harness.CtxWithCaller(ctx, harness.CallerContext{ActorID: actor.SystemActorID, ChannelID: h.channelID, AllowProvidedSenderKind: true})
	_, _ = h.chain.Write(cctx, env)
}

// RegisterComputeActors registers an attaching compute's actors into truth and
// publishes their request types — the compute holds no truth, so the home does
// this on its behalf at attach (the fleet calls it). After this, a request for
// one of the compute's types passes the harness type check and the fanout routes
// it down the wire to the hosting compute.
func (h *ChannelHome) RegisterComputeActors(ctx context.Context, decls []computebus.AttachDeclaration) error {
	h.logger.Info("fleet.attach", "actors", len(decls))
	h.metrics.IncCounter("fleet.attach", "channel", string(h.channelID))
	for _, d := range decls {
		if err := h.registry.Insert(ctx, actor.Record{ID: d.ActorID, Kind: d.Kind, Binding: d.Binding}); err != nil {
			return fmt.Errorf("channelhost: register compute actor %s: %w", d.ActorID, err)
		}
		for _, t := range d.Types {
			if _, err := h.typeReg.Upsert(ctx, message.TypeRow{
				Type: t, HandlerActorID: d.ActorID, HandlerBinding: d.Binding, MaxPendingMs: d.MaxPendingMs,
				AllowedKinds: []message.Kind{message.KindEvent, message.KindRequest, message.KindResponse},
			}); err != nil {
				return fmt.Errorf("channelhost: publish compute type %s: %w", t, err)
			}
		}
	}
	return nil
}

// MaterialiseComputeDeath is the home-side收口 for a death that happened on an
// attached compute: the compute cell has NO local truth, so when its supervisor
// observes death it sends a DeathFrame UP and the fleet calls this. The home —
// which DOES own truth — materialises receiver_unavailable for the dead actor's
// in-flight requests (same author #3 as a local cell death). server.Run wires
// this into fleet.SetOnDeath. Without it a compute cell death is a black hole on
// the home side (the P0 "死 cell 黑洞" extended across the wire).
func (h *ChannelHome) MaterialiseComputeDeath(ctx context.Context, dead actor.ActorID) {
	reqs, err := h.messages.OpenRequestsForActor(ctx, dead, 0)
	if err != nil {
		return
	}
	clock := func() time.Time { return time.UnixMilli(h.nowMs()) }
	sys := message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID}
	for i := range reqs {
		req := reqs[i]
		term, berr := behavior.BuildResponseFromRequest(&req, clock, sys,
			behavior.CorrelationKey(req.ID),
			behavior.ResponseSpec{Status: "failed", Reason: string(message.TerminalReceiverUnavailable)})
		if berr != nil {
			continue
		}
		// System-authored terminal (harness Step 8 substrateDeath author); the
		// fanout chain delivers it to the waiting caller. Caller context = system.
		cctx := harness.CtxWithCaller(ctx, harness.CallerContext{ActorID: actor.SystemActorID, ChannelID: h.channelID, AllowProvidedSenderKind: true})
		_, _ = h.chain.Write(cctx, term)
	}
}

// SetRemoteDispatch wires the fleet's compute-dispatch seam so requests for
// actors hosted on an attached compute are routed down the wire. It feeds the
// fanout chain's remote leg, so EVERY committed request (ingress or
// cell-originated) reaches a remote actor — not just ingress.
func (h *ChannelHome) SetRemoteDispatch(fn func(actor.ActorID, *message.Envelope) bool) {
	h.chain.remote = fn
}

// Dispatch writes env into channel truth (9-step harness); the fanout chain then
// delivers the committed envelope to its audience (local cells / remote compute).
// This is the client/SDK ingress seam — a thin wrapper over the universal
// write→fanout path that every cell write also takes.
func (h *ChannelHome) Dispatch(ctx context.Context, env *message.Envelope) (khrn.WriteResult, error) {
	return h.chain.Write(ctx, env)
}

// Config configures a channel home.
type Config struct {
	ChannelID channel.ID
	DBPath    string
	NowMs     func() int64
	// Logger + Metrics are the obs seams; cmd injects concrete backends
	// (slog + obs/metrics). nil → no-op (never panics).
	Logger  behavior.Logger
	Metrics behavior.Metrics
}

// New opens the channel's truth store and assembles the home (truth-flip: truth
// lives here at the server, not the daemon). Genesis is server-local — no
// placement-CAS/reclaim (proto-v2-physical-revision §3).
func New(ctx context.Context, cfg Config) (*ChannelHome, error) {
	if cfg.ChannelID == "" {
		return nil, fmt.Errorf("channelhost: ChannelID required")
	}
	nowMs := cfg.NowMs
	if nowMs == nil {
		nowMs = func() int64 { return time.Now().UnixMilli() }
	}
	logger := cfg.Logger
	if logger == nil {
		logger = behavior.NoopLogger{}
	}
	metrics := cfg.Metrics
	if metrics == nil {
		metrics = behavior.NoopMetrics{}
	}

	db, err := store.OpenChannel(ctx, cfg.DBPath, store.OpenOptions{})
	if err != nil {
		return nil, fmt.Errorf("channelhost: open channel store: %w", err)
	}

	messages := store.NewMessages(db)      // harness.MessageLog
	registry := store.NewActorRegistry(db) // actor.Registry
	// Bootstrap: the channel system actor is a固有 actor — register it so its
	// substrate-death terminals (author#3) pass harness sender validation.
	_ = registry.Insert(ctx, actor.Record{ID: actor.SystemActorID, Kind: actor.KindSystem})
	typeReg := store.NewTypeRegistry(db, nowMs)               // message.TypeRegistry + harness view
	lookup := store.NewRequestLookup(messages, cfg.ChannelID) // message.RequestLookup

	rawChain, err := harness.New(harness.Deps{
		ChannelID:     cfg.ChannelID,
		ActorRegistry: registry,
		TypeRegistry:  typeReg.HarnessView(),
		Log:           messages,
		NowMs:         nowMs,
	})
	if err != nil {
		return nil, fmt.Errorf("channelhost: assemble harness: %w", err)
	}
	// Wrap the harness in the fanout chain — the universal write→deliver path.
	// Every cell + the death closure + ingress write through this, so a committed
	// envelope always reaches its audience. cells is back-filled once channelkit
	// builds the runtime (no write happens during construction).
	chain := &fanoutChain{inner: rawChain}

	// The channel system actor is a固有 cell on the home (advisory presence /
	// readiness / catalog). It writes its own answers through the fanout chain.
	system := sysactor.New(sysactor.Deps{
		ChannelID: cfg.ChannelID,
		Registry:  registry,
		Chain:     chain,
		Lookup:    lookup,
		Clock:     func() time.Time { return time.UnixMilli(nowMs()) },
	})

	ch := channelkit.New(channelkit.Config{
		ChannelID:    cfg.ChannelID,
		System:       system,
		Chain:        chain,    // death-signal closure (author #3) — terminals fan out
		OpenRequests: messages, // store.Messages implements OpenRequestSource
		Clock:        func() time.Time { return time.UnixMilli(nowMs()) },
	})
	chain.cells = ch.Cells() // back-fill the fanout target now the runtime exists

	h := &ChannelHome{
		channelID: cfg.ChannelID,
		db:        db,
		messages:  messages,
		registry:  registry,
		typeReg:   typeReg,
		lookup:    lookup,
		chain:     chain,
		channel:   ch,
		system:    system,
		nowMs:     nowMs,
		logger:    logger,
		metrics:   metrics,
		hub:       newPushHub(),
	}
	// Mailbox-full / cell-stopped on a request → close it out (needs the built
	// home for the seam bundle).
	chain.onUndeliverable = h.materialiseUnavailableForRequest
	chain.onCommit = h.hub.notify // wake client streams on every commit
	logger.Info("channelhost.ready", "channel", string(cfg.ChannelID))
	return h, nil
}

// Subscribe registers a client stream and returns a wake signal + cancel. The
// signal fires on every committed envelope; the caller reads forward from its
// own seq cursor via ReadAfterSeq (lossy signal, seq-correct read).
func (h *ChannelHome) Subscribe() (<-chan struct{}, func()) { return h.hub.subscribe() }

// MaxSeq returns the channel's current head seq (client cursor anchor).
func (h *ChannelHome) MaxSeq(ctx context.Context) (int64, error) {
	return h.messages.MaxSeq(ctx, h.channelID)
}

// ReadAfterSeq returns committed envelopes with seq > afterSeq (client tail).
func (h *ChannelHome) ReadAfterSeq(ctx context.Context, afterSeq int64, limit int) ([]message.Envelope, error) {
	return h.messages.ReadAfterSeq(ctx, h.channelID, afterSeq, limit)
}

// materialiseUnavailableForRequest writes a system-authored receiver_unavailable
// terminal for a single request whose delivery to its target cell failed. The
// request envelope is in hand (no lookup), and the fanout chain delivers the
// terminal to the waiting caller.
func (h *ChannelHome) materialiseUnavailableForRequest(ctx context.Context, req *message.Envelope) {
	clock := func() time.Time { return time.UnixMilli(h.nowMs()) }
	sys := message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID}
	term, err := behavior.BuildResponseFromRequest(req, clock, sys,
		behavior.CorrelationKey(req.ID),
		behavior.ResponseSpec{Status: "failed", Reason: string(message.TerminalReceiverUnavailable)})
	if err != nil {
		return
	}
	cctx := harness.CtxWithCaller(ctx, harness.CallerContext{ActorID: actor.SystemActorID, ChannelID: h.channelID, AllowProvidedSenderKind: true})
	_, _ = h.chain.Write(cctx, term)
}

// Chain exposes the fanout write path (the only way to mutate channel truth):
// every committed envelope fans out to its audience. The fleet writes compute
// emits through it, so a compute cell's response reaches a local waiting caller.
func (h *ChannelHome) Chain() khrn.Chain { return h.chain }

// Registry exposes the actor registry projection.
func (h *ChannelHome) Registry() *store.ActorRegistry { return h.registry }

// TypeRegistry exposes the type registry.
func (h *ChannelHome) TypeRegistry() *store.TypeRegistry { return h.typeReg }

// Lookup exposes the request-lookup seam (F5).
func (h *ChannelHome) Lookup() *store.RequestLookup { return h.lookup }

// Channel exposes the assembled channelkit (cells + policy + supervision).
func (h *ChannelHome) Channel() *channelkit.Channel { return h.channel }

// Messages exposes the messages store (truth read side; for tests + death scan).
func (h *ChannelHome) Messages() *store.Messages { return h.messages }
