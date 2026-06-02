package channelhost

import (
	"context"
	"database/sql"
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

	chain   *harness.Chain
	channel *channelkit.Channel
	system  *sysactor.SystemActor
	nowMs   func() int64

	// remoteDispatch routes a request to an actor hosted on an attached compute
	// (injected by server.Run with fleet.Dispatch). nil → no remote computes.
	remoteDispatch func(target actor.ActorID, env *message.Envelope) bool
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
		ChannelID: h.channelID,
		Chain:     h.chain,
		Lookup:    h.lookup,
		Registry:  h.registry,
		TypeReg:   h.typeReg,
		Clock:     func() time.Time { return time.UnixMilli(h.nowMs()) },
	})
	if err != nil {
		return "", err
	}
	h.channel.Cells().Spawn(res.ActorID, res.Actor)
	return res.ActorID, nil
}

// MaterialiseComputeDeath is the home-side收口 for a death that happened on an
// attached compute: the compute cell has NO local truth, so when its supervisor
// observes death it sends a DeathFrame UP and the fleet calls this. The home —
// which DOES own truth — materialises receiver_unavailable for the dead actor's
// in-flight requests (same author #3 as a local cell death). server.Run wires
// this into fleet.SetOnDeath. Without it a compute cell death is a black hole on
// the home side (the P0 "死 cell 黑洞" extended across the wire).
func (h *ChannelHome) MaterialiseComputeDeath(ctx context.Context, dead actor.ActorID) {
	clock := func() time.Time { return time.UnixMilli(h.nowMs()) }
	channelkit.MaterialiseReceiverUnavailable(ctx, h.chain, h.messages, clock, h.channelID, dead)
}

// SetRemoteDispatch wires the fleet's compute-dispatch seam so requests for
// actors hosted on an attached compute are routed down the wire.
func (h *ChannelHome) SetRemoteDispatch(fn func(actor.ActorID, *message.Envelope) bool) {
	h.remoteDispatch = fn
}

// Dispatch writes env into channel truth (9-step harness) and, for requests,
// fans it out to the audience — local固有 cells (channelkit) or, if the actor
// is hosted on an attached compute, down the wire via remoteDispatch. This is
// the home-side router that makes truth-flip + compute hosting work end to end.
func (h *ChannelHome) Dispatch(ctx context.Context, env *message.Envelope) (khrn.WriteResult, error) {
	res, err := h.chain.Write(ctx, env)
	if err != nil || res.RejectReason != "" {
		return res, err
	}
	if env.Kind == message.KindRequest {
		for _, aid := range env.Audience {
			if h.channel.Cells().Has(aid) {
				_ = h.channel.Cells().Deliver(ctx, []actor.ActorID{aid}, env)
			} else if h.remoteDispatch != nil {
				h.remoteDispatch(aid, env)
			}
		}
	}
	return res, nil
}

// Config configures a channel home.
type Config struct {
	ChannelID channel.ID
	DBPath    string
	NowMs     func() int64
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

	chain, err := harness.New(harness.Deps{
		ChannelID:     cfg.ChannelID,
		ActorRegistry: registry,
		TypeRegistry:  typeReg.HarnessView(),
		Log:           messages,
		NowMs:         nowMs,
	})
	if err != nil {
		return nil, fmt.Errorf("channelhost: assemble harness: %w", err)
	}

	// The channel system actor is a固有 cell on the home (advisory presence /
	// readiness / catalog). It writes its own answers through the chain.
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
		Chain:        chain,    // death-signal closure (author #3)
		OpenRequests: messages, // store.Messages implements OpenRequestSource
		Clock:        func() time.Time { return time.UnixMilli(nowMs()) },
	})

	return &ChannelHome{
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
	}, nil
}

// Chain exposes the harness write path (the only way to mutate channel truth).
func (h *ChannelHome) Chain() *harness.Chain { return h.chain }

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
