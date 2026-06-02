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

	// remoteDispatch routes a request to an actor hosted on an attached compute
	// (injected by server.Run with fleet.Dispatch). nil → no remote computes.
	remoteDispatch func(target actor.ActorID, env *message.Envelope) bool
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

	messages := store.NewMessages(db)                         // harness.MessageLog
	registry := store.NewActorRegistry(db)                    // actor.Registry
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
