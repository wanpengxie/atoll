package channelhost

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/lib/channelkit"
	"github.com/wanpengxie/ActOS/lib/sysactor"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// ChannelHome is one channel's business-layer assembly (v2). It composes
// stores + channelkit (actorrt + sysactor + death edge wiring) into the
// per-channel truth holder. It does NOT own the write-fanout path or client
// push -- those live in the assembly root (server.go).
type ChannelHome struct {
	channelID channel.ID
	stores    Stores
	channel   *channelkit.Channel
	logger    *slog.Logger
	nowMs     func() int64
	closer    func() error
}

// Stores bundles the pre-opened storespec interfaces the channelhost needs.
// The caller (server.Run) opens the store via runtime.OpenChannel from a
// package that CAN import it, then passes the interfaces here.
type Stores struct {
	Log        storespec.MessageLog
	Query      storespec.MessageQuery
	Requests   storespec.RequestLookup
	Registry   storespec.Registry
	Membership storespec.MembershipControlPlane
	// Close releases the underlying store resources.
	Close func() error
}

// Config configures a channel home.
type Config struct {
	ChannelID channel.ID
	Stores    Stores
	Writer    harness.Writer // injected by assembly root, not created here
	NowMs     func() int64
	Logger    *slog.Logger
}

// New assembles the channel home from pre-opened stores and an injected
// Writer. The Writer is the post-commit write chain owned by the assembly
// root; channelhost only assembles the business-layer pieces around it.
func New(ctx context.Context, cfg Config) (*ChannelHome, error) {
	if cfg.ChannelID == "" {
		return nil, fmt.Errorf("channelhost: ChannelID required")
	}
	if cfg.Writer == nil {
		return nil, fmt.Errorf("channelhost: Writer required")
	}
	nowMs := cfg.NowMs
	if nowMs == nil {
		nowMs = func() int64 { return time.Now().UnixMilli() }
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	st := cfg.Stores

	// Bootstrap: the channel system actor is intrinsic -- register it so its
	// substrate-death terminals (author#3) pass harness sender validation.
	_ = st.Membership.Insert(ctx, storespec.Record{
		ID: actor.SystemActorID, Kind: actor.KindSystem, CreatedAt: nowMs(),
	})

	// Presence stat adapter: bridges actorrt.Runtime.Stat to sysactor.PresenceStat.
	presenceAdapter := &runtimePresenceAdapter{}

	// sysactor: the channel system cell (advisory directory / actor.list).
	clock := func() time.Time { return time.UnixMilli(nowMs()) }
	sys := sysactor.New(sysactor.Deps{
		Registry: st.Registry,
		Writer:   cfg.Writer,
		Lookup:   st.Requests,
		Clock:    clock,
		Stat:     presenceAdapter,
	})

	// channelkit: assembles actorrt runtime + sysactor + death edge wiring.
	ch := channelkit.New(channelkit.Config{
		ChannelID:    cfg.ChannelID,
		System:       sys,
		Writer:       cfg.Writer,
		OpenRequests: st.Query,
		Clock:        clock,
		Logger:       logger,
	})

	// Back-fill presence adapter with the runtime.
	presenceAdapter.rt = ch.Cells()

	h := &ChannelHome{
		channelID: cfg.ChannelID,
		stores:    st,
		channel:   ch,
		logger:    logger,
		nowMs:     nowMs,
		closer:    st.Close,
	}

	logger.Info("channelhost.ready", "channel", string(cfg.ChannelID))
	return h, nil
}

// SpawnCell registers (idempotently) and spawns one in-process actor cell —
// the server-side host surface (binding=embedded). Membership is durable
// registry truth: a pre-existing row (server restart) is reused, the live
// instance rebinds. The impl is opaque to platform (the assembly above this
// layer decides WHAT to spawn; channelhost only knows HOW).
func (h *ChannelHome) SpawnCell(ctx context.Context, id actor.ActorID, kind actor.Kind, impl actorrt.Actor) error {
	if id == "" || impl == nil {
		return fmt.Errorf("channelhost: SpawnCell id and impl required")
	}
	// Control-plane membership transition (the same op fleet attach uses):
	// absent -> insert, soft-deregistered -> reactivate, active -> no-op —
	// each with its system.actor.* mirror event in the same tx.
	if err := h.stores.Membership.ApplyMemberTransitions(ctx, h.channelID, []storespec.MemberActorAdd{{
		ID: id, Kind: kind, Binding: actor.BindingEmbedded, At: h.nowMs(),
	}}, nil); err != nil {
		return fmt.Errorf("channelhost: SpawnCell membership: %w", err)
	}
	h.channel.Cells().Spawn(id, impl)
	h.logger.Info("channelhost.cell.spawned", "channel", string(h.channelID), "actor", string(id), "kind", string(kind))
	return nil
}

// runtimePresenceAdapter bridges actorrt.Runtime.Stat -> sysactor.PresenceStat.
type runtimePresenceAdapter struct {
	rt *actorrt.Runtime
}

func (a *runtimePresenceAdapter) Stat(id actor.ActorID) (startedAt time.Time, present bool) {
	if a.rt == nil {
		return time.Time{}, false
	}
	stat, ok := a.rt.Stat(id)
	if !ok {
		return time.Time{}, false
	}
	return stat.StartedAt, true
}

// Runtime returns the actorrt runtime so the assembly root can pass it to
// fleet for Attach (registering remote actors as ports).
func (h *ChannelHome) Runtime() *actorrt.Runtime { return h.channel.Cells() }

// Deliverer returns the confined enqueue capability so the assembly root can
// build its postCommitWriter (write -> deliver -> notify).
func (h *ChannelHome) Deliverer() actorrt.Deliverer { return h.channel.Deliverer() }

// Channel exposes the assembled channelkit (cells + death wiring).
func (h *ChannelHome) Channel() *channelkit.Channel { return h.channel }

// ChannelID returns the channel's id.
func (h *ChannelHome) ChannelID() channel.ID { return h.channelID }

// MaxSeq returns the channel's current head seq (client cursor anchor).
func (h *ChannelHome) MaxSeq(ctx context.Context) (int64, error) {
	return h.stores.Query.MaxSeq(ctx)
}

// ReadAfterSeq returns committed envelopes with seq > afterSeq (client tail).
func (h *ChannelHome) ReadAfterSeq(ctx context.Context, afterSeq int64, limit int) ([]storespec.StoredRow, error) {
	return h.stores.Query.ReadAfterSeq(ctx, afterSeq, limit)
}

// ListActiveActors returns all active actors from the membership registry.
func (h *ChannelHome) ListActiveActors(ctx context.Context) ([]storespec.Record, error) {
	return h.stores.Registry.ListActive(ctx)
}

// Close tears down the channel home, releasing the store.
func (h *ChannelHome) Close() error {
	h.channel.Cells().StopAll()
	if h.closer != nil {
		return h.closer()
	}
	return nil
}
