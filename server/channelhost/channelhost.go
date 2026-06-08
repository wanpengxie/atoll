package channelhost

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/lib/channelkit"
	"github.com/wanpengxie/ActOS/lib/sysactor"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
	"github.com/wanpengxie/ActOS/wire/computebus"
)

// ChannelHome is one channel's truth-holding home (v2). It composes the
// deployment-agnostic core -- storespec interfaces (append-only truth) +
// runtime/harness (9-step write chain) + lib/channelkit (system cell + death
// edge wiring) -- into a process that owns channel truth.
type ChannelHome struct {
	channelID channel.ID

	log        storespec.MessageLog
	query      storespec.MessageQuery
	requests   storespec.RequestLookup
	registry   storespec.Registry
	membership storespec.MembershipControlPlane

	// writer is the fanout-wrapping write path: every committed envelope fans
	// out to its audience (local cells / remote computes / client push).
	writer  *fanoutWriter
	channel *channelkit.Channel

	nowMs  func() int64
	logger *slog.Logger

	hub *pushHub // client-push fan-out signal

	// closer is called by Close to release the underlying store resources.
	closer func() error
}

// Stores bundles the pre-opened storespec interfaces the channelhost needs.
// The caller (server.Run) opens the store via runtime/internal/store.OpenChannel
// from a package that CAN import it, then passes the interfaces here.
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
	NowMs     func() int64
	Logger    *slog.Logger
}

// New assembles the channel home from pre-opened stores (truth-flip: truth
// lives here at the server, not the daemon). Genesis is server-local.
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
		logger = slog.New(slog.DiscardHandler)
	}

	st := cfg.Stores

	// Bootstrap: the channel system actor is intrinsic -- register it so its
	// substrate-death terminals (author#3) pass harness sender validation.
	_ = st.Membership.Insert(ctx, storespec.Record{
		ID: actor.SystemActorID, Kind: actor.KindSystem, CreatedAt: nowMs(),
	})

	// Build harness chain.
	rawChain, err := harness.New(harness.Deps{
		ChannelID:     cfg.ChannelID,
		ActorRegistry: st.Registry,
		Log:           st.Log,
		NowMs:         nowMs,
		Logger:        logger,
	})
	if err != nil {
		return nil, fmt.Errorf("channelhost: assemble harness: %w", err)
	}

	// pushHub for client WS push.
	hub := newPushHub()

	// fanoutWriter wraps chain: Write success -> Deliver(local) +
	// remoteDispatch(wire) + hub.notify(client).
	// Deliverer is back-filled once channelkit builds the runtime.
	fw := &fanoutWriter{
		inner: rawChain,
		hub:   hub,
	}

	// Presence stat adapter: bridges actorrt.Runtime.Stat to sysactor.PresenceStat.
	presenceAdapter := &runtimePresenceAdapter{}

	// sysactor: the channel system cell (advisory directory / actor.list).
	clock := func() time.Time { return time.UnixMilli(nowMs()) }
	sys := sysactor.New(sysactor.Deps{
		Registry: st.Registry,
		Writer:   fw,
		Lookup:   st.Requests,
		Clock:    clock,
		Stat:     presenceAdapter,
	})

	// channelkit: assembles actorrt runtime + sysactor + death edge wiring.
	ch := channelkit.New(channelkit.Config{
		ChannelID:    cfg.ChannelID,
		System:       sys,
		Writer:       fw,
		OpenRequests: st.Query,
		Clock:        clock,
		Logger:       logger,
	})

	// Back-fill the fanout deliverer now the runtime exists.
	fw.deliverer = ch.Deliverer()
	// Back-fill presence adapter with the runtime.
	presenceAdapter.rt = ch.Cells()

	h := &ChannelHome{
		channelID:  cfg.ChannelID,
		log:        st.Log,
		query:      st.Query,
		requests:   st.Requests,
		registry:   st.Registry,
		membership: st.Membership,
		writer:     fw,
		channel:    ch,
		nowMs:      nowMs,
		logger:     logger,
		hub:        hub,
		closer:     st.Close,
	}

	logger.Info("channelhost.ready", "channel", string(cfg.ChannelID))
	return h, nil
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

// Writer returns the fanout writer (harness.Writer) -- the only way to mutate
// channel truth. Every committed envelope fans out to its audience.
func (h *ChannelHome) Writer() harness.Writer { return h.writer }

// SetRemoteDispatch wires the fleet's compute-dispatch seam so requests for
// actors hosted on an attached compute are routed down the wire.
func (h *ChannelHome) SetRemoteDispatch(fn func(actor.ActorID, *message.Envelope) bool) {
	h.writer.remoteDispatch = fn
}

// Channel exposes the assembled channelkit (cells + death wiring).
func (h *ChannelHome) Channel() *channelkit.Channel { return h.channel }

// ChannelID returns the channel's id.
func (h *ChannelHome) ChannelID() channel.ID { return h.channelID }

// Subscribe registers a client stream and returns a wake signal + cancel.
func (h *ChannelHome) Subscribe() (<-chan struct{}, func()) { return h.hub.subscribe() }

// MaxSeq returns the channel's current head seq (client cursor anchor).
func (h *ChannelHome) MaxSeq(ctx context.Context) (int64, error) {
	return h.query.MaxSeq(ctx)
}

// ReadAfterSeq returns committed envelopes with seq > afterSeq (client tail).
func (h *ChannelHome) ReadAfterSeq(ctx context.Context, afterSeq int64, limit int) ([]storespec.StoredRow, error) {
	return h.query.ReadAfterSeq(ctx, afterSeq, limit)
}

// ListActiveActors returns all active actors from the membership registry.
func (h *ChannelHome) ListActiveActors(ctx context.Context) ([]storespec.Record, error) {
	return h.registry.ListActive(ctx)
}

// RegisterComputeActors registers an attaching compute's actors into membership.
// The compute holds no truth, so the home does this on its behalf at attach.
func (h *ChannelHome) RegisterComputeActors(ctx context.Context, chID channel.ID, decls []computebus.AttachDeclaration) error {
	adds := make([]storespec.MemberActorAdd, 0, len(decls))
	now := h.nowMs()
	for _, d := range decls {
		adds = append(adds, storespec.MemberActorAdd{
			ID:      d.ActorID,
			Kind:    d.Kind,
			Binding: d.Binding,
			At:      now,
		})
	}
	return h.membership.ApplyMemberTransitions(ctx, chID, adds, nil)
}

// MaterialiseComputeDeath is the home-side closure for a death that happened on
// an attached compute: the compute cell has NO local truth, so the home
// materialises receiver_unavailable for the dead actor's in-flight requests
// (substrate closure author #3, across the wire).
func (h *ChannelHome) MaterialiseComputeDeath(ctx context.Context, dead actor.ActorID) {
	clock := func() time.Time { return time.UnixMilli(h.nowMs()) }
	sys := message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID}
	onFault := func(reqID message.ID, err error) {
		h.logger.Error("channelhost.compute_death.write_failed",
			"dead_actor", string(dead), "request", string(reqID), "err", err)
	}
	// Inject system caller context so the harness authenticates the write.
	cctx := harness.CtxWithCaller(ctx, harness.CallerContext{
		ActorID:   actor.SystemActorID,
		ChannelID: h.channelID,
	})
	_ = behavior.MaterialiseReceiverUnavailable(
		cctx, h.writer, h.query,
		clock, sys, dead, onFault,
	)
}

// Dispatch writes env into channel truth (9-step harness); the fanout writer
// then delivers the committed envelope to its audience. This is the client/SDK
// ingress seam.
func (h *ChannelHome) Dispatch(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	return h.writer.Write(ctx, env)
}

// Close tears down the channel home, releasing the store.
func (h *ChannelHome) Close() error {
	h.channel.Cells().StopAll()
	if h.closer != nil {
		return h.closer()
	}
	return nil
}
