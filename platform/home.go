// Package platform is the channel-home and attached-compute assembly: it puts the
// position-blind logical world (substrate) into the positioned physical world.
// home.go is the channel-home assembly root — it owns truth and presence wiring
// for one channel and delivers a narrow capability set (not an organ bag):
//
//	Open(cfg) → *Home
//	Gate()  harness.Writer    — the commit write门 (harness -> notify)
//	View()  View              — read-only observation set (ReadAfterSeq/MaxSeq/ListActors)
//	Spawn(ctx,id,kind,impl)   — in-process cell安置 (membership + spawn)
//	Links() *link.Acceptor    — attach受理面 (app hands an upgraded WS here)
//	Taps()  *tap.Signal       — subscription注册面 (client push)
//	Close() error
//
// Everything else (runtime, deliverer, membership, registry) is internal wiring.
// Post-commit effects are tap subscribers, not inline writer steps: cell delivery
// is a Pump over the commit Signal (持 Deliverer, DeliverResult observed here),
// client push is the Signal directly. Centralised multi-tenant = Open的工厂化, not a
// second Home shape.
package platform

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/wanpengxie/ActOS/lib/channelkit"
	"github.com/wanpengxie/ActOS/lib/sysactor"
	"github.com/wanpengxie/ActOS/platform/link"
	"github.com/wanpengxie/ActOS/platform/tap"
	"github.com/wanpengxie/ActOS/protocol/actor"
	channelpkg "github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// HomeConfig configures the channel-home assembly.
type HomeConfig struct {
	ChannelID channelpkg.ID
	DBPath    string
	Logger    *slog.Logger
}

// Home is the assembled channel-home. Its public surface is the capability set in
// the package doc —裸 Runtime/Deliverer/Membership/Registry never escape it (装配
// 只交钥匙). The app layer owns HTTP/transport; Home is pure Go.
type Home struct {
	channelID channelpkg.ID
	writer    harness.Writer
	channel   *channelkit.Channel
	cs        *runtime.ChannelStores
	signal    *tap.Signal
	delivery  *tap.Pump
	links     *link.Acceptor
	logger    *slog.Logger
	nowMs     func() int64
}

// Open assembles the channel home. Assembly is linearised by the tap seam (no
// construction cycle, no back-fill):
//
//	stores -> harness -> signal -> notify写门 -> channelkit(spawns sysactor
//	against the live runtime) -> delivery tap -> link acceptor.
func Open(cfg HomeConfig) (*Home, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if cfg.ChannelID == "" {
		return nil, fmt.Errorf("platform: ChannelID required")
	}
	ctx := context.Background()
	nowMs := func() int64 { return time.Now().UnixMilli() }

	// 1. Open channel stores (substrate).
	cs, err := runtime.OpenChannel(ctx, cfg.DBPath, runtime.OpenChannelOptions{})
	if err != nil {
		return nil, fmt.Errorf("platform: open channel store: %w", err)
	}

	// 2. Build raw harness chain (substrate).
	rawChain, err := harness.New(harness.Deps{
		ChannelID:     cfg.ChannelID,
		ActorRegistry: cs.Registry,
		Log:           cs.Log,
		Logger:        logger,
	})
	if err != nil {
		_ = cs.Close()
		return nil, fmt.Errorf("platform: build harness: %w", err)
	}

	// 3. Build the commit Signal (tap fan-out). It has NO dependencies — this is
	//    what dissolves the old construction cycle: the写门's only post-commit
	//    duty is Notify(), so it needs the Signal, not the Deliverer.
	signal := tap.NewSignal()

	// 4. Build the notify写门: write -> on commit, Notify(). No业务 effect, no
	//    dependency. Every effect (cell delivery, client push) is a downstream tap
	//    subscriber, never an inline writer step (写门零内联效果).
	writer := &notifyWriter{inner: rawChain, notify: signal.Notify}

	// 5. Bootstrap: register the intrinsic system actor so its substrate-death
	//    terminals pass harness sender validation.
	if err := cs.Membership.Insert(ctx, storespec.Record{
		ID: actor.SystemActorID, Kind: actor.KindSystem, CreatedAt: nowMs(),
	}); err != nil {
		_ = cs.Close()
		return nil, fmt.Errorf("platform: register system actor: %w", err)
	}

	// 6. channelkit: actorrt runtime + sysactor + death-edge wiring. The system
	//    cell is built against the LIVE runtime (factory) — its presence Stat seam
	//    reads the real runtime at construction, no back-filled pointer.
	clock := func() time.Time { return time.UnixMilli(nowMs()) }
	channel := channelkit.New(channelkit.Config{
		ChannelID: cfg.ChannelID,
		System: func(rt *actorrt.Runtime) actorrt.Actor {
			return sysactor.New(sysactor.Deps{
				Registry: cs.Registry,
				Writer:   writer,
				Lookup:   cs.Requests,
				Clock:    clock,
				Stat:     &runtimePresenceAdapter{rt: rt},
			})
		},
		Writer:       writer,
		OpenRequests: cs.Query,
		Clock:        clock,
		Logger:       logger,
	})

	// 7. Build the delivery tap: a Pump over the Signal持 Deliverer. cursor start
	//    = current MaxSeq (mailbox semantics: only new commits). DeliverResult
	//    lands here as structured per-audience logs.
	from, err := cs.Query.MaxSeq(ctx)
	if err != nil {
		channel.Cells().StopAll()
		_ = cs.Close()
		return nil, fmt.Errorf("platform: read max seq: %w", err)
	}
	deliver := deliveryHandle(channel.Deliverer(), cfg.ChannelID, logger)
	delivery := tap.NewPump(signal, cs.Query, from, deliver, logger)
	delivery.Start()

	// 8. Build the link acceptor (physical layer: WS mux + per-actor ipc streams
	//    + lease judgement for attached computes).
	links := link.NewAcceptor(link.Config{
		Writer:     writer,
		Runtime:    channel.Cells(),
		Membership: cs.Membership,
		ChannelID:  cfg.ChannelID,
		Logger:     logger,
	})

	logger.Info("platform.home.ready", "channel", string(cfg.ChannelID))
	return &Home{
		channelID: cfg.ChannelID,
		writer:    writer,
		channel:   channel,
		cs:        cs,
		signal:    signal,
		delivery:  delivery,
		links:     links,
		logger:    logger,
		nowMs:     nowMs,
	}, nil
}

// Gate returns the commit write门 (harness -> notify) — the pen an in-process
// cell, the client/SDK ingress, and the link emit-sink all write truth with.
func (h *Home) Gate() harness.Writer { return h.writer }

// View returns the read-only observation set (ReadAfterSeq / MaxSeq /
// ListActors). It carries no写 capability — observation only.
func (h *Home) View() View {
	return View{query: h.cs.Query, registry: h.cs.Registry}
}

// Spawn admits one actor into the channel as durable membership truth and, when
// impl is non-nil, places it as a live in-process cell (binding=embedded).
//
// Membership is the共同前缀 of both: a presence-bearing cell (impl != nil) and a
// presence-less member (impl == nil — e.g. a human user, who is a member but has
// no cell) take the SAME control-plane transition: absent -> insert, soft-
// deregistered -> reactivate, active -> no-op, each with its system.actor.*
// mirror event in the same tx. Membership ≠ presence is the substrate truth here;
// the cell, if any, is the presence层 placed on top. A pre-existing row (server
// restart) is reused — the live instance rebinds. The impl is opaque to platform
// (the app layer decides WHAT to place; Home only knows HOW).
func (h *Home) Spawn(ctx context.Context, id actor.ActorID, kind actor.Kind, impl actorrt.Actor) error {
	if id == "" {
		return fmt.Errorf("platform: Spawn id required")
	}
	binding := actor.Binding("")
	if impl != nil {
		binding = actor.BindingEmbedded
	}
	if err := h.cs.Membership.ApplyMemberTransitions(ctx, h.channelID, []storespec.MemberActorAdd{{
		ID: id, Kind: kind, Binding: binding, At: h.nowMs(),
	}}, nil); err != nil {
		return fmt.Errorf("platform: Spawn membership: %w", err)
	}
	if impl != nil {
		h.channel.Cells().Spawn(id, impl)
	}
	h.logger.Info("platform.member.spawned", "channel", string(h.channelID),
		"actor", string(id), "kind", string(kind), "cell", impl != nil)
	return nil
}

// Links returns the link acceptor (physical layer: the app hands an upgraded WS
// here so a daemon can attach its actor streams).
func (h *Home) Links() *link.Acceptor { return h.links }

// Taps returns the commit Signal (tap fan-out): client streams Subscribe to it
// and read forward from their own seq cursor.
func (h *Home) Taps() *tap.Signal { return h.signal }

// Close tears down the channel home in order: link acceptor (WS connections +
// per-actor streams) -> delivery tap -> cells -> channel stores (DB).
func (h *Home) Close() error {
	// 1. Link acceptor first: close all WS links, tear down every actor stream,
	//    wait for Serve goroutines. Stops external compute traffic before the
	//    runtime/stores underneath shut down.
	linkErr := h.links.Close()
	// 2. Delivery tap: stop the pump before tearing the runtime down.
	h.delivery.Close()
	// 3. Cells: stop actor cells (system actors included).
	h.channel.Cells().StopAll()
	// 4. Channel stores (DB) last.
	csErr := h.cs.Close()

	if linkErr != nil {
		return linkErr
	}
	return csErr
}

// ---------------------------------------------------------------------------
// View -- the read-only observation capability
// ---------------------------------------------------------------------------

// View is the channel-home's read-only observation set: committed message tail
// (ReadAfterSeq), head cursor (MaxSeq), and active actor roster (ListActors). It
// holds only read interfaces — there is no write path through a View.
type View struct {
	query    storespec.MessageQuery
	registry storespec.Registry
}

// ReadAfterSeq returns committed envelopes with seq > afterSeq (client tail).
func (v View) ReadAfterSeq(ctx context.Context, afterSeq int64, limit int) ([]storespec.StoredRow, error) {
	return v.query.ReadAfterSeq(ctx, afterSeq, limit)
}

// MaxSeq returns the channel's current head seq (client cursor anchor).
func (v View) MaxSeq(ctx context.Context) (int64, error) {
	return v.query.MaxSeq(ctx)
}

// ListActors returns all active actors from the membership registry.
func (v View) ListActors(ctx context.Context) ([]storespec.Record, error) {
	return v.registry.ListActive(ctx)
}

// ---------------------------------------------------------------------------
// runtimePresenceAdapter -- bridges actorrt.Runtime.Stat -> sysactor.PresenceStat
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// notifyWriter -- the one generic commit写门 wrapper
// ---------------------------------------------------------------------------

// notifyWriter wraps the raw harness chain with the single generic post-commit
// duty: on a committed (non-rejected) write, wake the commit Signal. It has no
// business effect and no dependency — every actual effect (cell delivery, client
// push) is a tap subscriber reading forward from its own cursor, never an inline
// writer step (写门零内联效果).
type notifyWriter struct {
	inner  harness.Writer
	notify func()
}

func (w *notifyWriter) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	res, err := w.inner.Write(ctx, env)
	if err != nil || res.RejectReason != "" {
		return res, err
	}
	w.notify()
	return res, err
}

// ---------------------------------------------------------------------------
// delivery tap handle -- the cell-delivery Pump's per-row work
// ---------------------------------------------------------------------------

// deliveryHandle is the delivery tap's per-row work: deliver the committed
// envelope to its audience cells and OBSERVE the per-audience Outcome (the
// substrate's structured DeliverResult lands here — NotHosted / MailboxFull /
// Stopped are logged, never silently dropped). It is best-effort (push-mailbox
// semantics): a not-hosted / full mailbox is observed, not retried, so the
// handle always returns nil and the pump cursor always advances.
func deliveryHandle(d actorrt.Deliverer, chID channelpkg.ID, logger *slog.Logger) func(storespec.StoredRow) error {
	return func(row storespec.StoredRow) error {
		env := row.Envelope
		res, err := d.Deliver(env.Audience, &env)
		if err != nil {
			logger.Error("platform.delivery.error",
				"channel", string(chID), "seq", row.Seq, "envelope", string(env.ID), "err", err)
			return nil
		}
		for id, outcome := range res.Per {
			if outcome == actorrt.Delivered {
				continue
			}
			logger.Warn("platform.delivery.outcome",
				"channel", string(chID), "seq", row.Seq, "envelope", string(env.ID),
				"audience", string(id), "outcome", outcomeString(outcome))
		}
		return nil
	}
}

// outcomeString names an actorrt.Outcome for structured logging (an observation
// label, not a semantic branch — the handle does not act differently per kind).
func outcomeString(o actorrt.Outcome) string {
	switch o {
	case actorrt.Delivered:
		return "delivered"
	case actorrt.NotHosted:
		return "not_hosted"
	case actorrt.MailboxFull:
		return "mailbox_full"
	case actorrt.Stopped:
		return "stopped"
	default:
		return "unknown"
	}
}
