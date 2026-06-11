// Package platform is the channel-home assembly root (v2): it owns the commit
// write门 (harness -> notify), the commit Signal (tap fan-out), and wires
// channelhost (business layer) + link acceptor (physical layer) + Gateway (client
// ingress) into a ChannelHome struct. Post-commit effects are tap subscribers,
// not inline writer steps: cell delivery is a Pump over the Signal (持 Deliverer,
// DeliverResult observed here), client push is the Signal directly. The app
// layer (cmd/server) selects concrete adapters, injects obs, and owns HTTP
// serving.
package platform

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/wanpengxie/ActOS/platform/channelhost"
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

// ChannelHome is the assembled channel-home: it holds all the wired parts
// (stores, harness, channelhost, link acceptor, gateway, signal, delivery tap)
// and exposes them via accessor methods. The app layer owns HTTP/transport;
// ChannelHome is pure Go.
type ChannelHome struct {
	writer   harness.Writer
	home     *channelhost.ChannelHome
	cs       *runtime.ChannelStores
	signal   *tap.Signal
	delivery *tap.Pump
	links    *link.Acceptor
	gw       *Gateway
}

// NewChannelHome assembles the channel home. Assembly is linearised by the tap
// seam (no construction cycle, no back-fill): stores -> harness -> signal ->
// notify写门 -> channelhost(spawns sysactor against live runtime) -> taps.
//
//  1. Open stores via runtime.OpenChannel
//  2. Build raw harness chain
//  3. Build the commit Signal (tap fan-out; no dependencies)
//  4. Build the notify写门 (rawChain + signal.Notify) — the pen cells write with
//  5. Build channelhost with the notify写门 as Writer
//  6. Build the delivery tap: a Pump持 Deliverer, cursor start = MaxSeq, started
//  7. Build the link acceptor with写门 + home.Runtime() + membership
//  8. Build Gateway with写门 + signal + query + registry
func NewChannelHome(cfg HomeConfig) (*ChannelHome, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	ctx := context.Background()

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

	// 4. Build the notify写门 (assembly root glue): write -> on commit, Notify().
	//    No业务 effect, no dependency. Every effect (cell delivery, client push)
	//    is a downstream tap subscriber, never an inline writer step.
	writer := &notifyWriter{inner: rawChain, notify: signal.Notify}

	// 5. Build channelhost (business layer: stores + channelkit + sysactor). The
	//    sysactor cell is spawned against the live runtime inside channelkit (no
	//    presence back-fill).
	home, err := channelhost.New(ctx, channelhost.Config{
		ChannelID: cfg.ChannelID,
		Stores: channelhost.Stores{
			Log:        cs.Log,
			Query:      cs.Query,
			Requests:   cs.Requests,
			Registry:   cs.Registry,
			Membership: cs.Membership,
			Close:      cs.Close,
		},
		Writer: writer,
		Logger: logger,
	})
	if err != nil {
		_ = cs.Close()
		return nil, err
	}

	// 6. Build the delivery tap: a Pump over the Signal持 Deliverer. cursor start
	//    = current MaxSeq (mailbox semantics: don't replay history, only new
	//    commits). DeliverResult lands here as structured per-audience logs.
	from, err := cs.Query.MaxSeq(ctx)
	if err != nil {
		_ = home.Close()
		return nil, fmt.Errorf("platform: read max seq: %w", err)
	}
	deliver := deliveryHandle(home.Deliverer(), cfg.ChannelID, logger)
	delivery := tap.NewPump(signal, cs.Query, from, deliver, logger)
	delivery.Start()

	// 7. Build the link acceptor (physical layer: WS mux + per-actor ipc streams
	//    + lease judgement for attached computes).
	links := link.NewAcceptor(link.Config{
		Writer:     writer,
		Runtime:    home.Runtime(),
		Membership: cs.Membership,
		ChannelID:  cfg.ChannelID,
		Logger:     logger,
	})

	// 8. Build Gateway (client/SDK ingress).
	gw := &Gateway{
		writer:    writer,
		signal:    signal,
		channelID: cfg.ChannelID,
		query:     cs.Query,
		registry:  cs.Registry,
	}

	return &ChannelHome{
		writer:   writer,
		home:     home,
		cs:       cs,
		signal:   signal,
		delivery: delivery,
		links:    links,
		gw:       gw,
	}, nil
}

// Runtime returns the actorrt.Runtime from the channelhost.
func (ch *ChannelHome) Runtime() *actorrt.Runtime { return ch.home.Runtime() }

// Writer is the commit write门 (harness -> notify) — the pen an in-process cell
// writes truth with.
func (ch *ChannelHome) Writer() harness.Writer { return ch.writer }

// SpawnCell registers + spawns one in-process actor cell (binding=embedded).
// The impl is opaque to platform; the app layer decides what to spawn.
func (ch *ChannelHome) SpawnCell(ctx context.Context, id actor.ActorID, kind actor.Kind, impl actorrt.Actor) error {
	return ch.home.SpawnCell(ctx, id, kind, impl)
}

// Deliverer returns the actorrt.Deliverer from the channelhost.
func (ch *ChannelHome) Deliverer() actorrt.Deliverer { return ch.home.Deliverer() }

// Query returns the message query store.
func (ch *ChannelHome) Query() storespec.MessageQuery { return ch.cs.Query }

// Registry returns the actor registry store.
func (ch *ChannelHome) Registry() storespec.Registry { return ch.cs.Registry }

// Membership returns the membership control plane store.
func (ch *ChannelHome) Membership() storespec.MembershipControlPlane { return ch.cs.Membership }

// Links returns the link acceptor (physical layer: the app hands an upgraded WS
// here so a daemon can attach its actor streams).
func (ch *ChannelHome) Links() *link.Acceptor { return ch.links }

// PushHub returns the client notification signal (tap fan-out): client streams
// Subscribe to it and read forward from their own seq cursor.
func (ch *ChannelHome) PushHub() *tap.Signal { return ch.signal }

// Gateway returns the client/SDK ingress gateway.
func (ch *ChannelHome) Gateway() *Gateway { return ch.gw }

// Close tears down the channel home in order: link acceptor (WS connections +
// per-actor streams) -> delivery tap -> channelhost (actors + business logic) ->
// channel stores (DB).
func (ch *ChannelHome) Close() error {
	// 1. Link acceptor first: close all WS links, tear down every actor stream,
	//    wait for Serve goroutines. This stops all external compute traffic
	//    before we shut down the runtime/stores underneath.
	linkErr := ch.links.Close()

	// 2. Delivery tap: stop the pump before tearing the runtime down.
	ch.delivery.Close()

	// 3. Channelhost: stops actor cells, system actors.
	homeErr := ch.home.Close()

	// 4. Channel stores (DB) last.
	csErr := ch.cs.Close()

	// Return the first error encountered.
	if linkErr != nil {
		return linkErr
	}
	if homeErr != nil {
		return homeErr
	}
	return csErr
}

// ---------------------------------------------------------------------------
// notifyWriter -- the one generic commit写门 wrapper
// ---------------------------------------------------------------------------

// notifyWriter wraps the raw harness chain with the single generic post-commit
// duty: on a committed (non-rejected) write, wake the commit Signal. It has no
// business effect and no dependency — every actual effect (cell delivery,
// client push) is a tap subscriber reading forward from its own cursor, never
// an inline writer step (写门零内联效果).
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
