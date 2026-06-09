// Package platform is the channel-home assembly root (v2): it owns the
// postCommitWriter (harness -> deliver -> notify), the PushHub (client
// notification), and wires channelhost (business layer) + fleet (physical
// layer) + Gateway (client ingress) into a ChannelHome struct. The app layer
// (cmd/server) selects concrete adapters, injects obs, and owns HTTP serving.
package platform

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
	"github.com/wanpengxie/ActOS/platform/channelhost"
	"github.com/wanpengxie/ActOS/platform/fleet"
)

// HomeConfig configures the channel-home assembly.
type HomeConfig struct {
	ChannelID channel.ID
	DBPath    string
	Logger    *slog.Logger
}

// ChannelHome is the assembled channel-home: it holds all the wired parts
// (stores, harness, channelhost, fleet, gateway, pushHub) and exposes them
// via accessor methods. The app layer owns HTTP/transport; ChannelHome is
// pure Go.
type ChannelHome struct {
	writer     *postCommitWriter
	home       *channelhost.ChannelHome
	cs         *runtime.ChannelStores
	hub        *PushHub
	flt        *fleet.Fleet
	gw         *Gateway
}

// NewChannelHome assembles the channel home. Assembly sequence per v2.4:
//
//  1. Open stores via runtime.OpenChannel
//  2. Build raw harness chain
//  3. Create backfillWriter placeholder (breaks channelkit <-> writer cycle)
//  4. Build channelhost.New with placeholder writer
//  5. Build PushHub
//  6. Build postCommitWriter with rawChain + home.Deliverer() + hub.Notify
//  7. Backfill the placeholder
//  8. Build fleet with writer + home.Runtime() + membership
//  9. Build Gateway with writer + hub + query + registry
func NewChannelHome(cfg HomeConfig) (*ChannelHome, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	ctx := context.Background()

	// 1. Open channel stores (substrate).
	cs, err := runtime.OpenChannel(ctx, cfg.DBPath, runtime.OpenChannelOptions{})
	if err != nil {
		return nil, fmt.Errorf("server: open channel store: %w", err)
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
		return nil, fmt.Errorf("server: build harness: %w", err)
	}

	// 3. Placeholder writer (breaks the cycle: channelkit needs writer,
	//    postCommitWriter needs deliverer from channelkit).
	var bw backfillWriter

	// 4. Build channelhost (business layer: stores + channelkit + sysactor).
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
		Writer: &bw,
		Logger: logger,
	})
	if err != nil {
		_ = cs.Close()
		return nil, err
	}

	// 5. Build PushHub (client notification).
	hub := NewPushHub()

	// 6. Build postCommitWriter (assembly root glue):
	//    write -> deliver to audience cells -> notify client subscribers.
	writer := &postCommitWriter{
		inner:     rawChain,
		deliverer: home.Deliverer(),
		notify:    hub.Notify,
	}

	// 7. Backfill the placeholder, closing the cycle.
	bw.fill(writer)

	// 8. Build fleet (physical layer: WS mux/demux for attached computes).
	flt := fleet.New(fleet.Config{
		Writer:     writer,
		Runtime:    home.Runtime(),
		Membership: cs.Membership,
		ChannelID:  cfg.ChannelID,
		Logger:     logger,
	})

	// 9. Build Gateway (client/SDK ingress).
	gw := &Gateway{
		writer:    writer,
		hub:       hub,
		channelID: cfg.ChannelID,
		query:     cs.Query,
		registry:  cs.Registry,
	}

	return &ChannelHome{
		writer: writer,
		home:   home,
		cs:     cs,
		hub:    hub,
		flt:    flt,
		gw:     gw,
	}, nil
}

// runtime returns the actorrt.Runtime from the channelhost.
func (ch *ChannelHome) runtime() *actorrt.Runtime { return ch.home.Runtime() }

// deliverer returns the actorrt.Deliverer from the channelhost.
func (ch *ChannelHome) deliverer() actorrt.Deliverer { return ch.home.Deliverer() }

// query returns the message query store.
func (ch *ChannelHome) query() storespec.MessageQuery { return ch.cs.Query }

// registry returns the actor registry store.
func (ch *ChannelHome) registry() storespec.Registry { return ch.cs.Registry }

// Membership returns the membership control plane store.
func (ch *ChannelHome) Membership() storespec.MembershipControlPlane { return ch.cs.Membership }

// Fleet returns the fleet (physical layer: WS mux/demux for attached computes).
func (ch *ChannelHome) Fleet() *fleet.Fleet { return ch.flt }

// PushHub returns the client notification hub.
func (ch *ChannelHome) PushHub() *PushHub { return ch.hub }

// Gateway returns the client/SDK ingress gateway.
func (ch *ChannelHome) Gateway() *Gateway { return ch.gw }

// Close tears down the channel home in order: fleet (WS connections + relay
// goroutines) -> channelhost (actors + business logic) -> channel stores (DB).
func (ch *ChannelHome) Close() error {
	// 1. Fleet first: close all WS connections, tear down virtual pipes, wait
	//    for relay goroutines. This stops all external compute traffic before
	//    we shut down the runtime/stores underneath.
	fltErr := ch.flt.Close()

	// 2. Channelhost: stops actor cells, system actors.
	homeErr := ch.home.Close()

	// 3. Channel stores (DB) last.
	csErr := ch.cs.Close()

	// Return the first error encountered.
	if fltErr != nil {
		return fltErr
	}
	if homeErr != nil {
		return homeErr
	}
	return csErr
}

// ---------------------------------------------------------------------------
// postCommitWriter -- assembly root private type
// ---------------------------------------------------------------------------

// postCommitWriter wraps the raw harness chain with post-commit side effects:
// deliver the committed envelope to its audience (actor cells) and notify
// client subscribers that new data is available.
type postCommitWriter struct {
	inner     harness.Writer
	deliverer actorrt.Deliverer
	notify    func()
}

func (w *postCommitWriter) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	res, err := w.inner.Write(ctx, env)
	if err != nil || res.RejectReason != "" {
		return res, err
	}
	w.deliverer.Deliver(env.Audience, env)
	w.notify()
	return res, err
}

// ---------------------------------------------------------------------------
// backfillWriter -- cycle-breaking placeholder
// ---------------------------------------------------------------------------

// backfillWriter is a placeholder harness.Writer that panics if Write is called
// before fill(). It breaks the construction cycle: channelkit needs a Writer,
// but the postCommitWriter needs Deliverer from channelkit.
//
// Safety: channelkit.New spawns the system actor cell, but cells only call
// Handle (and thus Writer.Write) when they receive a Deliver'd envelope. Since
// fill() is called before any Deliver is possible, the placeholder is never
// actually invoked in the nil state.
type backfillWriter struct {
	mu sync.RWMutex
	w  harness.Writer
}

func (b *backfillWriter) fill(w harness.Writer) {
	b.mu.Lock()
	b.w = w
	b.mu.Unlock()
}

func (b *backfillWriter) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	b.mu.RLock()
	w := b.w
	b.mu.RUnlock()
	if w == nil {
		panic("backfillWriter: Write called before fill -- construction order violated")
	}
	return w.Write(ctx, env)
}
