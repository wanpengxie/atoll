// Package server is the channel-home assembly root (v2): it owns the
// postCommitWriter (harness -> deliver -> notify), the pushHub (client
// notification), and wires channelhost (business layer) + fleet (physical
// layer) + gateway (client ingress) into a runnable process. cmd/server
// selects concrete adapters and injects obs.
package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/server/channelhost"
	"github.com/wanpengxie/ActOS/server/fleet"
)

// Config configures the channel-home process.
type Config struct {
	ChannelID  channel.ID
	DBPath     string
	ListenAddr string
	APIKey     string // authenticates attaching computes
	Logger     *slog.Logger
}

// Run assembles the channel home and serves. Assembly sequence per v2.4:
//
//  1. Open stores via runtime.OpenChannel
//  2. Build raw harness chain
//  3. Create backfillWriter placeholder (breaks channelkit <-> writer cycle)
//  4. Build channelhost.New with placeholder writer
//  5. Build pushHub
//  6. Build postCommitWriter with rawChain + home.Deliverer() + hub.notify
//  7. Backfill the placeholder
//  8. Build fleet with writer + home.Runtime() + membership
//  9. Build gateway with writer + hub + query + registry
//  10. Mount HTTP routes + serve
func Run(ctx context.Context, cfg Config) error {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	// 1. Open channel stores (substrate).
	cs, err := runtime.OpenChannel(ctx, cfg.DBPath, runtime.OpenChannelOptions{})
	if err != nil {
		return fmt.Errorf("server: open channel store: %w", err)
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
		return fmt.Errorf("server: build harness: %w", err)
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
		return err
	}
	defer func() { _ = home.Close() }()

	// 5. Build pushHub (client notification).
	hub := newPushHub()

	// 6. Build postCommitWriter (assembly root glue):
	//    write -> deliver to audience cells -> notify client subscribers.
	writer := &postCommitWriter{
		inner:     rawChain,
		deliverer: home.Deliverer(),
		notify:    hub.notify,
	}

	// 7. Backfill the placeholder, closing the cycle.
	bw.fill(writer)

	// 8. Build fleet (physical layer: WS mux/demux for attached computes).
	flt := fleet.New(fleet.Config{
		Writer:     writer,
		Runtime:    home.Runtime(),
		Membership: cs.Membership,
		ChannelID:  cfg.ChannelID,
		APIKey:     cfg.APIKey,
		Logger:     logger,
	})

	// 9. Build gateway (client/SDK ingress).
	gw := &gateway{
		writer:    writer,
		hub:       hub,
		channelID: cfg.ChannelID,
		query:     cs.Query,
		registry:  cs.Registry,
	}

	// 10. Mount HTTP routes + serve.
	mux := http.NewServeMux()
	mux.HandleFunc("/compute", flt.ServeWS)
	gw.mount(mux)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{Addr: cfg.ListenAddr, Handler: mux}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	logger.Info("server.listening", "addr", cfg.ListenAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// postCommitWriter — assembly root private type
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
// backfillWriter — cycle-breaking placeholder
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
		panic("backfillWriter: Write called before fill — construction order violated")
	}
	return w.Write(ctx, env)
}
