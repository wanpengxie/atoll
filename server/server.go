// Package server is the channel-home assembly (v2): channelhost (truth) + fleet
// (attached computes) + gateway ingress into a runnable process. cmd/server
// selects concrete adapters and injects obs.
package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/runtime"
	"github.com/wanpengxie/ActOS/server/channelhost"
	"github.com/wanpengxie/ActOS/server/fleet"
	"github.com/wanpengxie/ActOS/wire/placement"
)

// Config configures the channel-home process.
type Config struct {
	ChannelID  channel.ID
	DBPath     string
	ListenAddr string
	APIKey     string // authenticates attaching computes
	Logger     *slog.Logger
}

// Run assembles the channel home, mounts the compute fleet (attached daemons)
// and a client ingress, and serves. It holds channel truth (v2 truth-flip):
// client requests write into truth then fan out to local cells or down the
// wire to the compute hosting the target actor.
func Run(ctx context.Context, cfg Config) error {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	// Open channel store via the public runtime facade.
	cs, err := runtime.OpenChannel(ctx, cfg.DBPath, runtime.OpenChannelOptions{})
	if err != nil {
		return fmt.Errorf("server: open channel store: %w", err)
	}

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
		Logger: logger,
	})
	if err != nil {
		_ = cs.Close()
		return err
	}
	defer func() { _ = home.Close() }()

	plc := placement.New()

	flt := fleet.New(fleet.Config{
		Writer:    home.Writer(),
		ChannelID: cfg.ChannelID,
		APIKey:    cfg.APIKey,
		Placement: plc,
		OnDeath:   home.MaterialiseComputeDeath,
		OnAttach:  home.RegisterComputeActors,
		Logger:    logger,
	})

	// Wire the fleet's dispatch as the fanout writer's remote arm.
	home.SetRemoteDispatch(flt.Dispatch)

	mux := http.NewServeMux()
	// Attached computes (daemons) connect here (computebus WS).
	mux.HandleFunc("/compute", flt.ServeWS)
	// Client/SDK ingress: the SDK routes (cursor / messages / actors / ws).
	gw := &gateway{home: home, channelID: cfg.ChannelID}
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
