// Package server is the channel-home assembly (v2): it composes channelhost
// (truth) + fleet (attached computes) + gateway (client/UI/SDK ingress) into a
// runnable process. cmd/server selects concrete adapters and injects obs.
package server

import (
	"context"
	"net/http"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/server/channelhost"
)

// Config configures the channel-home process.
type Config struct {
	ChannelID  channel.ID
	DBPath     string
	ListenAddr string
}

// Run assembles the channel home and serves the gateway. It holds channel truth
// (v2 truth-flip). (Minimal gateway/fleet wiring for now; full client ingress +
// compute fleet land incrementally — the truth-flip compose is the core.)
func Run(ctx context.Context, cfg Config) error {
	home, err := channelhost.New(ctx, channelhost.Config{
		ChannelID: cfg.ChannelID,
		DBPath:    cfg.DBPath,
	})
	if err != nil {
		return err
	}
	_ = home // gateway/fleet handlers will close over the home's Chain/Registry.

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: cfg.ListenAddr, Handler: mux}

	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
