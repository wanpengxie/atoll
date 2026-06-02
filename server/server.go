// Package server is the channel-home assembly (v2): channelhost (truth) + fleet
// (attached computes) + gateway ingress into a runnable process. cmd/server
// selects concrete adapters and injects obs.
package server

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/server/channelhost"
	"github.com/wanpengxie/ActOS/server/fleet"
)

// Config configures the channel-home process.
type Config struct {
	ChannelID  channel.ID
	DBPath     string
	ListenAddr string
	APIKey     string // authenticates attaching computes
}

// Run assembles the channel home, mounts the compute fleet (attached daemons)
// and a client ingress, and serves. It holds channel truth (v2 truth-flip):
// client requests write into truth then fan out to local固有 cells or down the
// wire to the compute hosting the target actor.
func Run(ctx context.Context, cfg Config) error {
	home, err := channelhost.New(ctx, channelhost.Config{
		ChannelID: cfg.ChannelID,
		DBPath:    cfg.DBPath,
	})
	if err != nil {
		return err
	}

	flt := fleet.New(home.Chain(), cfg.APIKey)
	home.SetRemoteDispatch(flt.Dispatch)
	// Compute cell death (DeathFrame) materialises receiver_unavailable at the
	// home (substrate closure author #3 across the wire).
	flt.SetOnDeath(home.MaterialiseComputeDeath)
	// Caller-scoped closure loop (author #2): expired pending requests get a
	// caller-authored unanswered_timeout. Runs for the home's lifetime.
	go home.RunClosureScan(ctx, time.Second)

	mux := http.NewServeMux()
	// Attached computes (daemons) connect here (computebus WS).
	mux.HandleFunc("/compute", flt.ServeWS)
	// Client/SDK ingress: POST an envelope → written into truth + dispatched.
	mux.HandleFunc("/ingress", func(w http.ResponseWriter, r *http.Request) {
		var env message.Envelope
		if err := json.NewDecoder(r.Body).Decode(&env); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		res, err := home.Dispatch(r.Context(), &env)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(res)
	})
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
