// Command server runs the v2 channel-home (holds truth; serves the compute fleet
// + client ingress). This leaf injects the concrete obs backends (slog logger +
// metrics registry + debug server), which the server tier may not import.
package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/obs/metrics"
	"github.com/wanpengxie/ActOS/obs/observability"
	"github.com/wanpengxie/ActOS/server"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	debugAddr := flag.String("debug-addr", "", "obs debug/metrics listen address (empty = off)")
	ch := flag.String("channel", "default", "channel id")
	db := flag.String("db", "channel.db", "channel sqlite path")
	key := flag.String("key", "", "api key for attaching computes")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	reg := metrics.NewRegistry()

	// Concrete obs debug/metrics endpoint (server tier can't import obs).
	if *debugAddr != "" {
		dbg := observability.NewServer(*debugAddr, reg)
		go func() {
			if err := dbg.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Error("obs.debug.serve", "err", err.Error())
			}
		}()
	}

	if err := server.Run(context.Background(), server.Config{
		ChannelID:  channel.ID(*ch),
		DBPath:     *db,
		ListenAddr: *addr,
		APIKey:     *key,
		Logger:     logger,
	}); err != nil {
		log.Fatalf("server: %v", err)
	}
}
