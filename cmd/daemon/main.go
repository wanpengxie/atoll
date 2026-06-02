// Command daemon runs a v2 attached compute (hosts business cells; no truth).
// cloud daemon and user/proxy daemon are the same binary. This is the concrete
// adapter injection point + obs leaf: it selects product modules (by --adapters)
// and injects the concrete obs backends (slog + metrics).
package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"os"
	"strings"

	"github.com/wanpengxie/ActOS/adapters/feishu"
	"github.com/wanpengxie/ActOS/daemon"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/obs/metrics"
)

// buildModules constructs the concrete adapter Modules named in --adapters (the
// only place that imports concrete adapters, T3). Each is a real adapter cell on
// the compute; external creds/transport are the adapter's own config concern.
func buildModules(names []string, logger behavior.Logger, metrics behavior.Metrics) []behavior.Module {
	var mods []behavior.Module
	for _, n := range names {
		switch strings.TrimSpace(n) {
		case "":
			// skip empty entries
		case "feishu":
			mods = append(mods, feishu.New(
				feishu.WithDeps(behavior.Deps{Logger: logger, Metrics: metrics}),
			))
		default:
			log.Fatalf("daemon: unknown adapter %q", n)
		}
	}
	return mods
}

func main() {
	ws := flag.String("server", "ws://localhost:8080", "channel home ws url")
	key := flag.String("key", "", "api key")
	ch := flag.String("channel", "default", "channel id")
	adapters := flag.String("adapters", "", "comma-separated concrete adapters to host (e.g. feishu)")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	reg := metrics.NewRegistry()
	modules := buildModules(strings.Split(*adapters, ","), logger, reg)

	if err := daemon.Run(context.Background(), daemon.Config{
		ServerWS:  *ws,
		APIKey:    *key,
		ChannelID: channel.ID(*ch),
		Logger:    logger,
		Metrics:   reg,
	}, modules); err != nil {
		log.Fatalf("daemon: %v", err)
	}
}
