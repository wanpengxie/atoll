// Command daemon runs a v2 attached compute (hosts business cells; no truth).
// cloud daemon and user/proxy daemon are the same binary. This is the concrete
// adapter injection point (kimi/feishu/xhs/proxyfacade) — the leaf that selects
// product modules and hands them to daemon.Run.
package main

import (
	"context"
	"flag"
	"log"

	"github.com/wanpengxie/ActOS/daemon"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/lib/behavior"
)

func main() {
	ws := flag.String("server", "ws://localhost:8080", "channel home ws url")
	key := flag.String("key", "", "api key")
	ch := flag.String("channel", "default", "channel id")
	flag.Parse()

	// Concrete adapter modules are selected here (the only place that imports
	// concrete adapters, T3). Wiring of kimi/feishu/xhs/proxyfacade factories
	// with their Deps lands here.
	var modules []behavior.Module

	if err := daemon.Run(context.Background(), daemon.Config{
		ServerWS:  *ws,
		APIKey:    *key,
		ChannelID: channel.ID(*ch),
	}, modules); err != nil {
		log.Fatalf("daemon: %v", err)
	}
}
