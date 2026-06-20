// Command daemon runs a v2 attached compute (hosts actor cells; no truth).
// Cloud daemon and user/proxy daemon are the same binary. Which actor CLASSES it
// can host comes from the SELF-REGISTERING catalog (driver-registration pattern):
// each in-tree actor package's init() registers its class constructor. The daemon
// hosts one default instance of every compiled TOOL/DEVICE class — but NOT the
// "agent" class (the channel's orchestrator is server-placed; a daemon-hosted
// agent is a server-delivered composition decision, not a CLI flag). Adding an
// in-tree actor = a new package with an init() + one blank-import in actors/all;
// this file is NEVER edited.
package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/wanpengxie/ActOS/cmd/internal/dotenv"
	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/registry"

	// actors/all blank-imports every in-tree self-registering actor package (its
	// init() registers a decl). The import list is hand-maintained there; adding
	// an in-tree actor = one line in actors/all, never here.
	_ "github.com/wanpengxie/ActOS/actors/all"
	// The agent subsystem (agent/all) is deliberately NOT imported here: the daemon
	// hosts only TOOL/DEVICE classes; the channel orchestrator is server-placed, and
	// no daemon-side path builds the "agent" class. Importing agent/all would link
	// the LLM engine SDKs into the daemon binary for no consumer (substrate-purity
	// 铁律 #4). Wire it in only if a daemon-delivered agent build path actually lands.
)

// channelFromServerURL extracts the ?channel= query from the server WS URL.
func channelFromServerURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Query().Get("channel")
}

func main() {
	ws := flag.String("server", "ws://localhost:8080/compute", "server WS url")
	key := flag.String("key", "", "api key")
	name := flag.String("name", "", "device name; default: hostname")
	workspace := flag.String("workspace", "", "workspace root dir; default: ~/.coagent/workspace")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// Seed config from .env (dev convenience) before reading the environment. An
	// already-exported variable wins; a missing file is a no-op. The agent cell's
	// KIMI_* creds enter the process here.
	if n, err := dotenv.Load(".env"); err != nil {
		logger.Warn("daemon: .env load failed", "err", err.Error())
	} else if n > 0 {
		logger.Info("daemon: loaded .env", "vars_set", n)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Device identity + workspace root resolve first — both the device actor and
	// the agent's situation facts derive from them.
	deviceName := *name
	if deviceName == "" {
		host, err := os.Hostname()
		if err != nil {
			log.Fatalf("daemon: hostname: %v", err)
		}
		deviceName = host
	}
	wsRoot := *workspace
	if wsRoot == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			log.Fatalf("daemon: home dir: %v", err)
		}
		wsRoot = filepath.Join(home, ".coagent", "workspace")
	}

	deps := registry.Deps{
		ChannelID:    channel.ID(channelFromServerURL(*ws)),
		WorkspaceDir: wsRoot,
		DeviceName:   deviceName,
		Logger:       slog.Default(),
	}

	// Day-0 daemon composition (actor-instance-model §6/§7): the fat daemon hosts
	// its compiled TOOL/DEVICE classes — one default instance each
	// (InstanceSpec{} → class default id). The agent subsystem is NOT compiled into
	// the daemon (it imports actors/all only, never agent/all), so registry.Classes()
	// yields only TOOL/DEVICE classes here: the channel's orchestrator is
	// server-placed (agent:boost); a daemon-hosted agent would be a per-channel
	// composition decision delivered from the server (additive, not day-0) — never a
	// CLI flag. This is what removes the old agent:main double-build.
	var decls []platform.ActorDecl
	for _, class := range registry.Classes() {
		decl, berr := registry.Build(class, registry.InstanceSpec{}, deps)
		if berr != nil {
			log.Fatalf("daemon: %v", berr)
		}
		decls = append(decls, decl)
	}

	// The link layer is auth-agnostic: the api key rides the server WS url's query
	// string (?key=), which the app layer resolves on WS upgrade. There is no
	// separate credential field on ComputeConfig.
	serverWS := *ws
	if *key != "" {
		sep := "?"
		if strings.Contains(serverWS, "?") {
			sep = "&"
		}
		serverWS += sep + "key=" + url.QueryEscape(*key)
	}
	if err := platform.RunCompute(ctx, platform.ComputeConfig{ServerWS: serverWS}, decls); err != nil {
		log.Fatalf("daemon: %v", err)
	}
}
