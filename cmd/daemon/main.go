// Command daemon runs a v2 attached compute (hosts actor cells; no truth).
// Cloud daemon and user/proxy daemon are the same binary. Which actors it hosts
// comes from the SELF-REGISTERING actor registry (driver-registration pattern):
// each in-tree actor package's init() registers its decl, and the daemon builds
// ALL applicable actors by default (fat-daemon: one binary packages every impl)
// — or a subset via --actors. Adding an in-tree actor = a new package with an
// init() + one blank-import line in actors/all; this file is NEVER edited.
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

	"github.com/wanpengxie/ActOS/actors/registry"
	"github.com/wanpengxie/ActOS/cmd/internal/dotenv"
	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/channel"

	// actors/all blank-imports every in-tree self-registering actor package (its
	// init() registers a decl). The import list is hand-maintained there; adding
	// an in-tree actor = one line in actors/all, never here.
	_ "github.com/wanpengxie/ActOS/actors/all"
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
	actorsFlag := flag.String("actors", "", "comma-separated subset to host (default: all registered+applicable)")
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

	// Default = every registered+applicable actor (fat daemon). --actors narrows
	// to an explicit subset. Either way the actor list comes from the registry —
	// no hand-maintained manifest here.
	var (
		decls []platform.ActorDecl
		err   error
	)
	if strings.TrimSpace(*actorsFlag) == "" {
		decls, err = registry.BuildAll(deps)
		if err != nil {
			log.Fatalf("daemon: build actors: %v", err)
		}
	} else {
		for _, n := range strings.Split(*actorsFlag, ",") {
			n = strings.TrimSpace(n)
			if n == "" {
				continue
			}
			decl, berr := registry.Build(n, deps)
			if berr != nil {
				log.Fatalf("daemon: %v", berr)
			}
			decls = append(decls, decl)
		}
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
