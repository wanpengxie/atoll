// Command daemon runs a v2 attached compute (hosts actor cells; no truth).
// Cloud daemon and user/proxy daemon are the same binary — --actors selects
// which actor factories to instantiate.
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

	"github.com/wanpengxie/ActOS/actors/device"
	"github.com/wanpengxie/ActOS/actors/echo"
	"github.com/wanpengxie/ActOS/actors/kimi"
	agentactor "github.com/wanpengxie/ActOS/actors/kimiagent"
	"github.com/wanpengxie/ActOS/actors/xhs"
	"github.com/wanpengxie/ActOS/cmd/internal/dotenv"
	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

var registry = map[string]func(harness.Writer) actorrt.Actor{
	"echo": func(w harness.Writer) actorrt.Actor {
		return echo.NewActor(w)
	},
}

// hasName / removeName are tiny slice helpers for the --actors flag.
func hasName(names []string, want string) bool {
	for _, n := range names {
		if n == want {
			return true
		}
	}
	return false
}

func removeName(names []string, drop string) []string {
	out := names[:0]
	for _, n := range names {
		if n != drop {
			out = append(out, n)
		}
	}
	return out
}

// isSpecialActor reports whether an actor name is handled by a dedicated
// config-injecting block (below) rather than the generic registry loop.
func isSpecialActor(name string) bool {
	switch name {
	case "agent", "xhs", "kimi":
		return true
	}
	return false
}

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
	actorsFlag := flag.String("actors", "", "comma-separated actors to host (e.g. echo)")
	name := flag.String("name", "", "device name; default: hostname")
	workspace := flag.String("workspace", "", "workspace root dir; default: ~/.coagent/workspace")
	xhsAddr := flag.String("xhs-device-addr", "127.0.0.1:8090", "local WS addr the xhs browser extension connects in to")
	kimiAddr := flag.String("kimi-device-addr", "127.0.0.1:8091", "local WS addr the Kimi WebBridge extension connects in to")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// Seed config from .env (dev convenience) before reading the environment.
	// An already-exported variable wins; a missing file is a no-op. The agent
	// cell's KIMI_* creds enter the process here.
	if n, err := dotenv.Load(".env"); err != nil {
		logger.Warn("daemon: .env load failed", "err", err.Error())
	} else if n > 0 {
		logger.Info("daemon: loaded .env", "vars_set", n)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Parse actor names.
	var actorNames []string
	for _, n := range strings.Split(*actorsFlag, ",") {
		n = strings.TrimSpace(n)
		if n != "" {
			actorNames = append(actorNames, n)
		}
	}

	// Build actor declarations with factories. The special-cased actors
	// (agent / xhs / kimi) are handled in dedicated blocks below — they need
	// per-actor config injection — so the generic registry loop skips them.
	var decls []platform.ActorDecl
	for _, n := range actorNames {
		if isSpecialActor(n) {
			continue
		}
		factory, ok := registry[n]
		if !ok {
			log.Fatalf("daemon: unknown actor %q", n)
		}
		decls = append(decls, platform.ActorDecl{
			ID:      actor.ActorID(n),
			Kind:    actor.KindTool,
			Binding: actor.BindingRuntimeOutbound,
			Factory: factory,
		})
	}

	// Device identity + workspace root resolve first — both the device
	// actor and the agent's situation facts derive from them.
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

	// The agent brain is opt-in (--actors=agent,...): the fat daemon hosts
	// the same Bridge the server spawns as the built-in fallback — one
	// prototype, host decided by where this binary runs. It needs the channel
	// id (from the --server URL query) because a cell is channel-scoped.
	if hasName(actorNames, "agent") {
		actorNames = removeName(actorNames, "agent")
		chID := channelFromServerURL(*ws)
		if chID == "" {
			log.Fatalf("daemon: --actors=agent requires the --server URL to carry ?channel=<id>")
		}
		cfg, err := agentactor.NewConfigFromEnv(agentactor.BuildSystemPrompt(
			agentactor.Situation{Host: "daemon", HasWorkspace: true, WorkspaceDir: wsRoot},
			os.Getenv(agentactor.EnvKeyChannelType), os.Getenv(agentactor.EnvKeyDomainPrompt)))
		if err != nil {
			log.Fatalf("daemon: agent config: %v", err)
		}
		agentID := actor.ActorID("agent:main")
		decls = append(decls, platform.ActorDecl{
			ID:      agentID,
			Kind:    actor.KindAgent,
			Binding: actor.BindingRuntimeOutbound,
			Factory: func(w harness.Writer) actorrt.Actor {
				b, err := agentactor.NewBridge(cfg, agentID, channel.ID(chID), w)
				if err != nil {
					log.Fatalf("daemon: agent bridge: %v", err)
				}
				return b
			},
		})
	}

	// The xhs adapter is opt-in (--actors=xhs,...). It owns a PRIVATE loopback WS
	// endpoint the browser extension connects in to (transport inlined in the
	// adapter), so it is special-cased here to inject its listen addr. The
	// endpoint is keyless — the 127.0.0.1 bind is the trust boundary (the device
	// reaches it through the local daemon, same machine). Binding is
	// runtime_inbound_via_relay (the device connects IN; the substrate only
	// records the label, it does not route the transport).
	if hasName(actorNames, "xhs") {
		actorNames = removeName(actorNames, "xhs")
		xhsCfg := xhs.Config{
			ListenAddr: *xhsAddr,
			Logger:     slog.Default(),
		}
		decls = append(decls, platform.ActorDecl{
			ID:      xhs.DefaultActorID,
			Kind:    actor.KindTool,
			Binding: actor.BindingRuntimeInboundViaRelay,
			Factory: func(w harness.Writer) actorrt.Actor {
				return xhs.NewActor(w, xhsCfg)
			},
		})
	}

	// The kimi (Kimi WebBridge) adapter is opt-in (--actors=kimi,...). Same shape
	// as xhs: a PRIVATE loopback WS endpoint the browser extension connects in to
	// (transport inlined in the adapter), special-cased here to inject its listen
	// addr. Keyless — the 127.0.0.1 bind is the trust boundary. Binding is
	// runtime_inbound_via_relay (the device connects IN; the substrate only
	// records the label, it does not route the transport). The addr defaults to
	// :8091 so it never collides with xhs's :8090.
	if hasName(actorNames, "kimi") {
		actorNames = removeName(actorNames, "kimi")
		kimiCfg := kimi.Config{
			ListenAddr: *kimiAddr,
			Logger:     slog.Default(),
		}
		decls = append(decls, platform.ActorDecl{
			ID:      kimi.DefaultActorID,
			Kind:    actor.KindTool,
			Binding: actor.BindingRuntimeInboundViaRelay,
			Factory: func(w harness.Writer) actorrt.Actor {
				return kimi.NewActor(w, kimiCfg)
			},
		})
	}

	// The generic device actor is always hosted — attaching a daemon means
	// attaching a device. Its id carries the device identity.
	deviceID := actor.ActorID("device:" + deviceName)
	decls = append(decls, platform.ActorDecl{
		ID:      deviceID,
		Kind:    actor.KindTool,
		Binding: actor.BindingRuntimeOutbound,
		Factory: func(w harness.Writer) actorrt.Actor {
			return device.NewActor(w, deviceID, wsRoot, slog.Default())
		},
	})

	// The link layer is auth-agnostic: the api key rides the server WS url's
	// query string (?key=), which the app layer resolves on WS upgrade. There is
	// no separate credential field on ComputeConfig.
	serverWS := *ws
	if *key != "" {
		sep := "?"
		if strings.Contains(serverWS, "?") {
			sep = "&"
		}
		serverWS += sep + "key=" + url.QueryEscape(*key)
	}
	if err := platform.RunCompute(ctx, platform.ComputeConfig{
		ServerWS: serverWS,
	}, decls); err != nil {
		log.Fatalf("daemon: %v", err)
	}
}
