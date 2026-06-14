// Command daemon runs a v2 attached compute (hosts actor cells; no truth).
// Cloud daemon and user/proxy daemon are the same binary — --actors selects
// which actor factories to instantiate.
package main

import (
	"context"
	"flag"
	"fmt"
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

// actorDeps bundles every input an actor might need to declare itself at the
// composition root. Each builder takes what it needs and ignores the rest —
// there is no "generic vs special" actor, just a list of self-describing decls.
type actorDeps struct {
	channelID    channel.ID // from the --server URL ?channel= (a cell is channel-scoped)
	workspaceDir string     // workspace root for device + agent situation facts
	deviceName   string     // device identity
	logger       *slog.Logger
}

// builders maps an --actors name to its decl builder. Every actor declares its
// own (id, kind, binding, factory), pulling whatever config it needs from deps.
// echo is not "the normal case" — it is just the zero-config one.
var builders = map[string]func(actorDeps) (platform.ActorDecl, error){
	"echo":  echoDecl,
	"agent": agentDecl,
	"xhs":   xhsDecl,
	"kimi":  kimiDecl,
}

// echoDecl: the zero-config tool. Only needs a writer.
func echoDecl(actorDeps) (platform.ActorDecl, error) {
	return platform.ActorDecl{
		ID:      actor.ActorID("echo"),
		Kind:    actor.KindTool,
		Binding: actor.BindingRuntimeOutbound,
		Factory: func(w harness.Writer) actorrt.Actor { return echo.NewActor(w) },
	}, nil
}

// agentDecl: the agent brain (opt-in fat-daemon host of the same Bridge the
// server spawns as the built-in fallback — one prototype, host decided by where
// the binary runs). Needs the channel id, workspace situation, and KIMI_* creds.
func agentDecl(d actorDeps) (platform.ActorDecl, error) {
	if d.channelID == "" {
		return platform.ActorDecl{}, fmt.Errorf("--actors=agent requires the --server URL to carry ?channel=<id>")
	}
	cfg, err := agentactor.NewConfigFromEnv(agentactor.BuildSystemPrompt(
		agentactor.Situation{Host: "daemon", HasWorkspace: true, WorkspaceDir: d.workspaceDir},
		os.Getenv(agentactor.EnvKeyChannelType), os.Getenv(agentactor.EnvKeyDomainPrompt)))
	if err != nil {
		return platform.ActorDecl{}, fmt.Errorf("config: %w", err)
	}
	agentID := actor.ActorID("agent:main")
	chID := d.channelID
	return platform.ActorDecl{
		ID:      agentID,
		Kind:    actor.KindAgent,
		Binding: actor.BindingRuntimeOutbound,
		Factory: func(w harness.Writer) actorrt.Actor {
			b, err := agentactor.NewBridge(cfg, agentID, chID, w)
			if err != nil {
				log.Fatalf("daemon: agent bridge: %v", err)
			}
			return b
		},
	}, nil
}

// adapterDecl is the shared shape of a browser-extension adapter: it owns a
// PRIVATE loopback WS endpoint the extension connects in to (transport inlined),
// keyless (the 127.0.0.1 bind is the trust boundary), binding
// runtime_inbound_via_relay (the device connects IN; the substrate only records
// the label, it does not route the transport). xhs/kimi differ only by id+addr.
func xhsDecl(d actorDeps) (platform.ActorDecl, error) {
	cfg := xhs.Config{ListenAddr: xhs.DefaultListenAddr, Logger: d.logger}
	return platform.ActorDecl{
		ID:      xhs.DefaultActorID,
		Kind:    actor.KindTool,
		Binding: actor.BindingRuntimeInboundViaRelay,
		Factory: func(w harness.Writer) actorrt.Actor { return xhs.NewActor(w, cfg) },
	}, nil
}

func kimiDecl(d actorDeps) (platform.ActorDecl, error) {
	cfg := kimi.Config{ListenAddr: kimi.DefaultListenAddr, Logger: d.logger}
	return platform.ActorDecl{
		ID:      kimi.DefaultActorID,
		Kind:    actor.KindTool,
		Binding: actor.BindingRuntimeInboundViaRelay,
		Factory: func(w harness.Writer) actorrt.Actor { return kimi.NewActor(w, cfg) },
	}, nil
}

// deviceDecl: the generic device actor, always hosted — attaching a daemon means
// attaching a device. Its id carries the device identity.
func deviceDecl(d actorDeps) platform.ActorDecl {
	deviceID := actor.ActorID("device:" + d.deviceName)
	return platform.ActorDecl{
		ID:      deviceID,
		Kind:    actor.KindTool,
		Binding: actor.BindingRuntimeOutbound,
		Factory: func(w harness.Writer) actorrt.Actor {
			return device.NewActor(w, deviceID, d.workspaceDir, d.logger)
		},
	}
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
	actorsFlag := flag.String("actors", "", "comma-separated actors to host (e.g. echo,xhs,kimi,agent)")
	name := flag.String("name", "", "device name; default: hostname")
	workspace := flag.String("workspace", "", "workspace root dir; default: ~/.coagent/workspace")
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

	deps := actorDeps{
		channelID:    channel.ID(channelFromServerURL(*ws)),
		workspaceDir: wsRoot,
		deviceName:   deviceName,
		logger:       slog.Default(),
	}

	// Build the selected actors' decls — one lookup, one builder, no special
	// cases. Then the always-on device.
	var decls []platform.ActorDecl
	for _, n := range strings.Split(*actorsFlag, ",") {
		n = strings.TrimSpace(n)
		if n == "" {
			continue
		}
		build, ok := builders[n]
		if !ok {
			log.Fatalf("daemon: unknown actor %q", n)
		}
		decl, err := build(deps)
		if err != nil {
			log.Fatalf("daemon: actor %q: %v", n, err)
		}
		decls = append(decls, decl)
	}
	decls = append(decls, deviceDecl(deps))

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
