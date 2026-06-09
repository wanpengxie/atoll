// Command daemon runs a v2 attached compute (hosts actor cells; no truth).
// Cloud daemon and user/proxy daemon are the same binary — --actors selects
// which actor factories to instantiate.
package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/wanpengxie/ActOS/actors/device"
	"github.com/wanpengxie/ActOS/actors/echo"
	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

var registry = map[string]func(harness.Writer) actorrt.Actor{
	"echo": func(w harness.Writer) actorrt.Actor {
		return echo.NewActor(w)
	},
}

func main() {
	ws := flag.String("server", "ws://localhost:8080/compute", "server WS url")
	key := flag.String("key", "", "api key")
	actorsFlag := flag.String("actors", "", "comma-separated actors to host (e.g. echo)")
	name := flag.String("name", "", "device name; default: hostname")
	workspace := flag.String("workspace", "", "workspace root dir; default: ~/.coagent/workspace")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

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

	// Build actor declarations with factories.
	var decls []platform.ActorDecl
	for _, n := range actorNames {
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

	// The generic device actor is always hosted — attaching a daemon means
	// attaching a device. Its id carries the device identity.
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
	deviceID := actor.ActorID("device:" + deviceName)
	decls = append(decls, platform.ActorDecl{
		ID:      deviceID,
		Kind:    actor.KindTool,
		Binding: actor.BindingRuntimeOutbound,
		Factory: func(w harness.Writer) actorrt.Actor {
			return device.NewActor(w, deviceID, wsRoot, slog.Default())
		},
	})

	if err := platform.RunCompute(ctx, platform.ComputeConfig{
		ServerWS: *ws,
		APIKey:   *key,
	}, decls); err != nil {
		log.Fatalf("daemon: %v", err)
	}
}
