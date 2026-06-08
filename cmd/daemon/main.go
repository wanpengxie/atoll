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
	"strings"
	"syscall"

	"github.com/wanpengxie/ActOS/actors/feishu"
	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// ActorFactory creates an actorrt.Actor given a harness.Writer.
type ActorFactory func(w harness.Writer) actorrt.Actor

var registry = map[string]ActorFactory{
	"feishu": func(w harness.Writer) actorrt.Actor {
		creds, err := feishu.LoadCredentialsFromEnv()
		if err != nil {
			log.Fatalf("daemon: feishu credentials: %v", err)
		}
		a, err := feishu.NewActor(w, creds, slog.Default())
		if err != nil {
			log.Fatalf("daemon: feishu actor: %v", err)
		}
		return a
	},
}

func main() {
	ws := flag.String("server", "ws://localhost:8080/compute", "server WS url")
	key := flag.String("key", "", "api key")
	actorsFlag := flag.String("actors", "", "comma-separated actors to host (e.g. feishu)")
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
	for _, name := range actorNames {
		factory, ok := registry[name]
		if !ok {
			log.Fatalf("daemon: unknown actor %q", name)
		}
		decls = append(decls, platform.ActorDecl{
			ID:      actor.ActorID("tool:" + name),
			Kind:    actor.KindTool,
			Binding: actor.BindingRuntimeOutbound,
			Factory: factory,
		})
	}

	if err := platform.RunCompute(ctx, platform.ComputeConfig{
		ServerWS: *ws,
		APIKey:   *key,
	}, decls); err != nil {
		log.Fatalf("daemon: %v", err)
	}
}
