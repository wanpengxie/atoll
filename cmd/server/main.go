// Command server runs the atoll application server.
package main

import (
	"flag"
	"log"
	"log/slog"
	"os"

	"github.com/wanpengxie/atoll/app"
	"github.com/wanpengxie/atoll/cmd/internal/dotenv"

	// Composition root wires the catalog: the server-embedded agent (agent:boost)
	// is built via registry.Build("agent"), so the BINARY pins which "agent" impl
	// is compiled in — not the app library (which stays agent-impl-agnostic, so
	// `go test ./app` can register its own stub). agent/all aggregates the "agent"
	// class + its looper engines (go-kimi + claude); actors/ holds no agent.
	_ "github.com/wanpengxie/atoll/agent/all"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dbPath := flag.String("db", "atoll.db", "app database path")
	channelDBDir := flag.String("channel-db-dir", "/tmp/atoll-dev/channels", "directory for channel databases")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// Seed config from .env (dev convenience) before anything reads the
	// environment. An already-exported variable wins; a missing file is a
	// no-op. The built-in agent's KIMI_* creds enter the process here.
	if n, err := dotenv.Load(".env"); err != nil {
		logger.Warn("server: .env load failed", "err", err.Error())
	} else if n > 0 {
		logger.Info("server: loaded .env", "vars_set", n)
	}

	appDB, err := app.OpenDB(*dbPath)
	if err != nil {
		log.Fatalf("server: %v", err)
	}
	defer appDB.Close()

	a, err := app.New(app.Config{
		DB:           appDB,
		Logger:       logger,
		ChannelDBDir: *channelDBDir,
	})
	if err != nil {
		log.Fatalf("server: %v", err)
	}

	if err := a.Run(*addr); err != nil {
		log.Fatalf("server: %v", err)
	}
}
