// Command server runs the coagent application server.
package main

import (
	"flag"
	"log"
	"log/slog"
	"os"

	"github.com/wanpengxie/ActOS/app"
	"github.com/wanpengxie/ActOS/cmd/internal/dotenv"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dbPath := flag.String("db", "coagent.db", "app database path")
	channelDBDir := flag.String("channel-db-dir", "/tmp/coagent-dev/channels", "directory for channel databases")
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
