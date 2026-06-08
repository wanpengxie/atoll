// Command server runs the coagent application server.
package main

import (
	"flag"
	"log"
	"log/slog"
	"os"

	"github.com/wanpengxie/ActOS/app"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dbPath := flag.String("db", "coagent.db", "app database path")
	channelDBDir := flag.String("channel-db-dir", "/tmp/coagent-dev/channels", "directory for channel databases")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

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
