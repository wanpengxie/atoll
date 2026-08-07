// Command server runs the atoll application server (the bare engine — full
// node provisioning is `atoll up`'s job).
package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/wanpengxie/atoll/cmd/internal/dotenv"
	"github.com/wanpengxie/atoll/cmd/internal/engineboot"
	"github.com/wanpengxie/atoll/cmd/internal/homelock"

	// Composition root wires the catalog: the BINARY pins which classes are
	// compiled in — not the app library (which stays impl-agnostic, so
	// `go test ./app` can register its own stub). Both assembly roots (server +
	// daemon) import the SAME catalog so placement can name any class the server
	// might host (G21): whether it actually runs is answered honestly by
	// Build/creds, not gated by binary contents. agent/all = the agent engine
	// classes (codex + script); actors/all = tools + devices.
	_ "github.com/wanpengxie/atoll/drivers/agents/all"
	_ "github.com/wanpengxie/atoll/drivers/tools/all"
)

func main() {
	home := flag.String("home", defaultServerHome(), "server home: app db + channel stores (home 模型: 一个目录=一个 server 安装)")
	addr := flag.String("addr", ":8080", "listen address")
	dbPath := flag.String("db", "", "app database path (advanced override; default <home>/app.db)")
	channelDBDir := flag.String("channel-db-dir", "", "channel database dir (advanced override; default <home>/channels)")
	uiDist := flag.String("ui-dist", "", "path to the built web UI (atoll-web repo's dist/); empty = API-only")
	initDB := flag.Bool("init", false, "create a new app database; omit to open an existing database")
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

	db := *dbPath
	if db == "" {
		db = filepath.Join(*home, "app.db")
	}
	channels := *channelDBDir
	if channels == "" {
		channels = filepath.Join(*home, "channels")
	}
	if err := os.MkdirAll(channels, 0o755); err != nil {
		log.Fatalf("server: %v", err)
	}

	// One home, one live server: locks follow BOTH truth roots — the db's dir
	// and the channel-store dir — so no flag combination lets two servers
	// share either (same --home with different --db overrides would otherwise
	// hold different db locks while converging the same channel stores).
	lockDirs := map[string]bool{filepath.Dir(db): true, channels: true}
	for d := range lockDirs {
		release, err := homelock.Acquire(d, "server")
		if err != nil {
			log.Fatalf("server: %v", err)
		}
		defer release()
	}

	eng, err := engineboot.Boot(engineboot.Config{
		DBPath: db, ChannelDBDir: channels,
		Addr: *addr, UIDist: *uiDist, InitDB: *initDB,
	}, logger)
	if err != nil {
		log.Fatalf("server: %v", err)
	}

	// Rotate the local automation credential on EVERY start (contract D3 /
	// 交接包① · token 拍点: restart IS the rotation, in both packagings — a
	// leaked token dies on the next restart, split server included). The token
	// lands next to the app db; path is a release detail, not contract. (Full
	// node provisioning — channel + local daemon — is `atoll up`'s job; the
	// bare server binary keeps boot minimal so e2e and dev harnesses see no
	// surprise rows beyond the owner principal.)
	tokenPath := filepath.Join(filepath.Dir(db), "atoll-token")
	if err := eng.RotateOwnerToken(context.Background(), tokenPath); err != nil {
		_ = eng.Close(context.Background())
		if *initDB {
			// --init refuses an existing database, so a half-initialized install
			// must not survive this failure: the fresh db is dropped (after the
			// ordered close above) to keep --init retryable.
			_ = os.Remove(db)
		}
		log.Fatalf("server: %v", err)
	}
	logger.Info("server: owner token rotated", "path", tokenPath)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		stop() // restore default handling: a second signal hard-kills a stuck drain
	}()
	if err := eng.Serve(ctx); err != nil {
		log.Fatalf("server: %v", err)
	}
}

func defaultServerHome() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".atoll-server"
	}
	return filepath.Join(home, ".atoll", "server")
}
