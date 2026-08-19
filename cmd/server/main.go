// Command server runs the bare Atoll server. Full local-node provisioning and
// the well-known device carrier belong to `atoll up`.
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

	_ "github.com/wanpengxie/atoll/drivers/agents/all"
	_ "github.com/wanpengxie/atoll/drivers/tools/all"
)

func main() {
	home := flag.String("home", defaultServerHome(), "server home")
	addr := flag.String("addr", ":8080", "listen address")
	rootPassword := flag.String("root-password", "", "root password used only during installation")
	openReg := flag.Bool("open-registration", false, "expose system.principal.create to the lobby (default closed)")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if n, err := dotenv.Load(".env"); err != nil {
		logger.Warn("server: .env load failed", "err", err.Error())
	} else if n > 0 {
		logger.Info("server: loaded .env", "vars_set", n)
	}
	if *rootPassword == "" {
		*rootPassword = os.Getenv("ATOLL_ROOT_PASSWORD")
	}
	channels := filepath.Join(*home, "channels")
	for _, dir := range []string{*home, channels} {
		release, err := homelock.Acquire(dir, "server")
		if err != nil {
			log.Fatalf("server: %v", err)
		}
		defer release()
	}
	eng, err := engineboot.Boot(engineboot.Config{
		ChannelDBDir:     channels,
		Addr:             *addr,
		TokenPath:        filepath.Join(*home, "atoll-token"),
		RootPassword:     *rootPassword,
		OpenRegistration: *openReg,
	}, logger)
	if err != nil {
		log.Fatalf("server: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
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
