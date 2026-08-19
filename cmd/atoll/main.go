// Command atoll runs a complete personal node. `atoll up` installs or opens
// c0, starts the runtime, binds the public
// listener, and only then connects the well-known local device.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/wanpengxie/atoll/cmd/internal/dotenv"
	"github.com/wanpengxie/atoll/cmd/internal/engineboot"
	"github.com/wanpengxie/atoll/cmd/internal/homelock"
	"github.com/wanpengxie/atoll/drivers/devicehost"

	_ "github.com/wanpengxie/atoll/drivers/agents/all"
	_ "github.com/wanpengxie/atoll/drivers/tools/all"
)

const teardownGrace = 30 * time.Second

func main() {
	if len(os.Args) < 2 || os.Args[1] != "up" {
		fmt.Fprintln(os.Stderr, "usage: atoll up [--dir DIR] [--addr ADDR] [--root-password PASSWORD] [--steward CLASS] [--open-registration]")
		os.Exit(2)
	}
	fs := flag.NewFlagSet("up", flag.ExitOnError)
	dir := fs.String("dir", defaultDir(), "node home root")
	addr := fs.String("addr", "127.0.0.1:8832", "listen address")
	rootPassword := fs.String("root-password", "", "root password used only during installation")
	steward := fs.String("steward", "", "agent class carved as the c0 steward on first install (default codex; env ATOLL_STEWARD)")
	openReg := fs.Bool("open-registration", false, "expose system.principal.create to the lobby (default closed; env ATOLL_OPEN_REGISTRATION=1)")
	_ = fs.Parse(os.Args[2:])

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	if n, err := dotenv.Load(".env"); err != nil {
		logger.Warn("up: .env load failed", "err", err.Error())
	} else if n > 0 {
		logger.Info("up: loaded .env", "vars_set", n)
	}
	// <dir>/atoll.env is the installer's hand-off: ATOLL_ADDR / ATOLL_STEWARD /
	// ATOLL_ROOT_PASSWORD written once by scripts/install.sh so a bare `atoll up`
	// opens the same node. Explicit flags still win.
	if n, err := dotenv.Load(filepath.Join(*dir, "atoll.env")); err != nil {
		logger.Warn("up: atoll.env load failed", "err", err.Error())
	} else if n > 0 {
		logger.Info("up: loaded atoll.env", "dir", *dir, "vars_set", n)
	}
	if !flagGiven(fs, "addr") {
		if v := os.Getenv("ATOLL_ADDR"); v != "" {
			*addr = v
		}
	}
	if !flagGiven(fs, "open-registration") && os.Getenv("ATOLL_OPEN_REGISTRATION") == "1" {
		*openReg = true
	}
	if *steward == "" {
		*steward = os.Getenv("ATOLL_STEWARD")
	}
	if *rootPassword == "" {
		*rootPassword = os.Getenv("ATOLL_ROOT_PASSWORD")
	}

	serverHome := filepath.Join(*dir, "server")
	channelDir := filepath.Join(serverHome, "channels")
	deviceHome := filepath.Join(*dir, "device")
	for _, lock := range []struct{ dir, role string }{
		{serverHome, "server"}, {channelDir, "server"}, {deviceHome, "device"},
	} {
		release, err := homelock.Acquire(lock.dir, lock.role)
		if err != nil {
			log.Fatalf("up: %v", err)
		}
		defer release()
	}

	eng, err := engineboot.Boot(engineboot.Config{
		ChannelDBDir:     channelDir,
		Addr:             *addr,
		TokenPath:        filepath.Join(serverHome, "atoll-token"),
		RootPassword:     *rootPassword,
		StewardClass:     *steward,
		OpenRegistration: *openReg,
	}, logger)
	if err != nil {
		log.Fatalf("up: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	deviceKey, err := eng.LocalDeviceKey(ctx)
	if err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), teardownGrace)
		_ = eng.Close(closeCtx)
		cancel()
		if ctx.Err() != nil {
			return
		}
		log.Fatalf("up: local device key: %v", err)
	}

	engineCtx, cancelEngine := context.WithCancel(context.Background())
	deviceCtx, cancelDevice := context.WithCancel(context.Background())
	defer cancelEngine()
	defer cancelDevice()
	engineDone := make(chan error, 1)
	deviceDone := make(chan error, 1)
	go func() { engineDone <- eng.Serve(engineCtx) }()
	go func() {
		select {
		case <-eng.Ready():
		case <-deviceCtx.Done():
			deviceDone <- nil
			return
		}
		deviceName, _ := os.Hostname()
		if deviceName == "" {
			deviceName = "local"
		}
		deviceDone <- devicehost.Run(deviceCtx, devicehost.Config{
			ServerWS:   "ws://" + eng.BoundAddr() + "/compute",
			Credential: deviceKey,
			DeviceName: deviceName + "-local",
			AtollHome:  deviceHome,
			Logger:     logger.With("part", "local-device"),
		})
	}()

	exitCode := 0
	deviceExited, engineExited := false, false
	select {
	case <-ctx.Done():
		logger.Info("up: shutting down")
	case err := <-deviceDone:
		deviceExited, exitCode = true, 1
		logger.Error("up: local device exited", "err", errorText(err))
	case err := <-engineDone:
		engineExited, exitCode = true, 1
		logger.Error("up: engine exited", "err", errorText(err))
	}
	cancelDevice()
	if !deviceExited {
		select {
		case <-deviceDone:
		case <-time.After(teardownGrace):
			logger.Error("up: local device did not stop")
		}
	}
	cancelEngine()
	if !engineExited {
		if err := <-engineDone; err != nil {
			logger.Error("up: engine teardown", "err", err.Error())
			exitCode = 1
		}
	}
	if exitCode != 0 {
		os.Exit(exitCode)
	}
}

func errorText(err error) string {
	if err == nil {
		return "clean exit"
	}
	return err.Error()
}

func defaultDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".atoll-node"
	}
	return filepath.Join(home, ".atoll")
}

func flagGiven(fs *flag.FlagSet, name string) bool {
	given := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			given = true
		}
	})
	return given
}
