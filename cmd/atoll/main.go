// Command atoll runs a complete personal node. `atoll up` installs or opens
// c0, provisions the local system through registrar requests, binds the public
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
		fmt.Fprintln(os.Stderr, "usage: atoll up [--dir DIR] [--addr ADDR] [--root-password PASSWORD]")
		os.Exit(2)
	}
	fs := flag.NewFlagSet("up", flag.ExitOnError)
	dir := fs.String("dir", defaultDir(), "node home root")
	addr := fs.String("addr", "127.0.0.1:8832", "listen address")
	rootPassword := fs.String("root-password", "", "root password used only during installation")
	_ = fs.Parse(os.Args[2:])

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	if n, err := dotenv.Load(".env"); err != nil {
		logger.Warn("up: .env load failed", "err", err.Error())
	} else if n > 0 {
		logger.Info("up: loaded .env", "vars_set", n)
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
		ChannelDBDir: channelDir,
		Addr:         *addr,
		TokenPath:    filepath.Join(serverHome, "atoll-token"),
		RootPassword: *rootPassword,
	}, logger)
	if err != nil {
		log.Fatalf("up: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	provision, err := eng.ProvisionLocalNode(ctx)
	if err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), teardownGrace)
		_ = eng.Close(closeCtx)
		cancel()
		if ctx.Err() != nil {
			return
		}
		log.Fatalf("up: provision: %v", err)
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
			Credential: provision.DeviceKey,
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
