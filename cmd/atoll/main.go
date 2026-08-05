// Command atoll is the one-command personal node. `atoll up` boots the whole
// stack in a single process: engine (server assembly) + node provisioning
// (owner principal + rotated bearer token + home channel + local device, all
// convergent) + an in-process local device carrier dialing the engine's own
// /compute. First run creates everything; every later run converges and
// reopens. 一条命令起全套，零手动链接 (C1 roadmap).
//
// Phase order (议题4 拍点): Boot (convergence running, port closed) →
// provision (nobody outside can see the node yet) → Serve (bind LAST; a
// connectable node is a fully-provisioned node) → device (dials after the
// engine's own Ready). One supervisor owns every part (议题3 拍点): a member's
// terminal failure fails the whole node loudly — never a half-alive node with
// the server up and the local device silently gone.
//
// Disk layout is the home model's (议题1 拍点) and identical whether roles run
// unified or split: <dir>/server is exactly an atoll-server home, <dir>/device
// exactly an atoll-daemon home — `atoll up` is just both roles lit in one
// process, and moving to split binaries later means pointing each at the same
// directories.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/wanpengxie/atoll/app"
	"github.com/wanpengxie/atoll/cmd/internal/dotenv"
	"github.com/wanpengxie/atoll/cmd/internal/engineboot"
	"github.com/wanpengxie/atoll/cmd/internal/homelock"
	"github.com/wanpengxie/atoll/drivers/devicehost"

	// Availability (NOT auto-run): the single binary can host every in-tree
	// tool/device/engine class, server- or device-placed alike.
	_ "github.com/wanpengxie/atoll/drivers/agents/all"
	_ "github.com/wanpengxie/atoll/drivers/tools/all"
)

// teardownGrace bounds each supervised member's exit once shutdown starts; a
// member still running past it is wedged, and process death is the reclaim.
const teardownGrace = 30 * time.Second

func main() {
	if len(os.Args) < 2 || os.Args[1] != "up" {
		fmt.Fprintln(os.Stderr, "usage: atoll up [--dir DIR] [--addr ADDR] [--ui-dist DIST]")
		os.Exit(2)
	}
	fs := flag.NewFlagSet("up", flag.ExitOnError)
	dir := fs.String("dir", defaultDir(), "node home root (server/ + device/ + token)")
	addr := fs.String("addr", "127.0.0.1:8832", "listen address")
	uiDist := fs.String("ui-dist", "", "path to the built web UI; empty = API-only")
	_ = fs.Parse(os.Args[2:])

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	if n, err := dotenv.Load(".env"); err != nil {
		logger.Warn("up: .env load failed", "err", err.Error())
	} else if n > 0 {
		logger.Info("up: loaded .env", "vars_set", n)
	}

	serverHome := filepath.Join(*dir, "server")
	deviceHome := filepath.Join(*dir, "device")
	dbPath := filepath.Join(serverHome, "app.db")
	channelDir := filepath.Join(serverHome, "channels")
	// The token lives IN the server home — it is a projection of that db's
	// sessions row, so it follows the server, and a node split into
	// standalone binaries later finds it at the exact same path (布局同形).
	tokenPath := filepath.Join(serverHome, "atoll-token")
	if err := os.MkdirAll(channelDir, 0o755); err != nil {
		log.Fatalf("up: %v", err)
	}
	// Both role homes lock exactly as their standalone binaries would — a
	// separate atoll-server or atoll-daemon on the same homes is refused at the
	// door, not discovered as a double identity later. The channel-store dir
	// gets its own lock for the same reason cmd/server takes one: it is a
	// second truth root a rogue process could point at directly.
	for _, l := range []struct{ home, role string }{{serverHome, "server"}, {channelDir, "server"}, {deviceHome, "device"}} {
		release, err := homelock.Acquire(l.home, l.role)
		if err != nil {
			log.Fatalf("up: %v", err)
		}
		defer release()
	}
	_, statErr := os.Stat(dbPath)
	initDB := os.IsNotExist(statErr)
	// First-run rollback works from a PRECISE record of what this run creates
	// — "app.db was absent" proves nothing about the rest of the role home, so
	// record which of the fixed products already exist (those are the user's,
	// never this run's to delete) and snapshot the channel-store dir.
	var rollbackTargets []string
	preexistingChannels := map[string]bool{}
	if initDB {
		for _, p := range []string{dbPath, dbPath + "-wal", dbPath + "-shm", dbPath + ".lock", tokenPath} {
			if _, err := os.Stat(p); os.IsNotExist(err) {
				rollbackTargets = append(rollbackTargets, p)
			}
		}
		if entries, err := os.ReadDir(channelDir); err == nil {
			for _, e := range entries {
				preexistingChannels[e.Name()] = true
			}
		}
	}

	eng, err := engineboot.Boot(engineboot.Config{
		DBPath: dbPath, ChannelDBDir: channelDir,
		Addr: *addr, UIDist: *uiDist, InitDB: initDB,
	}, logger)
	if err != nil {
		if initDB {
			// Boot itself may have created the fresh db before failing.
			rollbackFirstRun(logger, rollbackTargets, channelDir, preexistingChannels)
		}
		log.Fatalf("up: %v", err)
	}

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-sigCtx.Done()
		stop() // second signal hard-kills a stuck teardown
	}()

	// failNode is the pre-Serve failure path: ordered engine close (the db
	// must be shut before anything else happens to its files), first-run
	// rollback if applicable, then exit loudly.
	failNode := func(err error) {
		closeCtx, cancel := context.WithTimeout(context.Background(), teardownGrace)
		closeErr := eng.Close(closeCtx)
		cancel()
		if initDB {
			rollbackFirstRun(logger, rollbackTargets, channelDir, preexistingChannels)
		}
		log.Fatalf("up: %v (close: %v)", err, closeErr)
	}

	// Provision against the booted, not-yet-listening engine: the convergence
	// arm is running (Boot started it), the port is closed — nobody can observe
	// a half-provisioned node. The device home's identity file is the claim;
	// the daemons row stays the authority (home 模型).
	ident, _, err := devicehost.LoadIdentity(deviceHome)
	if err != nil {
		failNode(err)
	}
	// The device's server origin is part of its identity (server↔key are a
	// bound pair — the same rule atoll-daemon enforces), so the persisted
	// record must always carry the origin this node's key belongs to. Before
	// the listener exists the flag string is the best truth available; once
	// Ready closes, the BOUND address is the truth (":0" resolves) and the
	// record converges onto it.
	originWS := "ws://" + *addr + "/compute"
	persistClaim := func(daemonID, daemonKey, origin string) error {
		if ident.DaemonID == daemonID && ident.APIKey == daemonKey && ident.ServerWS == origin {
			return nil
		}
		persisted := ident
		persisted.DaemonID, persisted.APIKey, persisted.ServerWS = daemonID, daemonKey, origin
		if err := devicehost.SaveIdentity(deviceHome, persisted); err != nil {
			return err
		}
		ident = persisted
		return nil
	}
	prov, err := eng.ProvisionLocalNode(sigCtx, app.ProvisionSpec{
		TokenPath: tokenPath, DaemonID: ident.DaemonID,
	})
	if err != nil {
		// A partial result still carries a freshly-minted device row (the bind
		// after it failed) — persist that claim NOW, or every retry would mint
		// another orphan row it then forgets.
		if prov.DaemonID != "" {
			if perr := persistClaim(prov.DaemonID, prov.DaemonKey, originWS); perr != nil {
				logger.Error("up: persisting device claim", "err", perr.Error())
			}
		}
		closeCtx, cancel := context.WithTimeout(context.Background(), teardownGrace)
		closeErr := eng.Close(closeCtx)
		cancel()
		if errors.Is(err, context.Canceled) {
			// Ctrl-C during provision is a normal shutdown, not an install
			// failure: nothing is rolled back (every step converges on the next
			// run), and the exit is clean.
			logger.Info("up: canceled during provision, shut down cleanly")
			return
		}
		if initDB {
			// A REAL first-run failure must not leave a half-initialized
			// install: remove exactly what this run created — after the
			// ordered close above, so nothing still holds the db — and a retry
			// is indistinguishable from a clean first run.
			rollbackFirstRun(logger, rollbackTargets, channelDir, preexistingChannels)
		}
		log.Fatalf("up: provision: %v (close: %v)", err, closeErr)
	}
	if err := persistClaim(prov.DaemonID, prov.DaemonKey, originWS); err != nil {
		failNode(err)
	}
	logger.Info("up: provisioned",
		"home_channel", prov.HomeChannelID, "daemon", prov.DaemonID, "token", prov.TokenPath)

	// Supervisor: engine and device under one lifecycle. The device dials only
	// after the engine's own Ready (the listener exists — a fact the engine
	// states, not one we infer by poking the port).
	engCtx, engCancel := context.WithCancel(context.Background())
	devCtx, devCancel := context.WithCancel(context.Background())
	defer engCancel()
	defer devCancel()
	engDone := make(chan error, 1)
	devDone := make(chan error, 1)
	go func() { engDone <- eng.Serve(engCtx) }()
	go func() {
		select {
		case <-eng.Ready():
		case <-devCtx.Done():
			// Cancellation and readiness may become observable together. If the
			// listener is already bound, converge its actual origin before exit.
			select {
			case <-eng.Ready():
			default:
				devDone <- nil
				return
			}
		}
		// The bound address is the dialable truth (":0" has resolved by now);
		// converge the persisted origin onto it if the flag string differed.
		boundOrigin := "ws://" + eng.BoundAddr() + "/compute"
		if boundOrigin != originWS {
			if err := persistClaim(prov.DaemonID, prov.DaemonKey, boundOrigin); err != nil {
				logger.Error("up: persisting device claim", "err", err.Error())
				// A failed save leaves the prior claim unchanged, so the next up
				// observes the unresolved origin and retries this convergence.
			}
		}
		if devCtx.Err() != nil {
			devDone <- nil
			return
		}
		deviceName, _ := os.Hostname()
		if deviceName == "" {
			deviceName = "local"
		}
		devDone <- devicehost.Run(devCtx, devicehost.Config{
			ServerWS:   boundOrigin,
			Credential: prov.DaemonKey,
			DeviceName: deviceName + "-local",
			AtollHome:  deviceHome,
			Logger:     logger.With("part", "local-device"),
		})
	}()

	go func() {
		select {
		case <-eng.Ready():
			logger.Info("up: engine listening", "addr", eng.BoundAddr())
		case <-engCtx.Done():
		}
	}()

	// One of three events ends the node; whichever fires, the teardown order
	// is the same — device first (join it), engine second — so the carrier
	// never races the server's own shutdown (the yamux broken-pipe noise).
	exitCode := 0
	devExited, engExited := false, false
	select {
	case <-sigCtx.Done():
		logger.Info("up: signal received, shutting down")
	case err := <-devDone:
		devExited = true
		exitCode = 1 // fail-loud: a dead local device is a dead node, never a half-alive one
		logger.Error("up: local device exited, failing the node", "err", errString(err))
	case err := <-engDone:
		engExited = true
		exitCode = 1
		logger.Error("up: engine exited, failing the node", "err", errString(err))
	}
	devCancel()
	if !devExited {
		select {
		case <-devDone:
		case <-time.After(teardownGrace):
			logger.Error("up: local device ignored teardown; abandoning it")
		}
	}
	engCancel()
	if !engExited {
		if err := <-engDone; err != nil {
			logger.Error("up: engine teardown", "err", err.Error())
			if exitCode == 0 {
				exitCode = 1
			}
		}
	}
	// A first run that failed before the node was ever visible (Ready never
	// closed — e.g. the bind failed with the port taken) rolls back like any
	// other pre-visibility failure. Once Ready was observed the node was live:
	// its data is the user's, never rolled back.
	if exitCode != 0 && initDB {
		select {
		case <-eng.Ready():
		default:
			rollbackFirstRun(logger, rollbackTargets, channelDir, preexistingChannels)
		}
	}
	if exitCode != 0 {
		os.Exit(exitCode)
	}
}

// rollbackFirstRun removes exactly what a failed FIRST run created — the
// fixed products recorded as absent before the run (fresh db + sidecars,
// token), and only the channel stores that appeared during it — never a whole
// directory: initDB proves app.db was absent, nothing more, so anything that
// existed before (user files, an old token, foreign channel stores, the held
// homelock) is not this run's to delete. The device identity file is likewise
// untouched: a persisted claim on a rolled-back row simply misses and
// re-mints.
func rollbackFirstRun(logger *slog.Logger, targets []string, channelDir string, preexisting map[string]bool) {
	all := append([]string(nil), targets...)
	if entries, err := os.ReadDir(channelDir); err == nil {
		for _, e := range entries {
			if !preexisting[e.Name()] {
				all = append(all, filepath.Join(channelDir, e.Name()))
			}
		}
	}
	for _, p := range all {
		if err := os.RemoveAll(p); err != nil {
			logger.Error("up: first-run rollback", "path", p, "err", err.Error())
		}
	}
}

func errString(err error) string {
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
