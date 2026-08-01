// Command daemon runs one authenticated device carrier and a compartment for
// every bound channel. Cloud and user/proxy daemons are the same binary.
//
// What the daemon RUNS is NOT "one of every compiled class" — it is exactly the
// sets the server assigns each channel (channel composition
// placement='daemon'), pulled over that channel's lane. Two
// orthogonal axes: compiled-in (availability — actors/all + agent/all are linked
// so the daemon CAN build any tool/looper/device) vs run (the pulled assignment
// decides). NOTHING auto-runs. tool / looper / device are uniform — all just
// rows in the assignment; claude runs here with the user's LOCAL login.
//
// Adding an in-tree actor/engine = a new package with an init() + one blank
// import in actors/all (tools/devices) or agent/all (engines); this file is
// NEVER edited.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/wanpengxie/atoll/cmd/daemon/internal/storagehost"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/compute"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"

	// Availability (NOT auto-run): blank-import every in-tree actor + engine so the
	// daemon CAN build any class the server assigns. actors/all = tools/devices;
	// agent/all = the LLM engine classes (claude / go-kimi). What actually runs is
	// the pulled assignment, never "one of each".
	_ "github.com/wanpengxie/atoll/drivers/agents/all"
	_ "github.com/wanpengxie/atoll/drivers/tools/all"
)

// shutdownGrace bounds the whole graceful teardown after the first signal.
// Twice the single-compartment join budget: a teardown still running past that
// is not finishing, it is wedged on something cancellation cannot reach.
const shutdownGrace = 60 * time.Second

// classFactories resolves one body's factory at BUILD time from the class and
// config that body's own desired carries — the daemon-side mirror of the server
// host's registry lookup. There is no plan snapshot here and no state at all:
// the desired the Host serves is the one plan ledger, and the registry is
// compiled-in code. A class this daemon cannot build fails that body alone
// (logged by the caller, retried on the Host's backoff) instead of holding the
// whole plan hostage.
type classFactories struct {
	chID, wsRoot, deviceName string
	logger                   *slog.Logger
}

func (f classFactories) BuildClass(
	id actor.ActorID,
	class string,
	config json.RawMessage,
) (platform.ActorFactory, bool) {
	decl, err := registry.Build(class, registry.InstanceSpec{
		ID:     id,
		Config: config,
	}, registry.Deps{
		ChannelID:    channel.ID(f.chID),
		WorkspaceDir: f.wsRoot,
		DeviceName:   f.deviceName,
		Logger:       f.logger,
	})
	if err != nil {
		f.logger.Error("daemon: build class failed",
			"channel", f.chID, "actor", id, "class", class, "err", err)
		return platform.ActorFactory{}, false
	}
	// A constructor that rewrites the id (device derives its own id from the
	// device identity) would produce a body claiming an identity the plan never
	// named. Refuse the build outright — there is no table to file it under, so
	// the only way it could leak is by being built, and it is not.
	if decl.ID != id {
		f.logger.Warn("daemon: class derived a different id",
			"actor", id, "class", class, "derived", decl.ID)
		return platform.ActorFactory{}, false
	}
	return decl.Factory, true
}

func main() {
	ws := flag.String("server", "ws://localhost:8080/compute", "server WS url")
	key := flag.String("key", "", "api key")
	name := flag.String("name", "", "device name; default: hostname")
	workspace := flag.String("workspace", "", "atoll home; default: ~/.atoll")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// A device daemon is a user-run process with no supervisor behind it, so
	// both escape hatches a supervised server takes for granted are built here:
	// the first signal starts the graceful teardown and immediately restores
	// default signal handling, so a second Ctrl-C hard-kills a wedged teardown;
	// and if the teardown itself exceeds the grace period — the shape is a lane
	// answer parked inside an uncancellable storage syscall — the process exits
	// on its own. Reconciliation is crash-safe, so exiting here is exactly one
	// crash, not corruption.
	go func() {
		<-ctx.Done()
		stop()
		time.Sleep(shutdownGrace)
		slog.Error("daemon: graceful shutdown exceeded its grace period; exiting")
		os.Exit(1)
	}()

	// Device identity + atoll home resolve first — an assigned device actor
	// derives its id from DeviceName; loopers' situation facts derive from the
	// workspace.
	deviceName := *name
	if deviceName == "" {
		host, err := os.Hostname()
		if err != nil {
			log.Fatalf("daemon: hostname: %v", err)
		}
		deviceName = host
	}
	atollHome := *workspace
	if atollHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			log.Fatalf("daemon: home dir: %v", err)
		}
		atollHome = filepath.Join(home, ".atoll")
	}

	if err := compute.Run(ctx, compute.Config{
		ServerWS:   *ws,
		Credential: *key,
		AtollHome:  atollHome,
		Logger:     logger,
		BuildCompartment: func(chID, workspaceDir string) (compute.CompartmentResources, error) {
			daemonRoot := filepath.Dir(filepath.Dir(workspaceDir))
			sh, err := storagehost.Open(daemonRoot, chID, logger.With("channel", chID))
			if err != nil {
				return compute.CompartmentResources{}, err
			}
			adapter := storageHostAdapter{host: sh}
			return compute.CompartmentResources{
				Factories: classFactories{
					chID: chID, wsRoot: workspaceDir, deviceName: deviceName,
					logger: logger.With("channel", chID),
				},
				StorageHost: adapter, LocalFileOpener: adapter, Close: sh.Close,
			}, nil
		},
	}); err != nil {
		log.Fatalf("daemon: %v", err)
	}
}
