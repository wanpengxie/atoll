// Command daemon runs a v2 attached compute (hosts actor cells; no truth).
// Cloud daemon and user/proxy daemon are the same binary.
//
// What the daemon RUNS is NOT "one of every compiled class" — it is exactly the
// set the SERVER assigns this channel (channel composition placement='daemon'),
// pulled over the authenticated link control stream. Two
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
	"errors"
	"flag"
	"log"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

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

// channelFromServerURL extracts the ?channel= query from the server WS URL.
func channelFromServerURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Query().Get("channel")
}

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
	workspace := flag.String("workspace", "", "workspace root dir; default: ~/.atoll/workspace")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Device identity + workspace root resolve first — an assigned device actor
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
	wsRoot := *workspace
	if wsRoot == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			log.Fatalf("daemon: home dir: %v", err)
		}
		wsRoot = filepath.Join(home, ".atoll", "workspace")
	}

	chID := channelFromServerURL(*ws)
	// Assembly-root check: a daemon hosts exactly ONE channel's assignment, named
	// by the server WS url's ?channel=. Missing it means we cannot know what to
	// build — fatal at the earliest point with a fix-it diagnostic (fetchPlan
	// would otherwise surface a murky 400/403 far downstream).
	if chID == "" {
		log.Fatalf("daemon: -server %q has no ?channel= query; pass e.g. -server ws://host:8080/compute?channel=<channel-id>", *ws)
	}

	// The daemon's compute plan is pulled over its authenticated link on every
	// reconcile pass and handed to the Host as one desired snapshot — the one
	// plan ledger. Factories resolve per body at build time from that desired's
	// own spec, exactly as the server host resolves against its registry.
	factories := classFactories{chID: chID, wsRoot: wsRoot, deviceName: deviceName, logger: logger}

	// The link layer is auth-agnostic: the api key rides the server WS url's query
	// string (?key=), which the app layer resolves on WS upgrade. There is no
	// separate credential field on compute.Config.
	serverWS := *ws
	if *key != "" {
		sep := "?"
		if strings.Contains(serverWS, "?") {
			sep = "&"
		}
		serverWS += sep + "key=" + url.QueryEscape(*key)
	}

	// Storage host (期11 §4): the file-kind resource axis's physical half —
	// os.Root-confined, this channel's own resources/<channelID>/{live,
	// staging} tree, a SIBLING of wsRoot/<channelID>'s device workspace tree
	// (never nested under it, §4.2). Opened unconditionally: a daemon that
	// never hosts a file-kind resource simply never receives an AllocRequest
	// for it (compute.Run's bridge only calls into StorageHost when the home
	// actually sends one), so there is no cost to always wiring it — and no
	// silent gap the day a channel this daemon serves DOES need file
	// placement.
	sh, err := storagehost.Open(wsRoot, chID, logger)
	if err != nil {
		log.Fatalf("daemon: open storage host: %v", err)
	}
	closeStorageRoot := true
	defer func() {
		if closeStorageRoot {
			_ = sh.Close()
		}
	}()

	if err := compute.Run(ctx, compute.Config{
		ServerWS:        serverWS,
		Logger:          logger.With("channel", chID),
		Factories:       factories,
		StorageHost:     storageHostAdapter{host: sh},
		LocalFileOpener: storageHostAdapter{host: sh},
	}); err != nil {
		if !shouldCloseStorageRoot(err) {
			closeStorageRoot = false
			logger.Error("daemon: storage root ownership transferred to process exit", "err", err)
			return
		}
		log.Fatalf("daemon: %v", err)
	}
}

func shouldCloseStorageRoot(err error) bool {
	return !errors.Is(err, compute.ErrForwardersLeaked)
}
