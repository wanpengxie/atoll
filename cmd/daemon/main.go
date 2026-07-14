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
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/wanpengxie/atoll/cmd/daemon/internal/storagehost"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/compute"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
	"github.com/wanpengxie/atoll/runtime/actorrt"

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

// planSource is the daemon's single applied compute-plan snapshot. The reconcile
// ring pulls the authenticated plan through its link, calls ApplyPlan, then reads
// Members and Lookup from this same atomically replaced desired/factory pair.
// A pull or build failure leaves the last-known-good snapshot intact, so the
// daemon stays connected and retries without introducing a second plan source.
type planSource struct {
	chID, wsRoot, deviceName string
	logger                   *slog.Logger

	mu          sync.Mutex
	lastDesired []actorrt.DesiredMember
	builders    map[actor.ActorID]platform.ActorFactory
	lastBuilt   int // -1 until the first successful fetch (to Info-log only on change)
}

func newPlanSource(chID, wsRoot, deviceName string, logger *slog.Logger) *planSource {
	return &planSource{
		chID: chID, wsRoot: wsRoot, deviceName: deviceName,
		logger:    logger,
		builders:  map[actor.ActorID]platform.ActorFactory{},
		lastBuilt: -1,
	}
}

func (p *planSource) Members(context.Context) ([]actorrt.DesiredMember, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]actorrt.DesiredMember(nil), p.lastDesired...), nil
}

// ApplyPlan builds both desired and factory halves off-lock, then publishes one
// atomic snapshot. Any invalid row rejects the whole candidate so the previous
// last-known-good snapshot remains authoritative.
func (p *planSource) ApplyPlan(plan []platform.PlanActor) error {
	var desired []actorrt.DesiredMember
	builders := map[actor.ActorID]platform.ActorFactory{}
	for _, asg := range plan {
		id := actor.ActorID(asg.InstanceID)
		// Desired is generated from the plan row alone, but publication is atomic:
		// unknown classes, build failures, and identity drift reject the full plan.
		kind, ok := registry.ClassKind(asg.Class)
		if !ok {
			return fmt.Errorf("daemon: plan instance %s has unknown class %q", asg.InstanceID, asg.Class)
		}
		desired = append(desired, actorrt.DesiredMember{ID: id, Kind: kind, Lifecycle: actorrt.LifecycleAlwaysOn, Epoch: asg.Epoch})
		decl, berr := registry.Build(asg.Class, registry.InstanceSpec{
			ID:     id,
			Config: asg.Config,
		}, registry.Deps{
			ChannelID:    channel.ID(p.chID),
			WorkspaceDir: p.wsRoot,
			DeviceName:   p.deviceName,
			Logger:       p.logger,
		})
		if berr != nil {
			return fmt.Errorf("daemon: build plan instance %s class %q: %w", asg.InstanceID, asg.Class, berr)
		}
		// The builder table is keyed on the PLAN's InstanceID (what desired carries
		// and what the ring Lookups), NOT decl.ID. A constructor that rewrites the id
		// (device derives its own id from the device identity, "ignores ID and derives
		// it") would otherwise file the factory under the derived id — permanently
		// unreachable by the ring's Lookup(InstanceID) → no_builder forever, yet Build
		// reported success. Treat an id drift as a full candidate failure so the
		// prior LKG remains intact rather than publishing an unreachable builder.
		if decl.ID != id {
			return fmt.Errorf("daemon: plan instance %s class %q built mismatched id %s", asg.InstanceID, asg.Class, decl.ID)
		}
		builders[id] = decl.Factory
	}
	p.mu.Lock()
	p.lastDesired = desired
	p.builders = builders
	changed := p.lastBuilt != len(desired)
	p.lastBuilt = len(desired)
	p.mu.Unlock()
	if changed {
		p.logger.Info("daemon: composition", "channel", p.chID,
			"assigned", len(plan), "desired", len(desired), "built", len(builders))
	}
	return nil
}

func (p *planSource) Lookup(id actor.ActorID) (platform.ActorFactory, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f, ok := p.builders[id]
	return f, ok
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
	// reconcile pass and applied as one desired-set/factory snapshot. Startup
	// connects first; pull failures retain the previous snapshot (initially empty)
	// and the next pass retries without a second plan source.
	source := newPlanSource(chID, wsRoot, deviceName, logger)

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
		Logger:          logger,
		PlanSource:      source,
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
