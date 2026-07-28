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
	"github.com/wanpengxie/atoll/runtime/actorhost"

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
// ring pulls the authenticated plan through its link, calls ApplyPlan, and then
// resolves each Host build claim through LookupExact against that same atomically
// replaced builder table. The desired half of the snapshot is NOT read back from
// here — the ring hands the plan rows to the Host directly, and the Host's own
// desired is the one anybody asks. A pull or build failure leaves the
// last-known-good snapshot intact, so the daemon stays connected and retries
// without introducing a second plan source.
type planSource struct {
	chID, wsRoot, deviceName string
	logger                   *slog.Logger

	mu        sync.Mutex
	builders  map[actor.ActorID]plannedBody
	lastBuilt int // -1 until the first successful fetch (to Info-log only on change)
}

type plannedBody struct {
	desired actorhost.BodyDesired
	factory platform.ActorFactory
}

func newPlanSource(chID, wsRoot, deviceName string, logger *slog.Logger) *planSource {
	return &planSource{
		chID: chID, wsRoot: wsRoot, deviceName: deviceName,
		logger:    logger,
		builders:  map[actor.ActorID]plannedBody{},
		lastBuilt: -1,
	}
}

// ApplyPlan builds the factory table off-lock, then publishes it as one atomic
// snapshot. Any invalid row rejects the whole candidate so the previous
// last-known-good snapshot remains authoritative.
func (p *planSource) ApplyPlan(plan []platform.PlanActor) error {
	builders := map[actor.ActorID]plannedBody{}
	for _, asg := range plan {
		id := asg.ActorID
		if _, duplicate := builders[id]; duplicate {
			return fmt.Errorf("daemon: duplicate plan instance %s", id)
		}
		// Desired is generated from the plan row alone, but publication is atomic:
		// unknown classes, build failures, and identity drift reject the full plan.
		//
		// The registry answers ONE question here — can this daemon build the class —
		// and its Kind is deliberately discarded. What the body IS belongs to the
		// Controller's desired projection, which is what the row carries; a daemon
		// holds no truth and must not derive that fact a second time. Two
		// derivations would meet at LookupExact's Equal with nothing to reconcile
		// them, and a disagreement there is a builder that never matches: a silent
		// build-fail loop with no log naming the reason.
		if _, ok := registry.ClassKind(asg.Class); !ok {
			return fmt.Errorf("daemon: plan instance %s has unknown class %q", asg.ActorID, asg.Class)
		}
		// The row's Kind is adopted, not trusted blindly: an unparseable one would
		// fail ExecutionSpec's canonicalization inside Equal, and Equal reports that
		// as "not a match" — the same silent no_builder loop by another door. Reject
		// the candidate loudly instead.
		if _, ok := actor.ParseKind(string(asg.Kind)); !ok {
			return fmt.Errorf("daemon: plan instance %s has invalid kind %q", asg.ActorID, asg.Kind)
		}
		bodyDesired := actorhost.BodyDesired{
			ActorID: id, AttemptKey: asg.AttemptKey,
			ExecutionSpec: actorhost.ExecutionSpec{
				Kind: asg.Kind, Class: asg.Class, Config: append(json.RawMessage(nil), asg.Config...),
			},
		}
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
			return fmt.Errorf("daemon: build plan instance %s class %q: %w", asg.ActorID, asg.Class, berr)
		}
		// The builder table is keyed on the PLAN's InstanceID (what desired carries
		// and what the ring Lookups), NOT decl.ID. A constructor that rewrites the id
		// (device derives its own id from the device identity, "ignores ID and derives
		// it") would otherwise file the factory under the derived id — permanently
		// unreachable by the ring's Lookup(InstanceID) → no_builder forever, yet Build
		// reported success. Treat an id drift as a full candidate failure so the
		// prior LKG remains intact rather than publishing an unreachable builder.
		if decl.ID != id {
			return fmt.Errorf("daemon: plan instance %s class %q built mismatched id %s", asg.ActorID, asg.Class, decl.ID)
		}
		builders[id] = plannedBody{desired: bodyDesired, factory: decl.Factory}
	}
	p.mu.Lock()
	p.builders = builders
	changed := p.lastBuilt != len(builders)
	p.lastBuilt = len(builders)
	p.mu.Unlock()
	if changed {
		p.logger.Info("daemon: composition", "channel", p.chID,
			"assigned", len(plan), "built", len(builders))
	}
	return nil
}

func (p *planSource) LookupExact(
	id actor.ActorID,
	attempt actorhost.AttemptKey,
	spec actorhost.ExecutionSpec,
) (platform.ActorFactory, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	body, ok := p.builders[id]
	if !ok ||
		body.desired.AttemptKey != attempt ||
		!body.desired.ExecutionSpec.Equal(spec) {
		return platform.ActorFactory{}, false
	}
	return body.factory, true
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
