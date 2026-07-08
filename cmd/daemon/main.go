// Command daemon runs a v2 attached compute (hosts actor cells; no truth).
// Cloud daemon and user/proxy daemon are the same binary.
//
// What the daemon RUNS is NOT "one of every compiled class" — it is exactly the
// set the SERVER assigns this channel (channel_actors placement='daemon'),
// PULLED at startup from GET /compute/plan. Two
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
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/wanpengxie/atoll/cmd/daemon/internal/storagehost"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
	"github.com/wanpengxie/atoll/runtime/actorrt"

	// Availability (NOT auto-run): blank-import every in-tree actor + engine so the
	// daemon CAN build any class the server assigns. actors/all = tools/devices;
	// agent/all = the LLM engine classes (claude / go-kimi). What actually runs is
	// the pulled assignment, never "one of each".
	_ "github.com/wanpengxie/atoll/actors/all"
	_ "github.com/wanpengxie/atoll/agent/all"
)

// channelFromServerURL extracts the ?channel= query from the server WS URL.
func channelFromServerURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Query().Get("channel")
}

// daemonAssignment mirrors the server's GET /compute/plan JSON. Decoded into
// the daemon's OWN struct — a loose HTTP contract; the daemon must not import
// the server app package.
type daemonAssignment struct {
	InstanceID string          `json:"instance_id"`
	Class      string          `json:"class"`
	Config     json.RawMessage `json:"config,omitempty"`
}

// planURLFromWS turns the server WS url (ws://h/compute) into the plan url
// (http(s)://h/compute/plan?key=&channel=).
func planURLFromWS(serverWS, key, chID string) (string, error) {
	u, err := url.Parse(serverWS)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "ws":
		u.Scheme = "http"
	case "wss":
		u.Scheme = "https"
	}
	u.Path = "/compute/plan"
	q := url.Values{}
	q.Set("key", key)
	q.Set("channel", chID)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// planHTTPClient bounds the plan pull — a long-running daemon must not hang
// forever on a wedged server.
var planHTTPClient = &http.Client{Timeout: 10 * time.Second}

// fetchPlan pulls this daemon's assignment for the channel from the server (the
// daemon builds EXACTLY this set — no blind-build).
func fetchPlan(ctx context.Context, serverWS, key, chID string) ([]daemonAssignment, error) {
	planURL, err := planURLFromWS(serverWS, key, chID)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, planURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := planHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out struct {
		Assignments []daemonAssignment `json:"assignments"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return out.Assignments, nil
}

// planSource is the daemon's LIVE compute-plan source: it is BOTH the reconcile
// ring's actorrt.DesiredSource (Members) and its platform.ComputeBuilder (Lookup),
// sharing one fetched-plan snapshot. The reconcile ring calls Members every poll
// tick (compute.runLink), so Members RE-FETCHES /compute/plan each tick and
// rebuilds the desired set + the id→factory table together — a plan changed on the
// server converges WITHOUT a daemon restart (SW-6). A fetch failure is NON-FATAL:
// Members logs and returns the last-known-good set (empty until the first success),
// so the daemon stays connected and keeps trying ("connect first, pull later" —
// no 3-try-fatal startup gate). Members updates the builder table BEFORE returning,
// so the ring's subsequent per-id Lookup sees a consistent snapshot.
type planSource struct {
	ws, key, chID, wsRoot, deviceName string
	logger                            *slog.Logger

	mu          sync.Mutex
	lastDesired []actorrt.DesiredMember
	builders    map[actor.ActorID]platform.ActorFactory
	lastBuilt   int // -1 until the first successful fetch (to Info-log only on change)
}

func newPlanSource(ws, key, chID, wsRoot, deviceName string, logger *slog.Logger) *planSource {
	return &planSource{
		ws: ws, key: key, chID: chID, wsRoot: wsRoot, deviceName: deviceName,
		logger:    logger,
		builders:  map[actor.ActorID]platform.ActorFactory{},
		lastBuilt: -1,
	}
}

func (p *planSource) Members(ctx context.Context) ([]actorrt.DesiredMember, error) {
	plan, err := fetchPlan(ctx, p.ws, p.key, p.chID)
	if err != nil {
		p.logger.Warn("daemon: refetch plan failed, using last-known-good", "err", err.Error())
		p.mu.Lock()
		d := p.lastDesired
		p.mu.Unlock()
		return d, nil // never error the ring; last-known-good keeps hosted work alive
	}
	var desired []actorrt.DesiredMember
	builders := map[actor.ActorID]platform.ActorFactory{}
	for _, asg := range plan {
		id := actor.ActorID(asg.InstanceID)
		// Desired is generated from the plan row ALONE (ClassKind, a pure pre-Build
		// table lookup), decoupled from Build. So a per-row Build failure (missing
		// creds, transient) keeps the id IN desired — the ring finds no builder,
		// records it infeasible, and retries next tick, while computeRing's削臂
		// (prevCurrent−current) never culls a live cell that is still in the plan.
		// Only a plan that genuinely drops the row removes it from desired. An
		// unknown class has no derivable kind (unactivatable) — skipped, as the
		// server-side compositionDesired does.
		kind, ok := registry.ClassKind(asg.Class)
		if !ok {
			p.logger.Error("daemon: unknown class in plan, skipping",
				"instance", asg.InstanceID, "class", asg.Class)
			continue
		}
		desired = append(desired, actorrt.DesiredMember{ID: id, Kind: kind, Lifecycle: actorrt.LifecycleAlwaysOn})
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
			p.logger.Error("daemon: build assigned instance",
				"instance", asg.InstanceID, "class", asg.Class, "err", berr.Error())
			continue
		}
		// The builder table is keyed on the PLAN's InstanceID (what desired carries
		// and what the ring Lookups), NOT decl.ID. A constructor that rewrites the id
		// (device derives its own id from the device identity, "ignores ID and derives
		// it") would otherwise file the factory under the derived id — permanently
		// unreachable by the ring's Lookup(InstanceID) → no_builder forever, yet Build
		// reported success. Treat an id drift as a build failure: skip the row loud
		// (desired keeps it, ring records no_builder, retries) rather than file a
		// silently-dead entry. (痛感前哨 for a future ClassDecl.IDPolicy; 止血 here.)
		if decl.ID != id {
			p.logger.Warn("daemon: built instance id differs from plan instance id, skipping",
				"instance", asg.InstanceID, "class", asg.Class, "built_id", string(decl.ID))
			continue
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
	return desired, nil
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

	// The daemon's compute plan is pulled LIVE, not snapshotted: planSource re-fetches
	// /compute/plan every reconcile tick and rebuilds the desired set + factory table
	// together (a plan change on the server converges with no daemon restart — SW-6).
	// Startup does NOT gate on a first fetch: RunCompute connects the link, then the
	// ring calls Members, which tolerates a fetch failure (last-known-good, initially
	// empty) — connect first, pull later, keep retrying.
	source := newPlanSource(*ws, *key, chID, wsRoot, deviceName, logger)

	// The link layer is auth-agnostic: the api key rides the server WS url's query
	// string (?key=), which the app layer resolves on WS upgrade. There is no
	// separate credential field on ComputeConfig.
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
	// for it (RunCompute's bridge only calls into StorageHost when the home
	// actually sends one), so there is no cost to always wiring it — and no
	// silent gap the day a channel this daemon serves DOES need file
	// placement.
	sh, err := storagehost.Open(wsRoot, chID, logger)
	if err != nil {
		log.Fatalf("daemon: open storage host: %v", err)
	}
	defer func() { _ = sh.Close() }()

	if err := platform.RunCompute(ctx, platform.ComputeConfig{
		ServerWS:        serverWS,
		Logger:          logger,
		Desired:         source,
		Builder:         source,
		StorageHost:     storageHostAdapter{host: sh},
		LocalFileOpener: storageHostAdapter{host: sh},
	}); err != nil {
		log.Fatalf("daemon: %v", err)
	}
}
