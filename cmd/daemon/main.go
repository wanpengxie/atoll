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
	"syscall"
	"time"

	"github.com/wanpengxie/atoll/lib/pathsafe"
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

// staticSource wraps a one-time-fetched compute plan (registry.Build's decls) as
// an actorrt.DesiredSource: the daemon pulls its assignment ONCE at startup (no
// blind-build, no polling against the server) and the reconcile ring reads that
// same fixed set every tick. Every wrapped instance is AlwaysOn — a daemon has no
// delivery-seam analogue for lazy activation.
type staticSource []actorrt.DesiredMember

func (s staticSource) Members(context.Context) ([]actorrt.DesiredMember, error) { return s, nil }

// staticBuilder resolves each fetched instance's id to the ActorFactory
// registry.Build already produced for it — the daemon's ComputeBuilder over a
// fixed plan.
type staticBuilder map[actor.ActorID]platform.ActorFactory

func (b staticBuilder) Lookup(id actor.ActorID) (platform.ActorFactory, bool) {
	f, ok := b[id]
	return f, ok
}

// localStateSlot gives a daemon-placed instance a DAEMON-LOCAL durable state
// slot: state follows execution locus — a daemon-placed looper resumes from
// local state, the server holds none. dir is platform-managed under the
// workspace; the looper is the slot's only author.
//
// A mkdir failure is RETURNED (not silently downgraded to ephemeral): the caller
// skips that instance observably, because claude's resume contract depends on a
// real durable Dir/Seed/Store. Store writes atomically (temp + rename) so a
// crash mid-write never leaves a torn checkpoint.
func localStateSlot(wsRoot, chID, instanceID string) (registry.StateSlot, error) {
	dir := filepath.Join(wsRoot, "agent-state", pathsafe.Segment(chID), pathsafe.Segment(instanceID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return registry.StateSlot{}, fmt.Errorf("state dir %s: %w", dir, err)
	}
	seedPath := filepath.Join(dir, "checkpoint.json")
	var seed json.RawMessage
	if b, err := os.ReadFile(seedPath); err == nil && len(b) > 0 {
		seed = b
	}
	return registry.StateSlot{
		Dir:  dir,
		Seed: seed,
		Store: func(blob json.RawMessage) error {
			tmp := seedPath + ".tmp"
			if err := os.WriteFile(tmp, blob, 0o644); err != nil {
				return err
			}
			return os.Rename(tmp, seedPath) // atomic replace
		},
	}, nil
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

	// Pull this channel's daemon-placed assignment from the server, then build
	// EXACTLY that set. A build failure for one instance is logged + skipped
	// (mirrors the server's spawnComposition tolerance) — it must not down the
	// whole daemon.
	// Bounded retry: a transient server hiccup should not permanently kill
	// daemon startup. After 3 tries we fatal (a supervisor restarts us).
	var (
		plan []daemonAssignment
		err  error
	)
	for attempt := 1; ; attempt++ {
		plan, err = fetchPlan(ctx, *ws, *key, chID)
		if err == nil {
			break
		}
		if attempt >= 3 {
			log.Fatalf("daemon: fetch composition plan (after %d tries): %v", attempt, err)
		}
		logger.Warn("daemon: fetch plan failed, retrying", "attempt", attempt, "err", err.Error())
		select {
		case <-ctx.Done():
			log.Fatalf("daemon: cancelled during plan fetch")
		case <-time.After(time.Duration(attempt) * time.Second):
		}
	}
	var (
		desired  staticSource
		builders = staticBuilder{}
	)
	for _, asg := range plan {
		// A daemon-placed instance needs a real durable local state slot; a mkdir
		// failure is observable (skip this instance, logged), never silent ephemeral.
		slot, serr := localStateSlot(wsRoot, chID, asg.InstanceID)
		if serr != nil {
			logger.Error("daemon: state slot", "instance", asg.InstanceID, "err", serr.Error())
			continue
		}
		deps := registry.Deps{
			ChannelID:    channel.ID(chID),
			WorkspaceDir: wsRoot,
			DeviceName:   deviceName,
			Logger:       slog.Default(),
			State:        slot,
		}
		decl, berr := registry.Build(asg.Class, registry.InstanceSpec{
			ID:     actor.ActorID(asg.InstanceID),
			Config: asg.Config,
		}, deps)
		if berr != nil {
			logger.Error("daemon: build assigned instance",
				"instance", asg.InstanceID, "class", asg.Class, "err", berr.Error())
			continue
		}
		desired = append(desired, actorrt.DesiredMember{ID: decl.ID, Kind: decl.Kind, Lifecycle: actorrt.LifecycleAlwaysOn})
		builders[decl.ID] = decl.Factory
	}
	logger.Info("daemon: composition", "channel", chID, "assigned", len(plan), "built", len(desired))

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
	if err := platform.RunCompute(ctx, platform.ComputeConfig{
		ServerWS: serverWS,
		Logger:   logger,
		Desired:  desired,
		Builder:  builders,
	}); err != nil {
		log.Fatalf("daemon: %v", err)
	}
}
