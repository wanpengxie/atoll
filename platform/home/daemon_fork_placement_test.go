package home

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/compute"
	"github.com/wanpengxie/atoll/platform/daemonhost"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// T24 + T25. Fork is the one lifecycle verb that crosses the link in the
// UPSTREAM direction, and its child's placement is inherited, not stated. So
// the only honest way to check where a daemon-placed Fork lands is over a real
// wire in both directions: the space's device host hands the daemon its plan
// (downstream), the daemon-hosted parent forks (upstream), and the resulting
// control rows must fall in the daemon's execution domain — carrier on the
// server, body on the daemon, and the daemon must really build that body and
// attach a route back for it.
//
// Nothing here is a stub. Since the connection-is-a-device rewrite, a channel
// Home never serves a socket itself: it publishes ONE capability bundle
// (platform.DaemonMembrane, home.go's h.daemonMembrane) to a space-wide
// platform/daemonhost.Host, exactly as platform/channelhost.Open does for
// every production channel (home.DaemonMembrane(h) → Host.Register). These
// fixtures reproduce that one registration step directly — the daemon side is
// production compute.Run — so the link, the fork, and the build are all real.
const (
	daemonForkParentClass = "daemon-fork-parent"
	daemonForkChildClass  = "daemon-fork-child"
	daemonForkDecl        = "decl:daemon-fork"
	daemonForkDomain      = "box-alpha"
	daemonForkOtherDomain = "box-beta"
	// daemonForkMembraneGen is the generation Register is told about. Home
	// itself mints no generation number (that ledger is channelhost's, one
	// layer up); a fixed 1 is enough for a fixture that opens exactly one Home
	// against exactly one daemonhost.Host.
	daemonForkMembraneGen = 1
)

// daemonForkBirth is the parent's account of its own Fork call.
type daemonForkBirth struct {
	parent actor.ActorID
	child  actor.ActorID
	err    string
}

// daemonForkPlan is the daemon-side factory source: it resolves a class into a
// test proc at body-build time, from the spec the daemon Host's own desired
// carries — exactly the production shape (platform/compute.ActorFactorySource),
// so a body can only be built for a coordinate the server actually published.
type daemonForkPlan struct {
	births chan daemonForkBirth
	starts chan actor.ActorID
}

func newDaemonForkPlan() *daemonForkPlan {
	return &daemonForkPlan{
		births: make(chan daemonForkBirth, 4),
		starts: make(chan actor.ActorID, 8),
	}
}

func (p *daemonForkPlan) BuildClass(
	_ actor.ActorID,
	class string,
	_ json.RawMessage,
) (platform.ActorFactory, bool) {
	switch class {
	case daemonForkParentClass:
		return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
			return p.parentProc(), nil
		}}}, true
	case daemonForkChildClass:
		return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
			return p.childProc(), nil
		}}}, true
	default:
		return platform.ActorFactory{}, false
	}
}

// parentProc forks exactly once, over the link, with NO placement in the spec —
// the child's placement is whatever inheritance decides, which is the thing
// under test.
func (p *daemonForkPlan) parentProc() actorbase.Proc {
	return func(sys actorbase.Sys) error {
		p.starts <- sys.Self()
		birth := daemonForkBirth{parent: sys.Self()}
		// The outbound arms are fail-closed until this body's slot is bound to
		// the live session, and a lifecycle transport failure is returned as-is
		// (the substrate never re-does a Fork across a rebind). Retrying under
		// the SAME RequestID is therefore the actor's own job, and the fork
		// table makes it exactly-once whichever attempt gets through.
		requestID := message.ID("fork:" + uuid.NewString())
		deadline := time.Now().Add(restartWaitBudget)
		for {
			child, err := sys.Fork(requestID, actorcaps.ForkSpec{
				Kind: actor.KindAgent, Class: daemonForkChildClass, NameHint: "worker",
				Config: json.RawMessage(`{"role":"worker"}`),
			})
			birth.child, birth.err = child, ""
			if err == nil {
				break
			}
			birth.err = err.Error()
			if time.Now().After(deadline) {
				break
			}
			select {
			case <-sys.Life().Done():
			case <-time.After(restartPollEvery):
			}
			if sys.Life().Err() != nil {
				break
			}
		}
		p.births <- birth
		<-sys.Life().Done()
		return nil
	}
}

func (p *daemonForkPlan) childProc() actorbase.Proc {
	return func(sys actorbase.Sys) error {
		p.starts <- sys.Self()
		<-sys.Life().Done()
		return nil
	}
}

// daemonForkStarted drains the start reports into a set the test can ask about.
type daemonForkStarted struct {
	mu   sync.Mutex
	seen map[actor.ActorID]int
}

func (s *daemonForkStarted) collect(plan *daemonForkPlan) {
	for {
		select {
		case id := <-plan.starts:
			s.mu.Lock()
			s.seen[id]++
			s.mu.Unlock()
		default:
			return
		}
	}
}

func (s *daemonForkStarted) count(plan *daemonForkPlan, id actor.ActorID) int {
	s.collect(plan)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seen[id]
}

// openDaemonForkChannel boots a channel whose only declaration is placed on a
// daemon, binds that daemon, and returns the Home. The server's composition
// resolver knows NOTHING: a daemon-placed body must never be built here.
//
// routes is the space device host the Home is wired to (platform.DaemonRoutes,
// satisfied by *daemonhost.Host structurally, exactly as app.go wires
// a.daemonHost in) — supplied by the caller because in production it is
// space-scoped, constructed once, well outside any one channel's Open call.
func openDaemonForkChannel(
	t *testing.T,
	channelID channel.ID,
	dbPath string,
	routes platform.DaemonRoutes,
) *Home {
	t.Helper()
	placement, err := storespec.NewDaemonPlacement(daemonForkDomain)
	if err != nil {
		t.Fatal(err)
	}
	h, err := Open(Config{
		ChannelID:            channelID,
		DBPath:               dbPath,
		CompositionResolver:  emptyCompositionResolver{},
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		Bootstrap:            true,
		DaemonRoutes:         routes,
		BootstrapDeclarations: []DeclareRequest{{
			SourceDeclID: daemonForkDecl, Kind: actor.KindAgent,
			Class: daemonForkParentClass, Placement: placement,
			CreatedAt: time.Now().UnixMilli(),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	if _, err := h.opEntry.AttachDaemon(context.Background(), channelspec.DaemonRequest{
		Ref: "test:attach:" + uuid.NewString(), DaemonID: daemonForkDomain,
	}); err != nil {
		t.Fatalf("bind the daemon: %v", err)
	}
	return h
}

// runDaemonFor publishes h's own capability bundle to host — the one step
// platform/channelhost.Open takes for every real channel (Register(chID,
// generation, home.DaemonMembrane(h))) — then attaches a real daemon carrier
// to host's own HTTP endpoint and runs it, over a real websocket, until the
// test ends. host.Serve is the space device host's connection acceptor
// (platform/daemonhost.Host.Serve); the daemon side is production compute.Run.
func runDaemonFor(
	t *testing.T,
	h *Home,
	channelID channel.ID,
	host *daemonhost.Host,
	plan *daemonForkPlan,
) {
	t.Helper()
	host.Register(channelID, daemonForkMembraneGen, h.daemonMembrane)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host.Serve(w, r, daemonForkDomain)
	}))
	t.Cleanup(srv.Close)
	// t.TempDir must be called on the test goroutine, not inside the compute
	// goroutine below: its cleanup registration races the harness's own
	// last-registered-first teardown order otherwise.
	atollHome := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- compute.Run(ctx, compute.Config{
			ServerWS:   "ws" + strings.TrimPrefix(srv.URL, "http") + "/compute",
			Credential: "daemon-fork-test-secret",
			AtollHome:  atollHome,
			BuildCompartment: func(string, string) (compute.CompartmentResources, error) {
				return compute.CompartmentResources{Factories: plan}, nil
			},
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(restartWaitBudget):
			t.Error("timed out shutting the daemon down")
		}
	})
}

// daemonForkDesired indexes one execution domain's desired level by actor.
func daemonForkDesired(
	t *testing.T,
	h *Home,
	domain actorhost.ExecutionDomain,
) map[actor.ActorID]actorhost.Desired {
	t.Helper()
	rows, err := h.controller.DesiredFor(domain, "server")
	if err != nil {
		t.Fatalf("DesiredFor(%s): %v", domain, err)
	}
	out := make(map[actor.ActorID]actorhost.Desired, len(rows))
	for _, row := range rows {
		switch typed := row.(type) {
		case actorhost.BodyDesired:
			out[typed.ActorID] = typed
		case actorhost.CarrierDesired:
			out[typed.ActorID] = typed
		default:
			t.Fatalf("unknown desired row %T", row)
		}
	}
	return out
}

func TestDaemonPlacedForkLandsItsControlRowsInTheDaemonDomain(t *testing.T) {
	const channelID = channel.ID("daemon-fork")
	ctx := context.Background()
	host := daemonhost.New(daemonhost.Config{})
	t.Cleanup(func() { _ = host.Close(context.Background()) })
	h := openDaemonForkChannel(t, channelID, filepath.Join(t.TempDir(), "channel.sqlite"), host)
	plan := newDaemonForkPlan()
	runDaemonFor(t, h, channelID, host, plan)

	parent := routingAgent(t, h, daemonForkDecl)
	birth := restartRecv(t, "the daemon-hosted parent to fork over the link", plan.births)
	if birth.err != "" {
		t.Fatalf("fork over the link: %s", birth.err)
	}
	if birth.parent != parent {
		t.Fatalf("the forking body is %s, want the declared instance %s", birth.parent, parent)
	}
	child := birth.child
	if child == "" || child == parent {
		t.Fatalf("fork returned child %q", child)
	}

	// T24 — the child's control rows. On the SERVER the child is a carrier
	// pointing at the daemon; in the DAEMON's domain it is a body carrying the
	// forked execution spec; in any other domain it does not exist at all.
	serverRows := daemonForkDesired(t, h, "server")
	carrier, ok := serverRows[child].(actorhost.CarrierDesired)
	if !ok {
		t.Fatalf("server desired row for the fork child = %T (%+v)", serverRows[child], serverRows[child])
	}
	if carrier.PeerDomain != daemonForkDomain {
		t.Fatalf("fork child carrier points at %q, want %q", carrier.PeerDomain, daemonForkDomain)
	}
	if _, wrong := serverRows[parent].(actorhost.CarrierDesired); !wrong {
		t.Fatalf("server desired row for the parent = %T", serverRows[parent])
	}

	daemonRows := daemonForkDesired(t, h, daemonForkDomain)
	body, ok := daemonRows[child].(actorhost.BodyDesired)
	if !ok {
		t.Fatalf("daemon desired row for the fork child = %T (%+v)", daemonRows[child], daemonRows[child])
	}
	if body.ExecutionSpec.Class != daemonForkChildClass ||
		body.ExecutionSpec.Kind != actor.KindAgent ||
		string(body.ExecutionSpec.Config) != `{"role":"worker"}` {
		t.Fatalf("fork child body desired = %+v", body)
	}
	// The two projections are of ONE record, so they carry ONE term: a carrier
	// and its body are the same attempt seen from two domains.
	if body.AttemptKey == "" || body.AttemptKey != carrier.AttemptKey {
		t.Fatalf("carrier/body terms disagree: %q vs %q", carrier.AttemptKey, body.AttemptKey)
	}

	if strays := daemonForkDesired(t, h, daemonForkOtherDomain); len(strays) != 0 {
		t.Fatalf("an unrelated execution domain was handed rows: %+v", strays)
	}

	// The daemon's own plan projection — the value that actually crosses the
	// wire — carries both bodies and nothing else.
	planned, err := h.planForDaemon(ctx, daemonForkDomain)
	if err != nil {
		t.Fatalf("planForDaemon: %v", err)
	}
	seen := map[actor.ActorID]platform.PlanActor{}
	for _, row := range planned {
		seen[row.ActorID] = row
	}
	if len(seen) != 2 || seen[parent].Class != daemonForkParentClass ||
		seen[child].Class != daemonForkChildClass {
		t.Fatalf("daemon plan = %+v, want exactly the parent and its child", planned)
	}
}

func TestDaemonForkChildIsBuiltOnTheDaemonAndRoutedFromTheServer(t *testing.T) {
	const channelID = channel.ID("daemon-fork-live")
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
	host := daemonhost.New(daemonhost.Config{})
	t.Cleanup(func() { _ = host.Close(context.Background()) })
	h := openDaemonForkChannel(t, channelID, dbPath, host)
	plan := newDaemonForkPlan()
	runDaemonFor(t, h, channelID, host, plan)
	started := &daemonForkStarted{seen: map[actor.ActorID]int{}}

	parent := routingAgent(t, h, daemonForkDecl)
	birth := restartRecv(t, "the daemon-hosted parent to fork over the link", plan.births)
	if birth.err != "" {
		t.Fatalf("fork over the link: %s", birth.err)
	}
	child := birth.child

	// T25, downstream half: the poke that the accepted Fork produced carries the
	// new row to the daemon, which builds the child body itself.
	// The build itself is the proof the pulled plan carried the child's row:
	// the factory source is stateless, so the only spec a build can consume is
	// the one the daemon Host's desired serves — which is the pulled plan.
	restartEventually(t, "the daemon to build the forked child body", func() bool {
		return started.count(plan, child) == 1
	})

	// T25, upstream half: the server holds a ROUTE for both — never a body. A
	// body here would mean the server built a daemon-placed actor locally, which
	// it could not even do (its composition resolver knows no class at all).
	for _, id := range []actor.ActorID{parent, child} {
		restartEventually(t, "the server to hold a route for "+string(id), func() bool {
			snapshot, ok := h.serverHost.Inspect(id)
			return ok && snapshot.Actual == actorhost.ActualRoute && snapshot.Binding.Valid()
		})
		snapshot, _ := h.serverHost.Inspect(id)
		if snapshot.Unit != nil {
			t.Fatalf("the server built a local body for daemon-placed %s", id)
		}
	}

	// The child is a full member of the channel from the server's side.
	if active, err := h.controller.IsActive(ctx, child); err != nil || !active {
		t.Fatalf("forked child active=%v err=%v", active, err)
	}
	facts, found, err := h.controller.ActorFacts(ctx, child)
	if err != nil || !found || facts.Kind != actor.KindAgent {
		t.Fatalf("forked child facts=%+v found=%v err=%v", facts, found, err)
	}

	// Lineage precision. The sponsor edge was deleted with the census (spec §8.1
	// N5), so the terminal form of "the child inherits no standing from its
	// parent" is: no declaration lineage, no durable row, and a parent terminal
	// that does not cascade. The child answers to the fork table alone.
	instances, err := h.controller.DeclaredInstances(daemonForkDecl)
	if err != nil || len(instances) != 1 || instances[0] != parent {
		t.Fatalf("declaration instances = %v err=%v, want only the parent", instances, err)
	}
	records := restartActiveRecords(t, channelID, dbPath)
	if record, durable := records[child]; durable {
		t.Fatalf("the forked child left a durable row: %+v", record)
	}
	if _, durable := records[parent]; !durable {
		t.Fatalf("the declared parent lost its durable row: %+v", records)
	}
}
