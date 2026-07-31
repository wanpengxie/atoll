package compute

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/daemonhost"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type emptyFactories struct{}

func (emptyFactories) BuildClass(actor.ActorID, string, json.RawMessage) (platform.ActorFactory, bool) {
	return platform.ActorFactory{}, false
}

func waitCompute(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !predicate() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not converge")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCompartmentBuildsAndClosesOnlyByExplicitCommand(t *testing.T) {
	host := daemonhost.New(daemonhost.Config{ScanInterval: time.Hour})
	t.Cleanup(func() { _ = host.Close() })
	var bound atomic.Bool
	bound.Store(true)
	host.Register("channel-a", 1, platform.DaemonMembrane{
		Plan:    func(context.Context, string) ([]platform.PlanActor, error) { return nil, nil },
		IsBound: func(context.Context, string) (bool, error) { return bound.Load(), nil },
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host.Serve(w, r, "daemon-a")
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Config{
			ServerWS:   "ws" + strings.TrimPrefix(server.URL, "http"),
			Credential: "secret", AtollHome: t.TempDir(),
			BuildCompartment: func(string, string) (CompartmentResources, error) {
				return CompartmentResources{Factories: emptyFactories{}}, nil
			},
		})
	}()
	waitCompute(t, func() bool {
		host.Scan()
		return host.LaneAttached("daemon-a", "channel-a")
	})

	bound.Store(false)
	waitCompute(t, func() bool {
		host.Scan()
		return !host.LaneAttached("daemon-a", "channel-a") &&
			len(host.LaneView("daemon-a")) == 0
	})
	if !host.DaemonOnline("daemon-a") {
		t.Fatal("compartment close tore down the carrier")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("compute did not join")
	}
}

func TestOneCarrierServicesTwoCompartmentsAndDetachIsLocal(t *testing.T) {
	host := daemonhost.New(daemonhost.Config{ScanInterval: time.Hour})
	t.Cleanup(func() { _ = host.Close() })
	var boundA, boundB atomic.Bool
	boundA.Store(true)
	boundB.Store(true)
	membrane := func(bound *atomic.Bool) platform.DaemonMembrane {
		return platform.DaemonMembrane{
			Plan:    func(context.Context, string) ([]platform.PlanActor, error) { return nil, nil },
			IsBound: func(context.Context, string) (bool, error) { return bound.Load(), nil },
		}
	}
	host.Register("a", 1, membrane(&boundA))
	host.Register("b", 1, membrane(&boundB))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host.Serve(w, r, "daemon-a")
	}))
	defer server.Close()

	var mu sync.Mutex
	builds := map[string]int{}
	closes := map[string]int{}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Config{
			ServerWS:   "ws" + strings.TrimPrefix(server.URL, "http"),
			Credential: "secret", AtollHome: t.TempDir(),
			BuildCompartment: func(chID, _ string) (CompartmentResources, error) {
				mu.Lock()
				builds[chID]++
				mu.Unlock()
				return CompartmentResources{
					Factories: emptyFactories{},
					Close: func() error {
						mu.Lock()
						closes[chID]++
						mu.Unlock()
						return nil
					},
				}, nil
			},
		})
	}()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("compute did not join")
		}
	}()
	waitCompute(t, func() bool {
		host.Scan()
		return host.LaneAttached("daemon-a", "a") &&
			host.LaneAttached("daemon-a", "b") &&
			len(host.LaneView("daemon-a")) == 2
	})

	boundA.Store(false)
	waitCompute(t, func() bool {
		host.Scan()
		mu.Lock()
		aClosed := closes["a"]
		mu.Unlock()
		return !host.LaneAttached("daemon-a", "a") &&
			host.LaneAttached("daemon-a", "b") && aClosed == 1
	})
	mu.Lock()
	aClosed, bClosed := closes["a"], closes["b"]
	mu.Unlock()
	if aClosed != 1 || bClosed != 0 {
		t.Fatalf("detach close counts: a=%d b=%d", aClosed, bClosed)
	}
	if !host.DaemonOnline("daemon-a") {
		t.Fatal("detaching a compartment closed the shared carrier")
	}
}

func TestDaemonRootLockRejectsSecondProcessOwner(t *testing.T) {
	root := t.TempDir()
	first, err := lockDaemonRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	command := exec.Command(os.Args[0], "-test.run=^TestDaemonRootLockHelper$")
	command.Env = append(os.Environ(),
		"ATOLL_LOCK_HELPER=1",
		"ATOLL_LOCK_ROOT="+root,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("lock helper: %v\n%s", err, output)
	}
}

func TestDaemonRootLockHelper(t *testing.T) {
	if os.Getenv("ATOLL_LOCK_HELPER") != "1" {
		return
	}
	lock, err := lockDaemonRoot(os.Getenv("ATOLL_LOCK_ROOT"))
	if err == nil {
		lock.Close()
		t.Fatal("second process entered an already-owned daemon root")
	}
}

func TestCoordinatePathRejectsEscapeAndKeepsDaemonTreesDistinct(t *testing.T) {
	base := t.TempDir()
	if _, err := coordinatePath(base, "../escape"); err == nil {
		t.Fatal("path traversal coordinate accepted")
	}
	a, err := coordinatePath(base, "daemon-a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := coordinatePath(base, "daemon-b")
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("distinct daemon identities shared a root")
	}
	target := t.TempDir()
	symlink := filepath.Join(base, "symlink")
	if err := os.Symlink(target, symlink); err != nil {
		t.Fatal(err)
	}
	if err := ensureDirectory(symlink); err == nil {
		t.Fatal("workspace symlink accepted as a compartment directory")
	}
}

func TestCarrierTerminalHTTPStopsRedial(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		http.Error(w, "revoked", http.StatusUnauthorized)
	}))
	defer server.Close()
	err := Run(t.Context(), Config{
		ServerWS:   "ws" + strings.TrimPrefix(server.URL, "http"),
		Credential: "secret", AtollHome: t.TempDir(),
		BuildCompartment: func(string, string) (CompartmentResources, error) {
			return CompartmentResources{Factories: emptyFactories{}}, nil
		},
	})
	var terminal terminalCarrierError
	if !errors.As(err, &terminal) {
		t.Fatalf("terminal HTTP result=%v", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("terminal credential rejection redialed %d times", got)
	}
}

func TestRetryAfterControlsRetryableHTTPDelay(t *testing.T) {
	now := time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)
	response := &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Header:     http.Header{"Retry-After": []string{"7"}},
	}
	if got := retryAfter(response, time.Second, now); got != 7*time.Second {
		t.Fatalf("delta retry-after=%v", got)
	}
	response.Header.Set("Retry-After", now.Add(9*time.Second).Format(http.TimeFormat))
	if got := retryAfter(response, time.Second, now); got != 9*time.Second {
		t.Fatalf("date retry-after=%v", got)
	}
}

func TestCarrier_TombstoneStopsRedial(t *testing.T) {
	host := daemonhost.New(daemonhost.Config{ScanInterval: time.Hour})
	t.Cleanup(func() { _ = host.Close() })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host.Serve(w, r, "daemon-a")
	}))
	defer server.Close()
	done := make(chan error, 1)
	go func() {
		done <- Run(t.Context(), Config{
			ServerWS:   "ws" + strings.TrimPrefix(server.URL, "http"),
			Credential: "secret", AtollHome: t.TempDir(),
			BuildCompartment: func(string, string) (CompartmentResources, error) {
				return CompartmentResources{Factories: emptyFactories{}}, nil
			},
		})
	}()
	waitCompute(t, func() bool { return host.DaemonOnline("daemon-a") })
	host.RevokeDaemon("daemon-a")
	select {
	case err := <-done:
		var terminal terminalCarrierError
		if !errors.As(err, &terminal) {
			t.Fatalf("live terminal result=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("live terminal reject did not stop the daemon")
	}
}

func startTestCompute(
	t *testing.T,
	host *daemonhost.Host,
	builder CompartmentBuilder,
) (context.CancelFunc, <-chan error) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host.Serve(w, r, "daemon-a")
	}))
	t.Cleanup(server.Close)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Config{
			ServerWS:   "ws" + strings.TrimPrefix(server.URL, "http"),
			Credential: "secret", AtollHome: t.TempDir(), BuildCompartment: builder,
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("compute shutdown: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("compute did not join")
		}
	})
	return cancel, done
}

func TestCompartment_RebuildsAfterCloseWhenRebound(t *testing.T) {
	host := daemonhost.New(daemonhost.Config{ScanInterval: time.Hour})
	t.Cleanup(func() { _ = host.Close() })
	var bound atomic.Bool
	bound.Store(true)
	host.Register("a", 1, platform.DaemonMembrane{
		Plan:    func(context.Context, string) ([]platform.PlanActor, error) { return nil, nil },
		IsBound: func(context.Context, string) (bool, error) { return bound.Load(), nil },
	})
	var builds atomic.Int32
	closeEntered := make(chan struct{})
	closeRelease := make(chan struct{})
	var entered sync.Once
	startTestCompute(t, host, func(string, string) (CompartmentResources, error) {
		generation := builds.Add(1)
		return CompartmentResources{
			Factories: emptyFactories{},
			Close: func() error {
				if generation == 1 {
					entered.Do(func() { close(closeEntered) })
					<-closeRelease
				}
				return nil
			},
		}, nil
	})
	waitCompute(t, func() bool {
		host.Scan()
		return host.LaneAttached("daemon-a", "a")
	})
	bound.Store(false)
	host.Scan()
	select {
	case <-closeEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("old compartment did not enter close")
	}
	bound.Store(true)
	host.Scan()
	close(closeRelease)
	waitCompute(t, func() bool {
		return builds.Load() == 2 && host.LaneAttached("daemon-a", "a")
	})
	if got := builds.Load(); got != 2 {
		t.Fatalf("rebind built %d compartment sets, want exactly 2", got)
	}
}

func TestClosingCompartmentCommandRegisterUsesLastLane(t *testing.T) {
	run := func(t *testing.T, finalBound bool, replacePending bool) int32 {
		host := daemonhost.New(daemonhost.Config{ScanInterval: time.Hour})
		t.Cleanup(func() { _ = host.Close() })
		var bound atomic.Bool
		bound.Store(true)
		host.Register("a", 1, platform.DaemonMembrane{
			Plan: func(context.Context, string) ([]platform.PlanActor, error) {
				return nil, nil
			},
			IsBound: func(context.Context, string) (bool, error) { return bound.Load(), nil },
		})
		var builds atomic.Int32
		closeEntered := make(chan struct{})
		closeRelease := make(chan struct{})
		var entered sync.Once
		startTestCompute(t, host, func(string, string) (CompartmentResources, error) {
			generation := builds.Add(1)
			return CompartmentResources{
				Factories: emptyFactories{},
				Close: func() error {
					if generation == 1 {
						entered.Do(func() { close(closeEntered) })
						<-closeRelease
					}
					return nil
				},
			}, nil
		})
		waitCompute(t, func() bool {
			host.Scan()
			return host.LaneAttached("daemon-a", "a")
		})
		bound.Store(false)
		host.Scan()
		select {
		case <-closeEntered:
		case <-time.After(2 * time.Second):
			t.Fatal("old compartment did not enter close")
		}
		bound.Store(true)
		host.Scan()
		if replacePending {
			host.RetireLane("daemon-a", "a")
			host.Scan()
		}
		if !finalBound {
			bound.Store(false)
			host.Scan()
			// Let the spine reader commit the explicit close into the
			// closing compartment's command register before close completes.
			time.Sleep(20 * time.Millisecond)
		}
		close(closeRelease)
		if finalBound {
			waitCompute(t, func() bool {
				return builds.Load() == 2 && host.LaneAttached("daemon-a", "a")
			})
		} else {
			time.Sleep(50 * time.Millisecond)
			if host.LaneAttached("daemon-a", "a") {
				t.Fatal("last close command left a pending lane active")
			}
		}
		return builds.Load()
	}
	t.Run("open_then_close", func(t *testing.T) {
		if got := run(t, false, false); got != 1 {
			t.Fatalf("last close command rebuilt %d resource sets", got)
		}
	})
	t.Run("newest_open_replaces_pending", func(t *testing.T) {
		if got := run(t, true, true); got != 2 {
			t.Fatalf("latest pending lane built %d resource sets", got)
		}
	})
}

func TestLaneRetirementPreservesCompartmentAndPullsFullPlan(t *testing.T) {
	host := daemonhost.New(daemonhost.Config{ScanInterval: time.Hour})
	t.Cleanup(func() { _ = host.Close() })
	var pulls atomic.Int32
	host.Register("a", 1, platform.DaemonMembrane{
		Plan: func(context.Context, string) ([]platform.PlanActor, error) {
			pulls.Add(1)
			return nil, nil
		},
		IsBound: func(context.Context, string) (bool, error) { return true, nil },
	})
	var builds, closes atomic.Int32
	startTestCompute(t, host, func(string, string) (CompartmentResources, error) {
		builds.Add(1)
		return CompartmentResources{
			Factories: emptyFactories{},
			Close: func() error {
				closes.Add(1)
				return nil
			},
		}, nil
	})
	waitCompute(t, func() bool {
		host.Scan()
		return host.LaneAttached("daemon-a", "a") && pulls.Load() >= 1
	})
	host.RetireLane("daemon-a", "a")
	waitCompute(t, func() bool { return len(host.LaneView("daemon-a")) == 0 })
	host.Scan()
	waitCompute(t, func() bool {
		return host.LaneAttached("daemon-a", "a") && pulls.Load() >= 2
	})
	if builds.Load() != 1 || closes.Load() != 0 {
		t.Fatalf("lane replacement changed body resources: builds=%d closes=%d",
			builds.Load(), closes.Load())
	}
}

func TestLongCompartmentBuildSurvivesLaneChurnWithoutDoubleBuild(t *testing.T) {
	host := daemonhost.New(daemonhost.Config{ScanInterval: time.Hour})
	t.Cleanup(func() { _ = host.Close() })
	host.Register("a", 1, platform.DaemonMembrane{
		Plan:    func(context.Context, string) ([]platform.PlanActor, error) { return nil, nil },
		IsBound: func(context.Context, string) (bool, error) { return true, nil },
	})
	buildEntered := make(chan struct{})
	buildRelease := make(chan struct{})
	var builds atomic.Int32
	var once sync.Once
	startTestCompute(t, host, func(string, string) (CompartmentResources, error) {
		builds.Add(1)
		once.Do(func() { close(buildEntered) })
		<-buildRelease
		return CompartmentResources{Factories: emptyFactories{}}, nil
	})
	host.Scan()
	select {
	case <-buildEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("compartment build did not start")
	}
	for i := 0; i < 3; i++ {
		host.RetireLane("daemon-a", "a")
		host.Scan()
	}
	time.Sleep(50 * time.Millisecond)
	if builds.Load() != 1 {
		t.Fatalf("lane churn launched %d concurrent builds", builds.Load())
	}
	close(buildRelease)
	waitCompute(t, func() bool { return host.LaneAttached("daemon-a", "a") })
}

func TestBlockedCompartmentDoesNotStarveSibling(t *testing.T) {
	host := daemonhost.New(daemonhost.Config{ScanInterval: time.Hour})
	t.Cleanup(func() { _ = host.Close() })
	for _, id := range []string{"a", "b"} {
		host.Register(channel.ID(id), 1, platform.DaemonMembrane{
			Plan:    func(context.Context, string) ([]platform.PlanActor, error) { return nil, nil },
			IsBound: func(context.Context, string) (bool, error) { return true, nil },
		})
	}
	blockA := make(chan struct{})
	enteredA := make(chan struct{})
	var once sync.Once
	startTestCompute(t, host, func(chID, _ string) (CompartmentResources, error) {
		if chID == "a" {
			once.Do(func() { close(enteredA) })
			<-blockA
		}
		return CompartmentResources{Factories: emptyFactories{}}, nil
	})
	host.Scan()
	select {
	case <-enteredA:
	case <-time.After(2 * time.Second):
		t.Fatal("A did not enter its blocked build")
	}
	waitCompute(t, func() bool { return host.LaneAttached("daemon-a", "b") })
	if host.LaneAttached("daemon-a", "a") {
		t.Fatal("blocked A was reported ready")
	}
	close(blockA)
	waitCompute(t, func() bool { return host.LaneAttached("daemon-a", "a") })
}

func TestBlockedRebindPlanDoesNotBlockSiblingLaneAdmission(t *testing.T) {
	host := daemonhost.New(daemonhost.Config{ScanInterval: time.Hour})
	t.Cleanup(func() { _ = host.Close() })
	var blockA atomic.Bool
	enteredA := make(chan struct{})
	releaseA := make(chan struct{})
	var entered sync.Once
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(releaseA) }) })
	var pullsB atomic.Int32
	host.Register("a", 1, platform.DaemonMembrane{
		Plan: func(context.Context, string) ([]platform.PlanActor, error) {
			if blockA.Load() {
				entered.Do(func() { close(enteredA) })
				<-releaseA
			}
			return nil, nil
		},
		IsBound: func(context.Context, string) (bool, error) { return true, nil },
	})
	host.Register("b", 1, platform.DaemonMembrane{
		Plan: func(context.Context, string) ([]platform.PlanActor, error) {
			pullsB.Add(1)
			return nil, nil
		},
		IsBound: func(context.Context, string) (bool, error) { return true, nil },
	})
	startTestCompute(t, host, func(string, string) (CompartmentResources, error) {
		return CompartmentResources{Factories: emptyFactories{}}, nil
	})
	waitCompute(t, func() bool {
		host.Scan()
		return host.LaneAttached("daemon-a", "a") &&
			host.LaneAttached("daemon-a", "b")
	})
	blockA.Store(true)
	host.RetireLane("daemon-a", "a")
	host.Scan()
	select {
	case <-enteredA:
	case <-time.After(2 * time.Second):
		t.Fatal("A rebind did not enter blocked plan pull")
	}
	host.RetireLane("daemon-a", "b")
	host.Scan()
	waitCompute(t, func() bool { return pullsB.Load() >= 2 })
	release.Do(func() { close(releaseA) })
}

func TestCondemnedCompartmentNeverBuildsSecondResourceSet(t *testing.T) {
	host := daemonhost.New(daemonhost.Config{ScanInterval: time.Hour})
	t.Cleanup(func() { _ = host.Close() })
	var bound atomic.Bool
	bound.Store(true)
	host.Register("a", 1, platform.DaemonMembrane{
		Plan:    func(context.Context, string) ([]platform.PlanActor, error) { return nil, nil },
		IsBound: func(context.Context, string) (bool, error) { return bound.Load(), nil },
	})
	var builds atomic.Int32
	startTestCompute(t, host, func(string, string) (CompartmentResources, error) {
		builds.Add(1)
		return CompartmentResources{
			Factories: emptyFactories{},
			Close:     func() error { return errors.New("injected close failure") },
		}, nil
	})
	waitCompute(t, func() bool {
		host.Scan()
		return host.LaneAttached("daemon-a", "a")
	})
	bound.Store(false)
	host.Scan()
	waitCompute(t, func() bool { return !host.LaneAttached("daemon-a", "a") })
	bound.Store(true)
	for i := 0; i < 3; i++ {
		host.Scan()
		time.Sleep(10 * time.Millisecond)
	}
	if builds.Load() != 1 {
		t.Fatalf("condemned coordinate built %d resource sets", builds.Load())
	}
	if host.LaneAttached("daemon-a", "a") {
		t.Fatal("condemned coordinate answered ready")
	}
}

func TestCompartmentFaultRetriesInPlaceAndBecomesReady(t *testing.T) {
	host := daemonhost.New(daemonhost.Config{ScanInterval: time.Hour})
	t.Cleanup(func() { _ = host.Close() })
	host.Register("a", 1, platform.DaemonMembrane{
		Plan:    func(context.Context, string) ([]platform.PlanActor, error) { return nil, nil },
		IsBound: func(context.Context, string) (bool, error) { return true, nil },
	})
	var attempts atomic.Int32
	startTestCompute(t, host, func(string, string) (CompartmentResources, error) {
		if attempts.Add(1) == 1 {
			return CompartmentResources{}, errors.New("temporary disk fault")
		}
		return CompartmentResources{Factories: emptyFactories{}}, nil
	})
	waitCompute(t, func() bool {
		host.Scan()
		return attempts.Load() >= 1 && len(host.LaneView("daemon-a")) == 1
	})
	first := host.LaneView("daemon-a")[0].LaneGen
	if host.LaneAttached("daemon-a", "a") {
		t.Fatal("faulted compartment was reported ready")
	}
	waitCompute(t, func() bool {
		return attempts.Load() >= 2 && host.LaneAttached("daemon-a", "a")
	})
	second := host.LaneView("daemon-a")[0].LaneGen
	if first != second {
		t.Fatal("fault recovery reopened the lane instead of healing the compartment")
	}
}

func TestInFlightRPCsRetireWithTheirExactLane(t *testing.T) {
	lane := &clientLane{pending: make(map[string]chan link.LaneFrame)}
	before := make(chan link.LaneFrame, 1)
	lane.pending["before"] = before
	lane.deliver("before", link.LaneFrame{Kind: link.LanePlanReply, RequestID: "before"})
	if reply := <-before; reply.RequestID != "before" {
		t.Fatalf("delivered reply=%+v", reply)
	}
	after := make(chan link.LaneFrame, 1)
	lane.pending["after"] = after
	lane.markStreamRetired()
	if _, ok := <-after; ok {
		t.Fatal("undelivered waiter survived exact lane retirement")
	}
	late := make(chan link.LaneFrame, 1)
	lane.mu.Lock()
	lane.pending["after"] = late
	lane.mu.Unlock()
	lane.deliver("after", link.LaneFrame{Kind: link.LanePlanReply, RequestID: "after"})
	select {
	case <-late:
		t.Fatal("late g1 reply entered a post-retirement pending table")
	default:
	}
}

func TestClientLaneRPCWaitHasIndependentTimeout(t *testing.T) {
	previous := laneRPCTimeout
	laneRPCTimeout = 30 * time.Millisecond
	t.Cleanup(func() { laneRPCTimeout = previous })

	waiter := make(chan link.LaneFrame)
	streamDone := make(chan struct{})
	started := time.Now()
	_, err := waitClientLaneReply(context.Background(), waiter, streamDone)
	elapsed := time.Since(started)
	if !errors.Is(err, link.ErrLaneRPCTimeout) {
		t.Fatalf("wait error = %v, want %v", err, link.ErrLaneRPCTimeout)
	}
	if elapsed < laneRPCTimeout*8/10 {
		t.Fatalf("RPC wait returned after %v, before %v timeout", elapsed, laneRPCTimeout)
	}
	if elapsed > time.Second {
		t.Fatalf("RPC wait exceeded independent timeout: %v", elapsed)
	}
}

func TestGoneSendFailureDoesNotBlockCompartmentRemoval(t *testing.T) {
	manager := &compartmentManager{
		ctx: context.Background(), carrier: &link.ClientCarrier{},
		cells: make(map[string]*compartment),
	}
	cell := &compartment{
		manager: manager, chID: "a", closing: true, closeStarted: true,
		stopBuild: make(chan struct{}),
	}
	manager.cells["a"] = cell
	start := time.Now()
	cell.close()
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("failed gone send blocked local compartment removal")
	}
	if _, exists := manager.cells["a"]; exists {
		t.Fatal("failed gone send kept the dead compartment in the manager")
	}
}
