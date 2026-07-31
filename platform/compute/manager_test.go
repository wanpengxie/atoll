package compute

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
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

// testPresent is the realm channel directory the host enumerates when a device
// pulls its compartment snapshot. Every test that expects a compartment to be
// retired needs one, because a host that cannot enumerate the directory
// deliberately sends no snapshot at all.
func testPresent(ids ...channel.ID) func(context.Context) ([]channel.ID, error) {
	return func(context.Context) ([]channel.ID, error) { return ids, nil }
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
	host := daemonhost.New(daemonhost.Config{
		ScanInterval: time.Hour,
		Present:      testPresent("channel-a", "a", "b"),
	})
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
	host := daemonhost.New(daemonhost.Config{
		ScanInterval: time.Hour,
		Present:      testPresent("channel-a", "a", "b"),
	})
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
	host := daemonhost.New(daemonhost.Config{
		ScanInterval: time.Hour,
		Present:      testPresent("channel-a", "a", "b"),
	})
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

// TestChannelDeletedWhileOfflineIsRetiredOnReconnect is the case the old
// command-and-acknowledge shape could not reach at all: the channel goes away
// while the device is not connected, so no teardown command could ever have
// been delivered, and this host keeps no record that the device had anything
// there. The device carries the coordinate itself, so its first snapshot after
// reconnecting does not name the channel and it retires the compartment.
func TestChannelDeletedWhileOfflineIsRetiredOnReconnect(t *testing.T) {
	var present atomic.Value
	present.Store([]channel.ID{"a"})
	host := daemonhost.New(daemonhost.Config{
		ScanInterval: time.Hour,
		Present: func(context.Context) ([]channel.ID, error) {
			return present.Load().([]channel.ID), nil
		},
	})
	t.Cleanup(func() { _ = host.Close() })
	host.Register("a", 1, platform.DaemonMembrane{
		Plan:    func(context.Context, string) ([]platform.PlanActor, error) { return nil, nil },
		IsBound: func(context.Context, string) (bool, error) { return true, nil },
	})
	var closed atomic.Int32
	startTestCompute(t, host, func(string, string) (CompartmentResources, error) {
		return CompartmentResources{
			Factories: emptyFactories{},
			Close:     func() error { closed.Add(1); return nil },
		}, nil
	})
	waitCompute(t, func() bool {
		host.Scan()
		return host.LaneAttached("daemon-a", "a")
	})

	// The channel is destroyed: its Home unregisters and it leaves the realm
	// directory. Nothing is sent to the device, which may well be offline.
	host.Unregister("a", 1)
	present.Store([]channel.ID(nil))

	waitCompute(t, func() bool {
		host.Scan()
		return closed.Load() == 1
	})
}

// TestUnjudgeableChannelKeepsTheCompartment is the other half of the contract.
// A channel whose Home is closed cannot be judged, so the snapshot names it
// unknown and the device must leave the compartment exactly where it is.
func TestUnjudgeableChannelKeepsTheCompartment(t *testing.T) {
	host := daemonhost.New(daemonhost.Config{
		ScanInterval: time.Hour,
		Present:      testPresent("a"),
	})
	t.Cleanup(func() { _ = host.Close() })
	host.Register("a", 1, platform.DaemonMembrane{
		Plan:    func(context.Context, string) ([]platform.PlanActor, error) { return nil, nil },
		IsBound: func(context.Context, string) (bool, error) { return true, nil },
	})
	var closed atomic.Int32
	startTestCompute(t, host, func(string, string) (CompartmentResources, error) {
		return CompartmentResources{
			Factories: emptyFactories{},
			Close:     func() error { closed.Add(1); return nil },
		}, nil
	})
	waitCompute(t, func() bool {
		host.Scan()
		return host.LaneAttached("daemon-a", "a")
	})

	// Home closes but the channel still exists: unjudgeable, not gone.
	host.Unregister("a", 1)
	for i := 0; i < 5; i++ {
		host.Scan()
		time.Sleep(20 * time.Millisecond)
	}
	if closed.Load() != 0 {
		t.Fatalf("an unjudgeable channel destroyed its compartment %d times", closed.Load())
	}
}

func TestCompartment_RebuildsAfterCloseWhenRebound(t *testing.T) {
	host := daemonhost.New(daemonhost.Config{
		ScanInterval: time.Hour,
		Present:      testPresent("channel-a", "a", "b"),
	})
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
		host := daemonhost.New(daemonhost.Config{
			ScanInterval: time.Hour,
			Present:      testPresent("channel-a", "a", "b"),
		})
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
	host := daemonhost.New(daemonhost.Config{
		ScanInterval: time.Hour,
		Present:      testPresent("channel-a", "a", "b"),
	})
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
	host := daemonhost.New(daemonhost.Config{
		ScanInterval: time.Hour,
		Present:      testPresent("channel-a", "a", "b"),
	})
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
	host := daemonhost.New(daemonhost.Config{
		ScanInterval: time.Hour,
		Present:      testPresent("channel-a", "a", "b"),
	})
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
	// B converges while A is still inside its build. Readiness is A's own local
	// fact and this host never learns it, so the property under test is exactly
	// that B got its route and its build regardless of A being stuck.
	waitCompute(t, func() bool { return host.LaneAttached("daemon-a", "b") })
	close(blockA)
	waitCompute(t, func() bool { return host.LaneAttached("daemon-a", "a") })
}

func TestBlockedRebindPlanDoesNotBlockSiblingLaneAdmission(t *testing.T) {
	host := daemonhost.New(daemonhost.Config{
		ScanInterval: time.Hour,
		Present:      testPresent("channel-a", "a", "b"),
	})
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
	host := daemonhost.New(daemonhost.Config{
		ScanInterval: time.Hour,
		Present:      testPresent("channel-a", "a", "b"),
	})
	t.Cleanup(func() { _ = host.Close() })
	var bound atomic.Bool
	bound.Store(true)
	host.Register("a", 1, platform.DaemonMembrane{
		Plan:    func(context.Context, string) ([]platform.PlanActor, error) { return nil, nil },
		IsBound: func(context.Context, string) (bool, error) { return bound.Load(), nil },
	})
	var builds atomic.Int32
	var closeAttempts atomic.Int32
	startTestCompute(t, host, func(string, string) (CompartmentResources, error) {
		builds.Add(1)
		return CompartmentResources{
			Factories: emptyFactories{},
			Close: func() error {
				closeAttempts.Add(1)
				return errors.New("injected close failure")
			},
		}, nil
	})
	// Wait for the resource set itself, not for the route. A live lane no longer
	// implies a finished build, and this test needs the first set to exist
	// before it can ask whether a second one is ever created.
	waitCompute(t, func() bool {
		host.Scan()
		return builds.Load() == 1
	})
	bound.Store(false)
	host.Scan()
	// The failing Close is what condemns this coordinate, so wait for it rather
	// than for the route to drop.
	waitCompute(t, func() bool {
		host.Scan()
		return closeAttempts.Load() >= 1
	})
	bound.Store(true)
	for i := 0; i < 3; i++ {
		host.Scan()
		time.Sleep(10 * time.Millisecond)
	}
	if builds.Load() != 1 {
		t.Fatalf("condemned coordinate built %d resource sets", builds.Load())
	}
}

func TestCompartmentFaultRetriesInPlaceAndBecomesReady(t *testing.T) {
	host := daemonhost.New(daemonhost.Config{
		ScanInterval: time.Hour,
		Present:      testPresent("channel-a", "a", "b"),
	})
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
	// A fault is the compartment's own business: it retries in place and the
	// route is never disturbed, which is what the unchanged lane generation
	// across the retry proves.
	first := host.LaneView("daemon-a")[0].LaneGen
	waitCompute(t, func() bool { return attempts.Load() >= 2 })
	second := host.LaneView("daemon-a")[0].LaneGen
	if first != second {
		t.Fatal("fault recovery reopened the lane instead of healing the compartment")
	}
}

// TestSnapshotRetirementIsExactObject holds the teardown to the compartment the
// snapshot actually condemned. A snapshot is computed, then acted on; a rebind
// landing in between installs a newer compartment at the same coordinate, and
// re-looking the coordinate up would destroy that newer one instead.
func TestSnapshotRetirementIsExactObject(t *testing.T) {
	manager := newCompartmentManager(
		context.Background(), Config{}, slog.New(slog.DiscardHandler),
	)
	condemned := &compartment{manager: manager, chID: "a", stopBuild: make(chan struct{})}
	manager.cells["a"] = condemned

	// The rebind: a different compartment now occupies this coordinate.
	replacement := &compartment{manager: manager, chID: "a", stopBuild: make(chan struct{})}
	manager.cells["a"] = replacement

	manager.closeExactCompartment(condemned)

	manager.mu.Lock()
	survivor := manager.cells["a"]
	manager.mu.Unlock()
	if survivor != replacement {
		t.Fatalf("stale snapshot retired the replacement compartment: %p want %p",
			survivor, replacement)
	}
	replacement.mu.Lock()
	closing := replacement.closing
	replacement.mu.Unlock()
	if closing {
		t.Fatal("stale snapshot put the replacement compartment into teardown")
	}
}

// The first two generations are real ones captured from a failing run: they
// were minted inside the same millisecond, so their timestamp prefixes are
// identical and only the sub-millisecond sequence distinguishes them. An
// ordering that gave up at the millisecond would fail here, and the hazard
// only occurs when two lanes are opened this close together.
const (
	openedFirst  = link.LaneGeneration("019fb62d-8798-71be-a7a1-0a7e28255421")
	openedSecond = link.LaneGeneration("019fb62d-8798-7967-a764-3e1465ec07da")
	openedThird  = link.LaneGeneration("019fb62d-8799-7c04-b0f2-5d1a9e77f310")
)

// laneAdmissionFixture drives the real admission path with nothing else
// attached. The compartment it hands out already has a build in flight, so an
// admitted lane is installed and nothing further is started — admission is the
// only behaviour under test.
type laneAdmissionFixture struct {
	t       *testing.T
	manager *compartmentManager
	carrier *link.ClientCarrier
	cell    *compartment
	lanes   []*clientLane
}

func newLaneAdmissionFixture(t *testing.T) *laneAdmissionFixture {
	t.Helper()
	manager := newCompartmentManager(
		context.Background(), Config{}, slog.New(slog.DiscardHandler),
	)
	carrier := &link.ClientCarrier{}
	manager.carrier = carrier
	cell := &compartment{
		manager: manager, chID: "a",
		stopBuild: make(chan struct{}),
		buildDone: make(chan struct{}),
	}
	manager.cells["a"] = cell
	fixture := &laneAdmissionFixture{t: t, manager: manager, carrier: carrier, cell: cell}
	t.Cleanup(func() {
		for _, lane := range fixture.lanes {
			lane.retireLogical()
		}
		fixture.manager.wg.Wait()
	})
	return fixture
}

func (f *laneAdmissionFixture) lane(
	carrier *link.ClientCarrier, gen link.LaneGeneration,
) *clientLane {
	f.t.Helper()
	local, remote := net.Pipe()
	f.t.Cleanup(func() { _ = remote.Close() })
	stream, err := link.AdoptLane(carrier, link.DeviceStreamHeader{
		Kind: link.DeviceStreamLaneControl, Channel: "a", LaneGen: gen,
	}, local)
	if err != nil {
		f.t.Fatalf("adopt lane %q: %v", gen, err)
	}
	lane := newClientLane(f.manager, carrier, stream)
	f.lanes = append(f.lanes, lane)
	return lane
}

func (f *laneAdmissionFixture) slots() (lane, pending *clientLane) {
	f.cell.mu.Lock()
	defer f.cell.mu.Unlock()
	return f.cell.lane, f.cell.pending
}

// TestLaneAdmissionOrdersByGeneration is the deterministic form of the
// arrival-order hazard: two lanes for one coordinate are opened by the server
// in a known order, each header is parsed by its own worker, and they can
// therefore reach admission in either order.
func TestLaneAdmissionOrdersByGeneration(t *testing.T) {
	t.Run("out of order arrival is refused", func(t *testing.T) {
		fixture := newLaneAdmissionFixture(t)
		newer := fixture.lane(fixture.carrier, openedSecond)
		older := fixture.lane(fixture.carrier, openedFirst)

		fixture.manager.acceptLane(newer)
		fixture.manager.acceptLane(older)

		if lane, _ := fixture.slots(); lane != newer {
			t.Fatal("an older arrival displaced the generation the server routes on")
		}
		if !older.stream.Retired() {
			t.Fatal("the refused arrival was left running")
		}
		if newer.stream.Retired() {
			t.Fatal("admitting an older arrival retired the installed lane")
		}
	})
	// The reordering is what admission defends against, but the ordinary case
	// is these same two lanes arriving in the order they were opened. Both
	// generations were minted inside one millisecond, so an ordering that
	// stopped at the timestamp would refuse the lane the server just opened
	// and leave the coordinate on a route the server has already abandoned.
	t.Run("in order arrival installs", func(t *testing.T) {
		fixture := newLaneAdmissionFixture(t)
		older := fixture.lane(fixture.carrier, openedFirst)
		newer := fixture.lane(fixture.carrier, openedSecond)

		fixture.manager.acceptLane(older)
		fixture.manager.acceptLane(newer)

		if lane, _ := fixture.slots(); lane != newer {
			t.Fatal("the generation the server had just opened was refused")
		}
		if !older.stream.Retired() {
			t.Fatal("the superseded lane was left running")
		}
	})
}

// TestRetiredLaneDoesNotReopenItsCoordinateToOlderGenerations holds the
// watermark to the coordinate rather than to the slot. A lane retiring empties
// the slot, and reading the admitted generation back out of the slot would let
// anything at all in from that moment on.
func TestRetiredLaneDoesNotReopenItsCoordinateToOlderGenerations(t *testing.T) {
	fixture := newLaneAdmissionFixture(t)
	newer := fixture.lane(fixture.carrier, openedSecond)
	older := fixture.lane(fixture.carrier, openedFirst)

	fixture.manager.acceptLane(newer)
	newer.retireLogical()
	if lane, _ := fixture.slots(); lane != nil {
		t.Fatal("retirement did not empty the slot, so this test proves nothing")
	}

	fixture.manager.acceptLane(older)
	if lane, _ := fixture.slots(); lane != nil {
		t.Fatal("a generation the coordinate had already moved past was readmitted")
	}
}

// TestTeardownSlotKeepsTheNewestGeneration covers the same watermark question
// during teardown, where the installed slot is empty by construction and the
// arriving lane is parked for the compartment that will replace this one.
func TestTeardownSlotKeepsTheNewestGeneration(t *testing.T) {
	t.Run("older arrival is refused", func(t *testing.T) {
		fixture := newLaneAdmissionFixture(t)
		fixture.cell.closing = true
		newer := fixture.lane(fixture.carrier, openedSecond)
		older := fixture.lane(fixture.carrier, openedFirst)

		fixture.manager.acceptLane(newer)
		fixture.manager.acceptLane(older)

		if _, pending := fixture.slots(); pending != newer {
			t.Fatal("an older arrival took the slot the next compartment inherits")
		}
	})
	t.Run("newer arrival replaces", func(t *testing.T) {
		fixture := newLaneAdmissionFixture(t)
		fixture.cell.closing = true
		second := fixture.lane(fixture.carrier, openedSecond)
		third := fixture.lane(fixture.carrier, openedThird)

		fixture.manager.acceptLane(second)
		fixture.manager.acceptLane(third)

		if _, pending := fixture.slots(); pending != third {
			t.Fatal("the newest generation was not parked for the next compartment")
		}
		if !second.stream.Retired() {
			t.Fatal("the superseded lane was left running")
		}
	})
}

// TestRedeliveredGenerationIsRefused pins that a generation names exactly one
// stream: a second stream carrying a generation already admitted must not
// displace the one that is live.
func TestRedeliveredGenerationIsRefused(t *testing.T) {
	fixture := newLaneAdmissionFixture(t)
	installed := fixture.lane(fixture.carrier, openedSecond)
	duplicate := fixture.lane(fixture.carrier, openedSecond)

	fixture.manager.acceptLane(installed)
	fixture.manager.acceptLane(duplicate)

	if lane, _ := fixture.slots(); lane != installed {
		t.Fatal("a redelivered generation displaced the live lane")
	}
	if !duplicate.stream.Retired() {
		t.Fatal("the redelivered stream was left running")
	}
}

// TestLaneFromAStaleCarrierIsRefusedRegardlessOfGeneration pins that the
// carrier decides first. Generations order within one carrier only, so a lane
// whose carrier is gone is not admissible however new its generation looks —
// the redial loop does not wait for the dead carrier's stream workers.
func TestLaneFromAStaleCarrierIsRefusedRegardlessOfGeneration(t *testing.T) {
	fixture := newLaneAdmissionFixture(t)
	current := fixture.lane(fixture.carrier, openedFirst)
	fixture.manager.acceptLane(current)

	stale := fixture.lane(&link.ClientCarrier{}, openedThird)
	fixture.manager.acceptLane(stale)

	if lane, _ := fixture.slots(); lane != current {
		t.Fatal("a lane from a carrier this device no longer holds was installed")
	}
	if !stale.stream.Retired() {
		t.Fatal("the refused stream from the stale carrier was left running")
	}
}

// TestLaneFromAStaleCarrierCreatesNoCompartment is the other half of the
// carrier check: refusing after the lookup would still leave an empty
// compartment at a coordinate this device was never told to serve.
func TestLaneFromAStaleCarrierCreatesNoCompartment(t *testing.T) {
	fixture := newLaneAdmissionFixture(t)
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = remote.Close() })
	staleCarrier := &link.ClientCarrier{}
	stream, err := link.AdoptLane(staleCarrier, link.DeviceStreamHeader{
		Kind: link.DeviceStreamLaneControl, Channel: "unserved", LaneGen: openedThird,
	}, local)
	if err != nil {
		t.Fatalf("adopt lane: %v", err)
	}
	lane := newClientLane(fixture.manager, staleCarrier, stream)
	fixture.lanes = append(fixture.lanes, lane)

	fixture.manager.acceptLane(lane)

	fixture.manager.mu.Lock()
	_, conjured := fixture.manager.cells["unserved"]
	fixture.manager.mu.Unlock()
	if conjured {
		t.Fatal("a lane from a stale carrier conjured a compartment out of nothing")
	}
}

// TestCarrierDownClearsTheGenerationWatermark pins the scope of the ordering.
// Generation identity is really (carrier, lane); the next carrier mints its own
// series, so its first lane must be admissible even when it compares lower than
// what the previous carrier had reached.
func TestCarrierDownClearsTheGenerationWatermark(t *testing.T) {
	fixture := newLaneAdmissionFixture(t)
	fixture.manager.acceptLane(fixture.lane(fixture.carrier, openedThird))

	fixture.manager.carrierDown(fixture.carrier)
	next := &link.ClientCarrier{}
	fixture.manager.mu.Lock()
	fixture.manager.carrier = next
	fixture.manager.mu.Unlock()

	arrival := fixture.lane(next, openedFirst)
	fixture.manager.acceptLane(arrival)

	if lane, _ := fixture.slots(); lane != arrival {
		t.Fatal("the previous carrier's watermark barred the new carrier's lane")
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

func TestLaneSessionOpenErrorReturnsNilInterface(t *testing.T) {
	session := &laneSession{lane: &clientLane{
		manager: &compartmentManager{logger: slog.New(slog.DiscardHandler)},
	}}
	stream, err := session.OpenActorStream(
		context.Background(), "actor-a", "attempt-a",
	)
	if err == nil {
		t.Fatal("invalid physical lane opened an actor stream")
	}
	if stream != nil {
		t.Fatalf("open error returned non-nil typed interface %T", stream)
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

// TestCarrierDownDoesNotWaitForLaneCollection pins reconnection to the value
// decision alone. Closing the carrier closes its session, so the physical end
// is already reclaimed; what remains is each lane reader noticing, and a reader
// parked inside a local storage call may not notice for a long time. Waiting
// for it here would put one stuck compartment in front of the whole device
// coming back.
//
// The lane below is installed without a reader, which is the deterministic form
// of that hazard: nothing will ever collect it.
func TestCarrierDownDoesNotWaitForLaneCollection(t *testing.T) {
	fixture := newLaneAdmissionFixture(t)
	lane := fixture.lane(fixture.carrier, openedFirst)
	fixture.cell.mu.Lock()
	fixture.cell.lane = lane
	fixture.cell.latestLaneGen = openedFirst
	fixture.cell.mu.Unlock()

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		fixture.manager.carrierDown(fixture.carrier)
	}()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("carrierDown waited on a lane that nothing was going to collect")
	}

	select {
	case <-lane.stream.PhysicalDone():
		t.Fatal("the lane was collected after all, so this test proves nothing")
	default:
	}
	fixture.manager.mu.Lock()
	carrier := fixture.manager.carrier
	fixture.manager.mu.Unlock()
	if carrier != nil {
		t.Fatal("carrierDown returned without withdrawing the carrier")
	}
	if installed, _ := fixture.slots(); installed != nil {
		t.Fatal("carrierDown returned with the lane still installed")
	}
}

// TestTeardownStepsWithoutCancellationStayInsideTheJoinBudget pins the budget to
// the whole teardown, not to the steps that happen to accept a context. The
// compartment resources handed in by the host close through a plain func() error
// — arbitrary code this package cannot interrupt — and teardown holds the
// coordinate out of service the entire time it runs.
func TestTeardownStepsWithoutCancellationStayInsideTheJoinBudget(t *testing.T) {
	previous := compartmentJoinTimeout
	compartmentJoinTimeout = 50 * time.Millisecond
	t.Cleanup(func() { compartmentJoinTimeout = previous })
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	manager := newCompartmentManager(
		context.Background(), Config{}, slog.New(slog.DiscardHandler),
	)
	cell := &compartment{
		manager: manager, chID: "a", stopBuild: make(chan struct{}),
		resources: CompartmentResources{Close: func() error { <-release; return nil }},
	}
	manager.cells["a"] = cell

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		cell.close()
	}()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("teardown waited on a close step that accepts no cancellation")
	}

	cell.mu.Lock()
	condemned, reason := cell.condemned, cell.reason
	cell.mu.Unlock()
	if !condemned {
		t.Fatal("teardown gave up on a step and still reported a clean removal")
	}
	if !strings.Contains(reason, "join budget") {
		t.Fatalf("teardown reason=%q, want the step it gave up on", reason)
	}
}
