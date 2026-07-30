package daemonhost

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func dialTestCarrier(t *testing.T, host *Host) *link.ClientCarrier {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host.Serve(w, r, "daemon-a")
	}))
	t.Cleanup(server.Close)
	carrier, _, err := link.DialDeviceCarrier(
		t.Context(), "ws"+strings.TrimPrefix(server.URL, "http"), "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = carrier.Close() })
	var frame link.SpineFrame
	if err := carrier.ReadSpine(&frame); err != nil {
		t.Fatal(err)
	}
	if frame.Kind != link.SpineCarrierAccept {
		t.Fatalf("got verdict %q", frame.Kind)
	}
	return carrier
}

func waitFor(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !predicate() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not converge")
		}
		time.Sleep(time.Millisecond)
	}
}

func adoptAndSuperviseTestLane(t *testing.T, carrier *link.ClientCarrier) *link.LaneStream {
	t.Helper()
	conn, header, err := carrier.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	lane, err := link.AdoptLane(carrier, header, conn)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			var frame link.LaneFrame
			if lane.Decode(&frame) != nil {
				lane.CollectPhysical()
				return
			}
		}
	}()
	return lane
}

func TestLaneTermination_RetiresLaneOnly(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	t.Cleanup(func() { _ = host.Close() })
	var bound atomic.Bool
	bound.Store(true)
	host.Register("channel-a", 1, platform.DaemonMembrane{
		Plan:    func(context.Context, string) ([]platform.PlanActor, error) { return nil, nil },
		IsBound: func(context.Context, string) (bool, error) { return bound.Load(), nil },
	})
	carrier := dialTestCarrier(t, host)
	host.Scan()
	conn, header, err := carrier.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	first, err := link.AdoptLane(carrier, header, conn)
	if err != nil {
		t.Fatal(err)
	}
	if err := carrier.SendSpine(link.SpineFrame{
		Kind: link.SpineCompartmentState, Channel: channel.ID("channel-a"), State: "ready",
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return host.LaneAttached("daemon-a", "channel-a") })
	go func() {
		var frame link.LaneFrame
		_ = first.Decode(&frame)
	}()

	bound.Store(false)
	host.Scan()
	select {
	case <-first.Done():
	case <-time.After(time.Second):
		t.Fatal("retired lane did not terminate")
	}
	if !host.DaemonOnline("daemon-a") {
		t.Fatal("lane retirement tore down the carrier")
	}

	bound.Store(true)
	host.Scan()
	conn, header, err = carrier.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	second, err := link.AdoptLane(carrier, header, conn)
	if err != nil {
		t.Fatal(err)
	}
	first.RetireLogical()
	if second.Retired() {
		t.Fatal("late g1 retirement affected g2")
	}
}

func TestLaneAttached_RequiresPositiveReady(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	t.Cleanup(func() { _ = host.Close() })
	host.Register("channel-a", 1, platform.DaemonMembrane{
		Plan:    func(context.Context, string) ([]platform.PlanActor, error) { return nil, nil },
		IsBound: func(context.Context, string) (bool, error) { return true, nil },
	})
	carrier := dialTestCarrier(t, host)
	host.Scan()
	conn, header, err := carrier.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	lane, err := link.AdoptLane(carrier, header, conn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lane.RetireLogical)
	if host.LaneAttached("daemon-a", "channel-a") {
		t.Fatal("lane without ready declaration was reported attached")
	}
	_ = carrier.SendSpine(link.SpineFrame{
		Kind: link.SpineCompartmentState, Channel: "channel-a", State: "building",
	})
	time.Sleep(10 * time.Millisecond)
	if host.LaneAttached("daemon-a", "channel-a") {
		t.Fatal("building compartment was reported attached")
	}
	_ = carrier.SendSpine(link.SpineFrame{
		Kind: link.SpineCompartmentState, Channel: "channel-a", State: "ready",
	})
	waitFor(t, func() bool { return host.LaneAttached("daemon-a", "channel-a") })
}

func TestMembraneGenerationCASRejectsLateCallbacks(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	defer host.Close()
	bundle := platform.DaemonMembrane{
		IsBound: func(context.Context, string) (bool, error) { return true, nil },
	}
	host.Register("a", 2, bundle)
	host.Unregister("a", 1)
	if got := host.membranes["a"].generation; got != 2 {
		t.Fatalf("late old unregister removed generation %d", got)
	}
	host.Unregister("a", 2)
	host.Register("a", 2, bundle)
	if _, ok := host.membranes["a"]; ok {
		t.Fatal("late register resurrected a retired membrane generation")
	}
	host.Register("a", 3, bundle)
	if got := host.membranes["a"].generation; got != 3 {
		t.Fatalf("new generation=%d, want 3", got)
	}
}

func TestRevokeBeforeAdmissionLeavesTombstoneFence(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	defer host.Close()
	host.RevokeDaemon("daemon-a")
	carrier := &carrierRow{host: host, daemonID: "daemon-a", gen: "g1"}
	if err := host.admit(carrier); err == nil {
		t.Fatal("carrier admitted after its daemon tombstone was committed")
	}
}

func TestDaemonFactSweepRevokesWithoutPostCommitHint(t *testing.T) {
	var deleted atomic.Bool
	host := New(Config{
		ScanInterval: time.Hour,
		DaemonFact: func(context.Context, string) DaemonFact {
			if deleted.Load() {
				return DaemonDeleted
			}
			return DaemonAlive
		},
	})
	defer host.Close()
	carrier := dialTestCarrier(t, host)
	deleted.Store(true)
	host.Scan()
	waitFor(t, func() bool { return !host.DaemonOnline("daemon-a") })
	var terminal link.SpineFrame
	if err := carrier.ReadSpine(&terminal); err != nil {
		t.Fatal(err)
	}
	if terminal.Kind != link.SpineCarrierReject || terminal.Class != link.CarrierTerminal {
		t.Fatalf("authority sweep verdict=%+v", terminal)
	}
	diagnostics := host.Diagnostics("daemon-a")
	if len(diagnostics) == 0 || diagnostics[len(diagnostics)-1].Kind != "revoke" {
		t.Fatalf("revoke diagnostics=%+v", diagnostics)
	}
}

func TestUnknownCoordinateNeverInfersCompartmentClose(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	defer host.Close()
	carrier := dialTestCarrier(t, host)
	if err := carrier.SendSpine(link.SpineFrame{
		Kind: link.SpineCompartmentState, Channel: "unknown", State: "ready",
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		host.mu.RLock()
		row := host.daemons["daemon-a"]
		host.mu.RUnlock()
		if row == nil || row.current == nil {
			return false
		}
		row.current.mu.Lock()
		_, ok := row.current.compartments["unknown"]
		row.current.mu.Unlock()
		return ok
	})
	host.Scan()
	host.mu.RLock()
	current := host.daemons["daemon-a"].current
	host.mu.RUnlock()
	current.mu.Lock()
	view := current.compartments["unknown"]
	current.mu.Unlock()
	if view.closeSent {
		t.Fatal("missing membrane was collapsed into an unbind command")
	}
}

func TestHomeReplacementRetiresLaneWithoutClosingCompartment(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	defer host.Close()
	bundle := platform.DaemonMembrane{
		Plan:    func(context.Context, string) ([]platform.PlanActor, error) { return nil, nil },
		IsBound: func(context.Context, string) (bool, error) { return true, nil },
	}
	host.Register("a", 1, bundle)
	carrier := dialTestCarrier(t, host)
	host.Scan()
	conn, header, err := carrier.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	g1, err := link.AdoptLane(carrier, header, conn)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		defer g1.CollectPhysical()
		var frame link.LaneFrame
		_ = g1.Decode(&frame)
	}()
	if err := carrier.SendSpine(link.SpineFrame{
		Kind: link.SpineCompartmentState, Channel: "a", State: "ready",
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return host.LaneAttached("daemon-a", "a") })
	host.Register("a", 2, bundle)
	select {
	case <-g1.Done():
	case <-time.After(time.Second):
		t.Fatal("old Home generation lane was not retired")
	}
	conn, header, err = carrier.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	g2, err := link.AdoptLane(carrier, header, conn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		g2.RetireLogical()
		g2.CollectPhysical()
	}()
	host.mu.RLock()
	current := host.daemons["daemon-a"].current
	host.mu.RUnlock()
	current.mu.Lock()
	view := current.compartments["a"]
	current.mu.Unlock()
	if view.closeSent {
		t.Fatal("Home generation replacement sent compartment_close")
	}
}

func TestRetireLane_DeletesRowSynchronously(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	defer host.Close()
	host.Register("a", 1, platform.DaemonMembrane{
		IsBound: func(context.Context, string) (bool, error) { return true, nil },
	})
	carrier := dialTestCarrier(t, host)
	host.Scan()
	lane := adoptAndSuperviseTestLane(t, carrier)
	waitFor(t, func() bool { return len(host.LaneView("daemon-a")) == 1 })

	host.RetireLane("daemon-a", "a")
	if got := host.LaneView("daemon-a"); len(got) != 0 {
		t.Fatalf("retire returned before deleting the exact row: %+v", got)
	}
	waitFor(t, lane.Retired)
	if !host.DaemonOnline("daemon-a") {
		t.Fatal("logical lane retirement did not stay local to the lane")
	}
	waitFor(t, func() bool { return host.RetirementCount("daemon-a", "a") == 1 })
}

func TestReconcile_EnsuresLaneRegardlessOfCompartmentState(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	defer host.Close()
	host.Register("a", 1, platform.DaemonMembrane{
		IsBound: func(context.Context, string) (bool, error) { return true, nil },
	})
	carrier := dialTestCarrier(t, host)
	host.Scan()
	first := adoptAndSuperviseTestLane(t, carrier)
	if err := carrier.SendSpine(link.SpineFrame{
		Kind: link.SpineCompartmentState, Channel: "a", State: "ready",
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return host.LaneAttached("daemon-a", "a") })
	host.RetireLane("daemon-a", "a")
	<-first.Done()
	if host.LaneAttached("daemon-a", "a") {
		t.Fatal("retired row still answered attached")
	}
	host.Scan()
	second := adoptAndSuperviseTestLane(t, carrier)
	if second.Gen == first.Gen {
		t.Fatal("scan did not mint a fresh lane generation")
	}
	waitFor(t, func() bool { return host.LaneAttached("daemon-a", "a") })
}

func TestReconcile_ClosesUnboundCompartment(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	defer host.Close()
	var bound atomic.Bool
	bound.Store(true)
	host.Register("a", 1, platform.DaemonMembrane{
		IsBound: func(context.Context, string) (bool, error) { return bound.Load(), nil },
	})
	carrier := dialTestCarrier(t, host)
	host.Scan()
	_ = adoptAndSuperviseTestLane(t, carrier)
	if err := carrier.SendSpine(link.SpineFrame{
		Kind: link.SpineCompartmentState, Channel: "a", State: "ready",
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return host.LaneAttached("daemon-a", "a") })
	bound.Store(false)
	host.Scan()
	var command link.SpineFrame
	if err := carrier.ReadSpine(&command); err != nil {
		t.Fatal(err)
	}
	if command.Kind != link.SpineCompartmentClose || command.Channel != "a" {
		t.Fatalf("unbound command=%+v", command)
	}
	if host.LaneAttached("daemon-a", "a") || !host.DaemonOnline("daemon-a") {
		t.Fatal("unbound convergence did not revoke only the coordinate")
	}
}

func TestGoneTimeoutIsObservationOnlyAndDoesNotReset(t *testing.T) {
	now := time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)
	var clock atomic.Int64
	clock.Store(now.UnixNano())
	host := New(Config{
		ScanInterval: time.Hour,
		Now:          func() time.Time { return time.Unix(0, clock.Load()).UTC() },
	})
	defer host.Close()
	var bound atomic.Bool
	bound.Store(true)
	host.Register("a", 1, platform.DaemonMembrane{
		IsBound: func(context.Context, string) (bool, error) { return bound.Load(), nil },
	})
	carrier := dialTestCarrier(t, host)
	host.Scan()
	_ = adoptAndSuperviseTestLane(t, carrier)
	_ = carrier.SendSpine(link.SpineFrame{
		Kind: link.SpineCompartmentState, Channel: "a", State: "ready",
	})
	waitFor(t, func() bool { return host.LaneAttached("daemon-a", "a") })
	bound.Store(false)
	host.Scan()
	var closeFrame link.SpineFrame
	if err := carrier.ReadSpine(&closeFrame); err != nil {
		t.Fatal(err)
	}
	now = now.Add(defaultGoneTimeout + time.Second)
	clock.Store(now.UnixNano())
	host.Scan()
	diagnostics := host.Diagnostics("daemon-a")
	if len(diagnostics) != 1 || diagnostics[0].Kind != "gone_timeout" {
		t.Fatalf("gone diagnostics=%+v", diagnostics)
	}
	host.mu.RLock()
	current := host.daemons["daemon-a"].current
	host.mu.RUnlock()
	current.mu.Lock()
	view := current.compartments["a"]
	current.mu.Unlock()
	if !view.closeSent {
		t.Fatal("observation timeout changed the revocation verdict")
	}
}

func TestCarrierHalfOpenLeaseExpires(t *testing.T) {
	now := time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)
	var clock atomic.Int64
	clock.Store(now.UnixNano())
	host := New(Config{
		ScanInterval: time.Hour, LeaseTTL: time.Second,
		Now: func() time.Time { return time.Unix(0, clock.Load()).UTC() },
	})
	defer host.Close()
	first := dialTestCarrier(t, host)
	if !host.DaemonOnline("daemon-a") {
		t.Fatal("first carrier was not admitted")
	}
	now = now.Add(2 * time.Second)
	clock.Store(now.UnixNano())
	host.probeCarriers()
	waitFor(t, func() bool { return !host.DaemonOnline("daemon-a") })
	readDone := make(chan error, 1)
	go func() {
		var frame link.SpineFrame
		readDone <- first.ReadSpine(&frame)
	}()
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("expired carrier produced another spine frame")
		}
	case <-time.After(time.Second):
		t.Fatal("expired carrier physical transport stayed open")
	}
	second := dialTestCarrier(t, host)
	if second == first || !host.DaemonOnline("daemon-a") {
		t.Fatal("lease expiry did not permit a fresh carrier")
	}
}

func TestDuplicateCurrentIsRetryableAndKeepsIncumbent(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	defer host.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host.Serve(w, r, "daemon-a")
	}))
	defer server.Close()
	rawURL := "ws" + strings.TrimPrefix(server.URL, "http")
	first, _, err := link.DialDeviceCarrier(t.Context(), rawURL, "secret", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	var accepted link.SpineFrame
	if err := first.ReadSpine(&accepted); err != nil || accepted.Kind != link.SpineCarrierAccept {
		t.Fatalf("first verdict=%+v err=%v", accepted, err)
	}
	second, _, err := link.DialDeviceCarrier(t.Context(), rawURL, "secret", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	var rejected link.SpineFrame
	if err := second.ReadSpine(&rejected); err != nil {
		t.Fatal(err)
	}
	if rejected.Kind != link.SpineCarrierReject || rejected.Class != link.CarrierRetryable {
		t.Fatalf("duplicate verdict=%+v", rejected)
	}
	if !host.DaemonOnline("daemon-a") {
		t.Fatal("duplicate admission displaced the incumbent")
	}
}

func TestCompartmentDeclarationsRemainInSpineOrder(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	defer host.Close()
	carrier := dialTestCarrier(t, host)
	for _, state := range []string{"building", "ready", "fault"} {
		if err := carrier.SendSpine(link.SpineFrame{
			Kind: link.SpineCompartmentState, Channel: "a", State: state,
		}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool {
		host.mu.RLock()
		current := host.daemons["daemon-a"].current
		host.mu.RUnlock()
		current.mu.Lock()
		defer current.mu.Unlock()
		return current.compartments["a"].state == CompartmentFault
	})
}

func TestCoordinateExecutorsDoNotLetBlockedABarB(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	t.Cleanup(func() { _ = host.Close() })
	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	host.Register("a", 1, platform.DaemonMembrane{
		IsBound: func(context.Context, string) (bool, error) {
			enterOnce.Do(func() { close(entered) })
			<-release
			return true, nil
		},
	})
	host.Register("b", 1, platform.DaemonMembrane{
		IsBound: func(context.Context, string) (bool, error) { return true, nil },
	})
	carrier := dialTestCarrier(t, host)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("A reconcile did not enter its blocked authority read")
	}
	conn, header, err := carrier.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	if header.Channel != "b" {
		_ = conn.Close()
		t.Fatalf("blocked A prevented B admission; first lane=%q", header.Channel)
	}
	lane, err := link.AdoptLane(carrier, header, conn)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		var frame link.LaneFrame
		_ = lane.Decode(&frame)
		lane.CollectPhysical()
	}()
	releaseOnce.Do(func() { close(release) })
}
