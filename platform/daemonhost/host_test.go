package daemonhost

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/access"
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

// TestLaneAttachedAnswersFromThisHostsLedgerOnly pins the answer to the two
// facts this host owns. A device declaration cannot make it true and cannot
// make it false, so no reply from the device can ever wedge this coordinate.
func TestLaneAttachedAnswersFromThisHostsLedgerOnly(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	t.Cleanup(func() { _ = host.Close() })
	var bound atomic.Bool
	bound.Store(true)
	host.Register("channel-a", 1, platform.DaemonMembrane{
		Plan:    func(context.Context, string) ([]platform.PlanActor, error) { return nil, nil },
		IsBound: func(context.Context, string) (bool, error) { return bound.Load(), nil },
	})
	carrier := dialTestCarrier(t, host)
	if host.LaneAttached("daemon-a", "never-routed") {
		t.Fatal("coordinate with no lane row was reported attached")
	}
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
	waitFor(t, func() bool { return host.LaneAttached("daemon-a", "channel-a") })

	// The device says nothing at any point, and the answer still tracks the
	// route exactly: unbinding retires the row and the answer follows it down.
	bound.Store(false)
	host.Scan()
	waitFor(t, func() bool { return !host.LaneAttached("daemon-a", "channel-a") })
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
	terminal := readTestSpine(t, carrier)
	if terminal.Kind != link.SpineCarrierReject || terminal.Class != link.CarrierTerminal {
		t.Fatalf("authority sweep verdict=%+v", terminal)
	}
	diagnostics := host.Diagnostics("daemon-a")
	if len(diagnostics) == 0 || diagnostics[len(diagnostics)-1].Kind != "revoke" {
		t.Fatalf("revoke diagnostics=%+v", diagnostics)
	}
}

// TestDeviceTrafficAllocatesNoPerCoordinateState covers what the device can
// still send now that the spine carries no coordinate it can name: a flood of
// plan pulls. Each is answered from this host's own directory and must leave
// no per-coordinate residue behind.
//
// The older flood — a device naming 256 channels of its choosing — is not
// expressible any more: SpineFrame has no channel field for a device to fill,
// so the compiler forbids the shape this used to police at runtime.
func TestDeviceTrafficAllocatesNoPerCoordinateState(t *testing.T) {
	host := New(Config{
		ScanInterval: time.Hour,
		Present:      func(context.Context) ([]channel.ID, error) { return nil, nil },
	})
	defer host.Close()
	carrier := dialTestCarrier(t, host)
	for i := 0; i < 256; i++ {
		if err := carrier.SendSpine(link.SpineFrame{
			Kind: link.SpineCompartmentPlanPull, Nonce: fmt.Sprintf("pull-%d", i),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := carrier.SendSpine(link.SpineFrame{Kind: link.SpineProbe, Nonce: "flood-barrier"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		reply := readTestSpine(t, carrier)
		return reply.Kind == link.SpineProbeReply && reply.Nonce == "flood-barrier"
	})
	host.mu.RLock()
	current := host.daemons["daemon-a"].current
	host.mu.RUnlock()
	current.mu.Lock()
	lanes := len(current.lanes)
	coordLocks := len(current.coordLocks)
	coordTasks := len(current.coordTasks)
	current.mu.Unlock()
	if lanes != 0 || coordLocks != 0 || coordTasks != 0 {
		t.Fatalf("device traffic allocated carrier state: lanes=%d locks=%d tasks=%d",
			lanes, coordLocks, coordTasks)
	}
}

func TestHomeReplacementRetiresLaneWithoutClosingCompartment(t *testing.T) {
	host := New(Config{
		ScanInterval: time.Hour,
		Present: func(context.Context) ([]channel.ID, error) {
			return []channel.ID{"a"}, nil
		},
	})
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
	// Home replacement is a routing-generation event, not a value revocation:
	// the snapshot must still name this channel under serve, so the device has
	// no reason to retire the compartment it is rebinding.
	plan := pullTestPlan(t, carrier)
	if !containsChannel(plan.Serve, "a") {
		t.Fatalf("Home generation replacement dropped the channel from serve: %+v", plan)
	}
}

// pullTestPlan performs the device half of one compartment plan round trip.
func pullTestPlan(t *testing.T, carrier *link.ClientCarrier) link.SpineFrame {
	t.Helper()
	nonce := "plan-" + uuid.NewString()
	if err := carrier.SendSpine(link.SpineFrame{
		Kind: link.SpineCompartmentPlanPull, Nonce: nonce,
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 64; i++ {
		reply := readTestSpine(t, carrier)
		if reply.Kind == link.SpineCompartmentPlanReply && reply.Nonce == nonce {
			return reply
		}
	}
	t.Fatal("compartment plan reply never arrived")
	return link.SpineFrame{}
}

// readTestSpine returns the next spine frame that carries a decision. Pokes are
// contentless wakes the host may emit at any time, so a test that is waiting on
// a verdict must not mistake one for an answer.
func readTestSpine(t *testing.T, carrier *link.ClientCarrier) link.SpineFrame {
	t.Helper()
	for {
		var frame link.SpineFrame
		if err := carrier.ReadSpine(&frame); err != nil {
			t.Fatal(err)
		}
		if frame.Kind != link.SpineCompartmentPlanPoke {
			return frame
		}
	}
}

func containsChannel(ids []channel.ID, want channel.ID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func TestEnsureLaneRetiresOldExactObjectWithoutDeletingReplacement(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	defer host.Close()
	bundle := platform.DaemonMembrane{
		Plan:    func(context.Context, string) ([]platform.PlanActor, error) { return nil, nil },
		IsBound: func(context.Context, string) (bool, error) { return true, nil },
	}
	host.Register("a", 1, bundle)
	carrier := dialTestCarrier(t, host)
	host.Scan()
	g1 := adoptAndSuperviseTestLane(t, carrier)
	host.mu.RLock()
	current := host.daemons["daemon-a"].current
	host.mu.RUnlock()
	waitFor(t, func() bool {
		current.mu.Lock()
		defer current.mu.Unlock()
		lane := current.lanes["a"]
		return lane != nil && lane.stream.Gen == g1.Gen
	})
	current.ensureLane("a", membraneRow{generation: 2, bundle: bundle})
	g2 := adoptAndSuperviseTestLane(t, carrier)
	t.Cleanup(func() {
		g2.RetireLogical()
		select {
		case <-g2.PhysicalDone():
		case <-time.After(time.Second):
			t.Error("replacement lane did not complete physical collection")
		}
	})

	select {
	case <-g1.Done():
	case <-time.After(time.Second):
		t.Fatal("old lane was not retired after replacement installation")
	}
	current.mu.Lock()
	replacement := current.lanes["a"]
	current.mu.Unlock()
	if replacement == nil {
		t.Fatal("old lane retirement deleted the replacement row")
	}
	if replacement.stream.Gen != g2.Gen || replacement.stream.Retired() {
		t.Fatalf("current row = (%q, retired=%v), replacement = %q",
			replacement.stream.Gen, replacement.stream.Retired(), g2.Gen)
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

// TestUnboundCoordinateLeavesTheSnapshotAndNothingElse pins that revocation is
// one value write on this host's side. It retires the route and drops the
// channel out of serve; it issues no teardown command and records nothing
// about what the device does with the news.
func TestUnboundCoordinateLeavesTheSnapshotAndNothingElse(t *testing.T) {
	var bound atomic.Bool
	bound.Store(true)
	host := New(Config{
		ScanInterval: time.Hour,
		Present: func(context.Context) ([]channel.ID, error) {
			return []channel.ID{"a"}, nil
		},
	})
	defer host.Close()
	host.Register("a", 1, platform.DaemonMembrane{
		IsBound: func(context.Context, string) (bool, error) { return bound.Load(), nil },
	})
	carrier := dialTestCarrier(t, host)
	host.Scan()
	_ = adoptAndSuperviseTestLane(t, carrier)
	waitFor(t, func() bool { return host.LaneAttached("daemon-a", "a") })
	if plan := pullTestPlan(t, carrier); !containsChannel(plan.Serve, "a") {
		t.Fatalf("bound coordinate missing from serve: %+v", plan)
	}

	bound.Store(false)
	host.Scan()
	waitFor(t, func() bool { return !host.LaneAttached("daemon-a", "a") })
	plan := pullTestPlan(t, carrier)
	if containsChannel(plan.Serve, "a") || containsChannel(plan.Unknown, "a") {
		t.Fatalf("unbound coordinate still named by the snapshot: %+v", plan)
	}
	if !host.DaemonOnline("daemon-a") {
		t.Fatal("unbound convergence disturbed the carrier")
	}
}

// TestUnjudgeableCoordinateIsNamedUnknownAndNeverOmitted is the safety half of
// the snapshot contract. A channel whose Home is not registered cannot be
// judged, and omitting it would read to the device as "this channel is gone"
// and destroy a compartment that must live.
func TestUnjudgeableCoordinateIsNamedUnknownAndNeverOmitted(t *testing.T) {
	host := New(Config{
		ScanInterval: time.Hour,
		Present: func(context.Context) ([]channel.ID, error) {
			return []channel.ID{"a", "b"}, nil
		},
	})
	defer host.Close()
	host.Register("a", 1, platform.DaemonMembrane{
		IsBound: func(context.Context, string) (bool, error) { return true, nil },
	})
	carrier := dialTestCarrier(t, host)
	plan := pullTestPlan(t, carrier)
	if !containsChannel(plan.Serve, "a") {
		t.Fatalf("judgeable bound channel missing from serve: %+v", plan)
	}
	if !containsChannel(plan.Unknown, "b") {
		t.Fatalf("unregistered membrane was omitted instead of named unknown: %+v", plan)
	}
}

// TestDirectoryFailureSuppressesTheWholeSnapshot holds the other half: a
// snapshot this host cannot enumerate completely is not sent at all, because a
// partial one would name fewer channels than exist and the device retires by
// absence.
func TestDirectoryFailureSuppressesTheWholeSnapshot(t *testing.T) {
	host := New(Config{
		ScanInterval: time.Hour,
		Present: func(context.Context) ([]channel.ID, error) {
			return nil, errors.New("directory unreachable")
		},
	})
	defer host.Close()
	carrier := dialTestCarrier(t, host)
	if err := carrier.SendSpine(link.SpineFrame{
		Kind: link.SpineCompartmentPlanPull, Nonce: "suppressed",
	}); err != nil {
		t.Fatal(err)
	}
	if err := carrier.SendSpine(link.SpineFrame{
		Kind: link.SpineProbe, Nonce: "barrier",
	}); err != nil {
		t.Fatal(err)
	}
	reply := readTestSpine(t, carrier)
	if reply.Kind != link.SpineProbeReply || reply.Nonce != "barrier" {
		t.Fatalf("a snapshot was sent despite an unreadable directory: %+v", reply)
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
		for {
			var frame link.SpineFrame
			err := first.ReadSpine(&frame)
			if err != nil || frame.Kind != link.SpineCompartmentPlanPoke {
				readDone <- err
				return
			}
		}
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

func TestCoordinateBookkeepingReclaimsIdleEntries(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	defer host.Close()
	host.Register("a", 1, platform.DaemonMembrane{
		IsBound: func(context.Context, string) (bool, error) { return true, nil },
	})
	carrier := dialTestCarrier(t, host)
	host.Scan()
	_ = adoptAndSuperviseTestLane(t, carrier)

	waitFor(t, func() bool {
		host.mu.RLock()
		current := host.daemons["daemon-a"].current
		host.mu.RUnlock()
		current.mu.Lock()
		defer current.mu.Unlock()
		return len(current.coordLocks) == 0 && len(current.coordTasks) == 0
	})
}

func TestCoordinateGateSerializesAndReclaims(t *testing.T) {
	carrier := &carrierRow{coordLocks: make(map[channel.ID]*coordGate)}
	unlockFirst := carrier.lockCoord("a")
	enteredSecond := make(chan struct{})
	releaseSecond := make(chan struct{})
	secondDone := make(chan struct{})
	go func() {
		unlockSecond := carrier.lockCoord("a")
		close(enteredSecond)
		<-releaseSecond
		unlockSecond()
		close(secondDone)
	}()
	waitFor(t, func() bool {
		carrier.mu.Lock()
		defer carrier.mu.Unlock()
		return carrier.coordLocks["a"].refs == 2
	})
	select {
	case <-enteredSecond:
		t.Fatal("second coordinate executor entered before the first released")
	default:
	}
	unlockFirst()
	select {
	case <-enteredSecond:
	case <-time.After(time.Second):
		t.Fatal("second coordinate executor did not enter after release")
	}
	close(releaseSecond)
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("second coordinate executor did not release")
	}
	carrier.mu.Lock()
	remaining := len(carrier.coordLocks)
	carrier.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("coordinate gate map retained %d idle entries", remaining)
	}
}

func TestOpenTransfer_TTLReclaimsAbandonedTokens(t *testing.T) {
	now := time.Unix(1_000, 0)
	host := New(Config{
		ScanInterval: time.Hour,
		Now:          func() time.Time { return now },
	})
	t.Cleanup(func() { _ = host.Close() })
	carrier := &carrierRow{
		lanes: map[channel.ID]*serverLane{"a": {}},
	}
	host.mu.Lock()
	host.daemons["daemon-a"] = &daemonRow{current: carrier}
	host.mu.Unlock()
	t.Cleanup(func() {
		host.mu.Lock()
		delete(host.daemons, "daemon-a")
		host.mu.Unlock()
	})

	abandoned, err := host.OpenTransfer(
		context.Background(), "daemon-a", "a", "coord-a", access.OpRead, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(transferTicketTTL)
	current, err := host.OpenTransfer(
		context.Background(), "daemon-a", "a", "coord-b", access.OpRead, "",
	)
	if err != nil {
		t.Fatal(err)
	}

	host.mu.RLock()
	_, abandonedPresent := host.transfers[abandoned]
	_, currentPresent := host.transfers[current]
	count := len(host.transfers)
	host.mu.RUnlock()
	if abandonedPresent {
		t.Fatal("abandoned transfer survived its TTL")
	}
	if !currentPresent || count != 1 {
		t.Fatalf("transfer table after mint-time GC: current=%v count=%d", currentPresent, count)
	}
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
