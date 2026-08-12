package daemonhost

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	t.Cleanup(func() { _ = host.Close(context.Background()) })
	var bound atomic.Bool
	bound.Store(true)
	host.Register("channel-a", 1, platform.DaemonMembrane{ChannelName: "c0.test",
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
	t.Cleanup(func() { _ = host.Close(context.Background()) })
	var bound atomic.Bool
	bound.Store(true)
	host.Register("channel-a", 1, platform.DaemonMembrane{ChannelName: "c0.test",
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
	defer host.Close(context.Background())
	bundle := platform.DaemonMembrane{ChannelName: "c0.test",
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
	defer host.Close(context.Background())
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
	defer host.Close(context.Background())
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
	defer host.Close(context.Background())
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
	defer host.Close(context.Background())
	bundle := platform.DaemonMembrane{ChannelName: "c0.test",
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

// TestSnapshotAnswerSpendsOneBudgetAcrossAllChannels pins how budgets nest on
// the snapshot path: the device waits a fixed time for one reply, so the
// server's spend must stay below that no matter how many channels are present.
// Every binding query here blocks forever; the reply must still arrive within
// one pooled budget — naming each stuck channel Unknown — not after
// per-channel budgets stacked serially past any client wait.
func TestSnapshotAnswerSpendsOneBudgetAcrossAllChannels(t *testing.T) {
	previous := factTimeout
	factTimeout = 100 * time.Millisecond
	t.Cleanup(func() { factTimeout = previous })
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	const channels = 8
	present := make([]channel.ID, 0, channels)
	for i := 0; i < channels; i++ {
		present = append(present, channel.ID(fmt.Sprintf("ch-%02d", i)))
	}
	host := New(Config{
		ScanInterval: time.Hour,
		Present:      func(context.Context) ([]channel.ID, error) { return present, nil },
	})
	defer host.Close(context.Background())
	for _, chID := range present {
		host.Register(chID, 1, platform.DaemonMembrane{ChannelName: "c0.test",
			Plan: func(context.Context, string) ([]platform.PlanActor, error) { return nil, nil },
			IsBound: func(context.Context, string) (bool, error) {
				<-release
				return false, nil
			},
		})
	}
	carrier := dialTestCarrier(t, host)
	started := time.Now()
	plan := pullTestPlan(t, carrier)
	elapsed := time.Since(started)
	// Serial per-channel budgets would spend channels × factTimeout; half that
	// is unambiguous evidence of pooling.
	if elapsed > time.Duration(channels)*factTimeout/2 {
		t.Fatalf("snapshot answer took %v across %d stuck channels", elapsed, channels)
	}
	if len(plan.Unknown) != channels || len(plan.Serve) != 0 {
		t.Fatalf("stuck queries answered serve=%v unknown=%v", plan.Serve, plan.Unknown)
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
	defer host.Close(context.Background())
	bundle := platform.DaemonMembrane{ChannelName: "c0.test",
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
	// A concurrent level reconcile may already have superseded g2 with an even
	// newer live lane. The invariant is that g1's delayed retirement cannot
	// delete or retire whichever replacement is current.
	if replacement.stream.Gen == g1.Gen || replacement.stream.Retired() {
		t.Fatalf("current row = (%q, retired=%v), retired old = %q, observed replacement = %q",
			replacement.stream.Gen, replacement.stream.Retired(), g1.Gen, g2.Gen)
	}
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
	defer host.Close(context.Background())
	host.Register("a", 1, platform.DaemonMembrane{ChannelName: "c0.test",
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
	defer host.Close(context.Background())
	host.Register("a", 1, platform.DaemonMembrane{ChannelName: "c0.test",
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
	defer host.Close(context.Background())
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
	defer host.Close(context.Background())
	first := dialTestCarrier(t, host)
	if !host.DaemonOnline("daemon-a") {
		t.Fatal("first carrier was not admitted")
	}
	now = now.Add(2 * time.Second)
	clock.Store(now.UnixNano())
	// A lease reaps only over an unanswered question: the first probe asks,
	// and the half-open carrier's silence on it is what the second one
	// condemns.
	probeCarrierNow(t, host, "daemon-a")
	probeCarrierNow(t, host, "daemon-a")
	waitFor(t, func() bool { return !host.DaemonOnline("daemon-a") })
	readDone := make(chan error, 1)
	go func() {
		for {
			var frame link.SpineFrame
			err := first.ReadSpine(&frame)
			// Pokes and probes are contentless: a poke is a wake, a probe is
			// the liveness question the reap was preceded by. Neither is a
			// decision frame, and this reader is waiting for the wire to die.
			if err != nil || (frame.Kind != link.SpineCompartmentPlanPoke &&
				frame.Kind != link.SpineProbe) {
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
	defer host.Close(context.Background())
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
	defer host.Close(context.Background())
	host.Register("a", 1, platform.DaemonMembrane{ChannelName: "c0.test",
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

func TestCoordinateExecutorsDoNotLetBlockedABarB(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	t.Cleanup(func() { _ = host.Close(context.Background()) })
	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	host.Register("a", 1, platform.DaemonMembrane{ChannelName: "c0.test",
		IsBound: func(context.Context, string) (bool, error) {
			enterOnce.Do(func() { close(entered) })
			<-release
			return true, nil
		},
	})
	host.Register("b", 1, platform.DaemonMembrane{ChannelName: "c0.test",
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

type testClock struct{ nanos atomic.Int64 }

func newTestClock(at time.Time) *testClock {
	clock := &testClock{}
	clock.nanos.Store(at.UnixNano())
	return clock
}

func (c *testClock) now() time.Time          { return time.Unix(0, c.nanos.Load()).UTC() }
func (c *testClock) advance(d time.Duration) { c.nanos.Add(int64(d)) }

// spineBarrier proves this host has read everything the device sent before it.
// A device-initiated probe is answered inline by the spine reader, so the
// matching reply can only come back after the frames queued ahead of it were
// handled.
func spineBarrier(t *testing.T, carrier *link.ClientCarrier) {
	t.Helper()
	nonce := "barrier-" + uuid.NewString()
	if err := carrier.SendSpine(link.SpineFrame{Kind: link.SpineProbe, Nonce: nonce}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 64; i++ {
		reply := readTestSpine(t, carrier)
		if reply.Kind == link.SpineProbeReply && reply.Nonce == nonce {
			return
		}
	}
	t.Fatal("the spine barrier never came back")
}

// probeCarrierNow runs one liveness cycle for one daemon's current carrier,
// which is what the host's per-carrier supervisor does on its own ticker.
func probeCarrierNow(t *testing.T, host *Host, daemonID string) {
	t.Helper()
	host.mu.RLock()
	row := host.daemons[daemonID]
	var carrier *carrierRow
	if row != nil {
		carrier = row.current
	}
	host.mu.RUnlock()
	if carrier == nil {
		t.Fatalf("no current carrier for %q to probe", daemonID)
	}
	host.probeOnce(carrier)
}

func carrierLastSeen(t *testing.T, host *Host, daemonID string) time.Time {
	t.Helper()
	host.mu.RLock()
	defer host.mu.RUnlock()
	row := host.daemons[daemonID]
	if row == nil || row.current == nil {
		t.Fatal("no current carrier to read the lease from")
	}
	return row.current.lastSeen
}

// TestExpiredLeaseIsAskedBeforeItIsReaped pins the reap's evidence rule: a
// lease is only ever reaped over an unanswered probe, never over silence this
// host did not question. Without it, any LeaseTTL below the probe cadence
// reaps every healthy carrier at its first tick — the lease expires before
// the first question is even sent — and the space flaps forever on one
// misconfigured knob.
func TestExpiredLeaseIsAskedBeforeItIsReaped(t *testing.T) {
	clock := newTestClock(time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC))
	host := New(Config{ScanInterval: time.Hour, LeaseTTL: time.Second, Now: clock.now})
	defer host.Close(context.Background())
	carrier := dialTestCarrier(t, host)
	_ = carrier
	clock.advance(2 * time.Second)

	// Expired, but never probed: this tick must ask, not reap.
	probeCarrierNow(t, host, "daemon-a")
	if !host.DaemonOnline("daemon-a") {
		t.Fatal("an expired lease was reaped before its carrier was ever probed")
	}

	// The question went unanswered across another expired tick: now the reap
	// has its evidence.
	probeCarrierNow(t, host, "daemon-a")
	waitFor(t, func() bool { return !host.DaemonOnline("daemon-a") })
}

// TestDeviceTrafficDoesNotRenewTheLease pins what the lease attests: a round
// trip. Everything the device sends proves only that its own send path works.
// A carrier whose downstream direction is dead would otherwise stay recorded as
// current forever — it keeps talking, this host keeps believing it, and every
// frame this host sends is lost while no replacement can take the daemon.
func TestDeviceTrafficDoesNotRenewTheLease(t *testing.T) {
	clock := newTestClock(time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC))
	host := New(Config{ScanInterval: time.Hour, LeaseTTL: time.Second, Now: clock.now})
	defer host.Close(context.Background())
	carrier := dialTestCarrier(t, host)
	if !host.DaemonOnline("daemon-a") {
		t.Fatal("carrier was not admitted")
	}

	// Well-formed traffic, acted on by this host, arriving after the lease
	// would have run out. It is not evidence of a round trip: the first probe
	// asks, the traffic between the probes renews nothing, and the second
	// probe reaps over the unanswered first.
	clock.advance(2 * time.Second)
	probeCarrierNow(t, host, "daemon-a")
	spineBarrier(t, carrier)

	probeCarrierNow(t, host, "daemon-a")
	waitFor(t, func() bool { return !host.DaemonOnline("daemon-a") })
}

// TestBlockedSnapshotAnswerDoesNotStallTheLease pins that building a device's
// channel snapshot cannot stop that device's lease from being renewed.
//
// The snapshot costs one bounded lookup per channel, and those lookups hit a
// store that can be slow exactly when the space is under stress. Building it on
// the spine reader means the probe replies queued behind it are not read until
// it finishes — so the lease the reply was going to renew expires, and a
// perfectly healthy device is reaped because this host was busy answering it.
func TestBlockedSnapshotAnswerDoesNotStallTheLease(t *testing.T) {
	clock := newTestClock(time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC))
	release := make(chan struct{})
	entered := make(chan struct{})
	var once, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	host := New(Config{
		ScanInterval: time.Hour, LeaseTTL: time.Second, Now: clock.now,
		Present: func(context.Context) ([]channel.ID, error) {
			return []channel.ID{"channel-a"}, nil
		},
	})
	defer host.Close(context.Background())
	host.Register("channel-a", 1, platform.DaemonMembrane{ChannelName: "c0.test",
		Plan: func(context.Context, string) ([]platform.PlanActor, error) { return nil, nil },
		IsBound: func(context.Context, string) (bool, error) {
			once.Do(func() { close(entered) })
			<-release
			return true, nil
		},
	})
	carrier := dialTestCarrier(t, host)
	admitted := carrierLastSeen(t, host, "daemon-a")

	if err := carrier.SendSpine(link.SpineFrame{
		Kind: link.SpineCompartmentPlanPull, Nonce: "pull-1",
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the host never began building the snapshot")
	}

	// The snapshot answer is stuck. A probe round trip must still complete.
	clock.advance(500 * time.Millisecond)
	probeCarrierNow(t, host, "daemon-a")
	probe := readTestSpine(t, carrier)
	if probe.Kind != link.SpineProbe {
		t.Fatalf("host sent %+v while the snapshot was blocked, want a probe", probe)
	}
	if err := carrier.SendSpine(link.SpineFrame{
		Kind: link.SpineProbeReply, Nonce: probe.Nonce,
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return carrierLastSeen(t, host, "daemon-a").After(admitted) })
	unblock()
}

// TestALateProbeReplyStillRenewsTheLease pins that a reply is judged by whether
// it answers a probe this host sent, never by whether it is the newest one.
//
// A round trip slower than the probe interval is slow, not false — it really
// did complete. Honouring only the newest probe makes every reply from such a
// device arrive against a nonce that has already been replaced, so its lease is
// never renewed and it is reaped while answering every single probe. Reaping
// then reconnects it into exactly the same conditions, so it never recovers.
func TestALateProbeReplyStillRenewsTheLease(t *testing.T) {
	clock := newTestClock(time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC))
	host := New(Config{ScanInterval: time.Hour, LeaseTTL: 30 * time.Second, Now: clock.now})
	defer host.Close(context.Background())
	carrier := dialTestCarrier(t, host)
	admitted := carrierLastSeen(t, host, "daemon-a")

	// Two probes go out; the device is still working on the first.
	probeCarrierNow(t, host, "daemon-a")
	first := readTestSpine(t, carrier)
	clock.advance(10 * time.Second)
	probeCarrierNow(t, host, "daemon-a")
	second := readTestSpine(t, carrier)
	if first.Kind != link.SpineProbe || second.Kind != link.SpineProbe ||
		first.Nonce == second.Nonce || first.Nonce == "" {
		t.Fatalf("host sent %+v then %+v, want two distinct probes", first, second)
	}

	// The answer to the FIRST one finally arrives.
	if err := carrier.SendSpine(link.SpineFrame{
		Kind: link.SpineProbeReply, Nonce: first.Nonce,
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return carrierLastSeen(t, host, "daemon-a").After(admitted) })
}

// TestOnlyTheMatchingProbeReplyRenewsTheLease is the other half: the reply that
// answers the probe this host actually sent does renew, and one that answers
// nothing does not.
func TestOnlyTheMatchingProbeReplyRenewsTheLease(t *testing.T) {
	clock := newTestClock(time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC))
	host := New(Config{ScanInterval: time.Hour, LeaseTTL: time.Second, Now: clock.now})
	defer host.Close(context.Background())
	carrier := dialTestCarrier(t, host)
	admitted := carrierLastSeen(t, host, "daemon-a")

	clock.advance(500 * time.Millisecond)
	probeCarrierNow(t, host, "daemon-a")
	probe := readTestSpine(t, carrier)
	if probe.Kind != link.SpineProbe || probe.Nonce == "" {
		t.Fatalf("host sent %+v, want a probe carrying a nonce", probe)
	}

	if err := carrier.SendSpine(link.SpineFrame{
		Kind: link.SpineProbeReply, Nonce: "answers-nothing",
	}); err != nil {
		t.Fatal(err)
	}
	spineBarrier(t, carrier)
	if got := carrierLastSeen(t, host, "daemon-a"); !got.Equal(admitted) {
		t.Fatal("a probe reply that answered no outstanding probe renewed the lease")
	}

	if err := carrier.SendSpine(link.SpineFrame{
		Kind: link.SpineProbeReply, Nonce: probe.Nonce,
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return carrierLastSeen(t, host, "daemon-a").After(admitted) })

	// 1.2s since admission, 0.7s since the answered probe: the renewal is what
	// keeps this carrier alive.
	clock.advance(700 * time.Millisecond)
	probeCarrierNow(t, host, "daemon-a")
	if !host.DaemonOnline("daemon-a") {
		t.Fatal("a carrier that answered this host's probe was reaped anyway")
	}
}

// TestClosedHostRefusesCarriersBeforeTheHandshake pins where the refusal
// happens. Upgrading first and rejecting afterwards spends a websocket on a
// host that will never serve it, and leaves the caller holding a carrier-level
// reject in place of an HTTP answer it can attribute.
func TestClosedHostRefusesCarriersBeforeTheHandshake(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host.Serve(w, r, "daemon-a")
	}))
	defer server.Close()
	if err := host.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	response, err := http.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("closed host answered %d, want 503", response.StatusCode)
	}
	if !strings.Contains(string(body), "daemon host closed") {
		t.Fatalf("closed host answered %q with no attribution", strings.TrimSpace(string(body)))
	}

	carrier, _, err := link.DialDeviceCarrier(
		t.Context(), "ws"+strings.TrimPrefix(server.URL, "http"), "test", nil)
	if err == nil {
		_ = carrier.Close()
		t.Fatal("a closed host upgraded a carrier")
	}
}

// wedgedLaneHost hands back a host with one lane whose reader is parked inside
// the membrane's Plan. This is the state every decision below has to survive:
// the reader is not blocked on a read, so retiring the lane cannot wake it, and
// the physical end of that lane will not be collected until the gate opens.
type wedgedLaneHost struct {
	host        *Host
	carrier     *link.ClientCarrier
	release     chan struct{}
	releaseOnce sync.Once
}

// unblock lets the parked membrane call return. Cleanup always calls it, so a
// test that never does cannot leave a goroutine parked in the test process.
func (w *wedgedLaneHost) unblock() {
	w.releaseOnce.Do(func() { close(w.release) })
}

func newWedgedLaneHost(t *testing.T, cfg Config) *wedgedLaneHost {
	t.Helper()
	entered := make(chan struct{})
	var once sync.Once
	cfg.Present = func(context.Context) ([]channel.ID, error) {
		return []channel.ID{"channel-a"}, nil
	}
	host := New(cfg)
	wedged := &wedgedLaneHost{host: host, release: make(chan struct{})}
	release := wedged.release
	t.Cleanup(func() {
		wedged.unblock()
		_ = host.Close(context.Background())
	})
	host.Register("channel-a", 1, platform.DaemonMembrane{ChannelName: "c0.test",
		Plan: func(context.Context, string) ([]platform.PlanActor, error) {
			once.Do(func() { close(entered) })
			<-release
			return nil, nil
		},
		IsBound: func(context.Context, string) (bool, error) { return true, nil },
	})
	wedged.carrier = dialTestCarrier(t, host)
	host.Scan()
	lane := adoptAndSuperviseTestLane(t, wedged.carrier)
	if err := lane.Send(link.LaneFrame{
		Kind: link.LanePlanPull, RequestID: "wedge",
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the lane reader never entered the membrane call")
	}
	return wedged
}

// within runs an operation and fails if it has not returned by the deadline. An
// explicit bound is the point: relying on the package timeout would report a
// hang without naming which decision hung.
func within(t *testing.T, what string, budget time.Duration, operation func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		operation()
	}()
	select {
	case <-done:
	case <-time.After(budget):
		t.Fatalf("%s did not return within %s while a lane reader was wedged", what, budget)
	}
}

// TestRevocationDoesNotWaitOnAWedgedLane pins the boundary between the value
// decision and the physical collection. Revoking a daemon is a ledger write;
// making it wait for a lane's reader to notice puts a device's stuck handler in
// front of an HTTP request that has nothing to do with it.
func TestRevocationDoesNotWaitOnAWedgedLane(t *testing.T) {
	wedged := newWedgedLaneHost(t, Config{ScanInterval: time.Hour})

	within(t, "RevokeDaemon", 2*time.Second, func() {
		wedged.host.RevokeDaemon("daemon-a")
	})

	if wedged.host.DaemonOnline("daemon-a") {
		t.Fatal("revocation returned without withdrawing the carrier")
	}
	if wedged.host.LaneAttached("daemon-a", "channel-a") {
		t.Fatal("revocation returned while its lane was still routable")
	}
	var revoked bool
	for _, diagnostic := range wedged.host.Diagnostics("daemon-a") {
		if diagnostic.Kind == "revoke" {
			revoked = true
		}
	}
	if !revoked {
		t.Fatal("revocation returned without recording itself")
	}
}

// TestScanAndLeaseSweepDoNotWaitOnAWedgedLane covers the two periodic
// decisions. Neither may block behind a lane reader stuck inside a membrane
// call: a scan that waits stops every daemon in the space from having lanes
// opened, reopened or reaped, and a liveness cycle that waits lets the lease it
// is supposed to renew run out.
func TestScanAndLeaseSweepDoNotWaitOnAWedgedLane(t *testing.T) {
	clock := newTestClock(time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC))
	var deleted atomic.Bool
	wedged := newWedgedLaneHost(t, Config{
		ScanInterval: time.Hour, LeaseTTL: time.Second, Now: clock.now,
		DaemonFact: func(context.Context, string) DaemonFact {
			if deleted.Load() {
				return DaemonDeleted
			}
			return DaemonAlive
		},
	})

	within(t, "probe", 2*time.Second, func() {
		probeCarrierNow(t, wedged.host, "daemon-a")
	})

	// The scan now finds this daemon deleted, which routes it into revocation.
	deleted.Store(true)
	within(t, "Scan", 2*time.Second, func() { wedged.host.Scan() })
}

// TestCarrierSupervisorJoinsItsWedgedLane is the other half: nobody on a
// decision path waits, but the lane's reader still has an owner. The carrier's
// supervisor is it, and Close joins the supervisors.
// TestWedgedLaneCloseIsBoundedAndAccounted replaces the retired
// TestCarrierSupervisorJoinsItsWedgedLane, whose doctrine — "Close must not
// return while a reader is parked" — assumed waiting always ends. A reader
// parked in a callback that ignores cancellation never returns; only process
// death reclaims it, and the data never depended on the join (that is the
// stores' crash safety). The threat the old wall held — a shutdown that fakes
// cleanliness — is held by accounting now: Close returns within the caller's
// budget, and an expired budget must carry the exact count of what was
// abandoned, never nil.
func TestWedgedLaneCloseIsBoundedAndAccounted(t *testing.T) {
	wedged := newWedgedLaneHost(t, Config{ScanInterval: time.Hour})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	var err error
	within(t, "bounded close", 5*time.Second, func() { err = wedged.host.Close(ctx) })
	if !errors.Is(err, ErrCloseAbandoned) {
		t.Fatalf("close error=%v, want the abandonment account", err)
	}
	if !strings.Contains(err.Error(), "1 carrier supervisors") ||
		!strings.Contains(err.Error(), "1 lane readers") {
		t.Fatalf("account=%q, want 1 supervisor and 1 reader named", err.Error())
	}

	// The other half of honesty: once the wedge releases, a re-close with an
	// open budget must drain fully and report clean.
	wedged.unblock()
	if err := wedged.host.Close(context.Background()); err != nil {
		t.Fatalf("close after release: %v", err)
	}
}

// TestCleanCloseReportsCleanWithinBudget is the inverse half: with nothing
// wedged, a bounded close must return nil inside its budget — an
// unconditional accounting error would pass the wall above too.
func TestCleanCloseReportsCleanWithinBudget(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	dialTestCarrier(t, host)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var err error
	within(t, "clean close", 6*time.Second, func() { err = host.Close(ctx) })
	if err != nil {
		t.Fatalf("clean teardown reported abandonment: %v", err)
	}
}

// TestFailedLaneOpenReturnsItsPhysicalTicket pins the accounting of the ticket
// that lets a carrier's supervisor join its lanes. The ticket is taken before
// the open, so it covers the whole span; an open that never produces a lane
// must hand it straight back. A ticket returned zero times is not a leak that
// shows up as a leak — it is a supervisor that never finishes, and with it a
// Close that never returns.
func TestFailedLaneOpenReturnsItsPhysicalTicket(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	t.Cleanup(func() { _ = host.Close(context.Background()) })
	// A carrier with no transport behind it: every open fails immediately,
	// which is the branch under test.
	carrier := &carrierRow{
		host: host, daemonID: "daemon-a", gen: "g1", wire: &link.ServerCarrier{},
		lanes:       make(map[channel.ID]*serverLane),
		retirements: make(map[channel.ID]uint64),
		coordLocks:  make(map[channel.ID]*coordGate),
		coordTasks:  make(map[channel.ID]*coordTask),
	}
	membrane := membraneRow{generation: 1, bundle: platform.DaemonMembrane{ChannelName: "c0.test",
		IsBound: func(context.Context, string) (bool, error) { return true, nil },
	}}
	for i := 0; i < 4; i++ {
		carrier.ensureLane(channel.ID(fmt.Sprintf("channel-%d", i)), membrane)
	}

	joined := make(chan struct{})
	go func() {
		defer close(joined)
		carrier.physical.Wait()
	}()
	select {
	case <-joined:
	case <-time.After(2 * time.Second):
		t.Fatal("a lane open that produced nothing kept its ticket, so this carrier can never be joined")
	}
}
