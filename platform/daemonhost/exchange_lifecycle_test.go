package daemonhost

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func newExchangeLifecycleCarrier(host *Host) *carrierRow {
	return &carrierRow{
		host: host, daemonID: "daemon-a", wire: &link.ServerCarrier{},
		lanes: make(map[channel.ID]*serverLane), retirements: make(map[channel.ID]uint64),
		coordLocks: make(map[channel.ID]*coordGate), coordTasks: make(map[channel.ID]*coordTask),
	}
}

func newExchangeLifecycleLane(t *testing.T, carrier *carrierRow, generation link.LaneGeneration) (*serverLane, net.Conn) {
	t.Helper()
	local, peer := net.Pipe()
	stream, err := link.AdoptLane(&link.ClientCarrier{}, link.DeviceStreamHeader{
		Kind: link.DeviceStreamLaneControl, Channel: "channel-a", LaneGen: generation,
	}, local)
	if err != nil {
		t.Fatal(err)
	}
	lane := newServerLane(carrier, stream, membraneRow{})
	stream.SetRetire(func(exact *link.LaneStream) {
		lane.markStreamRetired()
		carrier.mu.Lock()
		if carrier.lanes[exact.Channel] == lane {
			delete(carrier.lanes, exact.Channel)
		}
		carrier.mu.Unlock()
	})
	t.Cleanup(func() {
		stream.RetireLogical()
		stream.CollectPhysical()
		_ = peer.Close()
	})
	return lane, peer
}

type exchangeJoinProbe struct {
	peer    net.Conn
	closed  chan struct{}
	release chan struct{}
	joined  chan struct{}
}

func trackBlockingExchange(t *testing.T, lane *serverLane) *exchangeJoinProbe {
	t.Helper()
	local, peer := net.Pipe()
	cleanup, ok := lane.trackExchange(local)
	if !ok {
		t.Fatal("exchange was not tracked")
	}
	probe := &exchangeJoinProbe{
		peer: peer, closed: make(chan struct{}), release: make(chan struct{}), joined: make(chan struct{}),
	}
	go func() {
		_, _ = local.Read(make([]byte, 1))
		close(probe.closed)
		<-probe.release
		cleanup()
		close(probe.joined)
	}()
	t.Cleanup(func() { _ = peer.Close() })
	return probe
}

func assertExchangeClosedAndJoined(t *testing.T, probe *exchangeJoinProbe, retire func()) {
	t.Helper()
	retired := make(chan struct{})
	go func() {
		retire()
		close(retired)
	}()
	select {
	case <-probe.closed:
	case <-time.After(time.Second):
		t.Fatal("lane retirement did not close the exchange connection")
	}
	select {
	case <-retired:
		t.Fatal("lane retirement returned before the exchange handler joined")
	default:
	}
	close(probe.release)
	select {
	case <-probe.joined:
	case <-time.After(time.Second):
		t.Fatal("exchange handler did not join")
	}
	select {
	case <-retired:
	case <-time.After(time.Second):
		t.Fatal("lane retirement did not return after the exchange handler joined")
	}
	_ = probe.peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := probe.peer.Read(make([]byte, 1)); err == nil {
		t.Fatal("exchange peer remained open after lane retirement")
	}
}

func TestServerLaneRetirementClosesExchangeAndWaitsForHandler(t *testing.T) {
	carrier := newExchangeLifecycleCarrier(nil)
	lane, _ := newExchangeLifecycleLane(t, carrier, "generation-1")
	carrier.lanes["channel-a"] = lane
	assertExchangeClosedAndJoined(t, trackBlockingExchange(t, lane), lane.retireLogical)
}

func TestServerLaneGenerationReplacementRetiresAndJoinsOldExchange(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	t.Cleanup(func() { _ = host.Close(context.Background()) })
	unbound := platform.DaemonMembrane{
		IsBound: func(context.Context, string) (bool, error) { return false, nil },
	}
	host.Register("channel-a", 1, unbound)
	carrier := newExchangeLifecycleCarrier(host)
	oldLane, _ := newExchangeLifecycleLane(t, carrier, "generation-1")
	oldLane.membrane.generation = 1
	carrier.lanes["channel-a"] = oldLane
	host.mu.Lock()
	host.daemons["daemon-a"] = &daemonRow{current: carrier}
	host.mu.Unlock()

	assertExchangeClosedAndJoined(t, trackBlockingExchange(t, oldLane), func() {
		host.Register("channel-a", 2, unbound)
	})
	carrier.mu.Lock()
	current := carrier.lanes["channel-a"]
	carrier.mu.Unlock()
	host.mu.RLock()
	generation := host.membranes["channel-a"].generation
	host.mu.RUnlock()
	if current != nil || generation != 2 {
		t.Fatalf("generation replacement left lane=%p generation=%d", current, generation)
	}
}

func TestServerCarrierReplacementRetiresAndJoinsOldExchange(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	t.Cleanup(func() { _ = host.Close(context.Background()) })
	oldCarrier := newExchangeLifecycleCarrier(host)
	oldLane, _ := newExchangeLifecycleLane(t, oldCarrier, "generation-1")
	oldCarrier.lanes["channel-a"] = oldLane
	replacement := newExchangeLifecycleCarrier(host)
	replacement.gen = "carrier-2"
	host.mu.Lock()
	host.daemons["daemon-a"] = &daemonRow{current: replacement}
	host.mu.Unlock()

	assertExchangeClosedAndJoined(t, trackBlockingExchange(t, oldLane), func() {
		host.beginCarrierShutdown(oldCarrier)
	})
	host.mu.RLock()
	current := host.daemons["daemon-a"].current
	host.mu.RUnlock()
	if current != replacement || replacement.sealed.Load() {
		t.Fatal("retiring the old carrier disturbed its replacement")
	}
}
