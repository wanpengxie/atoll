package daemonhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

func TestNonCooperativePlatformCallbacksStayBoundedAndUnknown(t *testing.T) {
	previous := factTimeout
	factTimeout = 30 * time.Millisecond
	t.Cleanup(func() { factTimeout = previous })

	t.Run("present", func(t *testing.T) {
		release := make(chan struct{})
		t.Cleanup(func() { close(release) })
		host := New(Config{
			ScanInterval: time.Hour,
			Present: func(context.Context) ([]channel.ID, error) {
				<-release
				return []channel.ID{"channel-a"}, nil
			},
		})
		t.Cleanup(func() { _ = host.Close(context.Background()) })
		started := time.Now()
		ids, err := host.presentChannels()
		if err == nil || ids != nil || time.Since(started) > time.Second {
			t.Fatalf("present result=(%v,%v) elapsed=%v", ids, err, time.Since(started))
		}
	})

	t.Run("daemon fact", func(t *testing.T) {
		release := make(chan struct{})
		t.Cleanup(func() { close(release) })
		host := New(Config{
			ScanInterval: time.Hour,
			DaemonFact: func(context.Context, string) DaemonFact {
				<-release
				return DaemonDeleted
			},
		})
		t.Cleanup(func() { _ = host.Close(context.Background()) })
		started := time.Now()
		if fact := host.daemonFact("daemon-a"); fact != DaemonUnavailable ||
			time.Since(started) > time.Second {
			t.Fatalf("daemon fact=%v elapsed=%v", fact, time.Since(started))
		}
	})

	t.Run("binding", func(t *testing.T) {
		release := make(chan struct{})
		t.Cleanup(func() { close(release) })
		host := New(Config{ScanInterval: time.Hour})
		t.Cleanup(func() { _ = host.Close(context.Background()) })
		started := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), factTimeout)
		t.Cleanup(cancel)
		bound, err := host.isBound(ctx, "daemon-a", func(context.Context, string) (bool, error) {
			<-release
			return false, nil
		})
		if err == nil || bound || time.Since(started) > time.Second {
			t.Fatalf("binding result=(%v,%v) elapsed=%v", bound, err, time.Since(started))
		}
	})
}

func TestDiagnosticsAreBoundedFIFOAndRowsRespectOwnershipLifetime(t *testing.T) {
	now := time.Unix(2_000, 0)
	host := New(Config{
		ScanInterval: time.Hour,
		Now:          func() time.Time { return now },
	})
	t.Cleanup(func() { _ = host.Close(context.Background()) })
	for i := 0; i < diagnosticCapacity+3; i++ {
		host.recordDiagnostic("fifo", Diagnostic{
			Kind: string(rune('a' + i)), Time: now.Add(time.Duration(i)),
		})
	}
	diagnostics := host.Diagnostics("fifo")
	if len(diagnostics) != diagnosticCapacity {
		t.Fatalf("diagnostic length=%d", len(diagnostics))
	}
	if diagnostics[0].Kind != string(rune('a'+3)) ||
		diagnostics[len(diagnostics)-1].Kind != string(rune('a'+diagnosticCapacity+2)) {
		t.Fatalf("FIFO bounds first=%q last=%q", diagnostics[0].Kind, diagnostics[len(diagnostics)-1].Kind)
	}

	old := now.Add(-defaultDiagnosticTTL)
	host.mu.Lock()
	host.daemons["expired"] = &daemonRow{diagnostic: []Diagnostic{{Kind: "old", Time: old}}}
	host.daemons["tombstone"] = &daemonRow{
		tombstone: true, diagnostic: []Diagnostic{{Kind: "old", Time: old}},
	}
	sealedCurrent := &carrierRow{}
	sealedCurrent.sealed.Store(true)
	host.daemons["current"] = &daemonRow{
		current: sealedCurrent, diagnostic: []Diagnostic{{Kind: "old", Time: old}},
	}
	host.mu.Unlock()
	host.expireStaleDiagnostics()
	host.mu.RLock()
	_, expired := host.daemons["expired"]
	_, tombstone := host.daemons["tombstone"]
	_, current := host.daemons["current"]
	host.mu.RUnlock()
	if expired || !tombstone || !current {
		t.Fatalf("row lifetime expired=%v tombstone=%v current=%v", expired, tombstone, current)
	}
}

func TestUnexpectedSpineFrameClosesTheCarrier(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	t.Cleanup(func() { _ = host.Close(context.Background()) })
	carrier := dialTestCarrier(t, host)
	if err := carrier.SendSpine(link.SpineFrame{
		Kind: link.SpineCarrierAccept, DaemonID: "forged",
		CarrierGen: link.NewCarrierGeneration(),
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return !host.DaemonOnline("daemon-a") })
	readDone := make(chan error, 1)
	go func() {
		for {
			var frame link.SpineFrame
			err := carrier.ReadSpine(&frame)
			if err != nil || frame.Kind != link.SpineCompartmentPlanPoke {
				readDone <- err
				return
			}
		}
	}()
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("closed carrier produced another spine frame")
		}
	case <-time.After(time.Second):
		t.Fatal("unexpected spine frame left the physical carrier open")
	}
}

func TestHandshakeRejectClassification(t *testing.T) {
	if got := handshakeRejectClass(fmt.Errorf("wrapped: %w", link.ErrProtocolVersion)); got != link.CarrierTerminal {
		t.Fatalf("protocol mismatch class=%q", got)
	}
	if got := handshakeRejectClass(errors.New("malformed carrier header")); got != link.CarrierRetryable {
		t.Fatalf("ordinary handshake failure class=%q", got)
	}
}

func TestBeginCarrierShutdownIsIdempotentAtTheLedgerBoundary(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	t.Cleanup(func() { _ = host.Close(context.Background()) })
	carrier := &carrierRow{
		host: host, daemonID: "daemon-a", wire: &link.ServerCarrier{},
		lanes: make(map[channel.ID]*serverLane),
	}
	host.mu.Lock()
	host.daemons["daemon-a"] = &daemonRow{current: carrier}
	host.mu.Unlock()

	host.beginCarrierShutdown(carrier)
	host.beginCarrierShutdown(carrier)

	if !carrier.sealed.Load() {
		t.Fatal("shutdown did not seal the carrier")
	}
	if host.DaemonOnline("daemon-a") {
		t.Fatal("repeated shutdown left the carrier in the ledger")
	}
	carrier.mu.Lock()
	lanes := len(carrier.lanes)
	carrier.mu.Unlock()
	if lanes != 0 {
		t.Fatalf("repeated shutdown retained %d lanes", lanes)
	}
}

func TestScanPokesEveryCurrentCarrierExactlyOnce(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	t.Cleanup(func() { _ = host.Close(context.Background()) })
	dial := func(daemonID string) *link.ClientCarrier {
		t.Helper()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			host.Serve(w, r, daemonID)
		}))
		t.Cleanup(server.Close)
		carrier, _, err := link.DialDeviceCarrier(
			t.Context(), "ws"+strings.TrimPrefix(server.URL, "http"), "test", nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = carrier.Close() })
		var accepted link.SpineFrame
		if err := carrier.ReadSpine(&accepted); err != nil {
			t.Fatal(err)
		}
		if accepted.Kind != link.SpineCarrierAccept {
			t.Fatalf("%s verdict=%+v", daemonID, accepted)
		}
		return carrier
	}
	first := dial("daemon-a")
	second := dial("daemon-b")

	barrier := func(carrier *link.ClientCarrier, nonce string, allowPokes bool) {
		t.Helper()
		if err := carrier.SendSpine(link.SpineFrame{Kind: link.SpineProbe, Nonce: nonce}); err != nil {
			t.Fatal(err)
		}
		for {
			var frame link.SpineFrame
			if err := carrier.ReadSpine(&frame); err != nil {
				t.Fatal(err)
			}
			if frame.Kind == link.SpineProbeReply && frame.Nonce == nonce {
				return
			}
			if frame.Kind == link.SpineCompartmentPlanPoke && allowPokes {
				continue
			}
			t.Fatalf("barrier %q saw unexpected frame %+v", nonce, frame)
		}
	}
	barrier(first, "before-a", true)
	barrier(second, "before-b", true)

	host.Scan()
	for name, carrier := range map[string]*link.ClientCarrier{
		"daemon-a": first,
		"daemon-b": second,
	} {
		var frame link.SpineFrame
		if err := carrier.ReadSpine(&frame); err != nil {
			t.Fatal(err)
		}
		if frame.Kind != link.SpineCompartmentPlanPoke {
			t.Fatalf("%s scan frame=%+v", name, frame)
		}
		barrier(carrier, "after-"+name, false)
	}
}

func TestActorAdmissionIsOwnedByTheLaneHeaderMembrane(t *testing.T) {
	host := New(Config{
		ScanInterval: time.Hour,
		Present: func(context.Context) ([]channel.ID, error) {
			return []channel.ID{"a", "b"}, nil
		},
	})
	t.Cleanup(func() { _ = host.Close(context.Background()) })
	var authA, authB atomic.Int32
	attachedA := make(chan actor.ActorID, 1)
	host.Register("a", 1, platform.DaemonMembrane{ChannelName: "c0.test",
		IsBound: func(context.Context, string) (bool, error) { return true, nil },
		AuthorizeAttach: func(id actor.ActorID, _ actorhost.AttemptKey, domain actorhost.ExecutionDomain) error {
			authA.Add(1)
			if domain != "daemon-a" {
				t.Errorf("A authorization domain=%q", domain)
			}
			if id == "only-b" {
				return errors.New("actor does not belong to A")
			}
			return nil
		},
		AttachBinding: func(id actor.ActorID, _ actorhost.AttemptKey, _ actorhost.ExecutionDomain, _ actorhost.Binding) error {
			attachedA <- id
			return nil
		},
	})
	host.Register("b", 1, platform.DaemonMembrane{ChannelName: "c0.test",
		IsBound: func(context.Context, string) (bool, error) { return true, nil },
		AuthorizeAttach: func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain) error {
			authB.Add(1)
			return nil
		},
		AttachBinding: func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain, actorhost.Binding) error {
			return nil
		},
	})
	carrier := dialTestCarrier(t, host)
	host.Scan()
	lanes := make(map[channel.ID]*link.LaneStream)
	for len(lanes) < 2 {
		conn, header, err := carrier.AcceptStream()
		if err != nil {
			t.Fatal(err)
		}
		lane, err := link.AdoptLane(carrier, header, conn)
		if err != nil {
			t.Fatal(err)
		}
		lanes[lane.Channel] = lane
		t.Cleanup(func() {
			lane.RetireLogical()
			lane.CollectPhysical()
		})
	}
	key, err := actorhost.NewAttemptKey()
	if err != nil {
		t.Fatal(err)
	}
	writeHandshake := func(conn net.Conn, id actor.ActorID) {
		t.Helper()
		raw, err := json.Marshal(ipc.HandshakePayload{
			LeaseID: string(id), AttemptKey: string(key),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := ipc.NewCodec(conn, conn).Write(ipc.Frame{
			Kind: ipc.KindHandshake, Payload: raw,
		}); err != nil {
			t.Fatal(err)
		}
	}

	forged, err := carrier.OpenActor(t.Context(), "a", lanes["a"].Gen)
	if err != nil {
		t.Fatal(err)
	}
	writeHandshake(forged, "only-b")
	rejected := make(chan error, 1)
	go func() {
		_, err := ipc.NewCodec(forged, forged).Read()
		rejected <- err
	}()
	select {
	case err := <-rejected:
		if err == nil {
			t.Fatal("A accepted an actor only B authorizes")
		}
	case <-time.After(time.Second):
		t.Fatal("forged actor stream was not rejected")
	}
	if authA.Load() != 1 || authB.Load() != 0 {
		t.Fatalf("authorization routed A=%d B=%d", authA.Load(), authB.Load())
	}

	valid, err := carrier.OpenActor(t.Context(), "a", lanes["a"].Gen)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = valid.Close() })
	writeHandshake(valid, "actor-a")
	select {
	case id := <-attachedA:
		if id != "actor-a" {
			t.Fatalf("attached actor=%q", id)
		}
	case <-time.After(time.Second):
		t.Fatal("valid A actor was not attached")
	}
	if authB.Load() != 0 {
		t.Fatalf("B membrane observed A header admission %d times", authB.Load())
	}
}

func TestCarrierKindChildClosesTheWholeCarrier(t *testing.T) {
	host := New(Config{ScanInterval: time.Hour})
	t.Cleanup(func() { _ = host.Close(context.Background()) })
	client := dialTestCarrier(t, host)
	host.mu.RLock()
	serverCarrier := host.daemons["daemon-a"].current
	host.mu.RUnlock()
	if serverCarrier == nil {
		t.Fatal("carrier was not admitted")
	}
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = remote.Close() })
	host.acceptStream(serverCarrier, local, link.DeviceStreamHeader{
		Kind: link.DeviceStreamCarrier, ProtoVersion: link.ProtocolVersion,
	})
	_ = remote.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := remote.Read(make([]byte, 1)); err == nil {
		t.Fatal("carrier-kind child stream stayed open")
	}
	waitFor(t, func() bool { return !host.DaemonOnline("daemon-a") })
	readDone := make(chan error, 1)
	go func() {
		for {
			var frame link.SpineFrame
			err := client.ReadSpine(&frame)
			if err != nil || frame.Kind != link.SpineCompartmentPlanPoke {
				readDone <- err
				return
			}
		}
	}()
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("closed carrier produced another decision frame")
		}
	case <-time.After(time.Second):
		t.Fatal("carrier-kind child did not close the physical carrier")
	}
}

func TestCarrierAcceptWriteFailureWithdrawsLedgerAndJoinsSupervisor(t *testing.T) {
	previous := sendCarrierAccept
	attempted := make(chan struct{})
	sendCarrierAccept = func(*link.ServerCarrier, link.SpineFrame) error {
		close(attempted)
		return errors.New("injected accept write failure")
	}
	t.Cleanup(func() { sendCarrierAccept = previous })

	host := New(Config{ScanInterval: time.Hour})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host.Serve(w, r, "daemon-a")
	}))
	t.Cleanup(server.Close)
	client, _, err := link.DialDeviceCarrier(
		t.Context(), "ws"+strings.TrimPrefix(server.URL, "http"), "test", nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	select {
	case <-attempted:
	case <-time.After(time.Second):
		t.Fatal("carrier accept write was never attempted")
	}
	waitFor(t, func() bool { return !host.DaemonOnline("daemon-a") })

	closed := make(chan struct{})
	go func() {
		_ = host.Close(context.Background())
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("host could not join supervisor after accept write failure")
	}
	host.mu.RLock()
	row := host.daemons["daemon-a"]
	host.mu.RUnlock()
	if row != nil && row.current != nil {
		t.Fatal("accept write failure retained a current carrier")
	}
}
