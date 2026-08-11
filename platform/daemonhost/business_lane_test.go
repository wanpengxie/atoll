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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

type testStorageAuthority struct {
	mu sync.Mutex

	committedDaemon, reservation  string
	committedFound, committedLost bool
	committedErr                  error

	reclaimDaemon, tombstone string
	reclaimFound             bool
	reclaimErr               error

	reconcileDaemon string
	activeCoords    []string
	resources       []platform.StorageResourceCoord
	reservations    []platform.StorageReservationCoord
	tombstones      []platform.StorageTombstoneCoord
	reconcileErr    error
}

func (s *testStorageAuthority) Committed(
	_ context.Context, daemonID, reservationID string,
) (bool, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.committedDaemon, s.reservation = daemonID, reservationID
	return s.committedFound, s.committedLost, s.committedErr
}

func (s *testStorageAuthority) ReclaimAck(
	_ context.Context, daemonID, tombstoneID string,
) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reclaimDaemon, s.tombstone = daemonID, tombstoneID
	return s.reclaimFound, s.reclaimErr
}

func (s *testStorageAuthority) ReconcilePull(
	_ context.Context, daemonID string, active []string,
) (
	[]platform.StorageResourceCoord,
	[]platform.StorageReservationCoord,
	[]platform.StorageTombstoneCoord,
	error,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reconcileDaemon = daemonID
	s.activeCoords = append([]string(nil), active...)
	return s.resources, s.reservations, s.tombstones, s.reconcileErr
}

func openBusinessTestLane(
	t *testing.T,
	membrane platform.DaemonMembrane,
) (*Host, *link.ClientCarrier, *link.LaneStream) {
	t.Helper()
	host := New(Config{
		ScanInterval: time.Hour,
		Present: func(context.Context) ([]channel.ID, error) {
			return []channel.ID{"channel-a"}, nil
		},
	})
	t.Cleanup(func() { _ = host.Close(context.Background()) })
	if membrane.IsBound == nil {
		membrane.IsBound = func(context.Context, string) (bool, error) { return true, nil }
	}
	host.Register("channel-a", 1, membrane)
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
	t.Cleanup(func() {
		lane.RetireLogical()
		lane.CollectPhysical()
	})
	waitFor(t, func() bool { return host.LaneAttached("daemon-a", "channel-a") })
	return host, carrier, lane
}

func laneRoundTrip(t *testing.T, lane *link.LaneStream, request link.LaneFrame) link.LaneFrame {
	t.Helper()
	if err := lane.Send(request); err != nil {
		t.Fatal(err)
	}
	var reply link.LaneFrame
	if err := lane.Decode(&reply); err != nil {
		t.Fatal(err)
	}
	return reply
}

func TestLaneBusinessRequestsUseAuthenticatedCarrierAndReturnExactResults(t *testing.T) {
	storage := &testStorageAuthority{
		committedFound: true,
		reclaimFound:   true,
		resources:      []platform.StorageResourceCoord{{Coord: "resource-a"}},
		reservations: []platform.StorageReservationCoord{{
			ReservationID: "reservation-b", Coord: "pending-b",
		}},
		tombstones: []platform.StorageTombstoneCoord{{
			TombstoneID: "tombstone-c", Coord: "dead-c",
		}},
	}
	var plannedDaemon string
	actors := []platform.PlanActor{{ActorID: "actor-a", Class: "class-a"}}
	host, _, lane := openBusinessTestLane(t, platform.DaemonMembrane{
		Plan: func(_ context.Context, daemonID string) ([]platform.PlanActor, error) {
			plannedDaemon = daemonID
			return actors, nil
		},
		Storage: storage,
	})

	plan := laneRoundTrip(t, lane, link.LaneFrame{
		Kind: link.LanePlanPull, RequestID: "plan-a",
	})
	if plannedDaemon != "daemon-a" || plan.PlanReply == nil ||
		len(plan.PlanReply.Actors) != 1 || plan.PlanReply.Actors[0].ActorID != "actor-a" {
		t.Fatalf("plan daemon=%q reply=%+v", plannedDaemon, plan.PlanReply)
	}

	committed := laneRoundTrip(t, lane, link.LaneFrame{
		Kind: link.LaneCommitted, RequestID: "committed-a",
		Committed: &link.Committed{
			RequestID: "committed-a", ReservationID: "reservation-a",
		},
	})
	if committed.CommittedReply == nil || !committed.CommittedReply.Found ||
		committed.CommittedReply.Lost {
		t.Fatalf("committed reply=%+v", committed.CommittedReply)
	}
	if storage.committedDaemon != "daemon-a" || storage.reservation != "reservation-a" {
		t.Fatalf("committed authority=(%q,%q)", storage.committedDaemon, storage.reservation)
	}

	reclaim := laneRoundTrip(t, lane, link.LaneFrame{
		Kind: link.LaneReclaimAck, RequestID: "reclaim-a",
		ReclaimAck: &link.ReclaimAck{
			RequestID: "reclaim-a", TombstoneID: "tombstone-a",
		},
	})
	if reclaim.ReclaimAckReply == nil || !reclaim.ReclaimAckReply.Found {
		t.Fatalf("reclaim reply=%+v", reclaim.ReclaimAckReply)
	}
	if storage.reclaimDaemon != "daemon-a" || storage.tombstone != "tombstone-a" {
		t.Fatalf("reclaim authority=(%q,%q)", storage.reclaimDaemon, storage.tombstone)
	}

	reconcile := laneRoundTrip(t, lane, link.LaneFrame{
		Kind: link.LaneReconcilePull, RequestID: "reconcile-a",
		ReconcilePull: &link.ReconcilePull{
			RequestID: "reconcile-a", ActiveCoords: []string{"active-a"},
		},
	})
	if reconcile.ReconcilePullReply == nil ||
		len(reconcile.ReconcilePullReply.Resources) != 1 ||
		len(reconcile.ReconcilePullReply.PendingReservations) != 1 ||
		len(reconcile.ReconcilePullReply.PendingTombstones) != 1 {
		t.Fatalf("reconcile reply=%+v", reconcile.ReconcilePullReply)
	}
	if storage.reconcileDaemon != "daemon-a" ||
		len(storage.activeCoords) != 1 || storage.activeCoords[0] != "active-a" {
		t.Fatalf("reconcile authority=(%q,%v)", storage.reconcileDaemon, storage.activeCoords)
	}

	token, err := host.OpenTransfer(
		t.Context(), "daemon-a", "channel-a", "coord-a", access.OpWrite, "reservation-z",
	)
	if err != nil {
		t.Fatal(err)
	}
	resolve := laneRoundTrip(t, lane, link.LaneFrame{
		Kind: link.LaneResolveCoord, RequestID: "resolve-a",
		ResolveCoord: &link.ResolveCoordRequest{
			RequestID: "resolve-a", Token: token,
		},
	})
	if resolve.ResolveCoordReply == nil || !resolve.ResolveCoordReply.OK ||
		resolve.ResolveCoordReply.Coord != "coord-a" ||
		resolve.ResolveCoordReply.ReservationID != "reservation-z" {
		t.Fatalf("resolve reply=%+v", resolve.ResolveCoordReply)
	}
}

func TestLaneBusinessFailuresStayDistinguishableFromEmptySuccess(t *testing.T) {
	storage := &testStorageAuthority{
		committedErr: errors.New("commit refused"),
		reclaimErr:   errors.New("ack refused"),
		reconcileErr: errors.New("snapshot unavailable"),
	}
	_, _, lane := openBusinessTestLane(t, platform.DaemonMembrane{
		Plan: func(context.Context, string) ([]platform.PlanActor, error) {
			return nil, errors.New("plan unavailable")
		},
		Storage: storage,
	})
	tests := []struct {
		name    string
		request link.LaneFrame
		reason  func(link.LaneFrame) string
	}{
		{
			name: "plan",
			request: link.LaneFrame{
				Kind: link.LanePlanPull, RequestID: "plan-error",
			},
			reason: func(frame link.LaneFrame) string { return frame.PlanReply.Error },
		},
		{
			name: "committed",
			request: link.LaneFrame{
				Kind: link.LaneCommitted, RequestID: "commit-error",
				Committed: &link.Committed{
					RequestID: "commit-error", ReservationID: "reservation-a",
				},
			},
			reason: func(frame link.LaneFrame) string { return frame.CommittedReply.Reason },
		},
		{
			name: "reclaim ack",
			request: link.LaneFrame{
				Kind: link.LaneReclaimAck, RequestID: "ack-error",
				ReclaimAck: &link.ReclaimAck{
					RequestID: "ack-error", TombstoneID: "tombstone-a",
				},
			},
			reason: func(frame link.LaneFrame) string { return frame.ReclaimAckReply.Reason },
		},
		{
			name: "reconcile",
			request: link.LaneFrame{
				Kind: link.LaneReconcilePull, RequestID: "reconcile-error",
				ReconcilePull: &link.ReconcilePull{
					RequestID: "reconcile-error",
				},
			},
			reason: func(frame link.LaneFrame) string { return frame.ReconcilePullReply.Reason },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reply := laneRoundTrip(t, lane, test.request)
			if reason := test.reason(reply); reason == "" {
				t.Fatalf("failure was returned as empty success: %+v", reply)
			}
		})
	}
	if !hostLaneStillCurrent(lane) {
		t.Fatal("business-level failure retired the lane")
	}
}

func hostLaneStillCurrent(lane *link.LaneStream) bool {
	return lane != nil && !lane.Retired()
}

// TestStorageSiblingSharesItsLaneLifecycle pins that the pair is one
// lifecycle: the sibling carries its lane's generation and nothing of its own,
// so the lane's retirement takes it, its death takes the lane, and a
// generation already spent admits no sibling — while refusing one never harms
// the live lane it does not belong to.
func TestStorageSiblingSharesItsLaneLifecycle(t *testing.T) {
	t.Run("lane retirement takes the sibling", func(t *testing.T) {
		host, carrier, lane := openBusinessTestLane(t, platform.DaemonMembrane{})
		storage := openTestStorageSibling(t, host, carrier, lane)
		lane.RetireLogical()
		lane.CollectPhysical()
		expectStreamDeath(t, storage, "the lane retired but its storage sibling stayed live")
	})

	t.Run("sibling death retires the lane", func(t *testing.T) {
		host, carrier, lane := openBusinessTestLane(t, platform.DaemonMembrane{})
		storage := openTestStorageSibling(t, host, carrier, lane)
		storage.RetireLogical()
		storage.CollectPhysical()
		waitFor(t, func() bool { return !host.LaneAttached("daemon-a", "channel-a") })
		expectStreamDeath(t, lane, "the sibling died but the lane stayed live")
	})

	t.Run("a sibling for a spent generation is refused", func(t *testing.T) {
		host, carrier, lane := openBusinessTestLane(t, platform.DaemonMembrane{})
		openTestStorageSibling(t, host, carrier, lane)
		bogus, err := carrier.OpenStorage(
			context.Background(), "channel-a", link.LaneGeneration("0198f000-0000-7000-8000-00000000dead"))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			bogus.RetireLogical()
			bogus.CollectPhysical()
		}()
		var frame link.LaneFrame
		if err := bogus.Decode(&frame); err == nil {
			t.Fatalf("a sibling with a foreign generation was served a frame: %+v", frame)
		}
		if !host.LaneAttached("daemon-a", "channel-a") {
			t.Fatal("refusing the foreign sibling harmed the lane it did not belong to")
		}
	})
}

// expectStreamDeath reads the device end until the wire dies under it. Death
// is only observable to a reader — retirement on the far side closes the
// stream when the far reader collects, and this side notices as a read error.
func expectStreamDeath(t *testing.T, stream *link.LaneStream, message string) {
	t.Helper()
	dead := make(chan struct{})
	go func() {
		defer close(dead)
		for {
			var frame link.LaneFrame
			if stream.Decode(&frame) != nil {
				return
			}
		}
	}()
	select {
	case <-dead:
	case <-time.After(2 * time.Second):
		t.Fatal(message)
	}
}

// openTestStorageSibling performs the device half of the storage-sibling open
// and waits until the host has attached it — storage RPCs sent before the
// attach answer NotReady by design, which is not what these tests probe.
func openTestStorageSibling(
	t *testing.T, host *Host, carrier *link.ClientCarrier, lane *link.LaneStream,
) *link.LaneStream {
	t.Helper()
	storage, err := carrier.OpenStorage(context.Background(), lane.Channel, lane.Gen)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		storage.RetireLogical()
		storage.CollectPhysical()
	})
	waitFor(t, func() bool {
		host.mu.RLock()
		row := host.daemons["daemon-a"]
		var current *carrierRow
		if row != nil {
			current = row.current
		}
		host.mu.RUnlock()
		if current == nil {
			return false
		}
		current.mu.Lock()
		serverLane := current.lanes[lane.Channel]
		current.mu.Unlock()
		if serverLane == nil {
			return false
		}
		serverLane.mu.Lock()
		defer serverLane.mu.Unlock()
		return serverLane.storage != nil
	})
	return storage
}

func TestServerStorageRequestsRoundTripFailureAndIndependentTimeout(t *testing.T) {
	t.Run("missing lane", func(t *testing.T) {
		host := New(Config{ScanInterval: time.Hour})
		t.Cleanup(func() { _ = host.Close(context.Background()) })
		if err := host.SendAlloc(t.Context(), "daemon-a", "channel-a", "coord-a", false); !errors.Is(err, ErrLaneUnavailable) {
			t.Fatalf("SendAlloc error=%v", err)
		}
		if err := host.SendReclaim(t.Context(), "daemon-a", "channel-a", "coord-a"); !errors.Is(err, ErrLaneUnavailable) {
			t.Fatalf("SendReclaim error=%v", err)
		}
	})

	for _, test := range []struct {
		name      string
		operation func(*Host) error
		reply     func(link.LaneFrame) link.LaneFrame
		want      string
	}{
		{
			name: "alloc success",
			operation: func(host *Host) error {
				return host.SendAlloc(t.Context(), "daemon-a", "channel-a", "coord-a", true)
			},
			reply: func(request link.LaneFrame) link.LaneFrame {
				return link.LaneFrame{
					Kind: link.LaneAllocReply, RequestID: request.RequestID,
					AllocReply: &link.AllocReply{RequestID: request.RequestID, OK: true},
				}
			},
		},
		{
			name: "reclaim failure",
			operation: func(host *Host) error {
				return host.SendReclaim(t.Context(), "daemon-a", "channel-a", "coord-a")
			},
			reply: func(request link.LaneFrame) link.LaneFrame {
				return link.LaneFrame{
					Kind: link.LaneReclaimReply, RequestID: request.RequestID,
					ReclaimReply: &link.ReclaimReply{
						RequestID: request.RequestID, Reason: "disk busy",
					},
				}
			},
			want: "disk busy",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			host, carrier, lane := openBusinessTestLane(t, platform.DaemonMembrane{})
			storage := openTestStorageSibling(t, host, carrier, lane)
			deviceDone := make(chan error, 1)
			go func() {
				var request link.LaneFrame
				if err := storage.Decode(&request); err != nil {
					deviceDone <- err
					return
				}
				deviceDone <- storage.Send(test.reply(request))
			}()
			err := test.operation(host)
			if test.want == "" && err != nil {
				t.Fatal(err)
			}
			if test.want != "" && (err == nil || !strings.Contains(err.Error(), test.want)) {
				t.Fatalf("operation error=%v, want %q", err, test.want)
			}
			if err := <-deviceDone; err != nil {
				t.Fatal(err)
			}
		})
	}

	// A daemon that has not built its compartment yet answers not_ready, and
	// that answer is not a refusal — it says nothing was attempted. The home
	// must be able to tell the two apart, because it decides whether the
	// operation may be retried on that same daemon. Folding not_ready into the
	// !OK branch (the shape this replaced) made an unmade decision
	// indistinguishable from a made one, and the caller then treated a
	// still-building daemon as a hard create failure.
	t.Run("not ready is not a refusal", func(t *testing.T) {
		for _, test := range []struct {
			name      string
			operation func(*Host) error
			reply     func(link.LaneFrame) link.LaneFrame
		}{
			{
				name: "alloc",
				operation: func(host *Host) error {
					return host.SendAlloc(t.Context(), "daemon-a", "channel-a", "coord-a", false)
				},
				reply: func(request link.LaneFrame) link.LaneFrame {
					return link.LaneFrame{
						Kind: link.LaneAllocReply, RequestID: request.RequestID,
						AllocReply: &link.AllocReply{
							RequestID: request.RequestID,
							NotReady:  true, Reason: "compartment not built yet",
						},
					}
				},
			},
			{
				name: "reclaim",
				operation: func(host *Host) error {
					return host.SendReclaim(t.Context(), "daemon-a", "channel-a", "coord-a")
				},
				reply: func(request link.LaneFrame) link.LaneFrame {
					return link.LaneFrame{
						Kind: link.LaneReclaimReply, RequestID: request.RequestID,
						ReclaimReply: &link.ReclaimReply{
							RequestID: request.RequestID,
							NotReady:  true, Reason: "compartment not built yet",
						},
					}
				},
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				host, carrier, lane := openBusinessTestLane(t, platform.DaemonMembrane{})
				storage := openTestStorageSibling(t, host, carrier, lane)
				deviceDone := make(chan error, 1)
				go func() {
					var request link.LaneFrame
					if err := storage.Decode(&request); err != nil {
						deviceDone <- err
						return
					}
					deviceDone <- storage.Send(test.reply(request))
				}()
				err := test.operation(host)
				if !errors.Is(err, platform.ErrDaemonNotReady) {
					t.Fatalf("error=%v, want it to carry platform.ErrDaemonNotReady", err)
				}
				if err := <-deviceDone; err != nil {
					t.Fatal(err)
				}
			})
		}

		// The other half: a daemon that IS ready must not produce the sentinel,
		// or the guard above would hold for an implementation that returned it
		// unconditionally.
		t.Run("a ready daemon does not carry the sentinel", func(t *testing.T) {
			host, carrier, lane := openBusinessTestLane(t, platform.DaemonMembrane{})
			storage := openTestStorageSibling(t, host, carrier, lane)
			deviceDone := make(chan error, 1)
			go func() {
				var request link.LaneFrame
				if err := storage.Decode(&request); err != nil {
					deviceDone <- err
					return
				}
				deviceDone <- storage.Send(link.LaneFrame{
					Kind: link.LaneAllocReply, RequestID: request.RequestID,
					AllocReply: &link.AllocReply{
						RequestID: request.RequestID, Reason: "disk full",
					},
				})
			}()
			err := host.SendAlloc(t.Context(), "daemon-a", "channel-a", "coord-a", false)
			if err == nil || errors.Is(err, platform.ErrDaemonNotReady) {
				t.Fatalf("a refusal must stay a refusal, got %v", err)
			}
			if err := <-deviceDone; err != nil {
				t.Fatal(err)
			}
		})
	})

	t.Run("independent timeout", func(t *testing.T) {
		previous := laneRPCTimeout
		laneRPCTimeout = 30 * time.Millisecond
		t.Cleanup(func() { laneRPCTimeout = previous })
		host, carrier, lane := openBusinessTestLane(t, platform.DaemonMembrane{})
		storage := openTestStorageSibling(t, host, carrier, lane)
		received := make(chan struct{})
		go func() {
			var request link.LaneFrame
			if storage.Decode(&request) == nil {
				close(received)
			}
		}()
		started := time.Now()
		err := host.SendAlloc(context.Background(), "daemon-a", "channel-a", "coord-a", false)
		if !errors.Is(err, link.ErrLaneRPCTimeout) {
			t.Fatalf("timeout error=%v", err)
		}
		if time.Since(started) > time.Second {
			t.Fatalf("independent timeout took %v", time.Since(started))
		}
		select {
		case <-received:
		case <-time.After(time.Second):
			t.Fatal("request was never sent")
		}
	})
}

func TestTransferTicketAuthorizationPrecedesMutationAndIsIdempotent(t *testing.T) {
	now := time.Unix(1_000, 0)
	host := New(Config{
		ScanInterval: time.Hour,
		Now:          func() time.Time { return now },
	})
	t.Cleanup(func() { _ = host.Close(context.Background()) })
	host.mu.Lock()
	host.daemons["daemon-a"] = &daemonRow{current: &carrierRow{
		lanes: map[channel.ID]*serverLane{"channel-a": {}},
	}}
	host.mu.Unlock()
	t.Cleanup(func() {
		host.mu.Lock()
		delete(host.daemons, "daemon-a")
		host.mu.Unlock()
	})
	token, err := host.OpenTransfer(
		t.Context(), "daemon-a", "channel-a", "coord-a", access.OpRead, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, wrong := range []struct{ daemon, channel string }{
		{daemon: "daemon-b", channel: "channel-a"},
		{daemon: "daemon-a", channel: "channel-b"},
	} {
		if _, ok := host.resolveTransfer(wrong.daemon, wrong.channel, token); ok {
			t.Fatalf("unauthorized transfer resolved for (%q,%q)", wrong.daemon, wrong.channel)
		}
	}
	for i := 0; i < 2; i++ {
		ticket, ok := host.resolveTransfer("daemon-a", "channel-a", token)
		if !ok || ticket.coord != "coord-a" {
			t.Fatalf("authorized resolve %d=(%+v,%v)", i, ticket, ok)
		}
	}
	now = now.Add(transferTicketTTL)
	if _, ok := host.resolveTransfer("daemon-a", "channel-a", token); ok {
		t.Fatal("expired transfer resolved")
	}
	host.mu.RLock()
	_, retained := host.transfers[token]
	host.mu.RUnlock()
	if retained {
		t.Fatal("expired transfer was not deleted at redemption")
	}
}

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
	host.Register("a", 1, platform.DaemonMembrane{
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
	host.Register("b", 1, platform.DaemonMembrane{
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
