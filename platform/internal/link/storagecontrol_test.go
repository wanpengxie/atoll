package link

// Storage/plan control-RPC business round trips over the real wire: every
// request-default frame reaches its TRUE production handler (not a stub in
// the dispatch table) and the reply carries that handler's verdict back to
// the caller. These lock the "table swap did not break the handlers" half
// that parse-level tests cannot see.

import (
	"context"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/runtime/actorhost"
)

func dialStorageRPCDaemon(t *testing.T, rig *acceptorRig, cfg DialConfig) *Dialer {
	t.Helper()
	if cfg.SessionLedger == nil {
		cfg.SessionLedger = NewRemoteSessionLedger(nil)
	}
	dialer, err := Dial(t.Context(), rig.wsURL(), cfg, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = dialer.Close() })
	return dialer
}

// home → daemon: an alloc request reaches the daemon's real AllocHandler and
// its OK verdict travels back as the sender's nil error.
func TestAllocRequestRoundTripReachesDaemonAllocator(t *testing.T) {
	rig := newAcceptorRig(t, acceptorRigConfig{})
	var mu sync.Mutex
	var got []AllocRequest
	dialStorageRPCDaemon(t, rig, DialConfig{
		AllocHandler: func(req AllocRequest) AllocReply {
			mu.Lock()
			got = append(got, req)
			mu.Unlock()
			return AllocReply{RequestID: req.RequestID, OK: true}
		},
	})
	err := rig.acc.SendAllocRequest(t.Context(), "daemon-1", AllocRequest{
		ChannelID: "chan-1", Coord: "coord-alloc", Dir: true,
	})
	if err != nil {
		t.Fatalf("SendAllocRequest: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0].ChannelID != "chan-1" || got[0].Coord != "coord-alloc" || !got[0].Dir {
		t.Fatalf("daemon allocator saw %+v", got)
	}
}

// home → daemon: a reclaim request reaches the daemon's real opener and the
// reclaimed coord is the one the home named.
func TestReclaimRequestRoundTripReachesDaemonOpener(t *testing.T) {
	rig := newAcceptorRig(t, acceptorRigConfig{})
	opener := &reclaimRecordingOpener{}
	dialStorageRPCDaemon(t, rig, DialConfig{LocalFileOpener: opener})
	if err := rig.acc.SendReclaimRequest(t.Context(), "daemon-1", "coord-reclaim"); err != nil {
		t.Fatalf("SendReclaimRequest: %v", err)
	}
	if got := opener.coords(); len(got) != 1 || got[0] != "coord-reclaim" {
		t.Fatalf("daemon reclaimed %v, want [coord-reclaim]", got)
	}
}

type reclaimRecordingOpener struct {
	staticLaneOpener
	mu       sync.Mutex
	reclaimd []string
}

func (o *reclaimRecordingOpener) ReclaimCoord(coord string) error {
	o.mu.Lock()
	o.reclaimd = append(o.reclaimd, coord)
	o.mu.Unlock()
	return nil
}

func (o *reclaimRecordingOpener) coords() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.reclaimd...)
}

// daemon → home: a committed landing signal reaches the home's real storage
// control with the AUTHENTICATED sender identity, and its verdict rides back.
func TestCommittedRoundTripCarriesAuthenticatedSender(t *testing.T) {
	storage := &fakeStorageControl{}
	rig := newAcceptorRig(t, acceptorRigConfig{storage: storage})
	d := dialStorageRPCDaemon(t, rig, DialConfig{})
	reply, err := d.SendCommitted(t.Context(), "resv-9")
	if err != nil {
		t.Fatalf("SendCommitted: %v", err)
	}
	if !reply.Found || reply.Lost {
		t.Fatalf("reply=%+v want Found && !Lost", reply)
	}
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if len(storage.committedCalls) != 1 ||
		storage.committedCalls[0] != [2]string{"daemon-1", "resv-9"} {
		t.Fatalf("home storage saw %v", storage.committedCalls)
	}
}

// daemon → home: a reclaim-ack closure signal reaches the home's real storage
// control and the Found verdict is relayed intact.
func TestReclaimAckRoundTripRelaysVerdict(t *testing.T) {
	storage := &fakeStorageControl{reclaimFound: true}
	rig := newAcceptorRig(t, acceptorRigConfig{storage: storage})
	d := dialStorageRPCDaemon(t, rig, DialConfig{})
	reply, err := d.SendReclaimAck(t.Context(), "tomb-3")
	if err != nil {
		t.Fatalf("SendReclaimAck: %v", err)
	}
	if !reply.Found {
		t.Fatalf("reply=%+v want Found", reply)
	}
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if len(storage.reclaimAcks) != 1 ||
		storage.reclaimAcks[0] != [2]string{"daemon-1", "tomb-3"} {
		t.Fatalf("home storage saw %v", storage.reclaimAcks)
	}
}

// daemon → home: a reconcile pull forwards the daemon's active coords and the
// home's full recovery rows come back row-for-row.
func TestReconcilePullRoundTripCarriesRowsBothWays(t *testing.T) {
	storage := &fakeStorageControl{
		resources:    []ReconcileResource{{Coord: "res-1"}},
		reservations: []ReconcileReservation{{ReservationID: "rsv-1", Coord: "rc-1"}},
		tombstones:   []ReconcileTombstone{{TombstoneID: "tmb-1", Coord: "tc-1"}},
	}
	rig := newAcceptorRig(t, acceptorRigConfig{storage: storage})
	d := dialStorageRPCDaemon(t, rig, DialConfig{})
	reply, err := d.SendReconcilePull(t.Context(), []string{"live-1", "live-2"})
	if err != nil {
		t.Fatalf("SendReconcilePull: %v", err)
	}
	if len(reply.Resources) != 1 || reply.Resources[0].Coord != "res-1" ||
		len(reply.PendingReservations) != 1 || reply.PendingReservations[0].ReservationID != "rsv-1" ||
		len(reply.PendingTombstones) != 1 || reply.PendingTombstones[0].TombstoneID != "tmb-1" {
		t.Fatalf("reply rows=%+v", reply)
	}
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if len(storage.reconcilePulls) != 1 || len(storage.reconcilePulls[0]) != 2 ||
		storage.reconcilePulls[0][0] != "live-1" {
		t.Fatalf("home saw active coords %v", storage.reconcilePulls)
	}
}

// daemon → home: a plan pull runs the home's REAL plan callback (not a table
// stub) and its actors come back through the wire validation intact.
func TestPlanPullRoundTripRunsRealPlanCallback(t *testing.T) {
	key, err := actorhost.NewAttemptKey()
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var askedFor []string
	rig := newAcceptorRig(t, acceptorRigConfig{
		plan: func(_ context.Context, daemonID string) ([]platform.PlanActor, error) {
			mu.Lock()
			askedFor = append(askedFor, daemonID)
			mu.Unlock()
			return []platform.PlanActor{{
				ActorID: "agent:planned", AttemptKey: key, Kind: "agent", Class: "worker",
			}}, nil
		},
	})
	d := dialStorageRPCDaemon(t, rig, DialConfig{})
	actors, err := d.PullPlan(t.Context())
	if err != nil {
		t.Fatalf("PullPlan: %v", err)
	}
	if len(actors) != 1 || actors[0].ActorID != "agent:planned" || actors[0].Class != "worker" {
		t.Fatalf("actors=%+v", actors)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(askedFor) != 1 || askedFor[0] != "daemon-1" {
		t.Fatalf("plan callback asked for %v, want [daemon-1]", askedFor)
	}
}
