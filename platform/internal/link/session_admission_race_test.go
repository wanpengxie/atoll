package link

// Server-side session admission races, driven over a real carrier with the
// accept_test.go rig and rawDaemon so every verdict here is the one a
// production daemon actually meets:
//
//   - the admission barrier's EXACT window — an actor substream that is open
//     but has not yet produced an AttemptKey when the session is sealed;
//   - the delivery window in which the route is published but the remote cell
//     has not started reading;
//   - KickDaemon's bulk revocation across the half-attach window and every
//     extant generation, with IsAttached as the ledger's own answer;
//   - the ledger's terminal account after the real Dial→Close path.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

// --- shared harness for the session-race and close-lifecycle files ---------

// sessRaceStorage is a StorageHostControl whose Committed arm parks forever.
// It exists to hold ONE home control worker provably in flight: `entered`
// closes from inside the pool goroutine, so a test can seal or Close with a
// wedged worker without a sleep, and `release` unwedges it at cleanup so the
// acceptor teardown never actually takes the abandon path twice.
type sessRaceStorage struct {
	entered chan struct{}
	release chan struct{}

	enteredOnce sync.Once
	releaseOnce sync.Once
}

func newSessRaceStorage() *sessRaceStorage {
	return &sessRaceStorage{entered: make(chan struct{}), release: make(chan struct{})}
}

func (s *sessRaceStorage) Committed(context.Context, string, string) (bool, bool, error) {
	s.enteredOnce.Do(func() { close(s.entered) })
	<-s.release
	return true, false, nil
}

func (s *sessRaceStorage) ReclaimAck(context.Context, string, string) (bool, error) {
	return false, nil
}

func (s *sessRaceStorage) ReconcilePull(
	context.Context,
	string,
	[]string,
) ([]ReconcileResource, []ReconcileReservation, []ReconcileTombstone, error) {
	return nil, nil, nil, nil
}

// releaseAll unwedges every parked worker. Register it as a cleanup AFTER the
// rig's own so LIFO runs it first and acceptor shutdown stays fast.
func (s *sessRaceStorage) releaseAll() {
	s.releaseOnce.Do(func() { close(s.release) })
}

// sessRaceWedgeControlWorker parks one home control worker and returns only
// once that worker is provably inside the handler, so the caller's next step
// races nothing.
func sessRaceWedgeControlWorker(t *testing.T, daemon *rawDaemon, storage *sessRaceStorage) {
	t.Helper()
	raw, err := encodeStorageControl(storageControlFrame{
		Kind:      ctrlCommitted,
		Committed: &Committed{RequestID: "sess-race-wedge", ReservationID: "sess-race-resv"},
	})
	if err != nil {
		t.Fatal(err)
	}
	daemon.send(raw)
	select {
	case <-storage.entered:
	case <-time.After(15 * time.Second):
		t.Fatal("control worker never entered the wedged handler")
	}
}

// sessRaceOpenActorStream opens a tag=actor substream and writes ONLY its
// stream header. The handshake is deliberately left to the caller: between
// these two writes the server is inside onActor with no AttemptKey in hand,
// which is the window this file exercises.
func sessRaceOpenActorStream(t *testing.T, daemon *rawDaemon) (net.Conn, *ipc.Codec) {
	t.Helper()
	stream, err := daemon.ys.Open()
	if err != nil {
		t.Fatalf("open actor stream: %v", err)
	}
	if err := writeStreamHeader(stream, streamActor); err != nil {
		t.Fatalf("actor stream header: %v", err)
	}
	return stream, ipc.NewCodec(stream, stream)
}

func sessRaceWriteHandshake(
	t *testing.T,
	codec *ipc.Codec,
	id actor.ActorID,
	key actorhost.AttemptKey,
) error {
	t.Helper()
	payload, err := json.Marshal(ipc.HandshakePayload{
		LeaseID: string(id), AttemptKey: string(key),
	})
	if err != nil {
		t.Fatal(err)
	}
	return codec.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: payload})
}

// sessRaceEventually polls a condition instead of sleeping on a guess.
func sessRaceEventually(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// sessRaceLogSink captures the acceptor's own attribution. The late-reject
// COUNTER alone cannot tell which gate refused an attempt — actor_admission
// and route_publish_precheck both increment it and both end with no route
// published — so a test that means to pin the admission barrier has to read
// the gate the acceptor names.
type sessRaceLogSink struct {
	mu      sync.Mutex
	records []sessRaceLogRecord
}

type sessRaceLogRecord struct {
	msg   string
	attrs map[string]string
}

func (s *sessRaceLogSink) Enabled(context.Context, slog.Level) bool { return true }

func (s *sessRaceLogSink) Handle(_ context.Context, record slog.Record) error {
	captured := sessRaceLogRecord{msg: record.Message, attrs: map[string]string{}}
	record.Attrs(func(attr slog.Attr) bool {
		captured.attrs[attr.Key] = attr.Value.String()
		return true
	})
	s.mu.Lock()
	s.records = append(s.records, captured)
	s.mu.Unlock()
	return nil
}

func (s *sessRaceLogSink) WithAttrs([]slog.Attr) slog.Handler { return s }
func (s *sessRaceLogSink) WithGroup(string) slog.Handler      { return s }

// attrValues returns one attribute of every record with the given message,
// in order.
func (s *sessRaceLogSink) attrValues(msg, key string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, record := range s.records {
		if record.msg == msg {
			out = append(out, record.attrs[key])
		}
	}
	return out
}

// newSessRaceRig builds an Acceptor with a captured logger. It reuses
// acceptorRig's own accessors (wsURL/waitSession) and differs from
// newAcceptorRig only in that the acceptor's attribution is observable.
func newSessRaceRig(
	t *testing.T,
	storage StorageHostControl,
	attach func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain, actorhost.Binding) error,
	settlementWindow, joinWindow time.Duration,
) (*acceptorRig, *sessRaceLogSink) {
	t.Helper()
	logs := &sessRaceLogSink{}
	acc, err := NewAcceptor(Config{
		Ingress:   fakeIngress{},
		ChannelID: "acceptor-test-channel",
		Logger:    slog.New(logs),
		AuthorizeAttach: func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain) error {
			return nil
		},
		AttachBinding:      attach,
		BindingDown:        func(actor.ActorID, actorhost.Binding) {},
		StorageHostControl: storage,
		Plan:               func(context.Context, string) ([]platform.PlanActor, error) { return nil, nil },
		CanAttach:          func(context.Context, string) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewAcceptor: %v", err)
	}
	if settlementWindow > 0 {
		acc.sessions.settlementWindow = settlementWindow
	}
	if joinWindow > 0 {
		acc.sessions.sessionJoinWindow = joinWindow
	}
	rig := &acceptorRig{acc: acc}
	rig.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		rig.acc.Serve(w, req, "daemon-1")
	}))
	t.Cleanup(func() {
		_ = rig.acc.Close()
		rig.srv.Close()
	})
	return rig, logs
}

func sessRaceSnapshot(acc *Acceptor, generation SessionGeneration) SessionSnapshot {
	for _, snapshot := range acc.Sessions() {
		if snapshot.Generation == generation {
			return snapshot
		}
	}
	return SessionSnapshot{}
}

// --- T31 · admission barrier vs a half-open handshake ---------------------

// The admission barrier is read AFTER the handshake frame, so a session
// sealed while an actor substream is already open — but before that substream
// has produced any AttemptKey — must still be refused. Existing coverage only
// reaches the window after an AttemptKey exists (the route-publish sandwich).
//
// The earlier window is held open deliberately: one wedged control worker
// parks the seal's physical collection inside the settlement window, so the
// verdict is already in the ledger while the carrier is demonstrably still
// alive. The refusal therefore cannot be attributed to a dead transport.
//
// The assertion reads the acceptor's OWN gate attribution, not just the
// late-reject counter: the later route-publish precheck also refuses and also
// leaves no route published, so a counter-only assertion would pass even with
// the admission barrier deleted.
func TestActorHandshakeOpenedBeforeSealIsRefusedByAdmissionBarrier(t *testing.T) {
	storage := newSessRaceStorage()
	var attached atomic.Int64
	rig, logs := newSessRaceRig(t, storage, func(
		actor.ActorID,
		actorhost.AttemptKey,
		actorhost.ExecutionDomain,
		actorhost.Binding,
	) error {
		attached.Add(1)
		return nil
	}, 5*time.Second, 500*time.Millisecond)
	t.Cleanup(storage.releaseAll)

	daemon := dialRawDaemon(t, rig.wsURL(), true)
	active := rig.waitSession(t, 10*time.Second, func(s SessionSnapshot) bool {
		return s.Key == "daemon-1" && s.State == SessionActive
	})
	sessRaceWedgeControlWorker(t, daemon, storage)

	// Substream open, header sent, handshake withheld: no AttemptKey exists
	// anywhere on the server side for this route yet.
	stream, codec := sessRaceOpenActorStream(t, daemon)
	defer stream.Close()

	if !rig.acc.KickSession(active.Generation) {
		t.Fatal("exact kick was rejected")
	}
	// beginSeal is the decision write, so the ledger answer is already cut
	// the instant KickSession returns.
	if rig.acc.IsAttached("daemon-1") {
		t.Fatal("ledger still reported an attached daemon after the verdict was written")
	}
	closing := rig.waitSession(t, 5*time.Second, func(s SessionSnapshot) bool {
		return s.Generation == active.Generation && s.State == SessionClosing
	})
	if closing.Reason != SessionRevoked {
		t.Fatalf("reason=%s want revoked", closing.Reason)
	}

	// The carrier is still up — this write must succeed, which is what makes
	// the refusal below attributable to the admission barrier alone.
	if err := sessRaceWriteHandshake(t, codec, "agent:late", physicalKey(t)); err != nil {
		t.Fatalf("handshake write on a still-live carrier failed: %v", err)
	}
	if !sessRaceEventually(10*time.Second, func() bool { return rig.acc.lateRejected.Load() >= 1 }) {
		t.Fatal("a sealed session did not account the late actor handshake as rejected")
	}
	gates := logs.attrValues("link.session_late_rejected", "gate")
	if len(gates) != 1 || gates[0] != "actor_admission" {
		t.Fatalf("late-reject gates=%v want exactly [actor_admission]; the admission "+
			"barrier did not refuse the handshake before any route work began", gates)
	}
	if got := attached.Load(); got != 0 {
		t.Fatalf("attachBinding calls=%d; a sealed session published an exact route", got)
	}
	if got := rig.acc.compensated.Load(); got != 0 {
		t.Fatalf("compensated=%d; the refusal came from route-publish compensation, "+
			"not from the admission barrier", got)
	}
}

// --- T32 · the delivery window before the remote cell reads ---------------

// Between route publication and the remote cell's first read there is a real
// window in which the home can already deliver. Nothing in that window may be
// silently dropped: the envelopes wait on the published route and arrive
// intact and in order once the cell shows up. The other half of the same
// contract is that once the route IS gone, Deliver says so rather than
// accepting into the void.
func TestDeliveryBeforeRemoteCellReadsIsQueuedNotDropped(t *testing.T) {
	bindings := make(chan actorhost.Binding, 1)
	rig := newAcceptorRig(t, acceptorRigConfig{
		attachBinding: func(
			_ actor.ActorID,
			_ actorhost.AttemptKey,
			_ actorhost.ExecutionDomain,
			binding actorhost.Binding,
		) error {
			select {
			case bindings <- binding:
			default:
			}
			return nil
		},
	})
	daemon := dialRawDaemon(t, rig.wsURL(), true)
	stream, codec := sessRaceOpenActorStream(t, daemon)
	defer stream.Close()
	if err := sessRaceWriteHandshake(t, codec, "agent:test", physicalKey(t)); err != nil {
		t.Fatalf("handshake write: %v", err)
	}

	var binding actorhost.Binding
	select {
	case binding = <-bindings:
	case <-time.After(10 * time.Second):
		t.Fatal("exact route was never published")
	}

	// The daemon has not read one frame off this substream: from the home's
	// side the cell is not installed. Deliver anyway.
	const count = 4
	for i := 0; i < count; i++ {
		env := &message.Envelope{
			ID:        message.ID(fmt.Sprintf("m-%d", i)),
			ChannelID: "acceptor-test-channel",
			Type:      "test",
			Payload:   json.RawMessage(`{}`),
		}
		if err := binding.Deliver(env); err != nil {
			t.Fatalf("deliver %d inside the pre-install window: %v", i, err)
		}
	}

	// The cell arrives: every envelope is still there, in order.
	if err := stream.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		frame, err := codec.Read()
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if frame.Kind != ipc.KindDeliver {
			t.Fatalf("frame %d kind=%s want deliver", i, frame.Kind)
		}
		var payload ipc.DeliverPayload
		if err := json.Unmarshal(frame.Payload, &payload); err != nil {
			t.Fatalf("decode deliver %d: %v", i, err)
		}
		if want := message.ID(fmt.Sprintf("m-%d", i)); payload.Envelope.ID != want {
			t.Fatalf("frame %d id=%s want %s", i, payload.Envelope.ID, want)
		}
	}

	// Route gone → the loss is reported, never swallowed.
	_ = binding.Close()
	if err := binding.Deliver(&message.Envelope{
		ID: "m-after-close", ChannelID: "acceptor-test-channel", Type: "test",
		Payload: json.RawMessage(`{}`),
	}); err == nil {
		t.Fatal("Deliver on a torn-down route reported success")
	}
}

// --- T33 · KickDaemon across the half-attach window -----------------------

// KickDaemon is bulk revocation by authenticated key, not a current-session
// alias: it must reach a carrier that has only minted a candidate (the
// half-attach window), and it must reach EVERY extant generation, including
// the one already displaced by a successor. IsAttached is the ledger's own
// answer about the current pointer and must never report a candidate or a
// revoked generation as online.
func TestKickDaemonRevokesHalfAttachedAndEveryExtantGeneration(t *testing.T) {
	t.Run("half attach window", func(t *testing.T) {
		rig := newAcceptorRig(t, acceptorRigConfig{})
		dialRawCarrier(t, rig.wsURL(), true)
		candidate := rig.waitSession(t, 10*time.Second, func(s SessionSnapshot) bool {
			return s.Key == "daemon-1" && s.State == SessionCandidate
		})
		if rig.acc.IsAttached("daemon-1") {
			t.Fatal("a carrier that never attached was reported attached")
		}
		if ids := rig.acc.AttachedDaemonIDs(); len(ids) != 0 {
			t.Fatalf("attached ids=%v want none during the half-attach window", ids)
		}
		if got := rig.acc.KickDaemon("daemon-1"); got != 1 {
			t.Fatalf("KickDaemon=%d want 1 (the half-attached candidate)", got)
		}
		closed := rig.waitSession(t, 10*time.Second, func(s SessionSnapshot) bool {
			return s.Generation == candidate.Generation && s.State == SessionClosed
		})
		if closed.Reason != SessionRevoked {
			t.Fatalf("reason=%s want revoked", closed.Reason)
		}
		if got := rig.acc.KickDaemon("daemon-1"); got != 0 {
			t.Fatalf("re-kick=%d want 0; a closed generation was revoked twice", got)
		}
	})

	t.Run("every extant generation", func(t *testing.T) {
		rig := newAcceptorRig(t, acceptorRigConfig{})
		dialRawDaemon(t, rig.wsURL(), true)
		dialRawDaemon(t, rig.wsURL(), true)
		liveActive := func() int {
			count := 0
			for _, s := range rig.acc.Sessions() {
				if s.Key == "daemon-1" && s.State == SessionActive {
					count++
				}
			}
			return count
		}
		if !sessRaceEventually(10*time.Second, func() bool { return liveActive() == 2 }) {
			t.Fatalf("two active generations never coexisted; have %+v", rig.acc.Sessions())
		}
		if !rig.acc.IsAttached("daemon-1") {
			t.Fatal("current pointer missing while two active generations exist")
		}
		if ids := rig.acc.AttachedDaemonIDs(); len(ids) != 1 || ids[0] != "daemon-1" {
			t.Fatalf("attached ids=%v want exactly one daemon-1", ids)
		}
		if got := rig.acc.KickDaemon("daemon-1"); got != 2 {
			t.Fatalf("KickDaemon=%d want 2 (current + displaced)", got)
		}
		if !sessRaceEventually(10*time.Second, func() bool {
			for _, s := range rig.acc.Sessions() {
				if s.Key == "daemon-1" && s.State != SessionClosed {
					return false
				}
			}
			return true
		}) {
			t.Fatalf("bulk revocation left a live generation; have %+v", rig.acc.Sessions())
		}
		if rig.acc.IsAttached("daemon-1") {
			t.Fatal("IsAttached stayed true after every generation was revoked")
		}
		if ids := rig.acc.AttachedDaemonIDs(); len(ids) != 0 {
			t.Fatalf("attached ids=%v want none after bulk revocation", ids)
		}
	})

	t.Run("no target", func(t *testing.T) {
		rig := newAcceptorRig(t, acceptorRigConfig{})
		dialRawDaemon(t, rig.wsURL(), true)
		rig.waitSession(t, 10*time.Second, func(s SessionSnapshot) bool {
			return s.Key == "daemon-1" && s.State == SessionActive
		})
		if got := rig.acc.KickDaemon(""); got != 0 {
			t.Fatalf("KickDaemon(\"\")=%d want 0", got)
		}
		if got := rig.acc.KickDaemon("daemon-2"); got != 0 {
			t.Fatalf("KickDaemon on an unknown key=%d want 0", got)
		}
		if !rig.acc.IsAttached("daemon-1") {
			t.Fatal("a bulk kick aimed elsewhere disturbed the live daemon")
		}
	})
}

// --- T34 · the ledger's terminal account after the real dial path ---------

// Walk the whole production path — Dial, attach, adopt, carrier close — and
// prove the home ledger leaves no zombie online account behind: IsAttached is
// false, the current index is empty, no live row survives, and nothing can be
// routed to the key any more.
func TestProductionDialAndCloseLeaveNoAttachedLedgerRow(t *testing.T) {
	rig := newAcceptorRig(t, acceptorRigConfig{})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	dialer, err := Dial(ctx, rig.wsURL(), DialConfig{
		SessionLedger: NewRemoteSessionLedger(nil),
	}, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if !rig.acc.IsAttached("daemon-1") {
		t.Fatal("the real dial path produced no attached ledger row")
	}
	if ids := rig.acc.AttachedDaemonIDs(); len(ids) != 1 || ids[0] != "daemon-1" {
		t.Fatalf("attached ids=%v want exactly one daemon-1", ids)
	}
	if rig.acc.currentHandle("daemon-1") == nil {
		t.Fatal("an attached daemon had no routable control handle")
	}

	if err := dialer.Close(); err != nil {
		t.Fatalf("dialer close: %v", err)
	}

	if !sessRaceEventually(15*time.Second, func() bool {
		return !rig.acc.IsAttached("daemon-1") && len(rig.acc.AttachedDaemonIDs()) == 0
	}) {
		t.Fatal("the ledger kept a zombie online row after the carrier was closed")
	}
	closed := rig.waitSession(t, 15*time.Second, func(s SessionSnapshot) bool {
		return s.Key == "daemon-1" && s.State == SessionClosed
	})
	if closed.ClosedAt.IsZero() {
		t.Fatalf("closed row has no ClosedAt: %+v", closed)
	}
	for _, snapshot := range rig.acc.Sessions() {
		if snapshot.Key == "daemon-1" && snapshot.State != SessionClosed {
			t.Fatalf("residual non-closed row after the full production path: %+v", snapshot)
		}
	}
	if rig.acc.currentHandle("daemon-1") != nil {
		t.Fatal("a closed session still offered a routable control handle")
	}
	sendCtx, sendCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer sendCancel()
	if err := rig.acc.SendAllocRequest(sendCtx, "daemon-1", AllocRequest{}); err == nil {
		t.Fatal("a control request routed to a daemon with no live connection")
	}
}
