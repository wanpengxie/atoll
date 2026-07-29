package link

// Acceptor full-wire integration tests: a real Acceptor behind an httptest
// server, driven by a hand-crafted daemon endpoint (rawDaemon) that speaks
// the wire protocol directly. Covers the attach verdict point, liveness
// probing, the route-publish sandwich, control-frame strictness, and bounded
// seal collection.

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/ipc"
	"github.com/wanpengxie/atoll/runtime/remoteingress"
)

type fakeIngress struct{}

func (fakeIngress) Emit(
	context.Context,
	actor.ActorID,
	actorhost.AttemptKey,
	*message.Envelope,
) (harness.WriteResult, error) {
	return harness.WriteResult{}, nil
}

func (fakeIngress) Access(
	context.Context,
	actor.ActorID,
	actorhost.AttemptKey,
	remoteingress.AccessRequest,
) (remoteingress.AccessResponse, error) {
	return remoteingress.AccessResponse{}, nil
}

func (fakeIngress) Schedule(
	context.Context,
	actor.ActorID,
	remoteingress.ScheduleRequest,
) (remoteingress.ScheduleResponse, error) {
	return remoteingress.ScheduleResponse{ID: "test-timer"}, nil
}

func (fakeIngress) Fork(
	context.Context,
	actor.ActorID,
	actorhost.AttemptKey,
	remoteingress.ForkRequest,
) (actor.ActorID, error) {
	return "agent:child", nil
}

func (fakeIngress) EndSelf(
	context.Context,
	actor.ActorID,
	actorhost.AttemptKey,
	actorcaps.EndSelfRequest,
) error {
	return nil
}

type fakeStorageControl struct {
	committedDelay time.Duration
	reclaimFound   bool
	resources      []ReconcileResource
	reservations   []ReconcileReservation
	tombstones     []ReconcileTombstone

	mu             sync.Mutex
	committedCalls [][2]string // {senderDaemonID, reservationID}
	reclaimAcks    [][2]string // {senderDaemonID, tombstoneID}
	reconcilePulls [][]string  // activeCoords per pull
}

func (c *fakeStorageControl) Committed(_ context.Context, sender, reservationID string) (bool, bool, error) {
	if c.committedDelay > 0 {
		time.Sleep(c.committedDelay)
	}
	c.mu.Lock()
	c.committedCalls = append(c.committedCalls, [2]string{sender, reservationID})
	c.mu.Unlock()
	return true, false, nil
}
func (c *fakeStorageControl) ReclaimAck(_ context.Context, sender, tombstoneID string) (bool, error) {
	c.mu.Lock()
	c.reclaimAcks = append(c.reclaimAcks, [2]string{sender, tombstoneID})
	c.mu.Unlock()
	return c.reclaimFound, nil
}
func (c *fakeStorageControl) ReconcilePull(
	_ context.Context,
	_ string,
	activeCoords []string,
) ([]ReconcileResource, []ReconcileReservation, []ReconcileTombstone, error) {
	c.mu.Lock()
	c.reconcilePulls = append(c.reconcilePulls, append([]string(nil), activeCoords...))
	c.mu.Unlock()
	return c.resources, c.reservations, c.tombstones, nil
}

type acceptorRig struct {
	acc *Acceptor
	srv *httptest.Server
}

type acceptorRigConfig struct {
	probeInterval    time.Duration
	livenessTTL      time.Duration
	settlementWindow time.Duration
	joinWindow       time.Duration
	attachBinding    func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain, actorhost.Binding) error
	storage          StorageHostControl
	daemonID         func(*http.Request) string
	plan             func(context.Context, string) ([]platform.PlanActor, error)
}

func newAcceptorRig(t *testing.T, cfg acceptorRigConfig) *acceptorRig {
	t.Helper()
	attach := cfg.attachBinding
	if attach == nil {
		attach = func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain, actorhost.Binding) error {
			return nil
		}
	}
	storage := cfg.storage
	if storage == nil {
		storage = &fakeStorageControl{}
	}
	plan := cfg.plan
	if plan == nil {
		plan = func(context.Context, string) ([]platform.PlanActor, error) { return nil, nil }
	}
	acc, err := NewAcceptor(Config{
		Ingress:   fakeIngress{},
		ChannelID: "acceptor-test-channel",
		AuthorizeAttach: func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain) error {
			return nil
		},
		AttachBinding:      attach,
		BindingDown:        func(actor.ActorID, actorhost.Binding) {},
		StorageHostControl: storage,
		Plan:               plan,
		CanAttach:          func(context.Context, string) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewAcceptor: %v", err)
	}
	if cfg.probeInterval > 0 {
		acc.sessions.probeInterval = cfg.probeInterval
	}
	if cfg.livenessTTL > 0 {
		acc.sessions.livenessTTL = cfg.livenessTTL
	}
	if cfg.settlementWindow > 0 {
		acc.sessions.settlementWindow = cfg.settlementWindow
	}
	if cfg.joinWindow > 0 {
		acc.sessions.sessionJoinWindow = cfg.joinWindow
	}
	rig := &acceptorRig{acc: acc}
	rig.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		daemonID := "daemon-1"
		if cfg.daemonID != nil {
			daemonID = cfg.daemonID(req)
		}
		rig.acc.Serve(w, req, daemonID)
	}))
	t.Cleanup(func() {
		_ = rig.acc.Close()
		rig.srv.Close()
	})
	return rig
}

func (r *acceptorRig) wsURL() string { return "ws" + r.srv.URL[4:] }

func (r *acceptorRig) waitSession(
	t *testing.T,
	timeout time.Duration,
	want func(SessionSnapshot) bool,
) SessionSnapshot {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, snapshot := range r.acc.Sessions() {
			if want(snapshot) {
				return snapshot
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("session snapshot condition not reached; have %+v", r.acc.Sessions())
	return SessionSnapshot{}
}

// rawDaemon is a hand-driven daemon endpoint: it speaks the wire protocol
// directly so a test can choose exactly which frames it answers.
type rawDaemon struct {
	t              *testing.T
	ws             *websocket.Conn
	ys             *yamux.Session
	ctrl           net.Conn
	attachReplies  chan AttachReply
	resolveReplies chan ResolveCoordReply

	writeMu sync.Mutex
}

// dialRawCarrier establishes the carrier and control spine WITHOUT attaching,
// so a test can hand-craft its own attach frame (or none at all).
func dialRawCarrier(t *testing.T, wsURL string, answerProbes bool) *rawDaemon {
	t.Helper()
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}
	ys, err := yamux.Client(newWSByteStream(ws), linkYamuxConfig())
	if err != nil {
		t.Fatalf("yamux client: %v", err)
	}
	ctrl, err := ys.Open()
	if err != nil {
		t.Fatalf("open control stream: %v", err)
	}
	if err := writeStreamHeader(ctrl, streamControl); err != nil {
		t.Fatalf("control header: %v", err)
	}
	daemon := &rawDaemon{
		t: t, ws: ws, ys: ys, ctrl: ctrl,
		attachReplies:  make(chan AttachReply, 4),
		resolveReplies: make(chan ResolveCoordReply, 4),
	}
	t.Cleanup(func() {
		_ = ys.Close()
		_ = ws.Close()
	})
	go func() {
		decoder := json.NewDecoder(ctrl)
		for {
			var raw json.RawMessage
			if err := decoder.Decode(&raw); err != nil {
				return
			}
			frame, err := decodeControl(raw)
			if err != nil {
				continue
			}
			switch frame.Kind {
			case ctrlProbe:
				if answerProbes && frame.Probe != nil {
					reply, _ := encodeControl(controlFrame{
						Kind:       ctrlProbeReply,
						ProbeReply: &ProbeReply{Nonce: frame.Probe.Nonce},
					})
					daemon.send(reply)
				}
			case ctrlAttachReply:
				if frame.AttachReply != nil {
					select {
					case daemon.attachReplies <- *frame.AttachReply:
					default:
					}
				}
			case ctrlResolveCoordReply:
				laneFrame, laneErr := decodeLaneControl(raw)
				if laneErr == nil && laneFrame.ResolveCoordReply != nil {
					select {
					case daemon.resolveReplies <- *laneFrame.ResolveCoordReply:
					default:
					}
				}
			}
		}
	}()
	return daemon
}

func dialRawDaemon(t *testing.T, wsURL string, answerProbes bool) *rawDaemon {
	t.Helper()
	daemon := dialRawCarrier(t, wsURL, answerProbes)
	attachRaw, err := encodeControl(controlFrame{
		RequestID: "test-attach", Kind: ctrlAttach, Attach: &AttachRequest{Proto: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	daemon.send(attachRaw)
	reply := daemon.waitAttachReply()
	if !reply.Accepted {
		t.Fatalf("attach rejected: %s", reply.Reason)
	}
	return daemon
}

func (d *rawDaemon) waitAttachReply() AttachReply {
	d.t.Helper()
	select {
	case reply := <-d.attachReplies:
		return reply
	case <-time.After(3 * time.Second):
		d.t.Fatal("attach reply did not arrive")
		return AttachReply{}
	}
}

func (d *rawDaemon) waitResolveReply() ResolveCoordReply {
	d.t.Helper()
	select {
	case reply := <-d.resolveReplies:
		return reply
	case <-time.After(3 * time.Second):
		d.t.Fatal("resolve coord reply did not arrive")
		return ResolveCoordReply{}
	}
}

func (d *rawDaemon) send(raw []byte) {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	_, _ = d.ctrl.Write(append(raw, '\n'))
}

// A dead control reader is judged liveness_expired within the TTL.
func TestLivenessDeadControlReaderIsJudgedWithinTTL(t *testing.T) {
	rig := newAcceptorRig(t, acceptorRigConfig{
		probeInterval: 25 * time.Millisecond, livenessTTL: 150 * time.Millisecond,
	})
	dialRawDaemon(t, rig.wsURL(), false)
	snapshot := rig.waitSession(t, 5*time.Second, func(s SessionSnapshot) bool {
		return s.Key == "daemon-1" && s.State == SessionClosed
	})
	if snapshot.Reason != SessionLivenessExpired {
		t.Fatalf("reason=%s want liveness_expired", snapshot.Reason)
	}
}

// An idle session whose spine answers probes is never misjudged.
func TestLivenessIdleHealthySessionIsNotMisjudged(t *testing.T) {
	rig := newAcceptorRig(t, acceptorRigConfig{
		probeInterval: 40 * time.Millisecond, livenessTTL: 400 * time.Millisecond,
	})
	dialRawDaemon(t, rig.wsURL(), true)
	time.Sleep(1200 * time.Millisecond)
	rig.waitSession(t, time.Second, func(s SessionSnapshot) bool {
		return s.Key == "daemon-1" && s.State == SessionActive
	})
}

// A control task slower than the TTL does not kill a spine that keeps
// answering probes — probes bypass the task pool.
func TestLivenessSlowControlTaskDoesNotKillLiveSpine(t *testing.T) {
	rig := newAcceptorRig(t, acceptorRigConfig{
		probeInterval: 40 * time.Millisecond, livenessTTL: 400 * time.Millisecond,
		storage: &fakeStorageControl{committedDelay: 800 * time.Millisecond},
	})
	daemon := dialRawDaemon(t, rig.wsURL(), true)
	committed, err := encodeStorageControl(storageControlFrame{
		Kind:      ctrlCommitted,
		Committed: &Committed{RequestID: "req-slow", ReservationID: "resv-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	daemon.send(committed)
	time.Sleep(1000 * time.Millisecond)
	rig.waitSession(t, time.Second, func(s SessionSnapshot) bool {
		return s.Key == "daemon-1" && s.State == SessionActive
	})
}

// Busy control traffic does not refresh a spine that stopped answering
// probes — only probe replies feed lastSeen.
func TestLivenessBusyTrafficDoesNotRefreshDeadSpine(t *testing.T) {
	rig := newAcceptorRig(t, acceptorRigConfig{
		probeInterval: 25 * time.Millisecond, livenessTTL: 150 * time.Millisecond,
	})
	daemon := dialRawDaemon(t, rig.wsURL(), false)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(10 * time.Millisecond):
				raw, err := encodeControl(controlFrame{
					RequestID: "req-plan", Kind: ctrlPlanPull, PlanPull: &PlanPull{},
				})
				if err != nil {
					return
				}
				daemon.send(raw)
			}
		}
	}()
	snapshot := rig.waitSession(t, 5*time.Second, func(s SessionSnapshot) bool {
		return s.Key == "daemon-1" && s.State == SessionClosed
	})
	if snapshot.Reason != SessionLivenessExpired {
		t.Fatalf("reason=%s want liveness_expired", snapshot.Reason)
	}
}

// Route-publish sandwich: a displacement landing between the runtime commit
// and the Current postcheck is compensated — the exact binding is withdrawn
// and the compensation counter records it.
func TestRoutePublishDisplacedMidCommitIsCompensated(t *testing.T) {
	displaceReq := make(chan struct{})
	displaceDone := make(chan struct{})
	var once sync.Once
	var rig *acceptorRig
	rig = newAcceptorRig(t, acceptorRigConfig{
		attachBinding: func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain, actorhost.Binding) error {
			once.Do(func() {
				displaceReq <- struct{}{}
				<-displaceDone
			})
			return nil
		},
	})
	go func() {
		<-displaceReq
		successor, err := rig.acc.sessions.mint("daemon-1")
		if err == nil {
			_, err = rig.acc.sessions.activate(successor)
		}
		if err != nil {
			t.Errorf("displacement failed: %v", err)
		}
		close(displaceDone)
	}()

	daemon := dialRawDaemon(t, rig.wsURL(), true)
	stream, err := daemon.ys.Open()
	if err != nil {
		t.Fatalf("open actor stream: %v", err)
	}
	if err := writeStreamHeader(stream, streamActor); err != nil {
		t.Fatalf("actor header: %v", err)
	}
	codec := ipc.NewCodec(stream, stream)
	payload, err := json.Marshal(ipc.HandshakePayload{
		LeaseID:    "agent:test",
		AttemptKey: string(physicalKey(t)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := codec.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: payload}); err != nil {
		t.Fatalf("handshake write: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if rig.acc.compensated.Load() == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("compensated=%d want 1", rig.acc.compensated.Load())
}

// Kind boundary: a non-empty unknown kind is version-skew lifeline and is
// ignored; a frame with no kind at all is malformed and seals the session.
func TestUnknownKindIgnoredButMissingKindIsViolation(t *testing.T) {
	rig := newAcceptorRig(t, acceptorRigConfig{})
	daemon := dialRawDaemon(t, rig.wsURL(), true)
	daemon.send([]byte(`{"kind":"future_vocabulary"}`))
	time.Sleep(100 * time.Millisecond)
	rig.waitSession(t, time.Second, func(s SessionSnapshot) bool {
		return s.Key == "daemon-1" && s.State == SessionActive
	})
	daemon.send([]byte(`{"payload_without_kind":true}`))
	snapshot := rig.waitSession(t, 5*time.Second, func(s SessionSnapshot) bool {
		return s.Key == "daemon-1" && s.State == SessionClosed
	})
	if snapshot.Reason != SessionProtocolViolation {
		t.Fatalf("reason=%s want protocol_violation", snapshot.Reason)
	}
}

// Strict line: a frame claiming a known control kind with a malformed payload
// is a protocol violation and seals the session.
func TestMalformedKnownControlFrameIsProtocolViolation(t *testing.T) {
	rig := newAcceptorRig(t, acceptorRigConfig{})
	daemon := dialRawDaemon(t, rig.wsURL(), true)
	daemon.send([]byte(`{"kind":"plan_pull"}`))
	snapshot := rig.waitSession(t, 5*time.Second, func(s SessionSnapshot) bool {
		return s.Key == "daemon-1" && s.State == SessionClosed
	})
	if snapshot.Reason != SessionProtocolViolation {
		t.Fatalf("reason=%s want protocol_violation", snapshot.Reason)
	}
}

// Home side of the attach verdict point: an attach with no RequestID can
// never be correlated by the dialer, so it is malformed — not merely quiet.
func TestAttachMissingRequestIDIsProtocolViolation(t *testing.T) {
	rig := newAcceptorRig(t, acceptorRigConfig{})
	carrier := dialRawCarrier(t, rig.wsURL(), true)
	raw, err := encodeControl(controlFrame{Kind: ctrlAttach, Attach: &AttachRequest{Proto: 2}})
	if err != nil {
		t.Fatal(err)
	}
	carrier.send(raw)
	snapshot := rig.waitSession(t, 5*time.Second, func(s SessionSnapshot) bool {
		return s.Key == "daemon-1" && s.State == SessionClosed
	})
	if snapshot.Reason != SessionProtocolViolation {
		t.Fatalf("reason=%s want protocol_violation", snapshot.Reason)
	}
}

// Proto is the negotiated version field: an unknown value gets an honest
// rejection with attribution, never a silent compatibility path.
func TestAttachProtoMismatchIsRejectedWithAttribution(t *testing.T) {
	rig := newAcceptorRig(t, acceptorRigConfig{})
	carrier := dialRawCarrier(t, rig.wsURL(), true)
	raw, err := encodeControl(controlFrame{
		RequestID: "test-proto", Kind: ctrlAttach, Attach: &AttachRequest{Proto: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	carrier.send(raw)
	reply := carrier.waitAttachReply()
	if reply.Accepted || !strings.Contains(reply.Reason, "unsupported attach proto") {
		t.Fatalf("reply=%+v want proto rejection", reply)
	}
	snapshot := rig.waitSession(t, 5*time.Second, func(s SessionSnapshot) bool {
		return s.Key == "daemon-1" && s.State == SessionClosed
	})
	if snapshot.Reason != SessionAdmissionRejected {
		t.Fatalf("reason=%s want admission_rejected", snapshot.Reason)
	}
}

// A second control spine on one carrier breaks organ integrity.
func TestDuplicateControlSpineIsProtocolViolation(t *testing.T) {
	rig := newAcceptorRig(t, acceptorRigConfig{})
	daemon := dialRawDaemon(t, rig.wsURL(), true)
	second, err := daemon.ys.Open()
	if err != nil {
		t.Fatal(err)
	}
	if err := writeStreamHeader(second, streamControl); err != nil {
		t.Fatal(err)
	}
	snapshot := rig.waitSession(t, 5*time.Second, func(s SessionSnapshot) bool {
		return s.Key == "daemon-1" && s.State == SessionClosed
	})
	if snapshot.Reason != SessionProtocolViolation {
		t.Fatalf("reason=%s want protocol_violation", snapshot.Reason)
	}
}

// With a control task stuck far beyond every window, the seal collection
// still completes bounded and leaves one explicit abandoned account on the
// snapshot.
func TestCollectSessionBoundedWithStuckControlTask(t *testing.T) {
	rig := newAcceptorRig(t, acceptorRigConfig{
		settlementWindow: 100 * time.Millisecond,
		joinWindow:       200 * time.Millisecond,
		storage:          &fakeStorageControl{committedDelay: 5 * time.Second},
	})
	daemon := dialRawDaemon(t, rig.wsURL(), true)
	committed, err := encodeStorageControl(storageControlFrame{
		Kind:      ctrlCommitted,
		Committed: &Committed{RequestID: "req-stuck", ReservationID: "resv-stuck"},
	})
	if err != nil {
		t.Fatal(err)
	}
	daemon.send(committed)
	time.Sleep(150 * time.Millisecond)

	var generation SessionGeneration
	for _, snapshot := range rig.acc.Sessions() {
		if snapshot.Key == "daemon-1" && snapshot.State == SessionActive {
			generation = snapshot.Generation
		}
	}
	if generation == "" {
		t.Fatal("no active session to kick")
	}
	start := time.Now()
	if !rig.acc.KickSession(generation) {
		t.Fatal("kick was rejected")
	}
	snapshot := rig.waitSession(t, 3*time.Second, func(s SessionSnapshot) bool {
		return s.Generation == generation && s.State == SessionClosed
	})
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("collection took %v; windows are 100ms/200ms", elapsed)
	}
	if snapshot.Abandoned < 1 {
		t.Fatalf("abandoned=%d want >=1 for the stuck task", snapshot.Abandoned)
	}
}
