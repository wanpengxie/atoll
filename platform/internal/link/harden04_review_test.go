package link

// Review regressions for the harden04 carrier-session build:
//   - Phase C liveness acceptance (dead reader judged, idle healthy spared,
//     slow control task spared, busy data does not refresh a dead spine)
//   - route-publish sandwich compensation when displaced mid-commit
//   - seal unblocks a stuck open worker
//   - candidate-only evidence is a locked conditional write
//   - physical close gates late child registration
//   - a malformed known-kind control frame is a protocol violation

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/ipc"
	"github.com/wanpengxie/atoll/runtime/remoteingress"
)

type reviewIngress struct{}

func (reviewIngress) Emit(
	context.Context,
	actor.ActorID,
	actorhost.AttemptKey,
	*message.Envelope,
) (harness.WriteResult, error) {
	return harness.WriteResult{}, nil
}

func (reviewIngress) Access(
	context.Context,
	actor.ActorID,
	actorhost.AttemptKey,
	remoteingress.AccessRequest,
) (remoteingress.AccessResponse, error) {
	return remoteingress.AccessResponse{}, nil
}

func (reviewIngress) Schedule(
	context.Context,
	actor.ActorID,
	remoteingress.ScheduleRequest,
) (remoteingress.ScheduleResponse, error) {
	return remoteingress.ScheduleResponse{ID: "review-timer"}, nil
}

func (reviewIngress) Fork(
	context.Context,
	actor.ActorID,
	actorhost.AttemptKey,
	remoteingress.ForkRequest,
) (actor.ActorID, error) {
	return "agent:child", nil
}

func (reviewIngress) EndSelf(
	context.Context,
	actor.ActorID,
	actorhost.AttemptKey,
	actorcaps.EndSelfRequest,
) error {
	return nil
}

type reviewStorageControl struct{ committedDelay time.Duration }

func (c *reviewStorageControl) Committed(context.Context, string, string) (bool, bool, error) {
	if c.committedDelay > 0 {
		time.Sleep(c.committedDelay)
	}
	return true, false, nil
}
func (*reviewStorageControl) ReclaimAck(context.Context, string, string) (bool, error) {
	return false, nil
}
func (*reviewStorageControl) ReconcilePull(
	context.Context,
	string,
	[]string,
) ([]ReconcileResource, []ReconcileReservation, []ReconcileTombstone, error) {
	return nil, nil, nil, nil
}

type reviewRig struct {
	acc *Acceptor
	srv *httptest.Server
}

type reviewRigConfig struct {
	probeInterval time.Duration
	livenessTTL   time.Duration
	attachBinding func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain, actorhost.Binding) error
	storage       StorageHostControl
}

func newReviewRig(t *testing.T, cfg reviewRigConfig) *reviewRig {
	t.Helper()
	attach := cfg.attachBinding
	if attach == nil {
		attach = func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain, actorhost.Binding) error {
			return nil
		}
	}
	storage := cfg.storage
	if storage == nil {
		storage = &reviewStorageControl{}
	}
	acc, err := NewAcceptor(Config{
		Ingress:   reviewIngress{},
		ChannelID: "review-channel",
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
	if cfg.probeInterval > 0 {
		acc.sessions.probeInterval = cfg.probeInterval
	}
	if cfg.livenessTTL > 0 {
		acc.sessions.livenessTTL = cfg.livenessTTL
	}
	rig := &reviewRig{acc: acc}
	rig.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		rig.acc.Serve(w, req, "daemon-1")
	}))
	t.Cleanup(func() {
		_ = rig.acc.Close()
		rig.srv.Close()
	})
	return rig
}

func (r *reviewRig) wsURL() string { return "ws" + r.srv.URL[4:] }

func (r *reviewRig) waitSession(
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
	t    *testing.T
	ws   *websocket.Conn
	ys   *yamux.Session
	ctrl net.Conn

	writeMu sync.Mutex
}

func dialRawDaemon(t *testing.T, wsURL string, answerProbes bool) *rawDaemon {
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
	daemon := &rawDaemon{t: t, ws: ws, ys: ys, ctrl: ctrl}
	t.Cleanup(func() {
		_ = ys.Close()
		_ = ws.Close()
	})

	attachRaw, err := encodeControl(controlFrame{
		RequestID: "review-attach", Kind: ctrlAttach, Attach: &AttachRequest{Proto: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	daemon.send(attachRaw)

	attached := make(chan AttachReply, 1)
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
					case attached <- *frame.AttachReply:
					default:
					}
				}
			}
		}
	}()

	select {
	case reply := <-attached:
		if !reply.Accepted {
			t.Fatalf("attach rejected: %s", reply.Reason)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("attach reply did not arrive")
	}
	return daemon
}

func (d *rawDaemon) send(raw []byte) {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	_, _ = d.ctrl.Write(append(raw, '\n'))
}

// Phase C acceptance 1: a dead control reader is judged liveness_expired
// within the TTL.
func TestLivenessDeadControlReaderIsJudgedWithinTTL(t *testing.T) {
	rig := newReviewRig(t, reviewRigConfig{
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

// Phase C acceptance 2: an idle session whose spine answers probes is never
// misjudged.
func TestLivenessIdleHealthySessionIsNotMisjudged(t *testing.T) {
	rig := newReviewRig(t, reviewRigConfig{
		probeInterval: 40 * time.Millisecond, livenessTTL: 400 * time.Millisecond,
	})
	dialRawDaemon(t, rig.wsURL(), true)
	time.Sleep(1200 * time.Millisecond)
	rig.waitSession(t, time.Second, func(s SessionSnapshot) bool {
		return s.Key == "daemon-1" && s.State == SessionActive
	})
}

// Phase C acceptance 3: a control task slower than the TTL does not kill a
// spine that keeps answering probes — probes bypass the task pool.
func TestLivenessSlowControlTaskDoesNotKillLiveSpine(t *testing.T) {
	rig := newReviewRig(t, reviewRigConfig{
		probeInterval: 40 * time.Millisecond, livenessTTL: 400 * time.Millisecond,
		storage: &reviewStorageControl{committedDelay: 800 * time.Millisecond},
	})
	daemon := dialRawDaemon(t, rig.wsURL(), true)
	committed, err := encodeStorageControl(storageControlFrame{
		Kind:      ctrlCommitted,
		Committed: &Committed{RequestID: "review-slow", ReservationID: "resv-1"},
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

// Phase C acceptance 4: busy control traffic does not refresh a spine that
// stopped answering probes — only probe replies feed lastSeen.
func TestLivenessBusyTrafficDoesNotRefreshDeadSpine(t *testing.T) {
	rig := newReviewRig(t, reviewRigConfig{
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
					RequestID: "review-plan", Kind: ctrlPlanPull, PlanPull: &PlanPull{},
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
	var rig *reviewRig
	rig = newReviewRig(t, reviewRigConfig{
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
		LeaseID:    "agent:review",
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

// Seal unblocking: an open worker stuck inside the library's blocking Open is
// popped out by closing the carrier, and the mechanism joins afterwards.
func TestSealUnblocksStuckOpenWorker(t *testing.T) {
	carrierA, carrierB := net.Pipe()
	clientCfg := linkYamuxConfig()
	clientCfg.AcceptBacklog = 1
	client, err := yamux.Client(carrierA, clientCfg)
	if err != nil {
		t.Fatal(err)
	}
	server, err := yamux.Server(carrierB, linkYamuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close() // never Accepts: unacked SYNs hold the client backlog
	ls := newLinkSession(client, nil, nil, nil, nil,
		func(SessionEndReason, string, error) {}, nil)

	first, err := ls.openTagged(context.Background(), streamActor)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	defer first.Close()

	second := make(chan error, 1)
	go func() {
		_, err := ls.openTagged(context.Background(), streamActor)
		second <- err
	}()
	select {
	case err := <-second:
		t.Fatalf("second open did not block on the SYN backlog: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	_ = ls.closeCarrier()
	select {
	case err := <-second:
		if err == nil {
			t.Fatal("stuck open returned a live stream after seal")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("carrier close did not unblock the stuck open worker")
	}
	if !ls.waitWorkers(2 * time.Second) {
		t.Fatal("open worker did not join after carrier close")
	}
}

// Candidate-only evidence is confirmed under the same lock that writes the
// verdict: a handshake timeout racing a completed activate is refused as
// stale, and the session remains alive for real evidence.
func TestStaleCandidateEvidenceDoesNotSealActiveSession(t *testing.T) {
	registry := newSessionRegistry(nil)
	record := activeRecord(t, registry, "daemon-a")
	if got := registry.beginSeal(record, sessionEvidence{
		reason: SessionHandshakeTimeout, detail: "stale_timer",
	}); got != sealStaleEvidence {
		t.Fatalf("handshake timeout on active session sealed: verdict=%d", got)
	}
	if got := registry.beginSeal(record, sessionEvidence{
		reason: SessionAdmissionRejected, detail: "stale_reject",
	}); got != sealStaleEvidence {
		t.Fatalf("admission rejection on active session sealed: verdict=%d", got)
	}
	if !registry.admit(record).allows() {
		t.Fatal("stale evidence cut admission")
	}
	if got := registry.beginSeal(record, sessionEvidence{
		reason: SessionCarrierLost,
	}); got != sealCommitted {
		t.Fatalf("real evidence refused after stale refusals: verdict=%d", got)
	}
}

// The candidate TTL callback itself is conditional: an activated session's
// expired timer must not enqueue candidate-only evidence.
func TestCandidateTimerCallbackIsConditionalOnCandidateState(t *testing.T) {
	registry := newSessionRegistry(nil)
	record := activeRecord(t, registry, "daemon-a")
	registry.reportIfCandidate(record, SessionHandshakeTimeout, "fired_after_activate")
	select {
	case evidence := <-record.death:
		t.Fatalf("active session received candidate evidence: %+v", evidence)
	default:
	}
}

// Physical close gates late child registration in the same critical section
// as the shutdown snapshot: a binding arriving after Close is rejected even
// while the ledger still admits.
func TestPhysicalCloseGatesLateChildRegistration(t *testing.T) {
	registry := newSessionRegistry(nil)
	record := activeRecord(t, registry, "daemon-a")
	session, err := NewAuthenticatedLinkSession(AuthenticatedLinkSessionConfig{
		Peer:      "daemon-a",
		Authority: authorityPair(registry, record),
		OpenActorStream: func(context.Context, actor.ActorID, actorhost.AttemptKey) (ActorStreamResource, error) {
			return ActorStreamResource{Arms: physicalArms(), Close: func() error { return nil }}, nil
		},
		CloseTransport: func() error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = session.Close()

	if _, err := session.NewBinding(BindingConfig{
		Endpoint: newPhysicalEndpoint(),
		Close:    func() error { return nil },
	}); !errors.Is(err, ErrPhysicalSessionClosed) {
		t.Fatalf("late binding registration err=%v want closed", err)
	}
	if _, err := session.OpenActorStream(
		context.Background(), "agent:late", physicalKey(t),
	); !errors.Is(err, ErrPhysicalSessionClosed) {
		t.Fatalf("late stream open err=%v want closed", err)
	}
	if !registry.admit(record).allows() {
		t.Fatal("ledger admission changed; the local closed gate was not what rejected")
	}
	select {
	case <-session.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("session did not collect after close")
	}
}

// Strict line: a frame claiming a known control kind with a malformed payload
// is a protocol violation and seals the session.
func TestMalformedKnownControlFrameIsProtocolViolation(t *testing.T) {
	rig := newReviewRig(t, reviewRigConfig{})
	daemon := dialRawDaemon(t, rig.wsURL(), true)
	daemon.send([]byte(`{"kind":"plan_pull"}`))
	snapshot := rig.waitSession(t, 5*time.Second, func(s SessionSnapshot) bool {
		return s.Key == "daemon-1" && s.State == SessionClosed
	})
	if snapshot.Reason != SessionProtocolViolation {
		t.Fatalf("reason=%s want protocol_violation", snapshot.Reason)
	}
}
