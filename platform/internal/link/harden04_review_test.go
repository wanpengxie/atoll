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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/access"
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
	probeInterval    time.Duration
	livenessTTL      time.Duration
	settlementWindow time.Duration
	joinWindow       time.Duration
	attachBinding    func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain, actorhost.Binding) error
	storage          StorageHostControl
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
	if cfg.settlementWindow > 0 {
		acc.sessions.settlementWindow = cfg.settlementWindow
	}
	if cfg.joinWindow > 0 {
		acc.sessions.sessionJoinWindow = cfg.joinWindow
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
	t             *testing.T
	ws            *websocket.Conn
	ys            *yamux.Session
	ctrl          net.Conn
	attachReplies chan AttachReply

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
		attachReplies: make(chan AttachReply, 4),
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
			}
		}
	}()
	return daemon
}

func dialRawDaemon(t *testing.T, wsURL string, answerProbes bool) *rawDaemon {
	t.Helper()
	daemon := dialRawCarrier(t, wsURL, answerProbes)
	attachRaw, err := encodeControl(controlFrame{
		RequestID: "review-attach", Kind: ctrlAttach, Attach: &AttachRequest{Proto: 2},
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
// expired timer must not write any verdict.
func TestCandidateTimerCallbackIsConditionalOnCandidateState(t *testing.T) {
	registry := newSessionRegistry(nil)
	record := activeRecord(t, registry, "daemon-a")
	registry.reportIfCandidate(record, SessionHandshakeTimeout, "fired_after_activate")
	select {
	case <-record.sealed:
		t.Fatal("active session was sealed by a stale candidate timer")
	default:
	}
	if !registry.admit(record).allows() {
		t.Fatal("stale candidate timer cut admission")
	}
}

// Evidence is never parked where later evidence can be lost behind it: a
// stale candidate-only report on an active session writes nothing, and the
// next real reason still lands with its own attribution.
func TestRealEvidenceStillLandsAfterStaleCandidateReport(t *testing.T) {
	registry := newSessionRegistry(nil)
	record := activeRecord(t, registry, "daemon-a")
	record.report(SessionHandshakeTimeout, "stale_timer", nil)
	select {
	case <-record.sealed:
		t.Fatal("stale candidate evidence sealed an active session")
	default:
	}
	record.report(SessionRevoked, "real_reason", nil)
	select {
	case <-record.sealed:
	default:
		t.Fatal("real evidence was lost behind the stale report")
	}
	if reason := sealedReason(t, registry, record.generation); reason != SessionRevoked {
		t.Fatalf("reason=%s want revoked", reason)
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

// Kind boundary: a non-empty unknown kind is version-skew lifeline and is
// ignored; a frame with no kind at all is malformed and seals the session.
func TestUnknownKindIgnoredButMissingKindIsViolation(t *testing.T) {
	rig := newReviewRig(t, reviewRigConfig{})
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

// The process-level shared ledger is the one remote session truth: Dial
// refuses to manufacture a private registry per connection.
func TestDialRequiresSharedSessionLedger(t *testing.T) {
	if _, err := Dial(context.Background(), "ws://127.0.0.1:1", DialConfig{}, nil); err == nil ||
		err.Error() != "link: DialConfig.SessionLedger is required" {
		t.Fatalf("err=%v want ledger required", err)
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

// Verdict-once: an open that passed the admission gate is in-flight work and
// is never revoked by a second reading when a seal lands mid-open; the
// closed-gate handoff and shutdown own its collection instead.
func TestInFlightOpenSurvivesSealVerdict(t *testing.T) {
	registry := newSessionRegistry(nil)
	record := activeRecord(t, registry, "daemon-a")
	key := physicalKey(t)
	gate := make(chan struct{})
	entered := make(chan struct{})
	session, err := NewAuthenticatedLinkSession(AuthenticatedLinkSessionConfig{
		Peer:      "daemon-a",
		Authority: authorityPair(registry, record),
		OpenActorStream: func(context.Context, actor.ActorID, actorhost.AttemptKey) (ActorStreamResource, error) {
			close(entered)
			<-gate
			done := make(chan struct{})
			var once sync.Once
			return ActorStreamResource{
				Arms: physicalArms(),
				Close: func() error {
					once.Do(func() { close(done) })
					return nil
				},
				Done: done,
			}, nil
		},
		CloseTransport: func() error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	type opened struct {
		stream *ActorStream
		err    error
	}
	result := make(chan opened, 1)
	go func() {
		stream, openErr := session.OpenActorStream(context.Background(), "agent:a", key)
		result <- opened{stream: stream, err: openErr}
	}()
	<-entered
	if registry.beginSeal(record, sessionEvidence{reason: SessionRevoked}) != sealCommitted {
		t.Fatal("seal did not commit")
	}
	close(gate)
	got := <-result
	if got.err != nil {
		t.Fatalf("in-flight open was revoked by a second verdict: %v", got.err)
	}
	_ = session.Close()
	select {
	case <-got.stream.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not collect the in-flight stream")
	}
}

// Home side of the attach verdict point: an attach with no RequestID can
// never be correlated by the dialer, so it is malformed — not merely quiet.
func TestAttachMissingRequestIDIsProtocolViolation(t *testing.T) {
	rig := newReviewRig(t, reviewRigConfig{})
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
	rig := newReviewRig(t, reviewRigConfig{})
	carrier := dialRawCarrier(t, rig.wsURL(), true)
	raw, err := encodeControl(controlFrame{
		RequestID: "review-proto", Kind: ctrlAttach, Attach: &AttachRequest{Proto: 1},
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

// The transport-contract write budget: a stream whose peer stops reading is
// judged dead water within the budget and only that stream is closed.
func TestWriteBudgetJudgesDeadWaterStreamLocally(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	bounded := &boundedConn{Conn: left, budget: 50 * time.Millisecond}
	if _, err := bounded.Write(make([]byte, 1024)); err == nil {
		t.Fatal("write into a dead-water pipe did not hit the budget")
	}
	if _, err := left.Write([]byte("x")); err == nil {
		t.Fatal("dead-water stream was not closed after the budget verdict")
	}
}

// Phase B acceptance: with a control task stuck far beyond every window, the
// seal collection still completes bounded and leaves one explicit abandoned
// account on the snapshot.
func TestCollectSessionBoundedWithStuckControlTask(t *testing.T) {
	rig := newReviewRig(t, reviewRigConfig{
		settlementWindow: 100 * time.Millisecond,
		joinWindow:       200 * time.Millisecond,
		storage:          &reviewStorageControl{committedDelay: 5 * time.Second},
	})
	daemon := dialRawDaemon(t, rig.wsURL(), true)
	committed, err := encodeStorageControl(storageControlFrame{
		Kind:      ctrlCommitted,
		Committed: &Committed{RequestID: "review-stuck", ReservationID: "resv-stuck"},
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

// A panicking control handler is a local fault, never an unrecovered crash.
func TestControlHandlerPanicIsLocalFault(t *testing.T) {
	carrierA, carrierB := net.Pipe()
	client, err := yamux.Client(carrierA, linkYamuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	server, err := yamux.Server(carrierB, linkYamuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer server.Close()
	evidence := make(chan SessionEndReason, 1)
	ls := newLinkSession(client,
		func([]byte) { panic("review-boom") }, nil, nil, nil,
		func(reason SessionEndReason, _ string, _ error) { evidence <- reason }, nil)
	reader, writer := net.Pipe()
	defer reader.Close()
	go ls.readControl(reader)
	if _, err := writer.Write(append(encodePlanPoke(), '\n')); err != nil {
		t.Fatal(err)
	}
	select {
	case reason := <-evidence:
		if reason != SessionLocalFault {
			t.Fatalf("reason=%s want local_fault", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("panic was not reported as local fault")
	}
}

// A second control spine on one carrier breaks organ integrity.
func TestDuplicateControlSpineIsProtocolViolation(t *testing.T) {
	rig := newReviewRig(t, reviewRigConfig{})
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

// Stage-3 settlement of an abandoned open: the worker that completes after
// its caller left closes the stream itself and accounts a late close.
func TestLateOpenIsReapedAndAccounted(t *testing.T) {
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
	defer client.Close()
	defer server.Close()
	ls := newLinkSession(client, nil, nil, nil, nil,
		func(SessionEndReason, string, error) {}, nil)

	first, err := ls.openTagged(context.Background(), streamActor)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	defer first.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := ls.openTagged(ctx, streamActor); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second open err=%v want deadline", err)
	}
	// Now let the backlog drain: accepting the first stream ACKs it and
	// unblocks the abandoned worker, which must reap its own late stream.
	go func() {
		for {
			if _, acceptErr := server.AcceptStream(); acceptErr != nil {
				return
			}
		}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, late := ls.openCounts(); late == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	inFlight, late := ls.openCounts()
	t.Fatalf("late close not accounted: in_flight=%d late=%d", inFlight, late)
}

// A zombie control task is accounted exactly once across its abandon timer
// and a later drain timeout.
func TestZombieControlTaskIsCountedOnce(t *testing.T) {
	pool := newControlTaskPool(nil, nil)
	pool.abandonAfter = 50 * time.Millisecond
	release := make(chan struct{})
	defer close(release)
	pool.submit(func() { <-release }, nil)
	time.Sleep(150 * time.Millisecond)
	if _, abandoned := pool.drain(50 * time.Millisecond); abandoned != 1 {
		t.Fatalf("one zombie accounted %d times", abandoned)
	}
}

// A lane transfer ticket is single-use: consumed at its first valid
// redemption, so a replay within the TTL finds nothing.
func TestLaneTransferTokenIsSingleUse(t *testing.T) {
	acc := &Acceptor{lane: newLaneState(), sessions: newSessionRegistry(nil)}
	token, err := acc.OpenLaneTransfer(
		context.Background(), "target-daemon", "req-daemon", "coord-1", access.OpRead, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	redeem := func() laneAck {
		home, daemon := net.Pipe()
		defer daemon.Close()
		go acc.handleLaneRedeem("req-daemon", home)
		if err := writeLaneJSON(daemon, laneRedeemHeader{Token: token}); err != nil {
			t.Fatal(err)
		}
		var ack laneAck
		if err := readLaneJSON(daemon, &ack); err != nil {
			t.Fatal(err)
		}
		return ack
	}
	first := redeem()
	if first.OK || !strings.Contains(first.Reason, "no live link") {
		t.Fatalf("first redemption ack=%+v want target-unreachable", first)
	}
	second := redeem()
	if second.OK || !strings.Contains(second.Reason, "unknown or mismatched") {
		t.Fatalf("replayed ticket was honored: ack=%+v", second)
	}
}
