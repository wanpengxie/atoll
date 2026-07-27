package link

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/ipc"
	"github.com/wanpengxie/atoll/runtime/remoteingress"
)

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

const attachHandshakeTimeout = 10 * time.Second

// Config supplies the Server composition seams. The Acceptor owns physical
// sessions and exact Bindings only.
//
// Substrate capability work enters through ONE seam: Ingress. The link holds no
// Controller, no minter and no resolver — it cannot admit, mint or route, and
// therefore cannot assemble a judgment out of parts. It decodes a frame, calls
// the ingress with the coordinate its own authenticated endpoint carries, and
// encodes the answer.
type Config struct {
	Ingress   remoteingress.RemoteIngress
	ChannelID channel.ID
	Logger    *slog.Logger

	AuthorizeAttach func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain) error
	AttachBinding   func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain, actorhost.Binding) error
	BindingDown     func(actor.ActorID, actorhost.Binding)
	Observe         func(actor.ActorID, actorhost.AttemptKey, actorhost.Binding, actorrt.ObsKind, actorrt.ObsValue)
	ObserveDown     func(actor.ActorID, actorhost.AttemptKey, actorhost.Binding)
	CancelRequest   func(actor.ActorID, message.ID)

	StorageHostControl StorageHostControl
	Plan               func(context.Context, string) ([]platform.PlanActor, error)
	CanAttach          func(context.Context, string) error
}

// Acceptor owns authenticated daemon physical links. Link loss changes
// attachment/presence and exact Binding only; it never mutates actor desired
// lifecycle or replaces an incarnation.
type Acceptor struct {
	ingress   remoteingress.RemoteIngress
	channelID channel.ID
	logger    *slog.Logger

	authorizeAttach func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain) error
	attachBinding   func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain, actorhost.Binding) error
	bindingDown     func(actor.ActorID, actorhost.Binding)
	observe         func(actor.ActorID, actorhost.AttemptKey, actorhost.Binding, actorrt.ObsKind, actorrt.ObsValue)
	observeDown     func(actor.ActorID, actorhost.AttemptKey, actorhost.Binding)
	cancelReq       func(actor.ActorID, message.ID)

	storageControl StorageHostControl
	plan           func(context.Context, string) ([]platform.PlanActor, error)
	canAttach      func(context.Context, string) error

	ctx    context.Context
	cancel context.CancelFunc

	admissionMu  sync.Mutex
	closed       bool
	wg           sync.WaitGroup
	closeOnce    sync.Once
	closeDone    chan struct{}
	leaked       atomic.Int64
	compensated  atomic.Int64
	lateRejected atomic.Int64

	pendingAlloc   *pendingReplies[AllocReply]
	pendingReclaim *pendingReplies[ReclaimReply]
	sessions       *sessionRegistry
	lane           *laneState
}

type linkHandle struct {
	send     func([]byte) error
	openLane func(context.Context) (net.Conn, error)
}

func (h *linkHandle) sendControl(raw []byte) error {
	if h == nil || h.send == nil {
		return errLinkClosed
	}
	return h.send(raw)
}

func NewAcceptor(cfg Config) (*Acceptor, error) {
	switch {
	case cfg.Ingress == nil:
		return nil, errors.New("link: remote ingress is required")
	case cfg.AuthorizeAttach == nil || cfg.AttachBinding == nil || cfg.BindingDown == nil:
		return nil, errors.New("link: exact binding callbacks are required")
	case cfg.Plan == nil || cfg.CanAttach == nil:
		return nil, errors.New("link: daemon plan/admission callbacks are required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ctx, cancel := context.WithCancel(context.Background())
	acceptor := &Acceptor{
		ingress:         cfg.Ingress,
		channelID:       cfg.ChannelID,
		logger:          logger,
		authorizeAttach: cfg.AuthorizeAttach, attachBinding: cfg.AttachBinding,
		bindingDown: cfg.BindingDown,
		observe:     cfg.Observe, observeDown: cfg.ObserveDown, cancelReq: cfg.CancelRequest,
		storageControl: cfg.StorageHostControl, plan: cfg.Plan, canAttach: cfg.CanAttach,
		ctx: ctx, cancel: cancel, closeDone: make(chan struct{}),
		pendingAlloc:   newPendingReplies[AllocReply](),
		pendingReclaim: newPendingReplies[ReclaimReply](),
		lane:           newLaneState(),
	}
	acceptor.sessions = newSessionRegistry(logger)
	return acceptor, nil
}

func (a *Acceptor) Serve(w http.ResponseWriter, r *http.Request, daemonID string) {
	if daemonID == "" {
		http.Error(w, "authenticated daemon id required", http.StatusUnauthorized)
		return
	}
	if !a.beginServe() {
		http.Error(w, "link acceptor closed", http.StatusServiceUnavailable)
		return
	}
	defer a.wg.Done()
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	a.runLink(r.Context(), ws, daemonID)
}

func (a *Acceptor) beginServe() bool {
	a.admissionMu.Lock()
	defer a.admissionMu.Unlock()
	if a.closed {
		return false
	}
	a.wg.Add(1)
	return true
}

func (a *Acceptor) runLink(reqCtx context.Context, ws *websocket.Conn, daemonID string) {
	defer ws.Close()
	peer := actorhost.ExecutionDomain(daemonID)
	record, err := a.sessions.mint(daemonID)
	if err != nil {
		a.logger.Error("link.session_mint_failed", "key", daemonID, "error", err)
		return
	}

	var physical *AuthenticatedLinkSession
	var lc *linkSession
	authority := authorityPair(a.sessions, record)

	handlers := serverActorHandlers{
		emit:          a.emit,
		access:        a.relayAccess,
		schedule:      a.relaySchedule,
		fork:          a.ingress.Fork,
		endSelf:       a.ingress.EndSelf,
		cancelRequest: a.cancelReq,
		deliverResult: func(id actor.ActorID, request message.ID, outcome, detail string) {
			a.logger.Warn("platform.delivery.remote_outcome",
				"actor", id, "request", request, "outcome", outcome, "detail", detail)
		},
	}

	onActor := func(conn net.Conn) {
		_ = conn.SetReadDeadline(time.Now().Add(attachHandshakeTimeout))
		codec := ipc.NewCodec(conn, conn)
		frame, err := codec.Read()
		if err != nil || frame.Kind != ipc.KindHandshake {
			_ = conn.Close()
			return
		}
		var handshake ipc.HandshakePayload
		if err := json.Unmarshal(frame.Payload, &handshake); err != nil {
			_ = conn.Close()
			return
		}
		id := actor.ActorID(handshake.LeaseID)
		key, err := actorhost.ParseAttemptKey(handshake.AttemptKey)
		if err != nil {
			_ = conn.Close()
			return
		}
		if !authority.admits() || a.authorizeAttach(id, key, peer) != nil {
			_ = conn.Close()
			a.logLateReject(record, "actor_admission")
			return
		}
		_ = conn.SetReadDeadline(time.Time{})
		routeHandlers := handlers
		var hostBinding actorhost.Binding
		routeHandlers.obs = func(
			obsID actor.ActorID,
			obsKey actorhost.AttemptKey,
			kind actorrt.ObsKind,
			value actorrt.ObsValue,
		) {
			if a.observe != nil {
				a.observe(obsID, obsKey, hostBinding, kind, value)
			}
		}
		endpoint := newServerActorEndpoint(reqCtx, id, key, conn, codec, routeHandlers)
		var binding *Binding
		binding, err = physical.NewBinding(BindingConfig{
			Endpoint: endpoint,
			Run:      endpoint.Run,
			Close:    endpoint.Close,
			BeforeStart: func(exact *Binding) {
				hostBinding = exact.HostBinding()
			},
			OnDown: func(exact *Binding, runErr error) {
				a.bindingDown(id, exact.HostBinding())
				if a.observeDown != nil {
					a.observeDown(id, key, exact.HostBinding())
				}
				if runErr != nil && !errors.Is(runErr, context.Canceled) {
					a.logger.Debug("link.actor_binding_down", "actor", id, "err", runErr)
				}
			},
		})
		if err != nil {
			_ = endpoint.Close()
			return
		}
		// Route publication is convergent: Current precheck, runtime commit,
		// Current postcheck, exact compensation.
		if !authority.isCurrent() {
			a.logLateReject(record, "route_publish_precheck")
			_ = binding.Close()
			return
		}
		if err := a.attachBinding(id, key, peer, binding.HostBinding()); err != nil {
			_ = binding.Close()
			return
		}
		if !authority.isCurrent() {
			a.compensated.Add(1)
			a.logger.Warn("link.route_publish_compensated",
				"generation", record.generation, "key", record.key,
				"actor", id, "compensated", a.compensated.Load())
			_ = binding.Close()
		}
	}

	onLane := func(conn net.Conn) {
		if !authority.admits() {
			_ = conn.Close()
			a.logLateReject(record, "lane_admission")
			return
		}
		a.handleLaneRedeem(daemonID, conn)
	}

	router, err := buildHomeControlRouter(a, reqCtx, record, authority.admits)
	if err != nil {
		record.report(SessionLocalFault, "control_table_invalid", err)
		a.sessions.completeSeal(record, 0)
		return
	}
	onControl := func(payload []byte) {
		router.dispatch(controlDispatchInput{
			peerID: daemonID, session: record, link: lc,
		}, payload)
	}

	sessionLogger := a.logger.With(
		"generation", record.generation,
		"key", record.key,
	)
	lc, err = acceptLinkSession(
		ws, onControl, onActor, onLane,
		func() { a.sessions.touch(record, time.Now()) },
		func(reason SessionEndReason, detail string, evidenceErr error) {
			record.report(reason, detail, evidenceErr)
		},
		sessionLogger,
	)
	if err != nil {
		a.sessions.beginSeal(record, sessionEvidence{
			reason: SessionCarrierLost, detail: "carrier_accept_setup_failed", err: err,
		})
		a.sessions.completeSeal(record, 0)
		return
	}
	physical, err = NewAuthenticatedLinkSession(AuthenticatedLinkSessionConfig{
		Peer: peer, Authority: authority, CloseTransport: lc.closeCarrier,
	})
	if err != nil {
		record.report(SessionLocalFault, "physical_owner_setup_failed", err)
		_ = lc.closeCarrier()
		a.sessions.completeSeal(record, 0)
		return
	}
	handle := &linkHandle{
		send: lc.sendControl, openLane: lc.openLane,
	}
	record.setHandle(handle)
	lc.start()

	// report performs the locked decision write itself, so a verdict written by
	// any evidence source (probe watchdog, control reader, kick, task pool) is
	// already in the ledger when sealed closes; this loop only supplies the
	// physical observations and then collects. When a precise reason and its
	// physical consequence race, whichever verdict committed first is kept —
	// beginSeal refuses the loser as already decided.
	select {
	case <-a.ctx.Done():
		record.report(SessionRevoked, "acceptor_shutdown", nil)
	case <-lc.closed():
		record.report(SessionCarrierLost, "carrier_closed", nil)
	case <-record.sealed:
	}
	a.collectSession(record, physical, lc)
}

func (a *Acceptor) logLateReject(record *sessionRecord, gate string) {
	count := a.lateRejected.Add(1)
	a.logger.Info("link.session_late_rejected",
		"generation", record.generation, "key", record.key,
		"gate", gate, "rejected", count)
}

// collectSession runs after the verdict is already in the ledger: it performs
// the bounded physical teardown and the closing→closed completion write.
func (a *Acceptor) collectSession(
	record *sessionRecord,
	physical *AuthenticatedLinkSession,
	lc *linkSession,
) {
	start := time.Now()
	joinedTasks, abandoned := lc.drainControlTasks(a.sessions.settlementWindow)
	a.logger.Info("link.session_seal_step",
		"generation", record.generation, "key", record.key,
		"step", "settlement", "joined", joinedTasks, "elapsed", time.Since(start))
	_ = physical.Close()
	a.logger.Info("link.session_seal_step",
		"generation", record.generation, "key", record.key,
		"step", "carrier_closed", "elapsed", time.Since(start))
	if !lc.waitWorkers(a.sessions.sessionJoinWindow) {
		abandoned++
		a.logger.Warn("link.session_mechanism_abandoned",
			"generation", record.generation, "key", record.key)
	}
	select {
	case <-physical.Done():
	case <-time.After(a.sessions.sessionJoinWindow):
		abandoned++
		a.logger.Warn("link.session_children_abandoned",
			"generation", record.generation, "key", record.key)
	}
	a.sessions.completeSeal(record, abandoned)
}

func (a *Acceptor) probeSession(record *sessionRecord, lc *linkSession) {
	ticker := time.NewTicker(a.sessions.probeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-record.done:
			return
		case <-ticker.C:
			lastSeen, active := a.sessions.lastSeen(record)
			if !active {
				return
			}
			if time.Since(lastSeen) >= a.sessions.livenessTTL {
				record.report(SessionLivenessExpired, "control_probe_ttl_expired", nil)
				return
			}
			a.logger.Debug("link.session_liveness",
				"generation", record.generation, "key", record.key,
				"since_last_seen", time.Since(lastSeen),
				"ttl_remaining", a.sessions.livenessTTL-time.Since(lastSeen))
			nonce, err := mintSessionGeneration()
			if err != nil {
				record.report(SessionLocalFault, "probe_nonce_mint_failed", err)
				return
			}
			if err := lc.sendProbe(string(nonce)); err != nil {
				record.report(SessionCarrierLost, "control_probe_write_failed", err)
				return
			}
		}
	}
}

// Sessions lists candidates, active/closing sessions, and closed diagnostics
// retained within the configured TTL.
func (a *Acceptor) Sessions() []SessionSnapshot {
	if a == nil {
		return nil
	}
	return a.sessions.snapshots()
}

// KickSession revokes one exact generation and cannot touch a successor.
func (a *Acceptor) KickSession(generation SessionGeneration) bool {
	if a == nil {
		return false
	}
	record := a.sessions.record(generation)
	if record == nil {
		return false
	}
	record.report(SessionRevoked, "management_exact_kick", nil)
	return true
}

// KickDaemon is the explicit bulk-revocation operation used by daemon removal,
// not a current-session alias. Every extant generation for the key is revoked.
func (a *Acceptor) KickDaemon(id string) int {
	if a == nil || id == "" {
		return 0
	}
	count := 0
	for _, snapshot := range a.sessions.snapshots() {
		if snapshot.Key != id || snapshot.State == SessionClosed {
			continue
		}
		if a.KickSession(snapshot.Generation) {
			count++
		}
	}
	return count
}

// PokePlan sends an empty, coalescible level wake to every current physical
// session for one authenticated daemon. The daemon always pulls a fresh full
// Plan; this frame carries no actor or lifecycle state.
func (a *Acceptor) PokePlan(id string) int {
	record := a.sessions.currentRecord(id)
	if record == nil {
		return 0
	}
	handle := record.linkHandle()
	raw := encodePlanPoke()
	if handle != nil && handle.sendControl(raw) == nil {
		return 1
	}
	return 0
}

func (a *Acceptor) IsAttached(id string) bool {
	return a != nil && a.sessions.currentRecord(id) != nil
}

func (a *Acceptor) AttachedDaemonIDs() []string {
	if a == nil {
		return nil
	}
	return a.sessions.currentKeys()
}

// The three capability arms below are the whole server-side story of a remote
// operation: decode the frame, hand the ingress the endpoint's own coordinate
// plus the decoded payload, encode what comes back. There is no admission, no
// mint and no routing here — the verdict and the execution happen together
// inside the organ, exactly once, exactly as they do for a local body.

func (a *Acceptor) emit(
	ctx context.Context,
	id actor.ActorID,
	key actorhost.AttemptKey,
	env *message.Envelope,
) (ipc.EmitResult, error) {
	result, err := a.ingress.Emit(ctx, id, key, env)
	return ipc.EmitResult{
		MessageID: result.MessageID, Seq: result.Seq,
		RejectReason: string(result.RejectReason), RejectDetail: result.RejectDetail,
	}, err
}

func (a *Acceptor) relayAccess(
	ctx context.Context,
	id actor.ActorID,
	key actorhost.AttemptKey,
	payload []byte,
) ([]byte, error) {
	var request accessRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		return nil, fmt.Errorf("link: access payload decode: %w", err)
	}
	call, err := request.decode()
	if err != nil {
		return nil, err
	}
	response, err := a.ingress.Access(ctx, id, key, call)
	if err != nil {
		return nil, err
	}
	return json.Marshal(accessResponseOf(call.Kind, response))
}

func (a *Acceptor) relaySchedule(
	ctx context.Context,
	id actor.ActorID,
	payload []byte,
) ([]byte, error) {
	var request scheduleRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		return nil, fmt.Errorf("link: schedule payload decode: %w", err)
	}
	call, err := request.decode()
	if err != nil {
		return nil, err
	}
	response, err := a.ingress.Schedule(ctx, id, call)
	if err != nil {
		return nil, err
	}
	return json.Marshal(scheduleResponse{ID: response.ID})
}

func (a *Acceptor) sendStorageControl(lc *linkSession, frame storageControlFrame) {
	raw, err := encodeStorageControl(frame)
	if err == nil {
		_ = lc.sendControl(raw)
	}
}

func (a *Acceptor) sendCommittedBusy(lc *linkSession, requestID string) {
	a.sendStorageControl(lc, storageControlFrame{
		Kind: ctrlCommittedReply,
		CommittedReply: &CommittedReply{
			RequestID: requestID, Reason: "link: control task pool busy",
		},
	})
}

func (a *Acceptor) sendReclaimAckBusy(lc *linkSession, requestID string) {
	a.sendStorageControl(lc, storageControlFrame{
		Kind: ctrlReclaimAckReply,
		ReclaimAckReply: &ReclaimAckReply{
			RequestID: requestID, Reason: "link: control task pool busy",
		},
	})
}

func (a *Acceptor) sendReconcileBusy(lc *linkSession, requestID string) {
	a.sendStorageControl(lc, storageControlFrame{
		Kind: ctrlReconcilePullReply,
		ReconcilePullReply: &ReconcilePullReply{
			RequestID: requestID, Reason: "link: control task pool busy",
		},
	})
}

func (a *Acceptor) SendAllocRequest(
	ctx context.Context,
	daemonID string,
	request AllocRequest,
) error {
	handle := a.currentHandle(daemonID)
	if handle == nil {
		return fmt.Errorf("link: no live connection for daemon %q", daemonID)
	}
	if request.RequestID == "" {
		request.RequestID = newRequestID()
	}
	replies := a.pendingAlloc.register(request.RequestID)
	raw, err := encodeStorageControl(storageControlFrame{
		Kind: ctrlAllocRequest, AllocRequest: &request,
	})
	if err != nil {
		a.pendingAlloc.cancel(request.RequestID)
		return err
	}
	if err := handle.sendControl(raw); err != nil {
		a.pendingAlloc.cancel(request.RequestID)
		return err
	}
	reply, err := a.pendingAlloc.wait(ctx, request.RequestID, replies, a.ctx.Done())
	if err != nil {
		return err
	}
	if !reply.OK {
		return fmt.Errorf("link: alloc request denied by daemon %q: %s", daemonID, reply.Reason)
	}
	return nil
}

func (a *Acceptor) SendReclaimRequest(ctx context.Context, daemonID, coord string) error {
	handle := a.currentHandle(daemonID)
	if handle == nil {
		return fmt.Errorf("link: no live connection for daemon %q", daemonID)
	}
	request := ReclaimRequest{RequestID: newRequestID(), Coord: coord}
	replies := a.pendingReclaim.register(request.RequestID)
	raw, err := encodeStorageControl(storageControlFrame{
		Kind: ctrlReclaimRequest, ReclaimRequest: &request,
	})
	if err != nil {
		a.pendingReclaim.cancel(request.RequestID)
		return err
	}
	if err := handle.sendControl(raw); err != nil {
		a.pendingReclaim.cancel(request.RequestID)
		return err
	}
	reply, err := a.pendingReclaim.wait(ctx, request.RequestID, replies, a.ctx.Done())
	if err != nil {
		return err
	}
	if !reply.OK {
		return fmt.Errorf("link: reclaim request denied by daemon %q: %s", daemonID, reply.Reason)
	}
	return nil
}

func (a *Acceptor) currentHandle(id string) *linkHandle {
	if a == nil {
		return nil
	}
	record := a.sessions.currentRecord(id)
	if record == nil {
		return nil
	}
	return record.linkHandle()
}

func (a *Acceptor) handleCommitted(
	ctx context.Context,
	lc *linkSession,
	sender string,
	request *Committed,
) {
	reply := CommittedReply{RequestID: request.RequestID}
	if a.storageControl == nil {
		reply.Reason = "link: storage host control unavailable"
	} else {
		var err error
		reply.Found, reply.Lost, err = a.storageControl.Committed(
			ctx, sender, request.ReservationID,
		)
		if err != nil {
			reply.Reason = err.Error()
		}
	}
	a.sendStorageControl(lc, storageControlFrame{
		Kind: ctrlCommittedReply, CommittedReply: &reply,
	})
}

func (a *Acceptor) handleReclaimAck(
	ctx context.Context,
	lc *linkSession,
	sender string,
	request *ReclaimAck,
) {
	reply := ReclaimAckReply{RequestID: request.RequestID}
	if a.storageControl == nil {
		reply.Reason = "link: storage host control unavailable"
	} else {
		reply.Found, _ = a.storageControl.ReclaimAck(ctx, sender, request.TombstoneID)
	}
	a.sendStorageControl(lc, storageControlFrame{
		Kind: ctrlReclaimAckReply, ReclaimAckReply: &reply,
	})
}

func (a *Acceptor) handleReconcilePull(
	ctx context.Context,
	lc *linkSession,
	sender string,
	request *ReconcilePull,
) {
	reply := ReconcilePullReply{RequestID: request.RequestID}
	if a.storageControl == nil {
		reply.Reason = "link: storage host control unavailable"
	} else {
		var err error
		reply.Resources, reply.PendingReservations, reply.PendingTombstones, err =
			a.storageControl.ReconcilePull(ctx, sender, request.ActiveCoords)
		if err != nil {
			reply.Reason = err.Error()
		}
	}
	a.sendStorageControl(lc, storageControlFrame{
		Kind: ctrlReconcilePullReply, ReconcilePullReply: &reply,
	})
}

type StorageHostControl interface {
	Committed(context.Context, string, string) (bool, bool, error)
	ReclaimAck(context.Context, string, string) (bool, error)
	ReconcilePull(
		context.Context,
		string,
		[]string,
	) ([]ReconcileResource, []ReconcileReservation, []ReconcileTombstone, error)
}

func (a *Acceptor) Close() error {
	a.closeOnce.Do(func() {
		defer close(a.closeDone)
		a.admissionMu.Lock()
		a.closed = true
		a.admissionMu.Unlock()
		a.cancel()
		for _, snapshot := range a.sessions.snapshots() {
			if snapshot.State != SessionClosed {
				a.KickSession(snapshot.Generation)
			}
		}
		if !waitGroupWithin(&a.wg, 30*time.Second) {
			a.leaked.Add(1)
		}
	})
	<-a.closeDone
	return nil
}

func (a *Acceptor) Leaked() int64 { return a.leaked.Load() }
