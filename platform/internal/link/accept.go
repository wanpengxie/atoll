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

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/ipc"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

const attachHandshakeTimeout = 10 * time.Second

// Config supplies the Server composition seams. The Acceptor owns physical
// sessions and exact Bindings only; logical actor truth and lifecycle commands
// stay in actorctl behind these callbacks.
type Config struct {
	Minter       harness.AdmittedMinter
	Access       accessdoor.AdmittedMinter
	StateHandles accessdoor.AdmittedStateHandleResolver
	Schedule     schedule.AdmittedMinter
	Authority    storespec.CollaborationAuthority
	ChannelID    channel.ID
	Logger       *slog.Logger

	AuthorizeAttach func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain) error
	AttachBinding   func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain, actorhost.Binding) error
	BindingDown     func(actor.ActorID, actorhost.Binding)
	Fork            func(context.Context, actor.ActorID, actorhost.AttemptKey, message.ID, actorcaps.ForkSpec) (actor.ActorID, error)
	EndSelf         func(context.Context, actor.ActorID, actorhost.AttemptKey, actorcaps.EndSelfRequest) error
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
	minter       harness.AdmittedMinter
	access       accessdoor.AdmittedMinter
	stateHandles accessdoor.AdmittedStateHandleResolver
	sched        schedule.AdmittedMinter
	authority    storespec.CollaborationAuthority
	channelID    channel.ID
	logger       *slog.Logger

	authorizeAttach func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain) error
	attachBinding   func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain, actorhost.Binding) error
	bindingDown     func(actor.ActorID, actorhost.Binding)
	fork            func(context.Context, actor.ActorID, actorhost.AttemptKey, message.ID, actorcaps.ForkSpec) (actor.ActorID, error)
	endSelf         func(context.Context, actor.ActorID, actorhost.AttemptKey, actorcaps.EndSelfRequest) error
	observe         func(actor.ActorID, actorhost.AttemptKey, actorhost.Binding, actorrt.ObsKind, actorrt.ObsValue)
	observeDown     func(actor.ActorID, actorhost.AttemptKey, actorhost.Binding)
	cancelReq       func(actor.ActorID, message.ID)

	storageControl StorageHostControl
	plan           func(context.Context, string) ([]platform.PlanActor, error)
	canAttach      func(context.Context, string) error

	ctx    context.Context
	cancel context.CancelFunc

	admissionMu sync.Mutex
	closed      bool
	wg          sync.WaitGroup
	closeOnce   sync.Once
	closeDone   chan struct{}
	leaked      atomic.Int64

	attachedMu sync.Mutex
	attached   map[string]int

	linksMu sync.Mutex
	links   map[string]map[*linkHandle]struct{}

	pendingAlloc   *pendingReplies[AllocReply]
	pendingReclaim *pendingReplies[ReclaimReply]
	lane           *laneState
}

type linkHandle struct {
	closeOnce sync.Once
	close     func() error
	send      func([]byte) error
}

func (h *linkHandle) closeQuietly() {
	if h == nil {
		return
	}
	h.closeOnce.Do(func() {
		if h.close != nil {
			_ = h.close()
		}
	})
}

func (h *linkHandle) sendControl(raw []byte) error {
	if h == nil || h.send == nil {
		return errLinkClosed
	}
	return h.send(raw)
}

func NewAcceptor(cfg Config) (*Acceptor, error) {
	switch {
	case cfg.Minter == nil || cfg.Access == nil || cfg.StateHandles == nil || cfg.Schedule == nil:
		return nil, errors.New("link: capability minters are required")
	case cfg.Authority == nil:
		return nil, errors.New("link: actor authority is required")
	case cfg.AuthorizeAttach == nil || cfg.AttachBinding == nil || cfg.BindingDown == nil:
		return nil, errors.New("link: exact binding callbacks are required")
	case cfg.Fork == nil || cfg.EndSelf == nil:
		return nil, errors.New("link: lifecycle callbacks are required")
	case cfg.Plan == nil || cfg.CanAttach == nil:
		return nil, errors.New("link: daemon plan/admission callbacks are required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Acceptor{
		minter: cfg.Minter, access: cfg.Access, stateHandles: cfg.StateHandles,
		sched: cfg.Schedule, authority: cfg.Authority, channelID: cfg.ChannelID,
		logger:          logger,
		authorizeAttach: cfg.AuthorizeAttach, attachBinding: cfg.AttachBinding,
		bindingDown: cfg.BindingDown, fork: cfg.Fork,
		endSelf: cfg.EndSelf,
		observe: cfg.Observe, observeDown: cfg.ObserveDown, cancelReq: cfg.CancelRequest,
		storageControl: cfg.StorageHostControl, plan: cfg.Plan, canAttach: cfg.CanAttach,
		ctx: ctx, cancel: cancel, closeDone: make(chan struct{}),
		attached:       make(map[string]int),
		links:          make(map[string]map[*linkHandle]struct{}),
		pendingAlloc:   newPendingReplies[AllocReply](),
		pendingReclaim: newPendingReplies[ReclaimReply](),
		lane:           newLaneState(),
	}, nil
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
	var (
		boundMu sync.RWMutex
		bound   bool
	)
	var physical *AuthenticatedLinkSession
	var lc *linkSession

	handlers := serverActorHandlers{
		emit:          a.emit,
		access:        a.relayAccess,
		schedule:      a.relaySchedule,
		fork:          a.fork,
		endSelf:       a.endSelf,
		cancelRequest: a.cancelReq,
		deliverResult: func(id actor.ActorID, request message.ID, outcome, detail string) {
			a.logger.Warn("platform.delivery.remote_outcome",
				"actor", id, "request", request, "outcome", outcome, "detail", detail)
		},
	}

	onActor := func(conn net.Conn) {
		go func() {
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
			boundMu.RLock()
			isBound := bound
			boundMu.RUnlock()
			if !isBound || a.authorizeAttach(id, key, peer) != nil {
				_ = conn.Close()
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
			// Fresh Controller authorization immediately precedes publication;
			// a G1 stream paused after its first check cannot replace G2.
			if err := a.attachBinding(id, key, peer, binding.HostBinding()); err != nil {
				_ = binding.Close()
			}
		}()
	}

	onLane := func(conn net.Conn) {
		boundMu.RLock()
		ok := bound
		boundMu.RUnlock()
		if !ok {
			_ = conn.Close()
			return
		}
		a.handleLaneRedeem(daemonID, conn)
	}

	onControl := func(payload []byte) {
		switch peekControlKind(payload) {
		case ctrlAttach:
			frame, err := decodeControl(payload)
			if err != nil || frame.Attach == nil {
				return
			}
			reply := AttachReply{ChannelID: a.channelID, DaemonID: daemonID}
			if err := a.canAttach(reqCtx, daemonID); err != nil {
				reply.Reason = err.Error()
			} else {
				reply.Accepted = true
			}
			raw, _ := encodeControl(controlFrame{
				RequestID: frame.RequestID, Kind: ctrlAttachReply, AttachReply: &reply,
			})
			if err := lc.sendControl(raw); err != nil || !reply.Accepted {
				lc.kill("attach_rejected", err)
				return
			}
			boundMu.Lock()
			first := !bound
			bound = true
			boundMu.Unlock()
			if first {
				a.markAttached(daemonID)
				a.registerLaneLink(daemonID, lc)
			}
		case ctrlPlanPull:
			frame, err := decodeControl(payload)
			if err != nil || frame.PlanPull == nil {
				return
			}
			boundMu.RLock()
			ok := bound
			boundMu.RUnlock()
			reply := PlanReply{}
			if !ok {
				reply.Error = "link: plan pull before attach"
			} else {
				reply.Actors, err = a.plan(reqCtx, daemonID)
				if err != nil {
					reply.Error = err.Error()
				}
			}
			raw, _ := encodeControl(controlFrame{
				RequestID: frame.RequestID, Kind: ctrlPlanReply, PlanReply: &reply,
			})
			_ = lc.sendControl(raw)
		case ctrlAllocReply:
			frame, err := decodeStorageControl(payload)
			if err == nil && frame.AllocReply != nil {
				a.pendingAlloc.deliver(frame.AllocReply.RequestID, *frame.AllocReply)
			}
		case ctrlReclaimReply:
			frame, err := decodeStorageControl(payload)
			if err == nil && frame.ReclaimReply != nil {
				a.pendingReclaim.deliver(frame.ReclaimReply.RequestID, *frame.ReclaimReply)
			}
		case ctrlCommitted:
			frame, err := decodeStorageControl(payload)
			if err == nil && frame.Committed != nil {
				a.handleCommitted(reqCtx, lc, daemonID, frame.Committed)
			}
		case ctrlReclaimAck:
			frame, err := decodeStorageControl(payload)
			if err == nil && frame.ReclaimAck != nil {
				a.handleReclaimAck(reqCtx, lc, daemonID, frame.ReclaimAck)
			}
		case ctrlReconcilePull:
			frame, err := decodeStorageControl(payload)
			if err == nil && frame.ReconcilePull != nil {
				a.handleReconcilePull(reqCtx, lc, daemonID, frame.ReconcilePull)
			}
		case ctrlResolveCoord:
			frame, err := decodeLaneControl(payload)
			if err == nil && frame.ResolveCoord != nil {
				reply := a.handleResolveCoord(daemonID, frame.ResolveCoord)
				raw, _ := encodeLaneControl(laneControlFrame{
					Kind: ctrlResolveCoordReply, ResolveCoordReply: &reply,
				})
				_ = lc.sendControl(raw)
			}
		}
	}

	var err error
	lc, err = acceptLinkSession(ws, onControl, onActor, onLane, nil, a.logger)
	if err != nil {
		return
	}
	physical, err = NewAuthenticatedLinkSession(AuthenticatedLinkSessionConfig{
		Peer: peer, CloseTransport: lc.Close, TransportDone: lc.closed(),
	})
	if err != nil {
		_ = lc.Close()
		return
	}
	handle := &linkHandle{close: physical.Close, send: lc.sendControl}
	a.registerLink(daemonID, handle)
	defer a.deregisterLink(daemonID, handle)
	lc.start()
	select {
	case <-a.ctx.Done():
		_ = physical.Close()
	case <-lc.closed():
		_ = physical.Close()
	}
	<-physical.Done()
	boundMu.RLock()
	wasBound := bound
	boundMu.RUnlock()
	if wasBound {
		a.markDetached(daemonID)
		a.deregisterLaneLink(daemonID, lc)
	}
}

func (a *Acceptor) registerLink(id string, handle *linkHandle) {
	a.linksMu.Lock()
	set := a.links[id]
	if set == nil {
		set = make(map[*linkHandle]struct{})
		a.links[id] = set
	}
	set[handle] = struct{}{}
	a.linksMu.Unlock()
}

func (a *Acceptor) deregisterLink(id string, handle *linkHandle) {
	a.linksMu.Lock()
	if set := a.links[id]; set != nil {
		delete(set, handle)
		if len(set) == 0 {
			delete(a.links, id)
		}
	}
	a.linksMu.Unlock()
}

func (a *Acceptor) KickDaemon(id string) int {
	a.linksMu.Lock()
	var handles []*linkHandle
	for handle := range a.links[id] {
		handles = append(handles, handle)
	}
	a.linksMu.Unlock()
	for _, handle := range handles {
		handle.closeQuietly()
	}
	a.logger.Info("link.kick_daemon", "compute", id, "closed", len(handles))
	return len(handles)
}

// PokePlan sends an empty, coalescible level wake to every current physical
// session for one authenticated daemon. The daemon always pulls a fresh full
// Plan; this frame carries no actor or lifecycle state.
func (a *Acceptor) PokePlan(id string) int {
	a.linksMu.Lock()
	var handles []*linkHandle
	for handle := range a.links[id] {
		handles = append(handles, handle)
	}
	a.linksMu.Unlock()
	raw := encodePlanPoke()
	sent := 0
	for _, handle := range handles {
		if err := handle.sendControl(raw); err == nil {
			sent++
		}
	}
	return sent
}

func (a *Acceptor) markAttached(id string) {
	a.attachedMu.Lock()
	a.attached[id]++
	a.attachedMu.Unlock()
}

func (a *Acceptor) markDetached(id string) {
	a.attachedMu.Lock()
	a.attached[id]--
	if a.attached[id] <= 0 {
		delete(a.attached, id)
	}
	a.attachedMu.Unlock()
}

func (a *Acceptor) IsAttached(id string) bool {
	a.attachedMu.Lock()
	defer a.attachedMu.Unlock()
	return a.attached[id] > 0
}

func (a *Acceptor) AttachedDaemonIDs() []string {
	a.attachedMu.Lock()
	defer a.attachedMu.Unlock()
	out := make([]string, 0, len(a.attached))
	for id := range a.attached {
		out = append(out, id)
	}
	return out
}

func (a *Acceptor) admitIdentity(
	ctx context.Context,
	id actor.ActorID,
) (storespec.IdentityAdmission, error) {
	admission, ok, err := a.authority.AdmitIdentity(ctx, id)
	if err != nil {
		return storespec.IdentityAdmission{}, err
	}
	if !ok || !admission.Valid() {
		return storespec.IdentityAdmission{}, errors.New("link: actor inactive")
	}
	return admission, nil
}

func (a *Acceptor) emit(
	ctx context.Context,
	id actor.ActorID,
	env *message.Envelope,
) (ipc.EmitResult, error) {
	admission, err := a.admitIdentity(ctx, id)
	if err != nil {
		return ipc.EmitResult{}, err
	}
	result, err := a.minter.MintAdmitted(admission, a.channelID).Write(ctx, env)
	return ipc.EmitResult{
		MessageID: result.MessageID, Seq: result.Seq,
		RejectReason: string(result.RejectReason), RejectDetail: result.RejectDetail,
	}, err
}

func (a *Acceptor) relayAccess(
	ctx context.Context,
	id actor.ActorID,
	payload []byte,
) ([]byte, error) {
	var request accessRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		return nil, fmt.Errorf("link: access payload decode: %w", err)
	}
	var stateBinding accessdoor.AdmittedStateBinding
	if request.Kind == accessKindInvocation &&
		request.Scope == accessScopeState {
		if request.Inv == nil || request.Inv.Caller != "" {
			return nil, errors.New("link: invalid access invocation")
		}
		var err error
		stateBinding, err = a.stateHandles.ResolvePhysical(ctx, id)
		if err != nil {
			return nil, err
		}
	}
	admission, err := a.admitIdentity(ctx, id)
	if err != nil {
		return nil, err
	}
	switch request.Kind {
	case accessKindInvocation:
		if request.Inv == nil || request.Inv.Caller != "" {
			return nil, errors.New("link: invalid access invocation")
		}
		var handle accessdoor.AccessHandle
		switch request.Scope {
		case accessScopeChannel:
			handle = a.access.MintAdmitted(admission)
		case accessScopeState:
			if stateBinding == nil {
				return nil, errors.New("link: state backing unavailable")
			}
			handle = stateBinding.MintAdmitted(admission)
		default:
			return nil, errors.New("link: invalid access scope")
		}
		outcome, err := handle.Invoke(
			ctx, request.Inv.Operation, request.Inv.Resource,
			request.Inv.Args, request.Inv.Grant,
		)
		if err != nil {
			return nil, err
		}
		return json.Marshal(accessResponse{
			Kind: accessKindInvocation, Value: outcome.Value, Found: outcome.Found,
			RejectReason: outcome.RejectReason, Route: outcome.Route,
		})
	case accessKindCreate:
		if request.Create == nil {
			return nil, errors.New("link: missing access create")
		}
		outcome, err := a.access.MintAdmitted(admission).Create(
			ctx, request.Create.Resource, request.Create.Spec, request.Create.Initial,
		)
		if err != nil {
			return nil, err
		}
		return json.Marshal(accessResponse{
			Kind: accessKindCreate, Value: outcome.Value, Found: outcome.Found,
			RejectReason: outcome.RejectReason, Route: outcome.Route,
		})
	case accessKindQuery:
		if request.Query == nil {
			return nil, errors.New("link: missing access query")
		}
		handle := a.access.MintAdmitted(admission)
		switch request.Query.QueryKind {
		case accessQueryStat:
			result, err := handle.Stat(ctx, request.Query.Resource)
			if err != nil {
				return nil, err
			}
			return json.Marshal(accessResponse{
				Kind: accessKindQuery,
				Stat: &accessStatRespFields{
					Meta: result.Meta, Ops: result.Ops, Reject: result.Reject,
				},
			})
		case accessQueryList:
			if request.Query.List == nil {
				return nil, errors.New("link: missing access list")
			}
			result, err := handle.List(ctx, accessdoor.ListQuery{
				Prefix: request.Query.List.Prefix,
				Limit:  request.Query.List.Limit,
				Cursor: request.Query.List.Cursor,
			})
			if err != nil {
				return nil, err
			}
			return json.Marshal(accessResponse{
				Kind: accessKindQuery,
				List: &accessListRespFields{
					Entries: result.Entries, Next: result.Next, Reject: result.Reject,
				},
			})
		default:
			return nil, errors.New("link: invalid access query")
		}
	default:
		return nil, errors.New("link: invalid access request")
	}
}

func (a *Acceptor) relaySchedule(
	ctx context.Context,
	id actor.ActorID,
	payload []byte,
) ([]byte, error) {
	admission, err := a.admitIdentity(ctx, id)
	if err != nil {
		return nil, err
	}
	var request scheduleRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		return nil, fmt.Errorf("link: schedule payload decode: %w", err)
	}
	handle := a.sched.MintAdmitted(admission)
	switch request.Method {
	case scheduleMethodSchedule:
		timer, err := handle.Schedule(ctx, request.Req)
		if err != nil {
			return nil, err
		}
		return json.Marshal(scheduleResponse{ID: timer})
	case scheduleMethodCancel:
		return nil, handle.Cancel(ctx, request.ID)
	case scheduleMethodAck:
		return nil, handle.Ack(ctx, request.ID)
	default:
		return nil, errors.New("link: invalid schedule method")
	}
}

func (a *Acceptor) sendStorageControl(lc *linkSession, frame storageControlFrame) {
	raw, err := encodeStorageControl(frame)
	if err == nil {
		_ = lc.sendControl(raw)
	}
}

func (a *Acceptor) SendAllocRequest(
	ctx context.Context,
	daemonID string,
	request AllocRequest,
) error {
	handle := a.latestLink(daemonID)
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
	handle := a.latestLink(daemonID)
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

func (a *Acceptor) latestLink(id string) *linkHandle {
	a.linksMu.Lock()
	defer a.linksMu.Unlock()
	for handle := range a.links[id] {
		return handle
	}
	return nil
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
		a.linksMu.Lock()
		var handles []*linkHandle
		for _, set := range a.links {
			for handle := range set {
				handles = append(handles, handle)
			}
		}
		a.linksMu.Unlock()
		for _, handle := range handles {
			handle.closeQuietly()
		}
		if !waitGroupWithin(&a.wg, 30*time.Second) {
			a.leaked.Add(1)
		}
	})
	<-a.closeDone
	return nil
}

func (a *Acceptor) Leaked() int64 { return a.leaked.Load() }
