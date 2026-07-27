package link

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

// Dialer is one authenticated daemon transport. Logical actor ownership lives
// in actorctl/actorhost; exact ActorStream children are opened by the
// AuthenticatedLinkSession wrapper and are never indexed here by ActorID.
type Dialer struct {
	lc        *linkSession
	channelID string
	logger    *slog.Logger
	sessions  *sessionRegistry
	session   *sessionRecord

	// daemonID is the home-confirmed compute id (AttachReply.DaemonID), updated
	// under mu on every accepted attach reply. This is the one value per-channel resource root paths,
	// AllocRequest routing, and reservation/tombstone ownership may rely on
	// (see AttachReply.DaemonID's doc). Read via DaemonID().
	daemonID string

	mu sync.Mutex
	// closed is set under mu by the one evidence decision (onSessionEvidence).
	// The attach-reply adopt path checks it in the same critical section, so a
	// reply racing local close can never install an unsealable record into the
	// shared ledger.
	closed bool
	// pendingAttach correlates the one initial attach round-trip.
	pendingAttach *pendingReplies[AttachReply]
	pendingPlan   *pendingReplies[PlanReply]
	planChanged   func()

	// allocHandler answers an inbound AllocRequest (home→daemon, §4.7's first
	// frame): the daemon storage host's Allocator does the real mkdir/touch
	// and returns the verdict this Dialer relays back as an AllocReply.
	// Supplied in DialConfig before any read loop starts. nil → every
	// AllocRequest is answered OK:false (no
	// storage host wired on this compute — an honest reject, never a silent
	// drop: this RPC plane is request/response).
	allocHandler func(AllocRequest) AllocReply

	// pendingCommitted / pendingReclaim / pendingReconcile correlate this
	// Dialer's own OUTBOUND Committed/ReclaimAck/ReconcilePull sends with
	// the home's replies — the daemon-initiated three legs of the §4.7
	// control-RPC plane (AllocRequest is the one home-initiated leg,
	// answered via allocHandler above, never through these).
	pendingCommitted *pendingReplies[CommittedReply]
	pendingReclaim   *pendingReplies[ReclaimAckReply]
	pendingReconcile *pendingReplies[ReconcilePullReply]

	// pendingResolveCoord correlates this Dialer's own ResolveCoord sends
	// (§5's lane-control frame, lanecontrol.go) with home's replies.
	pendingResolveCoord *pendingReplies[ResolveCoordReply]

	// localFileOpener is the daemon-side same-machine byte-access capability
	// (lane.go's LocalFileOpener) — supplied in DialConfig. nil → every file byte redemption on this compute answers an
	// honest "no storage host wired" error (never a silent no-op).
	localFileOpener LocalFileOpener

	done      chan struct{}
	closeOnce sync.Once
}

// actorStream is one exact physical child. Its object identity, not ActorID, is
// the ownership coordinate.
type actorStream struct {
	id          actor.ActorID
	stream      io.ReadWriteCloser
	codec       *ipc.Codec
	writer      *RemoteWriter
	access      *relayClient // KindAccess FIFO round-trip (backs Access + State faces)
	sched       *relayClient // KindSchedule FIFO round-trip
	lifecycleV2 *remoteActorLifecycle
	dispatch    func(env *message.Envelope) error
	cancel      func(requestID message.ID)
	doneOnce    sync.Once
	done        chan struct{}
}

type DialConfig struct {
	// PlanChanged is a non-blocking edge notification after lifecycle/idle
	// receipts that can change this daemon's plan. The plan remains a pulled
	// level snapshot; this callback only advances convergence latency.
	PlanChanged     func()
	AllocHandler    func(AllocRequest) AllocReply
	LocalFileOpener LocalFileOpener
	SessionLedger   *RemoteSessionLedger
}

// RemoteSessionLedger is one daemon process's in-memory session truth. It is
// shared across reconnect attempts; Dial is the only operation that can adopt
// a home-minted generation into it.
type RemoteSessionLedger struct{ registry *sessionRegistry }

func NewRemoteSessionLedger(logger *slog.Logger) *RemoteSessionLedger {
	return &RemoteSessionLedger{registry: newSessionRegistry(logger)}
}

// Dial dials the home, sends the stream-0 attach, and waits for attach_reply.
// Exact actor streams are opened later through OpenExactActorStream.
func Dial(ctx context.Context, serverURL string, cfg DialConfig, logger *slog.Logger) (*Dialer, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ws, _, err := websocket.DefaultDialer.DialContext(ctx, serverURL, nil)
	if err != nil {
		return nil, err
	}
	sessions := newSessionRegistry(logger)
	if cfg.SessionLedger != nil && cfg.SessionLedger.registry != nil {
		sessions = cfg.SessionLedger.registry
	}
	d := &Dialer{
		channelID:           "",
		logger:              logger,
		sessions:            sessions,
		pendingCommitted:    newPendingReplies[CommittedReply](),
		pendingReclaim:      newPendingReplies[ReclaimAckReply](),
		pendingReconcile:    newPendingReplies[ReconcilePullReply](),
		pendingResolveCoord: newPendingReplies[ResolveCoordReply](),
		pendingAttach:       newPendingReplies[AttachReply](),
		pendingPlan:         newPendingReplies[PlanReply](),
		done:                make(chan struct{}),
		planChanged:         cfg.PlanChanged,
		allocHandler:        cfg.AllocHandler,
		localFileOpener:     cfg.LocalFileOpener,
	}

	onControl := func(payload []byte) {
		switch peekControlKind(payload) {
		case ctrlPlanPoke:
			if !validPlanPoke(payload) {
				d.onSessionEvidence(SessionProtocolViolation, "malformed_plan_poke", nil)
				return
			}
			d.signalPlanChanged()
			return
		case ctrlAllocRequest:
			sf, err := decodeStorageControl(payload)
			if err != nil || sf.AllocRequest == nil {
				d.onSessionEvidence(SessionProtocolViolation, "malformed_alloc_request", err)
				return
			}
			req := *sf.AllocRequest
			d.lc.submitControlTask(
				func() { d.handleAllocRequest(req) },
				func() { d.sendAllocBusy(req.RequestID) },
			)
			return
		case ctrlReclaimRequest:
			sf, err := decodeStorageControl(payload)
			if err != nil || sf.ReclaimRequest == nil {
				d.onSessionEvidence(SessionProtocolViolation, "malformed_reclaim_request", err)
				return
			}
			req := *sf.ReclaimRequest
			d.lc.submitControlTask(
				func() { d.handleReclaimRequest(req) },
				func() { d.sendReclaimBusy(req.RequestID) },
			)
			return
		case ctrlCommittedReply:
			sf, err := decodeStorageControl(payload)
			if err != nil || sf.CommittedReply == nil {
				d.onSessionEvidence(SessionProtocolViolation, "malformed_committed_reply", err)
				return
			}
			d.pendingCommitted.deliver(sf.CommittedReply.RequestID, *sf.CommittedReply)
			return
		case ctrlReclaimAckReply:
			sf, err := decodeStorageControl(payload)
			if err != nil || sf.ReclaimAckReply == nil {
				d.onSessionEvidence(SessionProtocolViolation, "malformed_reclaim_ack_reply", err)
				return
			}
			d.pendingReclaim.deliver(sf.ReclaimAckReply.RequestID, *sf.ReclaimAckReply)
			return
		case ctrlReconcilePullReply:
			sf, err := decodeStorageControl(payload)
			if err != nil || sf.ReconcilePullReply == nil {
				d.onSessionEvidence(SessionProtocolViolation, "malformed_reconcile_pull_reply", err)
				return
			}
			d.pendingReconcile.deliver(sf.ReconcilePullReply.RequestID, *sf.ReconcilePullReply)
			return
		case ctrlResolveCoordReply:
			lf, err := decodeLaneControl(payload)
			if err != nil || lf.ResolveCoordReply == nil {
				d.onSessionEvidence(SessionProtocolViolation, "malformed_resolve_coord_reply", err)
				return
			}
			d.pendingResolveCoord.deliver(lf.ResolveCoordReply.RequestID, *lf.ResolveCoordReply)
			return
		case ctrlPlanReply:
			cf, err := decodeControl(payload)
			if err != nil || cf.PlanReply == nil {
				d.onSessionEvidence(SessionProtocolViolation, "malformed_plan_reply", err)
				return
			}
			d.pendingPlan.deliver(cf.RequestID, *cf.PlanReply)
			return
		case ctrlAttachReply:
		default:
			// Unknown kinds stay ignored for forward compatibility; only a
			// known kind with a malformed payload is a protocol violation.
			return
		}
		cf, derr := decodeControl(payload)
		if derr != nil || cf.AttachReply == nil {
			d.onSessionEvidence(SessionProtocolViolation, "malformed_attach_reply", derr)
			return
		}
		d.mu.Lock()
		if d.closed {
			// The local evidence decision already ran; adopting now would
			// install a record no one can ever seal. Drop the late reply.
			d.mu.Unlock()
			return
		}
		// The attach reply publishes the home-confirmed daemon id — the
		// authoritative value AttachReply.
		// DaemonID's doc names (§4.7). An accepted reply always carries a
		// non-empty DaemonID (Acceptor.handleAttach stamps the authenticated id
		// unconditionally); a rejected one may not, so only update on Accepted
		// to avoid clobbering a previously-confirmed id with an empty string.
		if cf.AttachReply.Accepted {
			if cf.AttachReply.DaemonID == "" {
				d.mu.Unlock()
				d.onSessionEvidence(SessionProtocolViolation, "accepted_attach_missing_daemon_id", nil)
				return
			}
			if d.session != nil {
				d.mu.Unlock()
				d.onSessionEvidence(SessionProtocolViolation, "duplicate_accepted_attach_reply", nil)
				return
			}
			d.daemonID = cf.AttachReply.DaemonID
			record, err := d.sessions.adopt(cf.AttachReply.Generation, cf.AttachReply.DaemonID)
			if err != nil {
				d.mu.Unlock()
				d.onSessionEvidence(SessionProtocolViolation, "invalid_attach_generation", err)
				return
			}
			d.session = record
		}
		d.mu.Unlock()
		if cf.RequestID == "" {
			// Attach carries a RequestID, so a reply without one cannot be
			// correlated to the waiter — a protocol/
			// ordering anomaly, never silently dropped (F11).
			d.logger.Warn("link.attach_reply_no_request_id")
			return
		}
		d.pendingAttach.deliver(cf.RequestID, *cf.AttachReply)
	}
	// onLane handles a HOME-opened lane substream: the home is relaying a
	// redeemed transfer whose TARGET is this daemon (§5). It runs on the per-
	// substream dispatch goroutine (its own goroutine), so blocking on the byte
	// copy never stalls the accept loop.
	onLane := func(conn net.Conn) { d.handleLaneInbound(conn) }

	// Build the top-level yamux session over the raw WS byte stream and open the
	// control substream (dialLinkSession tags it and starts its read loop). The
	// session's own accept + control read loops run for the link's whole life;
	// the per-actor ipc READ loops (which invoke dispatch) start at Start(),
	// after every cell is installed, so a buffered deliver just waits for Start.
	ls, err := dialLinkSession(
		ctx, ws, onControl, onLane, nil,
		d.onSessionEvidence, logger,
	)
	if err != nil {
		_ = ws.Close()
		return nil, err
	}
	d.lc = ls
	// start() launches the read/accept loops only now that d.lc is assigned, so
	// onControl (which reaches back through d.lc) can never fire against a nil lc.
	d.lc.start()

	// Fold carrier collection into d.done, the single physical signal selected
	// by pending RPCs and the attach wait below.
	go func() {
		<-d.lc.closed()
		close(d.done)
	}()

	// Send the one attach on the control substream, correlated by RequestID.
	attachID := newRequestID()
	ch := d.pendingAttach.register(attachID)
	raw, err := encodeControl(controlFrame{
		RequestID: attachID, Kind: ctrlAttach, Attach: &AttachRequest{Proto: 2},
	})
	if err != nil {
		d.pendingAttach.cancel(attachID)
		_ = d.Close()
		return nil, err
	}
	if err := d.lc.sendControl(raw); err != nil {
		d.pendingAttach.cancel(attachID)
		_ = d.Close()
		return nil, err
	}

	// Wait on the correlation channel. The home ledger's candidate TTL is the
	// authoritative handshake bound; this side also honors caller cancellation.
	var reply AttachReply
	select {
	case reply = <-ch:
	case <-ctx.Done():
		d.pendingAttach.cancel(attachID)
		_ = d.Close()
		return nil, ctx.Err()
	case <-d.done:
		d.pendingAttach.cancel(attachID)
		_ = d.Close()
		return nil, errors.New("link: dial closed before attach reply")
	}
	if !reply.Accepted {
		_ = d.Close()
		reason := "link: attach rejected"
		if reply.Reason != "" {
			reason = "link: " + reply.Reason
		}
		return nil, errors.New(reason)
	}
	d.mu.Lock()
	d.channelID = string(reply.ChannelID)
	d.mu.Unlock()
	return d, nil
}

// PullPlan fetches one authenticated, bound daemon snapshot over the control
// stream. A failed pull leaves the caller's LKG untouched.
func (d *Dialer) PullPlan(ctx context.Context) ([]platform.PlanActor, error) {
	id := newRequestID()
	ch := d.pendingPlan.register(id)
	raw, err := encodeControl(controlFrame{RequestID: id, Kind: ctrlPlanPull, PlanPull: &PlanPull{}})
	if err != nil {
		d.pendingPlan.cancel(id)
		return nil, err
	}
	if err := d.lc.sendControl(raw); err != nil {
		d.pendingPlan.cancel(id)
		return nil, err
	}
	reply, err := d.pendingPlan.wait(ctx, id, ch, d.done)
	if err != nil {
		return nil, err
	}
	if reply.Error != "" {
		return nil, errors.New(reply.Error)
	}
	return reply.Actors, nil
}

// DaemonID returns the home-confirmed compute id. Dial blocks until the first
// accepted attach reply, so every caller reaching a live Dialer sees a non-empty
// authenticated value. This is the
// identity the storage host's per-channel resource root, AllocRequest
// routing, and reservation/tombstone ownership must all key on — never the
// sole daemon identity used by the link.
func (d *Dialer) DaemonID() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.daemonID
}

// Authority returns the opaque live credentials installed from the accepted
// attach reply. The generation is never minted on this side.
func (d *Dialer) Authority() SessionAuthority {
	if d == nil {
		return SessionAuthority{}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return authorityPair(d.sessions, d.session)
}

// OpenExactActorStream opens one fresh session-owned stream for an exact
// daemon Body. It starts the reader immediately: actorhost has already
// published the Unit before DaemonOutbound is allowed to converge a slot, so
// inbound delivery cannot race a half-built actor.
//
// There is no by-ActorID stream table. Object identity is its ownership
// coordinate; predecessor and successor
// streams may overlap until their owning session joins them.
func (d *Dialer) OpenExactActorStream(
	ctx context.Context,
	id actor.ActorID,
	key actorhost.AttemptKey,
	host *actorhost.HostSupervisor,
) (ActorStreamResource, error) {
	if d == nil || host == nil || id == "" {
		return ActorStreamResource{}, ErrInvalidPhysicalChild
	}
	if _, err := actorhost.ParseAttemptKey(string(key)); err != nil {
		return ActorStreamResource{}, err
	}
	s, err := d.lc.openStream(ctx)
	if err != nil {
		return ActorStreamResource{}, err
	}
	codec := ipc.NewCodec(s, s)
	raw, err := json.Marshal(ipc.HandshakePayload{
		LeaseID:    string(id),
		AttemptKey: string(key),
	})
	if err != nil {
		_ = s.Close()
		return ActorStreamResource{}, err
	}
	if err := codec.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: raw}); err != nil {
		_ = s.Close()
		return ActorStreamResource{}, err
	}

	writer := NewRemoteWriter(codec)
	accessRelay := newRelayClient(codec, ipc.KindAccess)
	scheduleRelay := newRelayClient(codec, ipc.KindSchedule)
	lifecycle := newRemoteActorLifecycle(codec)
	stream := &actorStream{
		id: id, stream: s, codec: codec, writer: writer,
		access: accessRelay, sched: scheduleRelay, lifecycleV2: lifecycle,
		dispatch: func(env *message.Envelope) error {
			return host.Deliver(id, env)
		},
		cancel: func(requestID message.ID) {
			host.CancelRequest(id, requestID)
		},
		done: make(chan struct{}),
	}
	go d.streamReadLoop(stream, stream.dispatch)

	return ActorStreamResource{
		Arms: RawActorArms{
			Pen:       writer,
			Access:    &remoteResourceHandle{relay: accessRelay, dialer: d},
			State:     &remoteAccessHandle{relay: accessRelay, scope: accessScopeState},
			Schedule:  &remoteScheduleHandle{relay: scheduleRelay},
			Lifecycle: lifecycle,
		},
		Close:         s.Close,
		Done:          stream.done,
		CancelRequest: writer.sendCancel,
		PublishObs:    writer.publishObs,
	}, nil
}

// streamReadLoop drives one actor stream's inbound ipc frames after the
// handshake: deliver work down to the cell, route emit-acks back to the
// RemoteWriter, and on EOF fail any pending emits and drop the stream.
func (d *Dialer) streamReadLoop(as *actorStream, dispatch func(env *message.Envelope) error) {
	defer func() {
		// The stream is gone: fail every in-flight round-trip on all arms (message
		// plane + access + schedule) so no cell blocks forever on a verdict that
		// will never return — the transport-death signal each arm surfaces to its
		// caller (outcome_unknown on access, error on emit/schedule).
		as.writer.Close()
		as.access.close()
		as.sched.close()
		if as.lifecycleV2 != nil {
			as.lifecycleV2.close()
		}
		if as.done != nil {
			as.doneOnce.Do(func() { close(as.done) })
		}
	}()
	for {
		frame, err := as.codec.Read()
		if err != nil {
			_ = as.stream.Close()
			return
		}
		switch frame.Kind {
		case ipc.KindDeliver:
			var dp ipc.DeliverPayload
			if err := json.Unmarshal(frame.Payload, &dp); err != nil {
				d.logger.Error("link.deliver_decode", "actor", string(as.id), "err", err)
				continue
			}
			env := dp.Envelope
			if err := dispatch(&env); err != nil {
				d.logger.Error("link.dispatch", "actor", string(as.id), "err", err)
			}
		// The three ack arms (emit / access / schedule) correlate FIFO-no-id: each
		// ack pops the queue head. A malformed ack cannot be matched to its op, so
		// SKIPPING the pop would permanently shift the arm by one and hand every
		// subsequent caller the previous op's verdict. Fail-closed instead: tear the
		// stream down so the deferred arm-close surfaces the honest unconfirmed
		// outcome (outcome_unknown / error) to every in-flight caller, mirroring the
		// port read loop's decode discipline. (KindDeliver / KindCancel below are not
		// FIFO-correlated, so a decode drop there is not a desync and stays non-fatal.)
		case ipc.KindEmitAck:
			var ap ipc.EmitAckPayload
			if err := json.Unmarshal(frame.Payload, &ap); err != nil {
				d.logger.Error("link.emit_ack_decode", "actor", string(as.id), "err", err)
				_ = as.stream.Close()
				return
			}
			as.writer.DeliverAck(ap)
		case ipc.KindAccessAck:
			var ap ipc.RelayAckPayload
			if err := json.Unmarshal(frame.Payload, &ap); err != nil {
				d.logger.Error("link.access_ack_decode", "actor", string(as.id), "err", err)
				_ = as.stream.Close()
				return
			}
			as.access.deliverAck(ap)
		case ipc.KindScheduleAck:
			var ap ipc.RelayAckPayload
			if err := json.Unmarshal(frame.Payload, &ap); err != nil {
				d.logger.Error("link.schedule_ack_decode", "actor", string(as.id), "err", err)
				_ = as.stream.Close()
				return
			}
			as.sched.deliverAck(ap)
		case ipc.KindSpawnAck:
			var ap ipc.SpawnAckPayload
			if err := json.Unmarshal(frame.Payload, &ap); err != nil {
				_ = as.stream.Close()
				return
			}
			if as.lifecycleV2 != nil {
				as.lifecycleV2.fork.deliverAck(ap)
			}
			d.signalPlanChanged()
		case ipc.KindEndAck:
			var ap ipc.EndAckPayload
			if err := json.Unmarshal(frame.Payload, &ap); err != nil {
				_ = as.stream.Close()
				return
			}
			if as.lifecycleV2 != nil {
				as.lifecycleV2.end.deliverAck(ap)
			}
			d.signalPlanChanged()
		case ipc.KindCancel:
			var cp ipc.CancelPayload
			if err := json.Unmarshal(frame.Payload, &cp); err != nil {
				d.logger.Error("link.cancel_decode", "actor", string(as.id), "err", err)
				continue
			}
			// Non-blocking hand-off into the target cell's pending-cancel organ
			// (§16): as.cancel → rt.CancelRequest → cell.cancelRequest merges the
			// id into a bounded set and lets that cell's single drain goroutine
			// dispatch it one-hop to the occupant's RequestCanceller — OFF this
			// read loop AND off the cell's serial Receive line. This arm therefore
			// no longer spawns a goroutine per frame; the old死锁 concern here
			// described the per-request reqCtx machine that 期10 S5 already铲除.
			// nil cancel (none installed) is a no-op.
			if as.cancel != nil {
				as.cancel(cp.RequestID)
			}
		default:
			// Fail-closed on an out-of-closed-set kind, mirroring the home port's
			// read-loop discipline (ipc kinds are a closed set): an unknown frame
			// may be an unmatchable ack occupying a FIFO slot, and skipping it
			// would silently shift an ack arm by one — every later caller would
			// get the previous op's verdict. Tearing the stream down lets the
			// deferred arm-close surface honest unconfirmed outcomes to all
			// in-flight callers, and the daemon's redial loop re-establishes.
			// NOTE (version skew): today both ends ship in one binary; when the
			// frame set grows (期10 wire extensions), mixed-version links will
			// close on first new frame — bump both ends together.
			d.logger.Error("link.unknown_kind", "actor", string(as.id), "kind", string(frame.Kind))
			_ = as.stream.Close()
			return
		}
	}
}

// handleAllocRequest answers one inbound AllocRequest on the control plane's
// read-loop goroutine (onControl runs synchronously per stream-0 frame, the
// same posture handleAttach already has home-side) — a real Allocator mkdir/
// touch is expected to be fast (a local filesystem op), so this is not
// bounced to a separate goroutine; a slow/wedged Allocator would delay
// further control-frame processing on this link, an accepted trade-off
// matching the existing synchronous-onControl discipline throughout this
// package.
func (d *Dialer) handleAllocRequest(req AllocRequest) {
	d.mu.Lock()
	handler := d.allocHandler
	d.mu.Unlock()
	var reply AllocReply
	if handler == nil {
		reply = AllocReply{RequestID: req.RequestID, OK: false, Reason: "link: no storage host wired on this compute"}
	} else {
		reply = handler(req)
		reply.RequestID = req.RequestID // the handler answers the ALLOC, not the envelope — id is ours to stamp
	}
	raw, err := encodeStorageControl(storageControlFrame{Kind: ctrlAllocReply, AllocReply: &reply})
	if err != nil {
		return
	}
	_ = d.lc.sendControl(raw)
}

func (d *Dialer) sendAllocBusy(requestID string) {
	raw, err := encodeStorageControl(storageControlFrame{
		Kind: ctrlAllocReply,
		AllocReply: &AllocReply{
			RequestID: requestID, OK: false, Reason: "link: control task pool busy",
		},
	})
	if err == nil {
		_ = d.lc.sendControl(raw)
	}
}

// handleReclaimRequest answers one inbound ReclaimRequest (期11 review §2.5
// #B, the content-less create loser's synchronous coord reclaim) on the same
// synchronous onControl goroutine handleAllocRequest uses — a local
// RemoveAll, expected fast. Reclaims coord's live bytes via the wired
// LocalFileOpener (idempotent: an already-empty coord is a clean OK). A nil
// opener (no storage host on this compute) answers OK:false with an honest
// Reason, never a silent drop, exactly like handleAllocRequest.
func (d *Dialer) handleReclaimRequest(req ReclaimRequest) {
	d.mu.Lock()
	opener := d.localFileOpener
	d.mu.Unlock()
	reply := ReclaimReply{RequestID: req.RequestID}
	switch {
	case opener == nil:
		reply.Reason = "link: no storage host wired on this compute"
	default:
		if err := opener.ReclaimCoord(req.Coord); err != nil {
			reply.Reason = err.Error()
		} else {
			reply.OK = true
		}
	}
	raw, err := encodeStorageControl(storageControlFrame{Kind: ctrlReclaimReply, ReclaimReply: &reply})
	if err != nil {
		return
	}
	_ = d.lc.sendControl(raw)
}

func (d *Dialer) sendReclaimBusy(requestID string) {
	raw, err := encodeStorageControl(storageControlFrame{
		Kind: ctrlReclaimReply,
		ReclaimReply: &ReclaimReply{
			RequestID: requestID, OK: false, Reason: "link: control task pool busy",
		},
	})
	if err == nil {
		_ = d.lc.sendControl(raw)
	}
}

// SendCommitted is the daemon's send-half of §4.7's second frame (create-
// outbox landing, after staging→fsync→rename completes for a content-bearing
// create): blocks for the correlated CommittedReply (or ctx/timeout/link-
// close). Fire only after bytes are durably renamed — never before (§1.5's
// "无半截可见").
func (d *Dialer) SendCommitted(ctx context.Context, reservationID string) (CommittedReply, error) {
	msg := Committed{RequestID: newRequestID(), ReservationID: reservationID}
	ch := d.pendingCommitted.register(msg.RequestID)
	raw, err := encodeStorageControl(storageControlFrame{Kind: ctrlCommitted, Committed: &msg})
	if err != nil {
		d.pendingCommitted.cancel(msg.RequestID)
		return CommittedReply{}, err
	}
	if err := d.lc.sendControl(raw); err != nil {
		d.pendingCommitted.cancel(msg.RequestID)
		return CommittedReply{}, err
	}
	return d.pendingCommitted.wait(ctx, msg.RequestID, ch, d.done)
}

// SendReclaimAck is SendCommitted's delete-side mirror (§4.7's third frame),
// fired after the Reclaimer confirms the tombstoned bytes are collected.
func (d *Dialer) SendReclaimAck(ctx context.Context, tombstoneID string) (ReclaimAckReply, error) {
	msg := ReclaimAck{RequestID: newRequestID(), TombstoneID: tombstoneID}
	ch := d.pendingReclaim.register(msg.RequestID)
	raw, err := encodeStorageControl(storageControlFrame{Kind: ctrlReclaimAck, ReclaimAck: &msg})
	if err != nil {
		d.pendingReclaim.cancel(msg.RequestID)
		return ReclaimAckReply{}, err
	}
	if err := d.lc.sendControl(raw); err != nil {
		d.pendingReclaim.cancel(msg.RequestID)
		return ReclaimAckReply{}, err
	}
	return d.pendingReclaim.wait(ctx, msg.RequestID, ch, d.done)
}

// SendReconcilePull is the Scrubber's periodic pull (§4.7's fourth frame,
// level-triggered — the daemon holds no local truth, so this is its ONLY
// source of "what should exist / what is pending" after a restart or on the
// Scrubber's normal ticker cadence). activeCoords is 期11 review's own
// narrowing addition — the caller's (platform.storageHostForwarder's) fresh
// snapshot of coords with a currently-open local WriteHandle, forwarded
// as-is so the home's liveness touch can bump exactly these rows and no
// others (ReconcilePull.ActiveCoords's own doc).
func (d *Dialer) SendReconcilePull(ctx context.Context, activeCoords []string) (ReconcilePullReply, error) {
	msg := ReconcilePull{RequestID: newRequestID(), ActiveCoords: activeCoords}
	ch := d.pendingReconcile.register(msg.RequestID)
	raw, err := encodeStorageControl(storageControlFrame{Kind: ctrlReconcilePull, ReconcilePull: &msg})
	if err != nil {
		d.pendingReconcile.cancel(msg.RequestID)
		return ReconcilePullReply{}, err
	}
	if err := d.lc.sendControl(raw); err != nil {
		d.pendingReconcile.cancel(msg.RequestID)
		return ReconcilePullReply{}, err
	}
	return d.pendingReconcile.wait(ctx, msg.RequestID, ch, d.done)
}

// handleLaneInbound answers one inbound lane data stream: read the Token,
// resolve it via ResolveCoord (this daemon must BE the transfer's target —
// home's handler enforces the sender==target assertion, §5 item 0), open
// the local handle, then copy bytes — read: local→stream; write: stream→
// local, Commit (firing Committed(ReservationID) when set) or Abort on a
// short read.
func (d *Dialer) handleLaneInbound(conn io.ReadWriteCloser) {
	defer conn.Close()
	var hdr laneRedeemHeader
	if err := readLaneJSON(conn, &hdr); err != nil {
		return
	}
	reply, err := d.SendResolveCoord(context.Background(), hdr.Token)
	if err != nil || !reply.OK {
		reason := "resolve failed"
		if err != nil {
			reason = err.Error()
		} else {
			reason = reply.Reason
		}
		_ = writeLaneJSON(conn, laneAck{OK: false, Reason: reason})
		return
	}
	d.mu.Lock()
	opener := d.localFileOpener
	d.mu.Unlock()
	if opener == nil {
		_ = writeLaneJSON(conn, laneAck{OK: false, Reason: "link: no storage host wired on this compute"})
		return
	}
	switch reply.Mode {
	case access.OpRead:
		rh, oerr := opener.OpenRead(reply.Coord)
		if oerr != nil {
			_ = writeLaneJSON(conn, laneAck{OK: false, Reason: oerr.Error()})
			return
		}
		defer rh.Close()
		if err := writeLaneJSON(conn, laneAck{OK: true}); err != nil {
			return
		}
		_, _ = io.Copy(conn, rh)
	case access.OpWrite:
		wh, oerr := opener.OpenWrite(reply.Coord)
		if oerr != nil {
			_ = writeLaneJSON(conn, laneAck{OK: false, Reason: oerr.Error()})
			return
		}
		// Reuse the local write route's completion wrapper (redeemFileRoute's SAME
		// ReservationID!="" construction condition): committingWriteHandle.Commit
		// fires Committed(reservationID) and — on a home Lost verdict — reclaims
		// this write's orphaned coord. #19 合并形: one commit-completion
		// implementation, not the lane's own hand-written SendCommitted copy.
		if reply.ReservationID != "" {
			wh = &committingWriteHandle{LocalWriteHandle: wh, dialer: d, reservationID: reply.ReservationID, coord: reply.Coord}
		}
		if err := writeLaneJSON(conn, laneAck{OK: true}); err != nil {
			_ = wh.Abort()
			return
		}
		if _, cerr := io.Copy(wh, conn); cerr != nil {
			_ = wh.Abort()
			return
		}
		if cerr := wh.Commit(); cerr != nil {
			// The lane protocol has NO completion-reply frame slot: the sender is
			// not waiting on one (恢复协议的"发送方知情"半格仍留 A4). A failed
			// commit — transport error, an explicit home NAK, or a Lost race whose
			// orphan bytes committingWriteHandle.Commit already reclaimed — is
			// surfaced as a Warn here, never a silent drop.
			d.logger.Warn("link.lane_commit_failed", "reservation_id", reply.ReservationID, "coord", reply.Coord, "err", cerr)
		}
	default:
		_ = writeLaneJSON(conn, laneAck{OK: false, Reason: "link: unknown lane mode " + string(reply.Mode)})
	}
}

// SendResolveCoord is the daemon's send-half of §5's ResolveCoord frame:
// resolves a Token into its coord/mode/reservation, blocking for the
// correlated reply (or ctx/timeout/link-close). Only the transfer's OWN
// target daemon may successfully resolve a given Token (home's sender-auth
// check, lanecontrol.go).
func (d *Dialer) SendResolveCoord(ctx context.Context, token string) (ResolveCoordReply, error) {
	msg := ResolveCoordRequest{RequestID: newRequestID(), Token: token}
	ch := d.pendingResolveCoord.register(msg.RequestID)
	raw, err := encodeLaneControl(laneControlFrame{Kind: ctrlResolveCoord, ResolveCoord: &msg})
	if err != nil {
		d.pendingResolveCoord.cancel(msg.RequestID)
		return ResolveCoordReply{}, err
	}
	if err := d.lc.sendControl(raw); err != nil {
		d.pendingResolveCoord.cancel(msg.RequestID)
		return ResolveCoordReply{}, err
	}
	return d.pendingResolveCoord.wait(ctx, msg.RequestID, ch, d.done)
}

// redeemFileRoute is the shared implementation behind
// remoteResourceHandle.Redeem — Local resolves coord directly (one small
// control RPC, zero lane byte-hop, true zerocopy for the actual file
// bytes) and opens the local handle; !Local opens a fresh stream on THIS
// daemon's OWN lane session, sends the redeem header, and hands the caller
// the raw stream as FileAccess.Stream.
func (d *Dialer) redeemFileRoute(ctx context.Context, route accessdoor.FileRoute) (accessdoor.FileAccess, error) {
	if route.Local {
		reply, err := d.SendResolveCoord(ctx, route.Token)
		if err != nil {
			return accessdoor.FileAccess{}, err
		}
		if !reply.OK {
			return accessdoor.FileAccess{}, laneErr("resolve coord: %s", reply.Reason)
		}
		d.mu.Lock()
		opener := d.localFileOpener
		d.mu.Unlock()
		if opener == nil {
			return accessdoor.FileAccess{}, errors.New("link: no local file opener wired on this compute")
		}
		if route.Dir {
			// Directory-shaped resource (workspace): hand out the os.Root subtree
			// lease (期11 丁12) regardless of read/write mode — a dir lease is
			// inherently both, with no Commit boundary (each os.* call lands
			// immediately in the real subtree). Cross-host dir leases are rejected
			// at the door (resolveFileRoute), so route.Dir implies route.Local.
			root, oerr := opener.OpenDir(reply.Coord)
			if oerr != nil {
				return accessdoor.FileAccess{}, oerr
			}
			return accessdoor.FileAccess{Local: &accessdoor.LocalFile{Dir: root}}, nil
		}
		switch route.Mode {
		case access.OpRead:
			rh, oerr := opener.OpenRead(reply.Coord)
			if oerr != nil {
				return accessdoor.FileAccess{}, oerr
			}
			return accessdoor.FileAccess{Local: &accessdoor.LocalFile{Read: rh}}, nil
		case access.OpWrite:
			wh, oerr := opener.OpenWrite(reply.Coord)
			if oerr != nil {
				return accessdoor.FileAccess{}, oerr
			}
			if reply.ReservationID != "" {
				wh = &committingWriteHandle{LocalWriteHandle: wh, dialer: d, reservationID: reply.ReservationID, coord: reply.Coord}
			}
			return accessdoor.FileAccess{Local: &accessdoor.LocalFile{Write: wh}}, nil
		default:
			return accessdoor.FileAccess{}, laneErr("unknown mode %q", route.Mode)
		}
	}

	// Flattened lane: a redeem is a fresh TOP-LEVEL substream tagged lane on
	// this link's own session (openLane writes the streamHeader{lane}); the
	// home's accept loop dispatches it to handleLaneRedeem. No nested yamux
	// session to guard — d.lc is live for any live Dialer.
	openCtx, cancelOpen := context.WithTimeout(context.Background(), streamWriteBudget)
	conn, err := d.lc.openLane(openCtx)
	cancelOpen()
	if err != nil {
		return accessdoor.FileAccess{}, fmt.Errorf("link: open lane redeem stream: %w", err)
	}
	if err := writeLaneJSON(conn, laneRedeemHeader{Token: route.Token}); err != nil {
		_ = conn.Close()
		return accessdoor.FileAccess{}, err
	}
	var ack laneAck
	if err := readLaneJSON(conn, &ack); err != nil {
		_ = conn.Close()
		return accessdoor.FileAccess{}, err
	}
	if !ack.OK {
		_ = conn.Close()
		return accessdoor.FileAccess{}, laneErr("redeem rejected: %s", ack.Reason)
	}
	return accessdoor.FileAccess{Stream: conn}, nil
}

// committingWriteHandle wraps a LocalWriteHandle so Commit ALSO fires
// Committed(reservationID) once the local fsync+rename lands — the
// create-with-content write route's own completion signal (§1.7); a plain
// OpWrite's route never sets ReservationID, so its handle is never wrapped.
type committingWriteHandle struct {
	accessdoor.LocalWriteHandle
	dialer        *Dialer
	reservationID string
	// coord is this write's OWN landed coord (期11 S2, transfer-lifecycle-
	// spec.md §3's #2) — carried alongside reservationID purely so Commit
	// can name WHICH local bytes to reclaim if the home reports this
	// reservation Lost; never sent over the wire itself (Committed only
	// ever carries the reservation id, §1.7 P0-2).
	coord string
}

func (h *committingWriteHandle) Commit() error {
	if err := h.LocalWriteHandle.Commit(); err != nil {
		return err
	}
	// The bytes are now durably fsync+renamed at h.coord. What remains is the
	// home landing the resource row from this reservation.
	reply, err := h.dialer.SendCommitted(context.Background(), h.reservationID)
	if err != nil {
		// A send failure is not success: bytes landed locally, but no resource
		// row is visible. The caller must retry with a new coordinate.
		return fmt.Errorf("link: committed relay for reservation %q failed after bytes landed at %q: %w", h.reservationID, h.coord, err)
	}
	if reply.Reason != "" && !reply.Lost {
		// 期11 review残余#2a: the transport round-trip succeeded but the home
		// itself explicitly NAK'd the commit (sender/placement mismatch, a
		// store error, or no storage control wired on this channel —
		// handleCommitted's own Reason-setting branches). The pre-fix code
		// only checked err/Lost, so this reply.Reason WAS silently dropped
		// and the caller was told nil (success) even though the row was
		// never landed home-side. This call therefore reports failure.
		return fmt.Errorf("link: committed relay for reservation %q rejected by home after bytes landed at %q: %s", h.reservationID, h.coord, reply.Reason)
	}
	if reply.Lost {
		// 期11 S2's "非-land 终态回收": this reservation lost the
		// same-resource_id race — the resource id already belongs to whichever
		// OTHER reservation landed first (ErrReservationLost's own doc), so THIS
		// write's already-renamed bytes at h.coord are orphaned and must be
		// collected, never retried. Lost is a DEFINITIVE store verdict (never a
		// transient), so reclaiming here is safe. Fail loud (期11 review #D:
		// a permanent reject is reported, never dressed as success).
		h.dialer.reclaimLostCoord(h.coord)
		return fmt.Errorf("link: create lost the race for its resource id (reservation %q); another create landed it first", h.reservationID)
	}
	// reply.Found==true landed the row; reply.Found==false is a benign no-op
	// (a concurrent completion already landed it, or the reservation was
	// superseded by a Delete). Deliberately NO reclaim on
	// found=false: it cannot be distinguished from a legitimately-committed-by-
	// an already-landed row, so reclaiming here would risk deleting live bytes —
	// data loss strictly worse than a rare leftover empty coord.
	return nil
}

// reclaimLostCoord is committingWriteHandle.Commit's Lost-branch helper,
// split out so it is unit-testable without a live wire (a Dialer with only
// localFileOpener set, no real websocket underneath). Logs — never panics
// or returns — on a nil opener (no LocalFileOpener wired: nothing to
// reclaim from) or a Reclaim failure (the NEXT Scrubber pass's own orphan
// sweep is the backstop, exactly as every other best-effort daemon-side
// cleanup in this file already documents).
func (d *Dialer) reclaimLostCoord(coord string) {
	d.mu.Lock()
	opener := d.localFileOpener
	d.mu.Unlock()
	if opener == nil {
		d.logger.Warn("link.reclaim_lost_coord_no_opener", "coord", coord)
		return
	}
	if err := opener.ReclaimCoord(coord); err != nil {
		d.logger.Warn("link.reclaim_lost_coord_failed", "coord", coord, "err", err)
	}
}

func (d *Dialer) signalPlanChanged() {
	d.mu.Lock()
	changed := d.planChanged
	d.mu.Unlock()
	if changed != nil {
		changed()
	}
}

// Done returns a channel closed when the carrier is collected.
func (d *Dialer) Done() <-chan struct{} { return d.done }

func (d *Dialer) onSessionEvidence(reason SessionEndReason, detail string, err error) {
	if d == nil {
		return
	}
	d.closeOnce.Do(func() {
		d.mu.Lock()
		d.closed = true
		record := d.session
		d.mu.Unlock()
		evidence := sessionEvidence{reason: reason, detail: detail, err: err}
		if record != nil {
			d.sessions.beginSeal(record, evidence)
		}
		go func() {
			var abandoned int64
			if d.lc != nil {
				_, abandoned = d.lc.drainControlTasks(defaultSettlementWindow)
				_ = d.lc.closeCarrier()
				if !d.lc.waitWorkers(defaultSessionJoinWindow) {
					abandoned++
				}
			}
			if record != nil {
				if physicalDone := record.physicalJoin(); physicalDone != nil {
					select {
					case <-physicalDone:
					case <-time.After(defaultSessionJoinWindow):
						abandoned++
					}
				}
				d.sessions.completeSeal(record, abandoned)
			}
		}()
	})
}

// Close is an explicit local revocation decision. Exact actor children remain
// owned and joined by AuthenticatedLinkSession.
func (d *Dialer) Close() error {
	d.onSessionEvidence(SessionRevoked, "local_close", nil)
	return nil
}
