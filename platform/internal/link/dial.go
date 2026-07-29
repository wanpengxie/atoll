package link

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
	// (§5 ResolveCoord, lanecontrol.go) with home's replies.
	pendingResolveCoord *pendingReplies[ResolveCoordReply]

	// localFileOpener is the daemon-side same-machine byte-access capability
	// (filebytes.go's LocalFileOpener) — supplied in DialConfig. nil → every
	// file byte redemption on this compute answers an honest "no storage host
	// wired" error (never a silent no-op).
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
	// The ledger is the one process-level session truth shared across
	// reconnect attempts. Manufacturing a private registry per connection
	// would let overlapping dials each believe they are Current — a second
	// session ledger, which must never exist.
	if cfg.SessionLedger == nil || cfg.SessionLedger.registry == nil {
		return nil, errors.New("link: DialConfig.SessionLedger is required")
	}
	sessions := cfg.SessionLedger.registry
	ws, _, err := websocket.DefaultDialer.DialContext(ctx, serverURL, nil)
	if err != nil {
		return nil, err
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

	router, err := buildDaemonControlRouter(d)
	if err != nil {
		_ = ws.Close()
		return nil, err
	}
	onControl := func(payload []byte) {
		router.dispatch(controlDispatchInput{peerID: "home", link: d.lc}, payload)
	}
	// Build the top-level yamux session over the raw WS byte stream and open the
	// control substream (dialLinkSession tags it and starts its read loop). The
	// session's own accept + control read loops run for the link's whole life;
	// the per-actor ipc READ loops (which invoke dispatch) start at Start(),
	// after every cell is installed, so a buffered deliver just waits for Start.
	ls, err := dialLinkSession(
		ctx, ws, onControl, nil,
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
	// The read loop only correlates and delivers. Adoption belongs to this
	// waiting side, after successful pairing, so an uncorrelated reply has no
	// code path that can publish session truth.
	if adoptErr := d.adoptAttachReply(reply); adoptErr != nil {
		_ = d.Close()
		return nil, adoptErr
	}
	return d, nil
}

// adoptAttachReply is the waiting side's attach commit point. Pairing has
// already consumed the pending request before this method runs; the local
// closed/session checks stay under d.mu with the ledger adoption so a Close
// between delivery and adoption cannot publish an uncollectable session.
func (d *Dialer) adoptAttachReply(reply AttachReply) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return errors.New("link: dial closed before attach adoption")
	}
	if d.session != nil {
		d.mu.Unlock()
		d.onSessionEvidence(SessionProtocolViolation, "duplicate_accepted_attach_reply", nil)
		return errors.New("link: duplicate accepted attach reply")
	}
	record, adoptErr := d.sessions.adopt(reply.Generation, reply.DaemonID)
	if adoptErr != nil {
		d.mu.Unlock()
		d.onSessionEvidence(SessionProtocolViolation, "invalid_attach_generation", adoptErr)
		return adoptErr
	}
	d.daemonID = reply.DaemonID
	d.session = record
	d.channelID = string(reply.ChannelID)
	d.mu.Unlock()
	return nil
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

// handleAllocRequest answers one inbound AllocRequest from the control table's
// bounded worker pool. The filesystem operation never blocks the control read
// loop; pool saturation is answered by sendAllocBusy.
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
// #B, the content-less create loser's synchronous coord reclaim) from the same
// bounded worker pool as AllocRequest. It reclaims coord's live bytes via the wired
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
// remoteResourceHandle.Redeem: resolve the ticket into a coord (one small
// control RPC, zero byte-hop, true zerocopy for the actual file bytes) and
// open the local handle. Every route reaching here is same-daemon — the door
// refuses any other caller before a route is ever minted.
func (d *Dialer) redeemFileRoute(ctx context.Context, route accessdoor.FileRoute) (accessdoor.FileAccess, error) {
	reply, err := d.SendResolveCoord(ctx, route.Token)
	if err != nil {
		return accessdoor.FileAccess{}, err
	}
	if !reply.OK {
		return accessdoor.FileAccess{}, fileRouteErr("resolve coord: %s", reply.Reason)
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
		// immediately in the real subtree).
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
		return accessdoor.FileAccess{}, fileRouteErr("unknown mode %q", route.Mode)
	}
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
				_, abandoned = d.lc.drainControlTasks(d.sessions.settlementWindow)
				_ = d.lc.closeCarrier()
				if !d.lc.waitWorkers(d.sessions.sessionJoinWindow) {
					abandoned++
				}
			}
			if record != nil {
				if physicalDone := record.physicalJoin(); physicalDone != nil {
					select {
					case <-physicalDone:
					case <-time.After(d.sessions.sessionJoinWindow):
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
