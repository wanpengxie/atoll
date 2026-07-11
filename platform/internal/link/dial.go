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

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/ipc"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// Dialer is the daemon end of the link: it dials the home, attaches the party
// (stream 0), and opens one stream per attached actor — each running the NATIVE
// port-wire protocol with a real handshake (LeaseID = actor id). A hosted cell's
// pen is the stream's RemoteWriter (emits flow UP, block on the home's
// EmitAck). Dial does WS + attach with NO inbound consumption; each actor arm is
// then built in three steps — OpenStream (handshake) → caller Spawn (install the
// cell) → StartStream (start the read loop) — so no dispatch races a half-built
// host. Start drives StartStream across the initial batch; the ring drives it
// per stream for a mid-life open.
type Dialer struct {
	lc        *linkSession
	channelID string
	computeID string
	logger    *slog.Logger

	// daemonID is the home-confirmed compute id (期11 spec §4.7's AttachReply.
	// DaemonID) — updated under mu on every attach_reply (initial AND every
	// Reattach), replacing whatever self-declared/random value computeID
	// started as. This is the one value per-channel resource root paths,
	// AllocRequest routing, and reservation/tombstone ownership may rely on
	// (see AttachReply.DaemonID's doc). Read via DaemonID().
	daemonID string

	mu       sync.Mutex
	streams  map[actor.ActorID]*actorStream
	attached chan struct{} // closed when attach_reply arrives
	reply    AttachReply
	// reattachWait, when non-nil, is the pending Reattach's reply channel — set
	// right before sending a post-initial ctrlAttach, cleared by onControl when
	// the matching attach_reply arrives (or by the waiter itself on timeout/
	// cancellation). nil means no Reattach is in flight, so any attach_reply
	// received then is either the initial one (still open, closes d.attached) or
	// a protocol anomaly (logged, dropped).
	pendingAttach *pendingReplies[AttachReply]
	// started flips true inside the SAME mu critical section that snapshots the
	// streams for the initial batch (Start). It makes Start idempotent and is the
	// boundary a post-Start OpenStream races against: a stream inserted before the
	// critical section is launched by Start; one inserted after is the ring's to
	// launch via StartStream (the fixed OpenStream→Spawn→StartStream arm order).
	started bool
	// despawnLocal is the host→remote despawn hook: on a KindDespawn frame the
	// stream read loop despawns the named local cell (ending its execution arm) and
	// replies KindDetach. Injected by RunCompute (→ rt.DespawnID) after Dial, before
	// Start (so it is set before any read loop runs). nil → a KindDespawn only
	// closes the stream (no local cell to end, e.g. a test dialer).
	despawnLocal func(actor.ActorID)

	// allocHandler answers an inbound AllocRequest (home→daemon, §4.7's first
	// frame): the daemon storage host's Allocator does the real mkdir/touch
	// and returns the verdict this Dialer relays back as an AllocReply.
	// Injected by RunCompute (mirrors despawnLocal's pattern) after Dial,
	// before Start. nil → every AllocRequest is answered OK:false (no
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
	// (lane.go's LocalFileOpener) — injected via SetLocalFileOpener, mirrors
	// SetAllocHandler/SetDespawnLocal's post-Dial, pre-Start injection
	// pattern. nil → every file byte redemption on this compute answers an
	// honest "no storage host wired" error (never a silent no-op).
	localFileOpener LocalFileOpener

	done chan struct{}
}

// actorStream is one hosted actor's link stream + its native ipc plumbing. The
// dispatch handler is captured at OpenStream but the read loop that invokes it
// only starts at StartStream — after the host has installed the cell — so an
// inbound deliver can never race a half-built host (the frame waits in the
// stream buffer until StartStream). loopStarted guards the read loop to exactly
// once, so Start's initial-batch launch and a later explicit StartStream compose
// without double-starting a stream.
type actorStream struct {
	id          actor.ActorID
	stream      io.ReadWriteCloser
	codec       *ipc.Codec
	writer      *RemoteWriter
	access      *relayClient // KindAccess FIFO round-trip (backs Access + State faces)
	sched       *relayClient // KindSchedule FIFO round-trip
	dispatch    func(env *message.Envelope) error
	cancel      func(requestID message.ID)
	loopStarted bool
}

// CellArms is the full capability bundle the daemon wires into a hosted cell's
// Caps for one attached actor: every plane a local cell's Caps carry, over the
// port wire. Access and State are two faces of the SAME access arm (channel- vs
// actor-scoped), so a cell's off-log capability is behaviourally identical to a
// local one (transport neutrality — a residual-capability arm would break parity).
// Access is the WIDE resource face (Invoke+Create+Stat+List, 期11 spec §3.1);
// State stays the narrow (Invoke-only) face — the scope law itself.
type CellArms struct {
	Pen      harness.Pen
	Access   accessdoor.ResourceAccessHandle
	State    accessdoor.AccessHandle
	Schedule schedule.ScheduleHandle
	Down     func(cause string)
}

type DialConfig struct {
	DespawnLocal    func(actor.ActorID)
	AllocHandler    func(AllocRequest) AllocReply
	LocalFileOpener LocalFileOpener
}

// Dial dials the home, sends the stream-0 attach, and waits for attach_reply. It
// does NOT open actor streams or start any demux — Start does that after the
// host is built. Window-period frames sit in the kernel socket buffer.
func Dial(ctx context.Context, serverURL, computeID string, decls []Declaration, cfg DialConfig, logger *slog.Logger) (*Dialer, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ws, _, err := websocket.DefaultDialer.DialContext(ctx, serverURL, nil)
	if err != nil {
		return nil, err
	}
	d := &Dialer{
		channelID:           "",
		computeID:           computeID,
		logger:              logger,
		streams:             map[actor.ActorID]*actorStream{},
		attached:            make(chan struct{}),
		pendingCommitted:    newPendingReplies[CommittedReply](),
		pendingReclaim:      newPendingReplies[ReclaimAckReply](),
		pendingReconcile:    newPendingReplies[ReconcilePullReply](),
		pendingResolveCoord: newPendingReplies[ResolveCoordReply](),
		pendingAttach:       newPendingReplies[AttachReply](),
		done:                make(chan struct{}),
		despawnLocal:        cfg.DespawnLocal,
		allocHandler:        cfg.AllocHandler,
		localFileOpener:     cfg.LocalFileOpener,
	}

	onControl := func(payload []byte) {
		switch peekControlKind(payload) {
		case ctrlAllocRequest:
			sf, err := decodeStorageControl(payload)
			if err != nil || sf.AllocRequest == nil {
				return
			}
			d.handleAllocRequest(*sf.AllocRequest)
			return
		case ctrlReclaimRequest:
			sf, err := decodeStorageControl(payload)
			if err != nil || sf.ReclaimRequest == nil {
				return
			}
			d.handleReclaimRequest(*sf.ReclaimRequest)
			return
		case ctrlCommittedReply:
			sf, err := decodeStorageControl(payload)
			if err != nil || sf.CommittedReply == nil {
				return
			}
			d.pendingCommitted.deliver(sf.CommittedReply.RequestID, *sf.CommittedReply)
			return
		case ctrlReclaimAckReply:
			sf, err := decodeStorageControl(payload)
			if err != nil || sf.ReclaimAckReply == nil {
				return
			}
			d.pendingReclaim.deliver(sf.ReclaimAckReply.RequestID, *sf.ReclaimAckReply)
			return
		case ctrlReconcilePullReply:
			sf, err := decodeStorageControl(payload)
			if err != nil || sf.ReconcilePullReply == nil {
				return
			}
			d.pendingReconcile.deliver(sf.ReconcilePullReply.RequestID, *sf.ReconcilePullReply)
			return
		case ctrlResolveCoordReply:
			lf, err := decodeLaneControl(payload)
			if err != nil || lf.ResolveCoordReply == nil {
				return
			}
			d.pendingResolveCoord.deliver(lf.ResolveCoordReply.RequestID, *lf.ResolveCoordReply)
			return
		}
		cf, derr := decodeControl(payload)
		if derr != nil || cf.Kind != ctrlAttachReply || cf.AttachReply == nil {
			return
		}
		d.mu.Lock()
		// Every attach_reply (initial AND every later Reattach) updates the
		// home-confirmed daemon id — the authoritative value AttachReply.
		// DaemonID's doc names (§4.7). An accepted reply always carries a
		// non-empty DaemonID (Acceptor.handleAttach stamps computeID
		// unconditionally); a rejected one may not, so only update on Accepted
		// to avoid clobbering a previously-confirmed id with an empty string.
		if cf.AttachReply.Accepted && cf.AttachReply.DaemonID != "" {
			d.daemonID = cf.AttachReply.DaemonID
		}
		if cf.RequestID != "" {
			d.mu.Unlock()
			d.pendingAttach.deliver(cf.RequestID, *cf.AttachReply)
			return
		}
		select {
		case <-d.attached:
			// No Reattach in flight and the initial attach already resolved: an
			// extra attach_reply is a protocol/ordering anomaly (F11 — reject
			// reasons, and anomalies generally, are never silently dropped).
			d.logger.Warn("link.unexpected_attach_reply")
		default:
			d.reply = *cf.AttachReply
			d.channelID = string(cf.AttachReply.ChannelID)
			close(d.attached)
		}
		d.mu.Unlock()
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
	ls, err := dialLinkSession(ws, onControl, onLane, logger)
	if err != nil {
		_ = ws.Close()
		return nil, err
	}
	d.lc = ls
	// start() launches the read/accept loops only now that d.lc is assigned, so
	// onControl (which reaches back through d.lc) can never fire against a nil lc.
	d.lc.start()

	// Fold session death into d.done — the single link-death signal every waiter
	// (pending RPCs, pingLoop, the attach wait below) selects on.
	go func() {
		<-d.lc.closed()
		close(d.done)
	}()

	// Send attach on the control substream.
	raw, err := encodeControl(controlFrame{Kind: ctrlAttach, Attach: &AttachRequest{
		ComputeID: computeID, Declarations: decls,
	}})
	if err != nil {
		_ = d.lc.Close()
		return nil, err
	}
	if err := d.lc.sendControl(raw); err != nil {
		_ = d.lc.Close()
		return nil, err
	}

	select {
	case <-d.attached:
	case <-ctx.Done():
		_ = d.lc.Close()
		return nil, ctx.Err()
	case <-d.done:
		_ = d.lc.Close()
		return nil, errors.New("link: dial closed before attach reply")
	}
	if !d.reply.Accepted {
		_ = d.lc.Close()
		reason := "link: attach rejected"
		if d.reply.Reason != "" {
			reason = "link: " + d.reply.Reason
		}
		return nil, errors.New(reason)
	}
	return d, nil
}

// DaemonID returns the home-confirmed compute id (期11 spec §4.7's
// AttachReply.DaemonID) — empty until the FIRST attach_reply lands (Dial
// already blocks until then, so any caller reaching a live *Dialer sees a
// non-empty value in every non-dev-self-declared deployment). This is the
// identity the storage host's per-channel resource root, AllocRequest
// routing, and reservation/tombstone ownership must all key on — never the
// possibly-random ComputeID passed to Dial.
func (d *Dialer) DaemonID() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.daemonID
}

// HasStream reports whether id currently has an open stream on THIS link — the
// stream-existence half of the reconcile ring's 补 diff (§10.13 推导6/F6): a
// hosted actor can be live in the runtime while its stream is gone, either
// because this is a fresh post-reconnect Dialer that has opened nothing yet, or
// because a single stream died while the link itself stayed up. Either way the
// ring's answer is the same — reopen that one stream — so the ring diffs
// live ∪ stream-existence, never live alone.
func (d *Dialer) HasStream(id actor.ActorID) bool {
	d.mu.Lock()
	_, ok := d.streams[id]
	d.mu.Unlock()
	return ok
}

// reattachTimeout bounds one Reattach round-trip — a wedged home must not hang
// the daemon's reconcile ring forever.
const reattachTimeout = 10 * time.Second

// Reattach re-declares this compute's FULL current actor set on stream 0 (the
// kubelet node-status idiom — always the whole set, never an increment, §S-P8)
// and waits for the home's verdict, so the caller can OpenStream a newly-desired
// actor only once the home's allowed set actually covers it. Only one Reattach
// may be in flight at a time (the reconcile ring drives it from a single
// goroutine; the guard keeps the contract honest regardless). A rejected reply's
// reason comes back in the error (F11 — reject reasons are never silently
// dropped).
func (d *Dialer) Reattach(ctx context.Context, decls []Declaration) error {
	id := newRequestID()
	ch := d.pendingAttach.register(id)

	raw, err := encodeControl(controlFrame{RequestID: id, Kind: ctrlAttach, Attach: &AttachRequest{
		ComputeID: d.computeID, Declarations: decls,
	}})
	if err != nil {
		d.pendingAttach.cancel(id)
		return err
	}
	if err := d.lc.sendControl(raw); err != nil {
		d.pendingAttach.cancel(id)
		return err
	}

	timeout := time.NewTimer(reattachTimeout)
	defer timeout.Stop()
	select {
	case reply := <-ch:
		if !reply.Accepted {
			reason := reply.Reason
			if reason == "" {
				reason = "rejected"
			}
			return fmt.Errorf("link: reattach rejected: %s", reason)
		}
		return nil
	case <-ctx.Done():
		d.pendingAttach.cancel(id)
		return ctx.Err()
	case <-d.done:
		d.pendingAttach.cancel(id)
		return errors.New("link: reattach: link closed")
	case <-timeout.C:
		d.pendingAttach.cancel(id)
		return errors.New("link: reattach: timed out waiting for attach_reply")
	}
}

// clearReattachWait drops the pending-Reattach marker IF it still points at ch
// (a concurrent onControl delivery may have already cleared and consumed it —
// pointer-compare avoids clobbering a later, unrelated Reattach's wait).
// OpenStream opens one actor's link stream, performs the native ipc handshake
// (LeaseID = actor id), and returns the cell's full capability arms (CellArms:
// Pen + Access/State + Schedule, all relaying over this one stream) plus a
// downHandler the host installs (close the stream UP on cell death). dispatch is
// invoked for each KindDeliver frame the home sends down this stream — the host
// routes it into the cell's mailbox. cancel is invoked for each KindCancel frame
// — the host fires the named request's reqCtx OFF the cell goroutine (the work
// it interrupts is the goroutine's occupant). OpenStream is step one of three:
// OpenStream (handshake + build the arm) → caller Spawn (install the cell) →
// StartStream (start the read loop). It never starts the read loop itself, so a
// deliver can never race a not-yet-spawned cell — true for the initial batch and
// for a post-Start open the ring adds mid-life.
func (d *Dialer) OpenStream(id actor.ActorID, dispatch func(env *message.Envelope) error, cancel func(requestID message.ID)) (CellArms, error) {
	// yamux assigns the substream id itself (the retired mux's nextID
	// hand-numbering is gone); openStream tags the substream tag=actor so the
	// home's accept loop routes it to runtime.Attach.
	s, err := d.lc.openStream()
	if err != nil {
		return CellArms{}, err
	}
	codec := ipc.NewCodec(s, s)

	// Native ipc handshake on the stream: present the lease credential (actor
	// id), read the home's bound-actor ack.
	hsPayload, err := json.Marshal(ipc.HandshakePayload{LeaseID: string(id)})
	if err != nil {
		_ = s.Close()
		return CellArms{}, err
	}
	if err := codec.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: hsPayload}); err != nil {
		_ = s.Close()
		return CellArms{}, fmt.Errorf("link: handshake write %s: %w", id, err)
	}
	ack, err := codec.Read()
	if err != nil {
		_ = s.Close()
		return CellArms{}, fmt.Errorf("link: handshake ack read %s: %w", id, err)
	}
	if ack.Kind != ipc.KindHandshakeAck {
		_ = s.Close()
		return CellArms{}, fmt.Errorf("link: expected handshake_ack for %s, got %s", id, ack.Kind)
	}

	rw := NewRemoteWriter(codec)
	accessRelay := newRelayClient(codec, ipc.KindAccess)
	schedRelay := newRelayClient(codec, ipc.KindSchedule)
	as := &actorStream{id: id, stream: s, codec: codec, writer: rw, access: accessRelay, sched: schedRelay, dispatch: dispatch, cancel: cancel}
	d.mu.Lock()
	d.streams[id] = as
	d.mu.Unlock()

	// NB: the per-stream read loop is NOT started here — StartStream launches it
	// once the host has installed the cell (Start drives it for the initial batch).
	// Deliver frames that arrive in the window between handshake and StartStream
	// wait in the stream buffer; starting dispatch before install would let an
	// envelope hit a not-yet-hosted actor and be silently dropped (the bug step 0
	// fixed, in per-stream form).

	downHandler := func(cause string) {
		downPayload, _ := json.Marshal(ipc.DownPayload{Reason: cause})
		_ = codec.Write(ipc.Frame{Kind: ipc.KindDown, Payload: downPayload})
		_ = s.Close()
	}
	return CellArms{
		Pen:      rw,
		Access:   &remoteResourceHandle{relay: accessRelay, dialer: d},
		State:    &remoteAccessHandle{relay: accessRelay, scope: accessScopeState},
		Schedule: &remoteScheduleHandle{relay: schedRelay},
		Down:     downHandler,
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
		// Pointer-guarded removal: delete the table entry only if it is still
		// THIS stream. A reconnect/rebuild may have already registered a NEW
		// stream under the same actor id (OpenStream overwrites d.streams[id]);
		// a bare delete-by-id here would tear the successor's entry out from
		// under it — the same alias bug pointer-identity discipline kills
		// everywhere else in the runtime.
		d.mu.Lock()
		if d.streams[as.id] == as {
			delete(d.streams, as.id)
		}
		d.mu.Unlock()
	}()
	for {
		frame, err := as.codec.Read()
		if err != nil {
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
		case ipc.KindDespawn:
			// Host→remote: end this actor's execution arm (§10.5). Despawn the local
			// cell (the injected hook → rt.DespawnID) and reply KindDetach before
			// dropping the stream, so the home port dies QUIET. Best-effort, no ack;
			// not FIFO-correlated, so a decode miss is non-fatal (reason is advisory).
			var dp ipc.DownPayload
			_ = json.Unmarshal(frame.Payload, &dp)
			d.mu.Lock()
			despawn := d.despawnLocal
			d.mu.Unlock()
			if despawn != nil {
				despawn(as.id)
			}
			detachPayload, _ := json.Marshal(ipc.DownPayload{Reason: "despawned"})
			_ = as.codec.Write(ipc.Frame{Kind: ipc.KindDetach, Payload: detachPayload})
			_ = as.stream.Close()
			return
		case ipc.KindCancel:
			var cp ipc.CancelPayload
			if err := json.Unmarshal(frame.Payload, &cp); err != nil {
				d.logger.Error("link.cancel_decode", "actor", string(as.id), "err", err)
				continue
			}
			// Fire the cancel OFF this read loop's goroutine — and crucially OFF the
			// cell goroutine the host routes it to. The request to cancel is the one
			// occupying that cell goroutine; queuing the cancel on-loop behind the
			// work it means to interrupt would deadlock. The host's CancelRequest
			// fires the reqCtx's CancelFunc (concurrent-safe), so a bare goroutine
			// is the right vehicle. nil cancel (none installed) is a no-op.
			if as.cancel != nil {
				go as.cancel(cp.RequestID)
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

// StartStream launches one actor stream's read loop — step three of the
// OpenStream→Spawn→StartStream arm order. Idempotent per stream (loopStarted): a
// second call, or a call for a stream Start already launched, is a no-op, so the
// initial-batch launch and a mid-life ring launch compose without racing.
// Deferring the loop to here (rather than starting it in OpenStream) is the
// dispatch-race fix: by the time any deliver is consumed the cell is installed,
// so an envelope can never hit a half-built host. No-op for an unknown id.
func (d *Dialer) StartStream(id actor.ActorID) {
	d.mu.Lock()
	as := d.streams[id]
	if as == nil || as.loopStarted {
		d.mu.Unlock()
		return
	}
	as.loopStarted = true
	d.mu.Unlock()
	go d.streamReadLoop(as, as.dispatch)
}

// Start launches every OPEN actor stream's read loop, then the idle-ping
// keepalive. Call once, after Dial + the initial batch of OpenStream + host
// install. Setting started and snapshotting the streams happen in the SAME mu
// critical section (F12): a stream inserted before it is in the batch snapshot;
// one inserted after is the ring's to launch via StartStream. Idempotent —
// started gates a second Start to a no-op. Frames buffered during the window are
// drained in receipt order when each loop starts.
func (d *Dialer) Start() {
	d.mu.Lock()
	if d.started {
		d.mu.Unlock()
		return
	}
	d.started = true
	ids := make([]actor.ActorID, 0, len(d.streams))
	for id := range d.streams {
		ids = append(ids, id)
	}
	d.mu.Unlock()
	for _, id := range ids {
		d.StartStream(id)
	}
	go d.pingLoop()
}

// pingLoop sends an idle keepalive on stream 0 every leasePing so the home's
// lease last-seen refreshes even with no actor traffic (no pong — refresh is the
// whole point). Exits when the link tears down.
func (d *Dialer) pingLoop() {
	t := time.NewTicker(leasePing)
	defer t.Stop()
	ping, _ := json.Marshal(struct{}{})
	for {
		select {
		case <-d.done:
			return
		case <-t.C:
			if err := d.lc.sendControl(ping); err != nil {
				return
			}
		}
	}
}

// SendObs forwards one obs snapshot the named hosted actor pushed UP the link as
// a KindObs frame (daemon-side arm of the actor-source obs PUSH axis: the home
// port relays it into the home runtime's obs fanout). Fire-and-forget: a write
// error on a dying stream is dropped (obs is non-truth — the next snapshot or the
// home lease supersedes). No-op if the actor has no open stream. The codec write
// mutex serialises this against the cell's KindEmit writes.
func (d *Dialer) SendObs(id actor.ActorID, kind string, value []byte) {
	d.mu.Lock()
	as := d.streams[id]
	d.mu.Unlock()
	if as == nil {
		return
	}
	payload, err := json.Marshal(ipc.ObsPayload{Kind: kind, Value: value})
	if err != nil {
		return
	}
	_ = as.codec.Write(ipc.Frame{Kind: ipc.KindObs, Payload: payload})
}

// cancelForwardWriteGrace bounds how long ONE cancel-forward frame write may
// occupy the link before it is abandoned as stuck. A write deadline that only
// failed this one call would leave the shared mux conn holding a partial
// length-prefixed frame — unsafe to keep writing to (see closeConn's own
// contract) — so on grace expiry the whole link is torn down instead, exactly
// mirroring the port escort's signalDespawn idiom (runtime/actorrt/port.go: a
// grace timer racing the write, and on timeout closeConn — not a bespoke
// per-write deadline — unblocks the stuck write from underneath it). A link
// death here is the SAME best-effort outcome this arm already tolerates
// (Rebind survives it — cellCancelForwarder/cellObsForwarder resend on
// whichever Dialer reconnect installs next), just reached via a stuck
// write instead of a read error.
var cancelForwardWriteGrace = 5 * time.Second

// SendCancelRequest forwards one caller-side cancel UP the named actor's stream as
// a KindCancelRequest frame (the daemon-hosted caller abandoning its OWN outbound
// request — the upstream twin of the home's host→remote KindCancel). It carries
// ONLY the request id: the home reverse-resolves the target from the request in the
// log and authenticates the sender == this stream's bound id, so the caller
// self-reports neither. Fire-and-forget, unidirectional, NO ack (same posture as
// SendObs): a write error on a dying stream is dropped — the request's own deadline
// and the caller's own terminal already close it. No-op if the actor has no open
// stream.
//
// The actual write runs OFF the caller's goroutine (often the cell/ledger
// goroutine abandoning its own outbound request — never something this signal
// may pin on a stuck peer) and is bounded by cancelForwardWriteGrace: a second
// goroutine races the write against a grace timer and force-closes the link if
// the timer wins, guaranteeing the write goroutine can never leak past grace
// (it unblocks either on write completion or on the closed conn erroring the
// write out). The codec write mutex still serialises this against the cell's
// other KindEmit/Access/Schedule writes on the same stream.
func (d *Dialer) SendCancelRequest(id actor.ActorID, requestID message.ID) {
	d.mu.Lock()
	as := d.streams[id]
	d.mu.Unlock()
	if as == nil {
		return
	}
	payload, err := json.Marshal(ipc.CancelPayload{RequestID: requestID})
	if err != nil {
		return
	}
	frameDone := make(chan struct{})
	go func() {
		defer close(frameDone)
		_ = as.codec.Write(ipc.Frame{Kind: ipc.KindCancelRequest, Payload: payload})
	}()
	go func() {
		select {
		case <-frameDone:
		case <-time.After(cancelForwardWriteGrace):
			_ = d.lc.Close() // stuck write: unblock it by killing the (evidently dead) link
		}
	}()
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
		if err := writeLaneJSON(conn, laneAck{OK: true}); err != nil {
			_ = wh.Abort()
			return
		}
		if _, cerr := io.Copy(wh, conn); cerr != nil {
			_ = wh.Abort()
			return
		}
		if cerr := wh.Commit(); cerr != nil {
			return
		}
		if reply.ReservationID != "" {
			// WARNING: transfer-lane completion is fire-and-forget. Multi-daemon
			// recovery remains frozen until this path has a synchronous completion
			// protocol; there is deliberately no hidden resend ledger.
			if _, err := d.SendCommitted(context.Background(), reply.ReservationID); err != nil {
				d.logger.Warn("link.transfer_committed_unconfirmed", "reservation_id", reply.ReservationID, "err", err)
			}
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
	conn, err := d.lc.openLane()
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

// SendDeliverResult reports one non-Delivered local-deliver outcome UP the named
// actor's stream as a KindDeliverResult frame (pure observation — the home logs it
// as a structured Warn). Fire-and-forget: a write error on a dying stream is
// dropped, and it is NOT correlated to any FIFO waiter. No-op if the actor has no
// open stream. The codec write mutex serialises this against the cell's writes.
func (d *Dialer) SendDeliverResult(id actor.ActorID, envID message.ID, outcome, detail string) {
	d.mu.Lock()
	as := d.streams[id]
	d.mu.Unlock()
	if as == nil {
		return
	}
	payload, err := json.Marshal(ipc.DeliverResultPayload{EnvelopeID: envID, Outcome: outcome, Detail: detail})
	if err != nil {
		return
	}
	_ = as.codec.Write(ipc.Frame{Kind: ipc.KindDeliverResult, Payload: payload})
}

// DetachStream sends a graceful KindDetach on ONE actor's stream (remote→host
// "I am removing this arm" — the home port reads it and dies QUIET, no down
// edge) then closes it. Used both by DetachAll (daemon shutdown) and the
// reconcile ring's 削 path (a locally-dropped desired member, §10.13). No-op if
// the actor has no open stream (already gone).
func (d *Dialer) DetachStream(id actor.ActorID) {
	d.mu.Lock()
	as := d.streams[id]
	d.mu.Unlock()
	if as == nil {
		return
	}
	payload, _ := json.Marshal(ipc.DownPayload{Reason: "detach"})
	_ = as.codec.Write(ipc.Frame{Kind: ipc.KindDetach, Payload: payload})
	_ = as.stream.Close()
}

// DetachAll sends a graceful KindDetach on every open actor stream then closes
// each. Called on daemon ctx-cancel (graceful shutdown) so the home's port-
// hosted actors fall silent instead of materialising receiver_unavailable; a
// hard link drop (kill -9) skips this and the home reads EOF as the loud,
// positively-observed down edge instead.
func (d *Dialer) DetachAll() {
	d.mu.Lock()
	ids := make([]actor.ActorID, 0, len(d.streams))
	for id := range d.streams {
		ids = append(ids, id)
	}
	d.mu.Unlock()
	for _, id := range ids {
		d.DetachStream(id)
	}
}

// Done returns a channel closed when the link tears down (peer gone, lease
// expiry on the home side, or Close).
func (d *Dialer) Done() <-chan struct{} { return d.done }

// Close tears the link down. Every actor stream EOFs, every pending emit fails.
func (d *Dialer) Close() error { return d.lc.Close() }
