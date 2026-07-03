package link

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"

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
	lc        *linkConn
	channelID string
	computeID string
	logger    *slog.Logger

	mu       sync.Mutex
	nextID   uint32
	streams  map[actor.ActorID]*actorStream
	attached chan struct{} // closed when attach_reply arrives
	reply    AttachReply
	// reattachWait, when non-nil, is the pending Reattach's reply channel — set
	// right before sending a post-initial ctrlAttach, cleared by onControl when
	// the matching attach_reply arrives (or by the waiter itself on timeout/
	// cancellation). nil means no Reattach is in flight, so any attach_reply
	// received then is either the initial one (still open, closes d.attached) or
	// a protocol anomaly (logged, dropped).
	reattachWait chan AttachReply
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
	stream      *stream
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
type CellArms struct {
	Pen      harness.Pen
	Access   accessdoor.AccessHandle
	State    accessdoor.AccessHandle
	Schedule schedule.ScheduleHandle
	Down     func(cause string)
}

// Dial dials the home, sends the stream-0 attach, and waits for attach_reply. It
// does NOT open actor streams or start any demux — Start does that after the
// host is built. Window-period frames sit in the kernel socket buffer.
func Dial(ctx context.Context, serverURL, computeID string, decls []Declaration, logger *slog.Logger) (*Dialer, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ws, _, err := websocket.DefaultDialer.DialContext(ctx, serverURL, nil)
	if err != nil {
		return nil, err
	}
	d := &Dialer{
		channelID: "",
		computeID: computeID,
		logger:    logger,
		nextID:    1,
		streams:   map[actor.ActorID]*actorStream{},
		attached:  make(chan struct{}),
		done:      make(chan struct{}),
	}

	onControl := func(payload []byte) {
		cf, derr := decodeControl(payload)
		if derr != nil || cf.Kind != ctrlAttachReply || cf.AttachReply == nil {
			return
		}
		d.mu.Lock()
		// A pending Reattach owns the NEXT attach_reply — deliver it there and
		// return; this is not the initial attach's reply (that already closed
		// d.attached long before any Reattach could be sent).
		if pending := d.reattachWait; pending != nil {
			d.reattachWait = nil
			d.mu.Unlock()
			select {
			case pending <- *cf.AttachReply:
			default: // waiter already gave up (ctx/timeout) — reply is moot.
			}
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
	d.lc = newLinkConn(&wsConn{ws: ws}, onControl, nil)

	// Send attach on stream 0.
	raw, err := encodeControl(controlFrame{Kind: ctrlAttach, Attach: &AttachRequest{
		ComputeID: computeID, Declarations: decls,
	}})
	if err != nil {
		_ = ws.Close()
		return nil, err
	}

	// The demux loop runs for the link's whole life. It only routes data frames
	// into per-stream buffers; the per-stream READ loops (which invoke dispatch)
	// start at Start(), after every cell is installed. So the demux running here
	// cannot race a half-built host — a buffered deliver just waits for Start.
	go func() {
		defer close(d.done)
		d.lc.run(nil)
	}()

	if err := d.lc.sendControl(raw); err != nil {
		_ = ws.Close()
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

// ChannelID returns the channel the home assigned on attach.
func (d *Dialer) ChannelID() string { return d.channelID }

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
	d.mu.Lock()
	if d.reattachWait != nil {
		d.mu.Unlock()
		return errors.New("link: reattach already in flight")
	}
	ch := make(chan AttachReply, 1)
	d.reattachWait = ch
	d.mu.Unlock()

	raw, err := encodeControl(controlFrame{Kind: ctrlAttach, Attach: &AttachRequest{
		ComputeID: d.computeID, Declarations: decls,
	}})
	if err != nil {
		d.clearReattachWait(ch)
		return err
	}
	if err := d.lc.sendControl(raw); err != nil {
		d.clearReattachWait(ch)
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
		d.clearReattachWait(ch)
		return ctx.Err()
	case <-d.done:
		d.clearReattachWait(ch)
		return errors.New("link: reattach: link closed")
	case <-timeout.C:
		d.clearReattachWait(ch)
		return errors.New("link: reattach: timed out waiting for attach_reply")
	}
}

// clearReattachWait drops the pending-Reattach marker IF it still points at ch
// (a concurrent onControl delivery may have already cleared and consumed it —
// pointer-compare avoids clobbering a later, unrelated Reattach's wait).
func (d *Dialer) clearReattachWait(ch chan AttachReply) {
	d.mu.Lock()
	if d.reattachWait == ch {
		d.reattachWait = nil
	}
	d.mu.Unlock()
}

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
	d.mu.Lock()
	sid := d.nextID
	d.nextID++
	d.mu.Unlock()

	s, err := d.lc.openStream(sid)
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
		Access:   &remoteAccessHandle{relay: accessRelay, scope: accessScopeChannel},
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
		d.mu.Lock()
		delete(d.streams, as.id)
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
			d.logger.Warn("link.unknown_kind", "actor", string(as.id), "kind", string(frame.Kind))
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

// SetDespawnLocal installs the host→remote despawn hook (see Dialer.despawnLocal).
// Call after Dial, before Start — it is read by the per-stream read loops.
func (d *Dialer) SetDespawnLocal(fn func(actor.ActorID)) {
	d.mu.Lock()
	d.despawnLocal = fn
	d.mu.Unlock()
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
