package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/ledger"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/runtime/ipc"
)

// IPCClient is the worker-side IPC client. It owns the bidirectional
// codec, generates request IDs, and dispatches replies back to in-flight
// request channels.
//
// One IPCClient per worker subprocess. The codec stream is provided by
// the host (worker reads from os.Stdin, writes to os.Stdout).
type IPCClient struct {
	codec *ipc.Codec

	mu       sync.Mutex
	idAlloc  atomic.Int64
	pending  map[string]chan ipc.Frame
	closed   bool
	stopCh   chan struct{}
	stopOnce sync.Once

	// triggerCh fan-outs daemon-pushed KindTrigger frames to whoever is
	// driving the worker's reaction loop (typically the Bridge). It is
	// buffered so a slow consumer does not stall the read loop's reply
	// dispatch path; overflow drops the oldest waiting frame and emits
	// a stderr warning (gateway redelivery makes this safe under L1
	// §6.1 at-least-once-by-message.id).
	triggerCh   chan ipc.TriggerPayload
	triggerDrop atomic.Int64

	// Snapshot established at handshake — every outbound non-handshake
	// frame gets these fields stamped.
	channelID     channel.ID
	workerID      string
	workerActorID string
	fencingToken  placement.FencingToken
	daemonEpoch   placement.DaemonEpoch
}

// triggerBufferSize bounds the IPCClient.triggerCh backlog. Sized big
// enough that the bridge processing one trigger does not lose subsequent
// pushes during a typical channel burst.
const triggerBufferSize = 32

// NewIPCClient builds an IPCClient over the supplied bidirectional pipe.
func NewIPCClient(in io.Reader, out io.Writer) *IPCClient {
	return &IPCClient{
		codec:     ipc.NewCodec(in, out),
		pending:   make(map[string]chan ipc.Frame),
		stopCh:    make(chan struct{}),
		triggerCh: make(chan ipc.TriggerPayload, triggerBufferSize),
	}
}

// Start launches the read-dispatch goroutine. Returns once the reader
// is running.
func (c *IPCClient) Start(ctx context.Context) {
	go c.readLoop(ctx)
}

// Stop shuts the read loop. triggerCh is NOT closed here — only the
// readLoop closes it (deferred), so dispatch can never send to a closed
// channel. Bridge.Run loops observing the channel will see it close
// once readLoop returns (which Stop ultimately triggers via the pipe
// closures owned by the caller).
func (c *IPCClient) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
		c.mu.Lock()
		c.closed = true
		for _, ch := range c.pending {
			close(ch)
		}
		c.pending = nil
		c.mu.Unlock()
	})
}

// Triggers returns the channel of daemon-pushed KindTrigger payloads.
// The Bridge ranges over it; the channel closes when the IPC client
// stops (handshake EOF, daemon-side shutdown, or Stop()). Drops are
// surfaced via TriggerDropCount.
func (c *IPCClient) Triggers() <-chan ipc.TriggerPayload {
	return c.triggerCh
}

// TriggerDropCount returns the total number of KindTrigger frames the
// IPC client had to drop because triggerCh was full. Non-zero in
// production usually means the bridge processing loop is slower than
// the daemon push rate; surface as an observability counter.
func (c *IPCClient) TriggerDropCount() int64 {
	return c.triggerDrop.Load()
}

func (c *IPCClient) readLoop(ctx context.Context) {
	// Close the trigger channel exactly once, after the read loop
	// stops reading + dispatching. This makes Bridge.Run loops observe
	// EOF deterministically when the IPC link tears down.
	defer close(c.triggerCh)
	for {
		if err := ctx.Err(); err != nil {
			c.Stop()
			return
		}
		frame, err := c.codec.Read()
		if err != nil {
			c.Stop()
			return
		}
		// FenceInvalid frames are routed by ID into the pending map like
		// any other reply; the caller (sendStamped) inspects Kind +
		// translates to *FenceInvalidError via FenceFromFrame.
		c.dispatch(frame)
	}
}

func (c *IPCClient) dispatch(frame ipc.Frame) {
	c.mu.Lock()
	ch, ok := c.pending[frame.ID]
	if ok {
		delete(c.pending, frame.ID)
	}
	c.mu.Unlock()
	if ok {
		ch <- frame
		close(ch)
		return
	}
	// Unsolicited frame. KindTrigger is the only documented daemon-push
	// kind today (M1.6-T1); route it to the trigger channel. Anything
	// else stays dropped per the original semantics.
	if frame.Kind == ipc.KindTrigger {
		var payload ipc.TriggerPayload
		if err := json.Unmarshal(frame.Payload, &payload); err != nil {
			return
		}
		select {
		case c.triggerCh <- payload:
		default:
			c.triggerDrop.Add(1)
			// Drop quietly into io.Discard so unit tests don't spam
			// stderr. The TriggerDropCount probe is the contract.
		}
	}
}

// Handshake performs the initial handshake. On success the client
// remembers the daemon-supplied fencing_token + daemon_epoch + worker_id.
func (c *IPCClient) Handshake(ctx context.Context, leaseID string) (ipc.HandshakeAckPayload, error) {
	payload, err := json.Marshal(ipc.HandshakePayload{LeaseID: leaseID})
	if err != nil {
		return ipc.HandshakeAckPayload{}, err
	}
	frame := ipc.Frame{
		ID:      c.nextID(),
		Kind:    ipc.KindHandshake,
		Payload: payload,
	}
	reply, err := c.send(ctx, frame)
	if err != nil {
		return ipc.HandshakeAckPayload{}, err
	}
	if reply.Kind != ipc.KindHandshakeAck {
		return ipc.HandshakeAckPayload{}, fmt.Errorf("worker: handshake got %s", reply.Kind)
	}
	var ack ipc.HandshakeAckPayload
	if err := json.Unmarshal(reply.Payload, &ack); err != nil {
		return ipc.HandshakeAckPayload{}, err
	}
	c.mu.Lock()
	c.channelID = ack.ChannelID
	c.workerID = ack.WorkerID
	c.workerActorID = ack.WorkerActorID
	c.fencingToken = ack.FencingToken
	c.daemonEpoch = ack.DaemonEpoch
	c.mu.Unlock()
	return ack, nil
}

// ChannelID returns the post-handshake channel id snapshot.
func (c *IPCClient) ChannelID() channel.ID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channelID
}

// WorkerActorID returns the post-handshake principal id that the
// worker MUST stamp onto envelope.sender.id for every WriteMessage
// (otherwise harness step 3 sender_mismatch rejects). Empty until
// Handshake succeeds.
func (c *IPCClient) WorkerActorID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.workerActorID
}

// WorkerID returns the post-handshake worker process id snapshot.
func (c *IPCClient) WorkerID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.workerID
}

// WriteMessage sends a write_message IPC.
func (c *IPCClient) WriteMessage(ctx context.Context, env message.Envelope) (ipc.WriteMessageResult, error) {
	payload, err := json.Marshal(ipc.WriteMessagePayload{Envelope: env})
	if err != nil {
		return ipc.WriteMessageResult{}, err
	}
	reply, err := c.sendStamped(ctx, ipc.KindWriteMessage, payload)
	if err != nil {
		return ipc.WriteMessageResult{}, err
	}
	if reply.Kind == ipc.KindFenceInvalid {
		return ipc.WriteMessageResult{}, FenceFromFrame(reply)
	}
	return decodeWriteResult(reply)
}

// ReserveLedger sends a reserve_ledger IPC.
func (c *IPCClient) ReserveLedger(ctx context.Context, entry ledger.Entry) (ipc.ReserveLedgerResult, error) {
	payload, err := json.Marshal(ipc.ReserveLedgerPayload{Entry: entry})
	if err != nil {
		return ipc.ReserveLedgerResult{}, err
	}
	reply, err := c.sendStamped(ctx, ipc.KindReserveLedger, payload)
	if err != nil {
		return ipc.ReserveLedgerResult{}, err
	}
	if reply.Kind == ipc.KindFenceInvalid {
		return ipc.ReserveLedgerResult{}, FenceFromFrame(reply)
	}
	return decodeReserveResult(reply)
}

// CommitLedger sends a commit_ledger IPC.
func (c *IPCClient) CommitLedger(ctx context.Context, key ledger.Key, committedAt int64) error {
	payload, err := json.Marshal(ipc.CommitLedgerPayload{Key: key, CommittedAt: committedAt})
	if err != nil {
		return err
	}
	reply, err := c.sendStamped(ctx, ipc.KindCommitLedger, payload)
	if err != nil {
		return err
	}
	if reply.Kind == ipc.KindFenceInvalid {
		return FenceFromFrame(reply)
	}
	var rp ipc.ReplyPayload
	if err := json.Unmarshal(reply.Payload, &rp); err != nil {
		return err
	}
	if !rp.OK {
		return errors.New(rp.Error)
	}
	return nil
}

// Heartbeat sends a heartbeat IPC.
func (c *IPCClient) Heartbeat(ctx context.Context, nowMs int64) error {
	payload, err := json.Marshal(ipc.HeartbeatPayload{NowMs: nowMs})
	if err != nil {
		return err
	}
	reply, err := c.sendStamped(ctx, ipc.KindHeartbeat, payload)
	if err != nil {
		return err
	}
	if reply.Kind == ipc.KindFenceInvalid {
		return FenceFromFrame(reply)
	}
	return nil
}

// Shutdown asks the daemon for a graceful shutdown ack.
func (c *IPCClient) Shutdown(ctx context.Context) error {
	frame := ipc.Frame{ID: c.nextID(), Kind: ipc.KindShutdown}
	if _, err := c.send(ctx, frame); err != nil {
		return err
	}
	return nil
}

// sendStamped wraps send with the post-handshake header stamps.
func (c *IPCClient) sendStamped(ctx context.Context, kind ipc.Kind, payload []byte) (ipc.Frame, error) {
	c.mu.Lock()
	frame := ipc.Frame{
		ID:           c.nextID(),
		Kind:         kind,
		ChannelID:    c.channelID,
		WorkerID:     c.workerID,
		FencingToken: c.fencingToken,
		DaemonEpoch:  c.daemonEpoch,
		Payload:      payload,
	}
	c.mu.Unlock()
	return c.send(ctx, frame)
}

func (c *IPCClient) send(ctx context.Context, frame ipc.Frame) (ipc.Frame, error) {
	ch := make(chan ipc.Frame, 1)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ipc.Frame{}, errors.New("worker: ipc client closed")
	}
	c.pending[frame.ID] = ch
	c.mu.Unlock()

	if err := c.codec.Write(frame); err != nil {
		c.mu.Lock()
		delete(c.pending, frame.ID)
		c.mu.Unlock()
		return ipc.Frame{}, err
	}
	select {
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, frame.ID)
		c.mu.Unlock()
		return ipc.Frame{}, ctx.Err()
	case <-c.stopCh:
		return ipc.Frame{}, errors.New("worker: ipc client stopped")
	case reply, ok := <-ch:
		if !ok {
			return ipc.Frame{}, errors.New("worker: ipc client closed before reply")
		}
		return reply, nil
	}
}

func (c *IPCClient) nextID() string {
	i := c.idAlloc.Add(1)
	return fmt.Sprintf("w-%d", i)
}

// decodeWriteResult is split out so callers (sendStamped) can reuse logic.
func decodeWriteResult(reply ipc.Frame) (ipc.WriteMessageResult, error) {
	var rp ipc.ReplyPayload
	if err := json.Unmarshal(reply.Payload, &rp); err != nil {
		return ipc.WriteMessageResult{}, err
	}
	if !rp.OK {
		return ipc.WriteMessageResult{}, errors.New(rp.Error)
	}
	var res ipc.WriteMessageResult
	if err := json.Unmarshal(rp.Result, &res); err != nil {
		return ipc.WriteMessageResult{}, err
	}
	return res, nil
}

func decodeReserveResult(reply ipc.Frame) (ipc.ReserveLedgerResult, error) {
	var rp ipc.ReplyPayload
	if err := json.Unmarshal(reply.Payload, &rp); err != nil {
		return ipc.ReserveLedgerResult{}, err
	}
	if !rp.OK {
		return ipc.ReserveLedgerResult{}, errors.New(rp.Error)
	}
	var res ipc.ReserveLedgerResult
	if err := json.Unmarshal(rp.Result, &res); err != nil {
		return ipc.ReserveLedgerResult{}, err
	}
	return res, nil
}
