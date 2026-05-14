package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/coagent-ai/coagent/kernel/channel"
	"github.com/coagent-ai/coagent/kernel/ledger"
	"github.com/coagent-ai/coagent/kernel/message"
	"github.com/coagent-ai/coagent/kernel/placement"
	"github.com/coagent-ai/coagent/runtime/ipc"
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

	// Snapshot established at handshake — every outbound non-handshake
	// frame gets these fields stamped.
	channelID    channel.ID
	workerID     string
	fencingToken placement.FencingToken
	daemonEpoch  placement.DaemonEpoch
}

// NewIPCClient builds an IPCClient over the supplied bidirectional pipe.
func NewIPCClient(in io.Reader, out io.Writer) *IPCClient {
	return &IPCClient{
		codec:   ipc.NewCodec(in, out),
		pending: make(map[string]chan ipc.Frame),
		stopCh:  make(chan struct{}),
	}
}

// Start launches the read-dispatch goroutine. Returns once the reader
// is running.
func (c *IPCClient) Start(ctx context.Context) {
	go c.readLoop(ctx)
}

// Stop shuts the read loop.
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

func (c *IPCClient) readLoop(ctx context.Context) {
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
		if frame.Kind == ipc.KindFenceInvalid {
			// FenceCheck reads from the dedicated channel below; we still
			// route by ID so it lands in the pending map for the caller.
		}
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
	}
	// Frames without a matching pending request are silently dropped —
	// they're either stale duplicates or unsolicited shutdown commands
	// which Stop() will surface via the read error.
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
	c.fencingToken = ack.FencingToken
	c.daemonEpoch = ack.DaemonEpoch
	c.mu.Unlock()
	return ack, nil
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
