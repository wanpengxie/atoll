// Package ipc defines the daemon ↔ worker IPC protocol — the length-
// prefixed JSON wire format both sides speak.
//
// Authoritative spec: launch-ticket notes §T3 (worker IPC protocol).
//
// Both runtime/workerhost (daemon side) and runtime/worker (worker
// subprocess side) import this package. It is deliberately small: only
// Frame + Kind constants + payload structs + a Codec on top of
// io.Reader / io.Writer. No sqlite, no exec, no concurrency primitives
// beyond a write mutex.
package ipc

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/ledger"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
)

// Kind is the closed set of IPC frame kinds exchanged daemon ↔ worker.
//
// Direction conventions:
//
//	handshake / write_message / reserve_ledger / commit_ledger /
//	heartbeat / shutdown                     — worker → daemon (request)
//	handshake_ack / reply / fence_invalid /
//	shutdown_ack                              — daemon → worker (response)
//	trigger                                   — daemon → worker (push)
//	trigger_ack                               — worker → daemon (trigger
//	                                            accept/reject response).
type Kind string

// Kind closed set.
const (
	KindHandshake     Kind = "handshake"
	KindHandshakeAck  Kind = "handshake_ack"
	KindWriteMessage  Kind = "write_message"
	KindReserveLedger Kind = "reserve_ledger"
	KindCommitLedger  Kind = "commit_ledger"
	KindHeartbeat     Kind = "heartbeat"
	KindReply         Kind = "reply"
	KindFenceInvalid  Kind = "fence_invalid"
	KindShutdown      Kind = "shutdown"
	KindShutdownAck   Kind = "shutdown_ack"
	// KindTrigger is the M1.6-T1 daemon → worker push of a post-harness
	// envelope addressed to the channel-agent target. The worker's
	// Bridge consumes these via IPCClient.Triggers(); it MAY call
	// WriteMessage to emit a reaction envelope. The worker MUST answer
	// each trigger with KindTriggerAck on the same frame ID once it has
	// either accepted the payload into its local trigger budget or
	// rejected it.
	KindTrigger Kind = "trigger"
	// KindTriggerAck is the worker → daemon acknowledgement for one
	// KindTrigger frame. Accepted=false is a negative acknowledgement:
	// the daemon treats PushTrigger as failed and leaves the originating
	// delivery eligible for retry by the caller's at-least-once policy.
	KindTriggerAck Kind = "trigger_ack"
)

// MaxFrameBytes caps one length-prefixed JSON frame at 16 MiB.
const MaxFrameBytes = 1 << 24

// WorkerID is the daemon-assigned worker subprocess identifier.
type WorkerID string

// String returns the wire form.
func (w WorkerID) String() string { return string(w) }

// Frame is the IPC wire envelope. Length-prefixed JSON: a uint32 BE
// length header followed by the JSON-marshalled Frame.
type Frame struct {
	ID           string                 `json:"id"`
	Kind         Kind                   `json:"kind"`
	ChannelID    channel.ID             `json:"channel_id,omitempty"`
	FencingToken placement.FencingToken `json:"fencing_token,omitempty"`
	DaemonEpoch  placement.DaemonEpoch  `json:"daemon_epoch,omitempty"`
	WorkerID     WorkerID               `json:"worker_id,omitempty"`
	Payload      json.RawMessage        `json:"payload,omitempty"`
}

// HandshakePayload is sent worker → daemon on startup.
type HandshakePayload struct {
	LeaseID string `json:"lease_id"`
}

// HandshakeAckPayload is daemon's reply.
type HandshakeAckPayload struct {
	WorkerID  WorkerID   `json:"worker_id"`
	ChannelID channel.ID `json:"channel_id"`
	// WorkerActorID is the principal the worker MUST stamp into
	// envelope.sender.id on every WriteMessage frame (otherwise
	// harness step 3 sender_mismatch will reject). Added in M1.6-T1
	// so the MockBridge knows its own actor identity without
	// out-of-band configuration.
	WorkerActorID actor.ActorID          `json:"worker_actor_id,omitempty"`
	FencingToken  placement.FencingToken `json:"fencing_token"`
	DaemonEpoch   placement.DaemonEpoch  `json:"daemon_epoch"`
	TurnDeadline  int64                  `json:"turn_deadline_ms"`
}

// WriteMessagePayload asks daemon to append an envelope.
type WriteMessagePayload struct {
	Envelope message.Envelope `json:"envelope"`
}

// WriteMessageResult is daemon's reply.
type WriteMessageResult struct {
	Seq     int64  `json:"seq"`
	Deduped bool   `json:"deduped"`
	Reason  string `json:"reason,omitempty"`
}

// ReserveLedgerPayload asks daemon to reserve a ledger key.
type ReserveLedgerPayload struct {
	Entry ledger.Entry `json:"entry"`
}

// ReserveLedgerResult is daemon's reply.
type ReserveLedgerResult struct {
	Entry    ledger.Entry `json:"entry"`
	Replayed bool         `json:"replayed"`
}

// CommitLedgerPayload commits a previously reserved key.
type CommitLedgerPayload struct {
	Key         ledger.Key `json:"key"`
	CommittedAt int64      `json:"committed_at"`
}

// HeartbeatPayload keeps the lease alive.
type HeartbeatPayload struct {
	NowMs int64 `json:"now_ms"`
}

// FenceInvalidPayload is daemon's reply when fencing fails. Worker MUST
// exit immediately.
type FenceInvalidPayload struct {
	ExpectedToken placement.FencingToken `json:"expected_token"`
	GotToken      placement.FencingToken `json:"got_token"`
	ExpectedEpoch placement.DaemonEpoch  `json:"expected_epoch"`
	GotEpoch      placement.DaemonEpoch  `json:"got_epoch"`
	Reason        string                 `json:"reason"`
}

// ReplyPayload carries a typed JSON result.
type ReplyPayload struct {
	OK     bool            `json:"ok"`
	Error  string          `json:"error,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
}

// TriggerPayload is the daemon → worker push body for KindTrigger
// frames. Carries one fully-resolved envelope (already through harness
// chain) plus the correlation_id the worker MUST propagate when
// emitting downstream envelopes (per proto-layer1 correlation
// propagation rules) and the channel cursor at the time the envelope
// was dispatched (the worker bridge uses this to align its read view
// against the channel log).
type TriggerPayload struct {
	Envelope      message.Envelope `json:"envelope"`
	CorrelationID message.ID       `json:"correlation_id,omitempty"`
	Cursor        int64            `json:"cursor,omitempty"`
}

// TriggerAckPayload is the worker → daemon response body for
// KindTriggerAck frames. Cursor mirrors the trigger cursor so the host
// can detect protocol mismatches while correlating by frame ID.
type TriggerAckPayload struct {
	Accepted bool   `json:"accepted"`
	Cursor   int64  `json:"cursor,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

// Codec encodes / decodes IPC frames on length-prefixed buffered IO.
type Codec struct {
	r    *bufio.Reader
	w    io.Writer
	wmu  sync.Mutex
	hdr  [4]byte
	rbuf []byte
}

// NewCodec wraps r/w as a frame Codec.
func NewCodec(r io.Reader, w io.Writer) *Codec {
	return &Codec{r: bufio.NewReader(r), w: w}
}

// Write marshals a Frame and emits the length-prefixed bytes.
func (c *Codec) Write(f Frame) error {
	raw, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("ipc: marshal: %w", err)
	}
	if len(raw) > MaxFrameBytes {
		return errors.New("ipc: frame too large")
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	binary.BigEndian.PutUint32(c.hdr[:], uint32(len(raw)))
	if _, err := c.w.Write(c.hdr[:]); err != nil {
		return err
	}
	if _, err := c.w.Write(raw); err != nil {
		return err
	}
	return nil
}

// Read pulls the next frame. Returns io.EOF when the connection closed.
func (c *Codec) Read() (Frame, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(c.r, hdr[:]); err != nil {
		return Frame{}, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > MaxFrameBytes {
		return Frame{}, fmt.Errorf("ipc: frame too large: %d > %d", n, MaxFrameBytes)
	}
	if int(n) > cap(c.rbuf) {
		c.rbuf = make([]byte, n)
	} else {
		c.rbuf = c.rbuf[:n]
	}
	if _, err := io.ReadFull(c.r, c.rbuf); err != nil {
		return Frame{}, err
	}
	var f Frame
	if err := json.Unmarshal(c.rbuf, &f); err != nil {
		return Frame{}, fmt.Errorf("ipc: unmarshal: %w", err)
	}
	return f, nil
}

// EncodeResult builds a Frame with Kind=Reply and a JSON-marshalled result.
func EncodeResult(id string, ok bool, errStr string, result any) (Frame, error) {
	var raw json.RawMessage
	if result != nil {
		b, err := json.Marshal(result)
		if err != nil {
			return Frame{}, err
		}
		raw = b
	}
	payload, err := json.Marshal(ReplyPayload{OK: ok, Error: errStr, Result: raw})
	if err != nil {
		return Frame{}, err
	}
	return Frame{ID: id, Kind: KindReply, Payload: payload}, nil
}
