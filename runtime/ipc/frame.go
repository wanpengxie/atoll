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
	"github.com/wanpengxie/ActOS/kernel/message"
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
//
// v2: the v1 "worker writes channel log" frames (write_message /
// reserve_ledger / commit_ledger) are REMOVED — truth lives on server, so a
// business cell EMITS its envelope upward (KindEmit) and the server harness
// is the single writer (runtime-construction-spec §4.4). KindDown carries a
// worker/actor death signal up to the host (closure §6). fence_invalid is
// re-anchored to the worker-LEASE (instance fence), not the channel-write
// fence.
const (
	KindHandshake    Kind = "handshake"
	KindHandshakeAck Kind = "handshake_ack"
	KindEmit         Kind = "emit" // worker → daemon: emit an envelope upward to server harness
	KindDown         Kind = "down" // worker → daemon: actor/worker death signal
	KindHeartbeat    Kind = "heartbeat"
	KindReply        Kind = "reply"
	KindFenceInvalid Kind = "fence_invalid"
	KindShutdown     Kind = "shutdown"
	KindShutdownAck  Kind = "shutdown_ack"
	// KindTrigger is the M1.6-T1 daemon → worker push of a post-harness
	// envelope addressed to the channel-agent target. The worker's
	// Bridge consumes these via IPCClient.Triggers(); it MAY call
	// WriteMessage to emit a reaction envelope. The worker MUST answer
	// each trigger with KindTriggerAck on the same frame ID once a bridge
	// has either handled the payload or rejected it. IPCClient queueing is
	// not an ACK boundary.
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
	ID        string     `json:"id"`
	Kind      Kind       `json:"kind"`
	ChannelID channel.ID `json:"channel_id,omitempty"`
	// LeaseToken is the opaque worker-LEASE token (instance fence) the host
	// assigns at spawn. v2 replaces the v1 channel-write (fencing_token,
	// daemon_epoch) pair: it guards against a zombie/reconnecting worker, not
	// against channel-log writers (the channel has one writer by construction).
	LeaseToken string          `json:"lease_token,omitempty"`
	WorkerID   WorkerID        `json:"worker_id,omitempty"`
	Payload    json.RawMessage `json:"payload,omitempty"`
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
	WorkerActorID actor.ActorID `json:"worker_actor_id,omitempty"`
	LeaseToken    string        `json:"lease_token"`
	TurnDeadline  int64         `json:"turn_deadline_ms"`
}

// EmitPayload is the worker → daemon emit of one envelope. The daemon host
// forwards it upward to the server harness (the single channel writer); the
// store-allocated seq comes back via the server, not the worker. (v2 replaces
// the v1 WriteMessage "worker writes channel log" path.)
type EmitPayload struct {
	Envelope message.Envelope `json:"envelope"`
}

// DownPayload is the worker → daemon death signal for an actor it hosts
// (closure §6 — the substrate materialises receiver_unavailable for the
// dead actor's in-flight requests, on the server side).
type DownPayload struct {
	Actor  actor.ActorID `json:"actor"`
	Reason string        `json:"reason,omitempty"`
}

// HeartbeatPayload keeps the lease alive.
type HeartbeatPayload struct {
	NowMs int64 `json:"now_ms"`
}

// FenceInvalidPayload is daemon's reply when the worker-LEASE token is stale
// (a zombie / reconnecting worker). Worker MUST exit immediately. (v2: the
// token is the opaque worker-lease instance token, not the v1 channel-write
// fencing_token/daemon_epoch pair.)
type FenceInvalidPayload struct {
	ExpectedToken string `json:"expected_token"`
	GotToken      string `json:"got_token"`
	Reason        string `json:"reason"`
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
// against the channel log). AckID is the trigger frame ID that must be
// echoed by KindTriggerAck.
type TriggerPayload struct {
	Envelope      message.Envelope `json:"envelope"`
	CorrelationID message.ID       `json:"correlation_id,omitempty"`
	Cursor        int64            `json:"cursor,omitempty"`
	AckID         string           `json:"ack_id,omitempty"`
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
