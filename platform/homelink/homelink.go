package homelink

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/platform/computebus"
)

// DispatchHandler is invoked for each envelope dispatched down from the home.
type DispatchHandler func(computebus.DispatchFrame)

// heartbeatEvery is the compute-to-home keepalive cadence. Well within the
// home-side presence lease window so the compute stays alive under normal jitter.
const heartbeatEvery = 10 * time.Second

// emitTimeout is the maximum time an Emit call blocks waiting for the home's
// EmitAck before returning a timeout error to the UplinkWriter.
const emitTimeout = 30 * time.Second

// Homelink is the compute-side connection to the channel home.
type Homelink struct {
	ws      *websocket.Conn
	writeMu sync.Mutex

	mu      sync.Mutex
	pending map[string]chan computebus.EmitAck

	onDispatch DispatchHandler
	computeID  string

	// channelID is the channel the home assigned on attach (from AttachReply).
	channelID string

	done chan struct{} // closed when readLoop exits
}

// Dial dials the home, sends AttachRequest, and waits for AttachReply. It does
// NOT start the readLoop or heartbeatLoop — no inbound frame is consumed until
// Start installs the dispatch handler. Window-period frames sit safely in the
// kernel socket buffer until Start drains them. Splitting dial from start closes
// the dispatch race: the caller installs the host before any frame is handled.
func Dial(ctx context.Context, serverURL, apiKey, computeID string, decls []computebus.AttachDeclaration) (*Homelink, error) {
	ws, _, err := websocket.DefaultDialer.DialContext(ctx, serverURL, nil)
	if err != nil {
		return nil, err
	}
	h := &Homelink{
		ws:        ws,
		pending:   make(map[string]chan computebus.EmitAck),
		computeID: computeID,
		done:      make(chan struct{}),
	}

	// Send AttachRequest.
	if err := h.send(computebus.Frame{
		Type: computebus.FrameAttach,
		Attach: &computebus.AttachRequest{
			APIKey:       apiKey,
			ComputeID:    computeID,
			Declarations: decls,
		},
	}); err != nil {
		_ = ws.Close()
		return nil, err
	}

	// Read AttachReply.
	_, raw, err := ws.ReadMessage()
	if err != nil {
		_ = ws.Close()
		return nil, err
	}
	reply, err := computebus.Decode(raw)
	if err != nil || reply.Reply == nil || !reply.Reply.Accepted {
		_ = ws.Close()
		reason := "homelink: attach rejected"
		if reply.Reply != nil && reply.Reply.Reason != "" {
			reason = "homelink: " + reply.Reply.Reason
		}
		return nil, errors.New(reason)
	}
	h.channelID = string(reply.Reply.ChannelID)
	return h, nil
}

// Start installs the dispatch handler and begins consuming inbound frames
// (readLoop) and emitting keepalives (heartbeatLoop). Call exactly once after
// Dial, with the host already constructed and all actors installed.
func (h *Homelink) Start(onDispatch DispatchHandler) {
	h.onDispatch = onDispatch
	go h.readLoop()
	go h.heartbeatLoop()
}

// ChannelID returns the channel assigned by the home on attach.
func (h *Homelink) ChannelID() string { return h.channelID }

// Emit sends a cell's output UP and blocks for the home's EmitAck (the
// authoritative WriteResult). This is the EmitFunc that daemon/host injects
// into UplinkWriter. EmitID correlation and timeout are owned here.
func (h *Homelink) Emit(ctx context.Context, ef computebus.EmitFrame) (computebus.EmitAck, error) {
	id := uuid.NewString()
	ch := make(chan computebus.EmitAck, 1)

	h.mu.Lock()
	h.pending[id] = ch
	h.mu.Unlock()

	if err := h.send(computebus.Frame{Type: computebus.FrameEmit, Emit: &ef, EmitID: id}); err != nil {
		h.removePending(id)
		return computebus.EmitAck{}, err
	}

	// Block until ack, context cancellation, or timeout.
	timer := time.NewTimer(emitTimeout)
	defer timer.Stop()
	select {
	case ack := <-ch:
		return ack, nil
	case <-ctx.Done():
		h.removePending(id)
		return computebus.EmitAck{}, ctx.Err()
	case <-timer.C:
		h.removePending(id)
		return computebus.EmitAck{}, errors.New("homelink: emit timeout")
	}
}

// SendDeath propagates a hosted cell's death UP to the home (FrameDeath) so the
// home materialises receiver_unavailable for the dead actor's in-flight
// requests. Fire-and-forget.
func (h *Homelink) SendDeath(a actor.ActorID, cause string) {
	_ = h.send(computebus.Frame{
		Type:  computebus.FrameDeath,
		Death: &computebus.DeathFrame{Actor: a, Cause: cause},
	})
}

// Close tears down the WebSocket connection. readLoop exits on the next
// ReadMessage error.
func (h *Homelink) Close() error { return h.ws.Close() }

// Done returns a channel that is closed when the readLoop exits (WS closed or
// read error). Callers can select on this to detect disconnection.
func (h *Homelink) Done() <-chan struct{} { return h.done }

// readLoop processes inbound frames from the home.
func (h *Homelink) readLoop() {
	defer close(h.done)
	for {
		_, raw, err := h.ws.ReadMessage()
		if err != nil {
			return
		}
		fr, err := computebus.Decode(raw)
		if err != nil {
			continue
		}
		switch fr.Type {
		case computebus.FrameDispatch:
			if fr.Dispatch != nil && h.onDispatch != nil {
				h.onDispatch(*fr.Dispatch)
			}
		case computebus.FrameEmitAck:
			if fr.Ack != nil {
				h.mu.Lock()
				ch := h.pending[fr.Ack.EmitID]
				delete(h.pending, fr.Ack.EmitID)
				h.mu.Unlock()
				if ch != nil {
					ch <- *fr.Ack
				}
			}
		}
	}
}

// heartbeatLoop sends periodic Heartbeat frames to keep the compute alive on
// the home side. Exits when a send fails (WS closed).
func (h *Homelink) heartbeatLoop() {
	t := time.NewTicker(heartbeatEvery)
	defer t.Stop()
	for range t.C {
		if err := h.send(computebus.Frame{
			Type: computebus.FrameHeartbeat,
			Beat: &computebus.Heartbeat{
				ComputeID: h.computeID,
			},
		}); err != nil {
			return
		}
	}
}

func (h *Homelink) send(f computebus.Frame) error {
	b, err := computebus.Encode(f)
	if err != nil {
		return err
	}
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	return h.ws.WriteMessage(websocket.TextMessage, b)
}

func (h *Homelink) removePending(id string) {
	h.mu.Lock()
	delete(h.pending, id)
	h.mu.Unlock()
}
