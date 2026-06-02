// Package homelink connects an attached compute to its channel home over the
// computebus WS. See doc.go. It dials the home, attaches with the api-key,
// receives DispatchFrames for hosted cells, and sends EmitFrames up — blocking
// on the home's EmitAck so the cell observes the authoritative WriteResult.
package homelink

import (
	"context"
	"errors"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/wire/computebus"
)

// DispatchHandler is invoked for each envelope dispatched down from the home.
type DispatchHandler func(computebus.DispatchFrame)

// Homelink is the compute-side connection to the channel home.
type Homelink struct {
	ws      *websocket.Conn
	writeMu sync.Mutex

	mu      sync.Mutex
	pending map[string]chan computebus.EmitAck

	onDispatch DispatchHandler
}

// Connect dials the home, attaches, and starts the read loop.
func Connect(ctx context.Context, serverURL, apiKey, computeID string, hosts []actor.ActorID, onDispatch DispatchHandler) (*Homelink, error) {
	ws, _, err := websocket.DefaultDialer.DialContext(ctx, serverURL, nil)
	if err != nil {
		return nil, err
	}
	h := &Homelink{ws: ws, pending: map[string]chan computebus.EmitAck{}, onDispatch: onDispatch}
	if err := h.send(computebus.Frame{Type: computebus.FrameAttach, Attach: &computebus.AttachRequest{APIKey: apiKey, ComputeID: computeID, Hosts: hosts}}); err != nil {
		_ = ws.Close()
		return nil, err
	}
	_, raw, err := ws.ReadMessage()
	if err != nil {
		_ = ws.Close()
		return nil, err
	}
	reply, err := computebus.Decode(raw)
	if err != nil || reply.Reply == nil || !reply.Reply.Accepted {
		_ = ws.Close()
		return nil, errors.New("homelink: attach rejected")
	}
	go h.readLoop()
	return h, nil
}

// Emit sends a cell's output up and blocks for the home's EmitAck (the
// authoritative WriteResult). This is the EmitFunc daemon/host injects.
func (h *Homelink) Emit(ctx context.Context, ef computebus.EmitFrame) (computebus.EmitAck, error) {
	id := uuid.NewString()
	ch := make(chan computebus.EmitAck, 1)
	h.mu.Lock()
	h.pending[id] = ch
	h.mu.Unlock()
	if err := h.send(computebus.Frame{Type: computebus.FrameEmit, Emit: &ef, EmitID: id}); err != nil {
		return computebus.EmitAck{}, err
	}
	select {
	case ack := <-ch:
		return ack, nil
	case <-ctx.Done():
		return computebus.EmitAck{}, ctx.Err()
	}
}

// SendDeath propagates a hosted cell's death UP to the home (FrameDeath) so the
// home materialises receiver_unavailable for the dead actor's in-flight
// requests. Fire-and-forget: the home owns the closure, the compute just reports.
func (h *Homelink) SendDeath(a actor.ActorID, cause string) {
	_ = h.send(computebus.Frame{Type: computebus.FrameDeath, Death: &computebus.DeathFrame{Actor: a, Cause: cause}})
}

func (h *Homelink) readLoop() {
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

func (h *Homelink) send(f computebus.Frame) error {
	b, err := computebus.Encode(f)
	if err != nil {
		return err
	}
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	return h.ws.WriteMessage(websocket.TextMessage, b)
}

// Close tears down the connection.
func (h *Homelink) Close() error { return h.ws.Close() }
