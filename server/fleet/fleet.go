// Package fleet manages attached computes (daemons) over the home↔compute WS.
// See doc.go. It receives attach (api-key), tracks actor→compute, dispatches
// envelopes DOWN to the hosting compute, and writes compute EmitFrames into the
// channel harness (truth), acking the WriteResult back.
package fleet

import (
	"context"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/wire/computebus"
)

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

type computeConn struct {
	id      string
	ws      *websocket.Conn
	writeMu sync.Mutex
}

func (c *computeConn) send(f computebus.Frame) error {
	b, err := computebus.Encode(f)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.ws.WriteMessage(websocket.TextMessage, b)
}

// Fleet is the home-side compute manager.
type Fleet struct {
	chain  harness.Chain
	apiKey string

	mu        sync.RWMutex
	computes  map[string]*computeConn
	actorHost map[actor.ActorID]string // actorID → computeID
}

// New constructs a fleet bound to the channel home's harness chain. apiKey
// authenticates attaching computes (empty = accept any, dev only).
func New(chain harness.Chain, apiKey string) *Fleet {
	return &Fleet{
		chain:     chain,
		apiKey:    apiKey,
		computes:  map[string]*computeConn{},
		actorHost: map[actor.ActorID]string{},
	}
}

// ServeWS upgrades an attaching compute connection and serves its frame loop.
func (f *Fleet) ServeWS(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer func() { _ = ws.Close() }()

	// First frame must be attach.
	_, raw, err := ws.ReadMessage()
	if err != nil {
		return
	}
	first, err := computebus.Decode(raw)
	if err != nil || first.Type != computebus.FrameAttach || first.Attach == nil {
		return
	}
	att := first.Attach
	if f.apiKey != "" && att.APIKey != f.apiKey {
		_ = ws.WriteJSON(computebus.Frame{Type: computebus.FrameAttachReply, Reply: &computebus.AttachReply{Accepted: false, Reason: "bad api-key"}})
		return
	}
	conn := &computeConn{id: att.ComputeID, ws: ws}
	f.register(conn, att.Hosts)
	defer f.unregister(conn, att.Hosts)
	_ = conn.send(computebus.Frame{Type: computebus.FrameAttachReply, Reply: &computebus.AttachReply{Accepted: true}})

	for {
		_, raw, err := ws.ReadMessage()
		if err != nil {
			return
		}
		fr, err := computebus.Decode(raw)
		if err != nil {
			continue
		}
		f.handleFrame(r.Context(), conn, fr)
	}
}

func (f *Fleet) handleFrame(ctx context.Context, conn *computeConn, fr computebus.Frame) {
	switch fr.Type {
	case computebus.FrameEmit:
		if fr.Emit == nil {
			return
		}
		// Write the compute cell's output into channel truth via the home
		// harness, then ack the WriteResult back so the cell observes it.
		res, err := f.chain.Write(ctx, fr.Emit.Envelope)
		ack := computebus.EmitAck{EmitID: fr.EmitID, MessageID: res.MessageID, RejectReason: string(res.RejectReason)}
		if err != nil {
			ack.Err = err.Error()
		}
		_ = conn.send(computebus.Frame{Type: computebus.FrameEmitAck, Ack: &ack})
	case computebus.FrameHeartbeat:
		// Lease re-arm (presence is advisory; nothing to write here in v1).
	case computebus.FrameDeath:
		// A compute cell died; the caller-scoped closure on the waiting side
		// collapses independently. (Death routing to home-side pending senders
		// lands with the full caller-side futureHub.)
	}
}

func (f *Fleet) register(conn *computeConn, hosts []actor.ActorID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.computes[conn.id] = conn
	for _, a := range hosts {
		f.actorHost[a] = conn.id
	}
}

func (f *Fleet) unregister(conn *computeConn, hosts []actor.ActorID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.computes[conn.id] == conn {
		delete(f.computes, conn.id)
	}
	for _, a := range hosts {
		if f.actorHost[a] == conn.id {
			delete(f.actorHost, a)
		}
	}
}

// Dispatch sends an envelope DOWN to the compute hosting target. Returns false
// if no compute hosts it (the caller's closure then collapses).
func (f *Fleet) Dispatch(target actor.ActorID, env *message.Envelope) bool {
	f.mu.RLock()
	cid, ok := f.actorHost[target]
	conn := f.computes[cid]
	f.mu.RUnlock()
	if !ok || conn == nil {
		return false
	}
	return conn.send(computebus.Frame{Type: computebus.FrameDispatch, Dispatch: &computebus.DispatchFrame{Target: target, Envelope: env}}) == nil
}
