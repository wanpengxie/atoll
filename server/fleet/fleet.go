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
	rtharness "github.com/wanpengxie/ActOS/runtime/harness"
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

	// onDeath materialises receiver_unavailable at the home for a compute cell
	// death (DeathFrame). server.Run injects home.MaterialiseComputeDeath.
	onDeath func(context.Context, actor.ActorID)
	// onAttach registers an attaching compute's actors + publishes their types
	// into home truth. server.Run injects home.RegisterComputeActors.
	onAttach func(context.Context, []computebus.AttachDeclaration) error
	// onPresence records an actor's physical presence at the home (attach /
	// heartbeat lease re-arm / detach). server.Run injects home.MarkPresence.
	onPresence func(context.Context, actor.ActorID, bool, int64)
}

// presenceLeaseMs is the lease window a compute heartbeat re-arms; a compute
// heartbeats well within it (homelink heartbeatEveryMs).
const presenceLeaseMs int64 = 30_000

// SetOnPresence wires the home-side presence projection invoked on
// attach/heartbeat/detach so the sysactor's advisory presence view tracks
// attached computes.
func (f *Fleet) SetOnPresence(fn func(context.Context, actor.ActorID, bool, int64)) {
	f.onPresence = fn
}

// SetOnDeath wires the home-side death收口 invoked when an attached compute
// reports a cell death (DeathFrame). Without it a compute cell death cannot
// close out the in-flight requests waiting on that actor at the home.
func (f *Fleet) SetOnDeath(fn func(context.Context, actor.ActorID)) { f.onDeath = fn }

// SetOnAttach wires the home-side registration invoked when a compute attaches,
// so its hosted actors are registered + their request types published into truth
// (without it the home rejects requests for the compute's types as unknown).
func (f *Fleet) SetOnAttach(fn func(context.Context, []computebus.AttachDeclaration) error) {
	f.onAttach = fn
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
	// Register the compute's actors + publish their types into home truth before
	// accepting — otherwise requests for those types are rejected as unknown.
	if f.onAttach != nil {
		if err := f.onAttach(r.Context(), att.Declarations); err != nil {
			_ = ws.WriteJSON(computebus.Frame{Type: computebus.FrameAttachReply, Reply: &computebus.AttachReply{Accepted: false, Reason: "register: " + err.Error()}})
			return
		}
	}
	conn := &computeConn{id: att.ComputeID, ws: ws}
	hosts := declActorIDs(att.Declarations)
	f.register(conn, hosts)
	defer f.unregister(conn, hosts)
	f.markPresence(r.Context(), hosts, true) // present on attach
	defer f.markPresence(context.Background(), hosts, false)
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
		// harness, then ack the WriteResult back so the cell observes it. Stamp
		// the caller identity from EmitFrame.Source so the harness ACL
		// authenticates the write (and a compute can't emit AS another actor —
		// step 1/3 compare envelope.sender.id against this principal).
		cctx := rtharness.CtxWithCaller(ctx, rtharness.CallerContext{
			ActorID: fr.Emit.Source, ChannelID: fr.Emit.Envelope.ChannelID, AllowProvidedSenderKind: true,
		})
		res, err := f.chain.Write(cctx, fr.Emit.Envelope)
		ack := computebus.EmitAck{EmitID: fr.EmitID, MessageID: res.MessageID, RejectReason: string(res.RejectReason)}
		if err != nil {
			ack.Err = err.Error()
		}
		_ = conn.send(computebus.Frame{Type: computebus.FrameEmitAck, Ack: &ack})
	case computebus.FrameHeartbeat:
		// Lease re-arm: refresh presence for the actors the compute reports live.
		if fr.Beat != nil {
			f.markPresence(ctx, fr.Beat.Present, true)
		}
	case computebus.FrameDeath:
		// A compute cell died. The compute has no local truth, so the home must
		// materialise receiver_unavailable for every in-flight request addressed
		// to the dead actor (substrate closure author #3, across the wire). The
		// dead actor is also no longer hosted — drop its routing entry.
		if fr.Death != nil {
			f.dropActor(conn, fr.Death.Actor)
			if f.onDeath != nil {
				f.onDeath(ctx, fr.Death.Actor)
			}
		}
	}
}

// markPresence projects presence for a set of actors through the home seam
// (no-op if unwired). Lease TTL is presenceLeaseMs on present, 0 on absent.
func (f *Fleet) markPresence(ctx context.Context, ids []actor.ActorID, present bool) {
	if f.onPresence == nil {
		return
	}
	ttl := presenceLeaseMs
	if !present {
		ttl = 0
	}
	for _, id := range ids {
		f.onPresence(ctx, id, present, ttl)
	}
}

// declActorIDs extracts the actor ids from a compute's attach declarations.
func declActorIDs(decls []computebus.AttachDeclaration) []actor.ActorID {
	ids := make([]actor.ActorID, 0, len(decls))
	for _, d := range decls {
		ids = append(ids, d.ActorID)
	}
	return ids
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

// dropActor removes a dead actor's routing entry (it is no longer hosted on the
// compute). Guarded by conn ownership so a stale frame can't unhost a relocated
// actor.
func (f *Fleet) dropActor(conn *computeConn, a actor.ActorID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.actorHost[a] == conn.id {
		delete(f.actorHost, a)
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
