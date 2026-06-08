package fleet

import (
	"context"
	"log/slog"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/wire/computebus"
	"github.com/wanpengxie/ActOS/wire/placement"
)

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

// computeConn is one attached compute's WS connection.
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

// OnDeath is called when a compute cell dies or a compute disconnects. The home
// materialises receiver_unavailable for the dead actor's in-flight requests.
type OnDeath func(ctx context.Context, dead actor.ActorID)

// OnAttach is called when a compute attaches, registering its actors into
// membership.
type OnAttach func(ctx context.Context, channelID channel.ID, decls []computebus.AttachDeclaration) error

// Fleet is the home-side compute manager.
type Fleet struct {
	writer    harness.Writer
	channelID channel.ID
	apiKey    string
	placement *placement.Registry
	logger    *slog.Logger

	mu       sync.RWMutex
	computes map[string]*computeConn

	onDeath  OnDeath
	onAttach OnAttach
}

// Config configures a Fleet.
type Config struct {
	Writer    harness.Writer
	ChannelID channel.ID
	APIKey    string
	Placement *placement.Registry
	OnDeath   OnDeath
	OnAttach  OnAttach
	Logger    *slog.Logger
}

// New constructs a fleet bound to the channel home.
func New(cfg Config) *Fleet {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Fleet{
		writer:    cfg.Writer,
		channelID: cfg.ChannelID,
		apiKey:    cfg.APIKey,
		placement: cfg.Placement,
		logger:    logger,
		computes:  map[string]*computeConn{},
		onDeath:   cfg.OnDeath,
		onAttach:  cfg.OnAttach,
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
		_ = sendFrame(ws, computebus.Frame{
			Type:  computebus.FrameAttachReply,
			Reply: &computebus.AttachReply{Accepted: false, Reason: "bad api-key"},
		})
		return
	}

	// Register the compute's actors into membership before accepting.
	if f.onAttach != nil {
		if err := f.onAttach(r.Context(), f.channelID, att.Declarations); err != nil {
			_ = sendFrame(ws, computebus.Frame{
				Type:  computebus.FrameAttachReply,
				Reply: &computebus.AttachReply{Accepted: false, Reason: "register: " + err.Error()},
			})
			return
		}
	}

	conn := &computeConn{id: att.ComputeID, ws: ws}

	// Register compute + assign actors in placement.
	f.registerCompute(conn, att.Declarations)
	defer f.disconnectCompute(conn)

	_ = conn.send(computebus.Frame{
		Type:  computebus.FrameAttachReply,
		Reply: &computebus.AttachReply{ChannelID: f.channelID, Accepted: true},
	})

	// Read loop.
	for {
		_, raw, err := ws.ReadMessage()
		if err != nil {
			return // disconnect -> defer disconnectCompute handles batch death
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
		f.handleEmit(ctx, conn, fr)
	case computebus.FrameHeartbeat:
		// Heartbeat: keepalive, non-fencing. Log only.
		f.logger.Debug("fleet.heartbeat", "compute", conn.id)
	case computebus.FrameDeath:
		f.handleDeath(ctx, conn, fr)
	}
}

func (f *Fleet) handleEmit(ctx context.Context, conn *computeConn, fr computebus.Frame) {
	if fr.Emit == nil {
		return
	}
	// Stamp the caller identity from EmitFrame.Source so the harness ACL
	// authenticates the write.
	cctx := harness.CtxWithCaller(ctx, harness.CallerContext{
		ActorID:   fr.Emit.Source,
		ChannelID: f.channelID,
	})
	res, err := f.writer.Write(cctx, fr.Emit.Envelope)
	ack := computebus.EmitAck{
		EmitID:       fr.EmitID,
		MessageID:    res.MessageID,
		RejectReason: string(res.RejectReason),
	}
	if err != nil {
		ack.Err = err.Error()
	}
	_ = conn.send(computebus.Frame{Type: computebus.FrameEmitAck, Ack: &ack})
}

func (f *Fleet) handleDeath(ctx context.Context, conn *computeConn, fr computebus.Frame) {
	if fr.Death == nil {
		return
	}
	dead := fr.Death.Actor
	f.placement.Remove(dead)
	f.logger.Info("fleet.death", "actor", string(dead), "compute", conn.id)
	if f.onDeath != nil {
		f.onDeath(ctx, dead)
	}
}

func (f *Fleet) registerCompute(conn *computeConn, decls []computebus.AttachDeclaration) {
	f.mu.Lock()
	f.computes[conn.id] = conn
	f.mu.Unlock()
	for _, d := range decls {
		f.placement.Assign(d.ActorID, conn.id)
	}
}

func (f *Fleet) disconnectCompute(conn *computeConn) {
	f.mu.Lock()
	if f.computes[conn.id] == conn {
		delete(f.computes, conn.id)
	}
	f.mu.Unlock()
	// RemoveCompute returns all actors that were assigned to this compute.
	// Each is now dead -- materialise receiver_unavailable.
	affected := f.placement.RemoveCompute(conn.id)
	f.logger.Info("fleet.disconnect", "compute", conn.id, "affected", len(affected))
	if f.onDeath != nil {
		for _, a := range affected {
			f.onDeath(context.Background(), a)
		}
	}
}

// Dispatch sends an envelope DOWN to the compute hosting target. Returns false
// if no compute hosts it.
func (f *Fleet) Dispatch(target actor.ActorID, env *message.Envelope) bool {
	computeID, ok := f.placement.Lookup(target)
	if !ok {
		return false
	}
	f.mu.RLock()
	conn := f.computes[computeID]
	f.mu.RUnlock()
	if conn == nil {
		return false
	}
	return conn.send(computebus.Frame{
		Type:     computebus.FrameDispatch,
		Dispatch: &computebus.DispatchFrame{Target: target, Envelope: env},
	}) == nil
}

func sendFrame(ws *websocket.Conn, f computebus.Frame) error {
	b, err := computebus.Encode(f)
	if err != nil {
		return err
	}
	return ws.WriteMessage(websocket.TextMessage, b)
}
