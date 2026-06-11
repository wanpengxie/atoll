package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/ipc"
	"github.com/wanpengxie/ActOS/runtime/storespec"
	"github.com/wanpengxie/ActOS/platform/computebus"
)

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

// Config configures a Fleet.
type Config struct {
	Writer     harness.Writer
	Runtime    *actorrt.Runtime
	Membership storespec.MembershipControlPlane
	ChannelID  channel.ID
	Logger     *slog.Logger
}

// Fleet is a WS-to-virtual-pipe multiplexer: the physical layer of the channel
// home. It owns no business logic -- that lives in channelhost / actorrt.
// Auth is the app layer's responsibility; fleet accepts a pre-authenticated
// daemonID via ServeWSWithDaemonID.
type Fleet struct {
	writer     harness.Writer
	runtime    *actorrt.Runtime
	membership storespec.MembershipControlPlane
	channelID  channel.ID
	logger     *slog.Logger

	// Lifecycle: cancel signals all active ServeWS goroutines to stop;
	// wg tracks them so Close() can wait for clean exit.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// mu guards activeComputes.
	mu              sync.Mutex
	activeComputes  []*computeState
}

// New constructs a Fleet.
func New(cfg Config) *Fleet {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Fleet{
		writer:     cfg.Writer,
		runtime:    cfg.Runtime,
		membership: cfg.Membership,
		channelID:  cfg.ChannelID,
		logger:     logger,
		ctx:        ctx,
		cancel:     cancel,
	}
}

// actorPipe tracks one daemon actor's virtual pipe.
type actorPipe struct {
	actorID   actor.ActorID
	fleetConn net.Conn // fleet side of the pipe -- relay reads from here
}

// computeState tracks one attached compute's resources.
type computeState struct {
	computeID    string
	pipes        []actorPipe
	teardownOnce sync.Once
}

// ServeWS upgrades an attaching compute connection and serves its frame loop.
// The daemonID parameter is the pre-authenticated daemon identifier resolved by
// the app layer. If empty, the compute's self-declared ID is used (dev mode).
func (f *Fleet) ServeWS(w http.ResponseWriter, r *http.Request, daemonID string) {
	// Reject new connections after Close().
	if f.ctx.Err() != nil {
		http.Error(w, "fleet closed", http.StatusServiceUnavailable)
		return
	}

	f.wg.Add(1)
	defer f.wg.Done()

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer func() { _ = ws.Close() }()

	state, err := f.handleAttach(r.Context(), ws, daemonID)
	if err != nil {
		f.logger.Info("fleet.attach_failed", "err", err)
		return
	}
	f.trackCompute(state)
	defer f.untrackCompute(state)
	defer f.teardownCompute(state)

	// Start relay goroutines: one per actor pipe, reading ipc.KindDeliver
	// frames from the fleet side and translating to computebus.DispatchFrame
	// sent over WS.
	var relayWG sync.WaitGroup
	var wsMu sync.Mutex // guards ws.WriteMessage
	for i := range state.pipes {
		ap := &state.pipes[i]
		relayWG.Add(1)
		go func() {
			defer relayWG.Done()
			f.relayLoop(ap, ws, &wsMu)
		}()
	}

	// Close the WS when fleet context is cancelled, causing wsReadLoop to exit.
	go func() {
		select {
		case <-f.ctx.Done():
			_ = ws.Close()
		case <-r.Context().Done():
		}
	}()

	// WS read loop.
	f.wsReadLoop(r.Context(), ws, &wsMu, state)

	// WS closed or errored -- tear down all pipes (defer teardownCompute), then
	// wait for relay goroutines to notice pipe closure and exit.
	f.teardownCompute(state)
	relayWG.Wait()
}

// handleAttach reads the AttachRequest, uses the pre-authenticated daemonID
// (provided by the app layer), registers actors in membership and sets up
// virtual pipes + actorrt.Attach for each declared actor. Returns the compute
// state or an error. If daemonID is empty, the compute's self-declared ID is
// used (dev mode).
func (f *Fleet) handleAttach(ctx context.Context, ws *websocket.Conn, daemonID string) (*computeState, error) {
	// First WS message must be AttachRequest.
	_, raw, err := ws.ReadMessage()
	if err != nil {
		return nil, fmt.Errorf("read attach: %w", err)
	}
	first, err := computebus.Decode(raw)
	if err != nil || first.Type != computebus.FrameAttach || first.Attach == nil {
		return nil, fmt.Errorf("expected attach frame")
	}
	att := first.Attach

	// Use pre-authenticated daemonID from app layer, fall back to
	// compute's self-declared ID (dev mode).
	computeID := att.ComputeID
	if daemonID != "" {
		computeID = daemonID
	}

	// Register compute actors into membership store.
	if f.membership != nil {
		nowMs := time.Now().UnixMilli()
		adds := make([]storespec.MemberActorAdd, len(att.Declarations))
		for i, d := range att.Declarations {
			adds[i] = storespec.MemberActorAdd{
				ID:      d.ActorID,
				Kind:    d.Kind,
				Binding: d.Binding,
				At:      nowMs,
			}
		}
		if err := f.membership.ApplyMemberTransitions(ctx, f.channelID, adds, nil); err != nil {
			_ = sendFrame(ws, computebus.Frame{
				Type:  computebus.FrameAttachReply,
				Reply: &computebus.AttachReply{Accepted: false, Reason: "register: " + err.Error()},
			})
			return nil, fmt.Errorf("membership register: %w", err)
		}
	}

	// Set up virtual pipes and actorrt.Attach for each declared actor.
	state := &computeState{computeID: computeID}
	for _, decl := range att.Declarations {
		ap, err := f.attachActor(decl.ActorID)
		if err != nil {
			// Clean up already-attached pipes.
			for _, prev := range state.pipes {
				_ = prev.fleetConn.Close()
			}
			// Roll back membership registration.
			if f.membership != nil {
				nowMs := time.Now().UnixMilli()
				removes := make([]storespec.MemberActorRemove, len(att.Declarations))
				for i, d := range att.Declarations {
					removes[i] = storespec.MemberActorRemove{ID: d.ActorID, At: nowMs}
				}
				_ = f.membership.ApplyMemberTransitions(ctx, f.channelID, nil, removes)
			}
			_ = sendFrame(ws, computebus.Frame{
				Type:  computebus.FrameAttachReply,
				Reply: &computebus.AttachReply{Accepted: false, Reason: "attach: " + err.Error()},
			})
			return nil, fmt.Errorf("attach actor %s: %w", decl.ActorID, err)
		}
		state.pipes = append(state.pipes, *ap)
	}

	// Send accepted reply.
	_ = sendFrame(ws, computebus.Frame{
		Type:  computebus.FrameAttachReply,
		Reply: &computebus.AttachReply{ChannelID: f.channelID, Accepted: true},
	})

	f.logger.Info("fleet.attached", "compute", computeID,
		"actors", len(att.Declarations))
	return state, nil
}

// attachActor creates a net.Pipe, writes a synthetic ipc handshake on the fleet
// side, and calls actorrt.Attach on the server side. net.Pipe is synchronous,
// so the handshake write (fleet side) and Attach read (server side) must happen
// concurrently. A goroutine writes KindHandshake + reads KindHandshakeAck on
// the fleet side while the main goroutine calls Attach on the server side.
func (f *Fleet) attachActor(actorID actor.ActorID) (*actorPipe, error) {
	serverConn, fleetConn := net.Pipe()

	hsPayload, err := json.Marshal(ipc.HandshakePayload{LeaseID: string(actorID)})
	if err != nil {
		_ = serverConn.Close()
		_ = fleetConn.Close()
		return nil, err
	}

	// Fleet-side goroutine: write synthetic KindHandshake, then read
	// KindHandshakeAck. Must run concurrently with Attach (which reads from
	// serverConn and writes back the ack).
	type hsResult struct{ err error }
	hsDone := make(chan hsResult, 1)
	fleetCodec := ipc.NewCodec(fleetConn, fleetConn)
	go func() {
		if writeErr := fleetCodec.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: hsPayload}); writeErr != nil {
			hsDone <- hsResult{fmt.Errorf("write handshake: %w", writeErr)}
			return
		}
		ackFrame, readErr := fleetCodec.Read()
		if readErr != nil {
			hsDone <- hsResult{fmt.Errorf("read handshake ack: %w", readErr)}
			return
		}
		if ackFrame.Kind != ipc.KindHandshakeAck {
			hsDone <- hsResult{fmt.Errorf("expected handshake_ack, got %s", ackFrame.Kind)}
			return
		}
		hsDone <- hsResult{}
	}()

	// noopEmitSink: non-nil (port.go requires it) but never actually called.
	// Emits flow via WS (FrameEmit), not through the pipe.
	noopEmit := func(_ context.Context, _ *message.Envelope) error { return nil }

	// resolveFunc: the fleet already knows the actorID (it assigned it).
	resolve := func(leaseID string) (actor.ActorID, error) {
		return actor.ActorID(leaseID), nil
	}

	// actorrt.Attach reads KindHandshake from serverConn (written by the
	// goroutine above on fleetConn), resolves the actor, writes
	// KindHandshakeAck, and registers the actor as a port presence.
	_, err = f.runtime.Attach(serverConn, noopEmit, resolve)
	if err != nil {
		_ = serverConn.Close()
		_ = fleetConn.Close()
		return nil, fmt.Errorf("actorrt.Attach: %w", err)
	}

	// Wait for the fleet-side handshake goroutine to complete.
	res := <-hsDone
	if res.err != nil {
		_ = serverConn.Close()
		_ = fleetConn.Close()
		return nil, res.err
	}

	return &actorPipe{
		actorID:   actorID,
		fleetConn: fleetConn,
	}, nil
}

const wsWriteTimeout = 10 * time.Second

// relayLoop reads ipc frames from the fleet side of a virtual pipe and
// translates KindDeliver frames into computebus.DispatchFrame sent over WS. It
// exits when the fleet conn is closed (pipe EOF) or the WS write fails.
func (f *Fleet) relayLoop(ap *actorPipe, ws *websocket.Conn, wsMu *sync.Mutex) {
	codec := ipc.NewCodec(ap.fleetConn, ap.fleetConn)
	for {
		frame, err := codec.Read()
		if err != nil {
			return
		}
		switch frame.Kind {
		case ipc.KindDeliver:
			var dp ipc.DeliverPayload
			if err := json.Unmarshal(frame.Payload, &dp); err != nil {
				f.logger.Error("fleet.relay.decode", "actor", ap.actorID, "err", err)
				continue
			}
			env := dp.Envelope
			wsFrame := computebus.Frame{
				Type: computebus.FrameDispatch,
				Dispatch: &computebus.DispatchFrame{
					Target:   ap.actorID,
					Envelope: &env,
				},
			}
			wsMu.Lock()
			_ = ws.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
			err = sendFrame(ws, wsFrame)
			wsMu.Unlock()
			if err != nil {
				f.logger.Error("fleet.relay.send", "actor", ap.actorID, "err", err)
				return
			}
		case ipc.KindControl:
			f.logger.Debug("fleet.relay.control", "actor", ap.actorID)
		default:
			f.logger.Warn("fleet.relay.unknown_kind", "actor", ap.actorID, "kind", frame.Kind)
		}
	}
}

// wsReadLoop reads WS frames from the compute and dispatches them.
func (f *Fleet) wsReadLoop(ctx context.Context, ws *websocket.Conn, wsMu *sync.Mutex, state *computeState) {
	for {
		_, raw, err := ws.ReadMessage()
		if err != nil {
			// WS closed or errored.
			return
		}
		fr, err := computebus.Decode(raw)
		if err != nil {
			continue
		}
		switch fr.Type {
		case computebus.FrameEmit:
			f.handleEmit(ctx, ws, wsMu, fr)
		case computebus.FrameHeartbeat:
			f.logger.Debug("fleet.heartbeat", "compute", state.computeID)
		case computebus.FrameDeath:
			f.handleDeath(state, fr)
		}
	}
}

// handleEmit writes the emit to truth via the harness writer and sends an ack
// back on the WS.
func (f *Fleet) handleEmit(ctx context.Context, ws *websocket.Conn, wsMu *sync.Mutex, fr computebus.Frame) {
	if fr.Emit == nil {
		return
	}
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
	wsMu.Lock()
	_ = ws.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
	_ = sendFrame(ws, computebus.Frame{Type: computebus.FrameEmitAck, Ack: &ack})
	wsMu.Unlock()
}

// handleDeath closes the pipe for the dead actor. The pipe close causes the
// port's readLoop to see EOF, triggering die() and the OnDown edge
// automatically -- no manual callback needed.
func (f *Fleet) handleDeath(state *computeState, fr computebus.Frame) {
	if fr.Death == nil {
		return
	}
	dead := fr.Death.Actor
	for i := range state.pipes {
		if state.pipes[i].actorID == dead {
			_ = state.pipes[i].fleetConn.Close()
			f.logger.Info("fleet.death", "actor", string(dead),
				"compute", state.computeID)
			return
		}
	}
}

// teardownCompute closes all virtual pipes for a compute. Each pipe close
// causes the actorrt port to see EOF and fire OnDown. Safe to call multiple
// times (idempotent via sync.Once).
func (f *Fleet) teardownCompute(state *computeState) {
	if state == nil {
		return
	}
	state.teardownOnce.Do(func() {
		for i := range state.pipes {
			_ = state.pipes[i].fleetConn.Close()
		}
	})
}

// trackCompute adds a compute state to the active set.
func (f *Fleet) trackCompute(state *computeState) {
	f.mu.Lock()
	f.activeComputes = append(f.activeComputes, state)
	f.mu.Unlock()
}

// untrackCompute removes a compute state from the active set.
func (f *Fleet) untrackCompute(state *computeState) {
	f.mu.Lock()
	for i, s := range f.activeComputes {
		if s == state {
			f.activeComputes = append(f.activeComputes[:i], f.activeComputes[i+1:]...)
			break
		}
	}
	f.mu.Unlock()
}

// Close tears down the fleet: cancels the fleet context (causing all active WS
// connections to close), tears down all tracked compute states (closing virtual
// pipes), and waits for all ServeWS goroutines to exit.
func (f *Fleet) Close() error {
	// Signal all active connections to stop.
	f.cancel()

	// Tear down all tracked computes to close pipes immediately rather than
	// waiting for the WS close to propagate.
	f.mu.Lock()
	computes := append([]*computeState(nil), f.activeComputes...)
	f.mu.Unlock()
	for _, state := range computes {
		f.teardownCompute(state)
	}

	// Wait for all ServeWS goroutines to finish.
	f.wg.Wait()
	f.logger.Info("fleet.closed")
	return nil
}

func sendFrame(ws *websocket.Conn, f computebus.Frame) error {
	b, err := computebus.Encode(f)
	if err != nil {
		return err
	}
	return ws.WriteMessage(websocket.TextMessage, b)
}
