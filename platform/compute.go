package platform

// compute.go is the daemon (attached-compute) assembly root: link.Dial (connect
// to the channel home) → actorrt.Runtime (business cells) → per-actor stream
// wiring → Start.
// Cloud daemon and user-proxy daemon are the same binary; cmd selects concrete
// actors.

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/uuid"

	"github.com/wanpengxie/ActOS/platform/internal/link"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// cellDownWatcher is the daemon's PresenceWatcher: when a hosted cell dies
// abnormally, OnDown fires that actor's downHandler (close its stream UP the
// link). The daemon holds no truth, so it cannot write receiver_unavailable
// itself — the home port reads EOF and the home's closure author#3 closes
// in-flight requests.
type cellDownWatcher struct {
	mu   sync.Mutex
	down map[actor.ActorID]func(cause string)
}

// OnDown implements actorrt.PresenceWatcher.
func (w *cellDownWatcher) OnDown(_ context.Context, id actor.ActorID, cause error) {
	w.mu.Lock()
	handler := w.down[id]
	w.mu.Unlock()
	if handler != nil {
		msg := ""
		if cause != nil {
			msg = cause.Error()
		}
		handler(msg)
	}
}

// obsForwardQueue bounds the daemon's async obs-forward buffer. obs is non-truth
// and best-effort, so a full queue drops the edge (superseded by the next edge /
// the home lease decay) rather than blocking the producer.
const obsForwardQueue = 64

type obsMsg struct {
	id   actor.ActorID
	kind string
	val  []byte
}

// cellObsForwarder is the daemon's ObsWatcher: when a hosted cell PublishObs's an
// opaque obs snapshot (e.g. an adapter's device-presence edge), forward it UP the
// link as a KindObs frame so the home runtime's obs consumers see it. The daemon
// holds no truth — obs is non-truth, best-effort; the home folds it into a
// volatile level + lease-decays it.
//
// OnObs runs on the PUBLISHING CELL's goroutine (runtime.publishObs fanout) and
// the ObsWatcher contract requires it be NON-BLOCKING — so OnObs only enqueues
// (best-effort, drop-on-full); a separate pump goroutine does the blocking socket
// write off the cell goroutine. This keeps the observation arm from ever back-
// pressuring the actor's work path.
type cellObsForwarder struct {
	d  *link.Dialer
	ch chan obsMsg
}

func newCellObsForwarder(d *link.Dialer) *cellObsForwarder {
	return &cellObsForwarder{d: d, ch: make(chan obsMsg, obsForwardQueue)}
}

// OnObs implements actorrt.ObsWatcher — non-blocking enqueue (drop on full).
func (f *cellObsForwarder) OnObs(_ context.Context, id actor.ActorID, kind actorrt.ObsKind, val actorrt.ObsValue) {
	m := obsMsg{id: id, kind: string(kind), val: append([]byte(nil), val...)}
	select {
	case f.ch <- m:
	default: // queue full: drop (obs is best-effort; next edge / lease decay covers it)
	}
}

// pump drains the queue onto the link OFF the cell goroutine. Exits when ctx is
// cancelled or the link tears down.
func (f *cellObsForwarder) pump(ctx context.Context, linkDone <-chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-linkDone:
			return
		case m := <-f.ch:
			f.d.SendObs(m.id, m.kind, m.val)
		}
	}
}

// ComputeConfig configures the attached compute. ServerWS carries any auth
// credential in its query string (the ?key= the app layer resolves on WS
// upgrade) — the link layer is auth-agnostic, so there is no separate key field.
type ComputeConfig struct {
	ServerWS  string
	ComputeID string
	Logger    *slog.Logger
}

// ActorDecl declares one actor the daemon will host. Factory constructs the
// actorrt.Actor given the cell's Pen (the link's out-of-process relay-only proxy
// pen) — the pen only exists after the actor's stream opens, so the actor cannot
// be pre-built: every cell that can emit needs its pen at construction, and in
// the actor model every actor can emit. There is no pen-less construction path.
// The proxy pen relays upward without injecting identity; the host emitSink's
// Mint welds the actor's authenticated bound id.
type ActorDecl struct {
	ID      actor.ActorID
	Kind    actor.Kind
	Binding actor.Binding
	Factory func(harness.Pen) actorrt.Actor
}

// RunCompute connects to the channel home and hosts the supplied actors as
// cells. The home dispatches envelopes down each actor's link stream into the
// cell's mailbox; the cell's emits flow UP that same stream as native ipc
// (blocking on the home's authoritative EmitAck). No local truth.
//
// RunCompute blocks until ctx is cancelled or the link disconnects.
func RunCompute(ctx context.Context, cfg ComputeConfig, actors []ActorDecl) error {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	computeID := cfg.ComputeID
	if computeID == "" {
		computeID = uuid.NewString()
	}

	decls := make([]link.Declaration, 0, len(actors))
	for _, a := range actors {
		decls = append(decls, link.Declaration{ActorID: a.ID, Kind: a.Kind, Binding: a.Binding})
	}

	// Dial first (WS + attach handshake, no actor streams yet), build the runtime,
	// open one stream per actor + spawn its cell, then Start. Start launches the
	// per-stream read loops only after every cell is spawned, so no inbound
	// dispatch can race a half-built runtime (deliver frames buffer until Start).
	d, err := link.Dial(ctx, cfg.ServerWS, computeID, decls, logger)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()

	// Cell running is the kernel: the daemon owns an actorrt.Runtime directly. The
	// only daemon-specific glue is the down watcher (close a dead cell's stream UP
	// the link); registered before any cell is spawned.
	rt, del := actorrt.New(actorrt.Config{})
	defer rt.StopAll()
	watcher := &cellDownWatcher{down: map[actor.ActorID]func(cause string){}}
	rt.WatchPresence(watcher)
	obsFwd := newCellObsForwarder(d)
	go obsFwd.pump(ctx, d.Done())

	for _, a := range actors {
		// Open the actor's link stream first: the RemoteWriter (the cell's pen)
		// must exist before the cell is spawned. One stream == one actor, so the
		// dispatch handler routes every envelope on this stream into THIS actor's
		// mailbox (the stream IS the target — no audience demux on the daemon).
		target := a.ID
		pen, downHandler, err := d.OpenStream(target, func(env *message.Envelope) error {
			_, err := del.Deliver([]actor.ActorID{target}, env)
			return err
		}, func(requestID message.ID) {
			rt.CancelRequest(target, requestID)
		})
		if err != nil {
			return err
		}
		if a.Factory == nil {
			return fmt.Errorf("actor %q: nil Factory", a.ID)
		}
		impl := a.Factory(pen)
		watcher.mu.Lock()
		watcher.down[a.ID] = downHandler
		watcher.mu.Unlock()
		// Register the obs forwarder BEFORE Spawn so no early presence edge is
		// missed (same discipline as WatchPresence).
		rt.WatchObs(a.ID, obsFwd)
		rt.Spawn(a.ID, impl)
	}

	d.Start()

	select {
	case <-ctx.Done():
		return nil
	case <-d.Done():
		return nil
	}
}
