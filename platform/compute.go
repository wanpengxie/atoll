// Package platform is the channel-home and attached-compute assembly. compute.go
// is the daemon (attached-compute) assembly root: link.Dial (connect to the
// channel home) → host (business cells) → per-actor stream wiring → Start. Cloud
// daemon and user-proxy daemon are the same binary; cmd selects concrete actors.
package platform

import (
	"context"
	"log/slog"

	"github.com/google/uuid"

	"github.com/wanpengxie/ActOS/platform/host"
	"github.com/wanpengxie/ActOS/platform/link"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// ComputeConfig configures the attached compute.
type ComputeConfig struct {
	ServerWS  string
	APIKey    string
	ComputeID string
	Logger    *slog.Logger
}

// ActorDecl declares one actor the daemon will host.
type ActorDecl struct {
	ID      actor.ActorID
	Kind    actor.Kind
	Binding actor.Binding
	// Impl is the actorrt.Actor implementation (mutually exclusive with Factory).
	Impl actorrt.Actor
	// Factory constructs an actorrt.Actor given the cell's writer (the link's
	// out-of-process pen). Use when the actor needs the writer at construction.
	Factory func(harness.Writer) actorrt.Actor
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

	// Dial first (WS + attach handshake, no actor streams yet), build the host,
	// open one stream per actor (each wiring its dispatch handler synchronously
	// before any frame is delivered), then Start the idle ping. No inbound
	// dispatch can race a half-built host: a stream exists only after OpenStream.
	d, err := link.Dial(ctx, cfg.ServerWS, cfg.APIKey, computeID, decls, logger)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()

	h := host.New()
	defer h.Stop()

	for _, a := range actors {
		// Open the actor's link stream first: the RemoteWriter (the cell's pen)
		// must exist before the cell is spawned. One stream == one actor, so the
		// dispatch handler routes every envelope on this stream into THIS actor's
		// mailbox (the stream IS the target — no audience demux on the daemon).
		target := a.ID
		writer, downHandler, err := d.OpenStream(target, func(env *message.Envelope) error {
			return h.Dispatch(target, env)
		})
		if err != nil {
			return err
		}
		impl := a.Impl
		if a.Factory != nil {
			impl = a.Factory(writer)
		}
		h.Install(a.ID, impl, downHandler)
	}

	d.Start()

	select {
	case <-ctx.Done():
		return nil
	case <-d.Done():
		return nil
	}
}
