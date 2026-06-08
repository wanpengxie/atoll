// Package daemon is the attached-compute assembly (v2): homelink (connect to
// server) + host (business cells) + localdevice. Cloud daemon and user-proxy
// daemon are the same binary; cmd selects concrete adapters.
package platform

import (
	"context"

	"github.com/google/uuid"

	"github.com/wanpengxie/ActOS/platform/computebus"
	"github.com/wanpengxie/ActOS/platform/homelink"
	"github.com/wanpengxie/ActOS/platform/host"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// Config configures the attached compute.
type ComputeConfig struct {
	ServerWS  string
	APIKey    string
	ComputeID string
}

// ActorDecl declares one actor the daemon will host.
type ActorDecl struct {
	ID      actor.ActorID
	Kind    actor.Kind
	Binding actor.Binding
	// Impl is the actorrt.Actor implementation (mutually exclusive with Factory).
	Impl actorrt.Actor
	// Factory constructs an actorrt.Actor given the UplinkWriter. Use when the
	// actor needs the writer at construction time.
	Factory func(harness.Writer) actorrt.Actor
}

// Run connects to the channel home and hosts the supplied actors as cells.
// DispatchFrames from the home flow into host cells; their emits flow UP via
// homelink (blocking on the home's EmitAck). No local truth.
//
// Run blocks until ctx is cancelled or the homelink disconnects.
func RunCompute(ctx context.Context, cfg ComputeConfig, actors []ActorDecl) error {
	computeID := cfg.ComputeID
	if computeID == "" {
		computeID = uuid.NewString()
	}

	// Build attach declarations from the actor list.
	decls := make([]computebus.AttachDeclaration, 0, len(actors))
	for _, a := range actors {
		decls = append(decls, computebus.AttachDeclaration{
			ActorID: a.ID,
			Kind:    a.Kind,
			Binding: a.Binding,
		})
	}

	// A mutable holder so the dispatch callback can reach the host after it is
	// constructed.
	var h *host.Host
	hl, err := homelink.Connect(ctx, cfg.ServerWS, cfg.APIKey, computeID, decls,
		func(df computebus.DispatchFrame) {
			if h != nil {
				_ = h.Dispatch(df)
			}
		})
	if err != nil {
		return err
	}
	defer func() { _ = hl.Close() }()

	h = host.New(hl.Emit, hl.SendDeath)
	defer h.Stop()

	// Install all declared actors as cells.
	for _, a := range actors {
		if a.Factory != nil {
			h.InstallFunc(a.ID, a.Factory)
		} else {
			h.Install(a.ID, a.Impl)
		}
	}

	// Block until context cancellation or homelink disconnect.
	select {
	case <-ctx.Done():
		return nil
	case <-hl.Done():
		return nil
	}
}
