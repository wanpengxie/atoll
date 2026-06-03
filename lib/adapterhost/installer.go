package adapterhost

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	rtharness "github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// InstallDeps bundles the channel-level services an adapter cell needs. The
// composition root (server channelhost for embedded adapters, daemon host for
// attached ones) supplies these and Spawns the returned actor.
type InstallDeps struct {
	ChannelID channel.ID
	Chain     rtharness.Writer
	Lookup    behavior.RequestLookup
	Registry  storespec.Registry
	Clock     func() time.Time
	Logger    *slog.Logger
	Metrics   behavior.Metrics
}

// InstallResult is the installer output for the host to Spawn (dismantle
// §2.5-A c: input module → {actorID, declaration, actor impl}). There is NO
// long-lived Manager — Install is a pure install-time factory.
type InstallResult struct {
	ActorID     actor.ActorID
	Declaration behavior.Declaration
	Actor       actorrt.Actor
}

// Install validates a module's declaration, publishes its type rows, and
// constructs an adapterActor cell (collapse of Manager.installOne manager.go:231).
func Install(ctx context.Context, mod behavior.Module, deps InstallDeps) (InstallResult, error) {
	if mod == nil {
		return InstallResult{}, errors.New("adapterhost: Install nil module")
	}
	if deps.Clock == nil {
		deps.Clock = time.Now
	}
	decl := mod.Declares()
	if decl.Name == "" || decl.ActorID == "" {
		return InstallResult{}, errors.New("adapterhost: declaration Name/ActorID required")
	}

	// Verify the handler actor exists and its binding matches (installOne step 2).
	if deps.Registry != nil {
		rec, ok, err := deps.Registry.Lookup(ctx, decl.ActorID)
		if err != nil {
			return InstallResult{}, fmt.Errorf("adapterhost: actor lookup %s: %w", decl.ActorID, err)
		}
		if !ok {
			return InstallResult{}, fmt.Errorf("adapterhost: actor %s not registered", decl.ActorID)
		}
		if decl.Binding != rec.Binding {
			return InstallResult{}, fmt.Errorf("adapterhost: binding mismatch actor=%s registry=%s declared=%s",
				decl.ActorID, rec.Binding, decl.Binding)
		}
	}

	// NOTE: type vocabulary is NOT published from here. type_registry left the
	// substrate (no type gate, no RPC dispatch); the type catalog is a domain
	// concern whose home is deferred to daemon implementation (pain-driven).

	a := &adapterActor{
		self:        decl.ActorID,
		module:      mod,
		declaration: decl,
		logger:      deps.Logger,
		metrics:     deps.Metrics,
		channelID:   deps.ChannelID,
		chain:       deps.Chain,
		lookup:      deps.Lookup,
		clock:       deps.Clock,
	}
	return InstallResult{ActorID: decl.ActorID, Declaration: decl, Actor: a}, nil
}
