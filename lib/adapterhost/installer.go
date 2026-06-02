package adapterhost

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
)

// InstallDeps bundles the channel-level services an adapter cell needs. The
// composition root (server channelhost for embedded adapters, daemon host for
// attached ones) supplies these and Spawns the returned actor.
type InstallDeps struct {
	ChannelID channel.ID
	Chain     harness.Chain
	Lookup    message.RequestLookup
	Registry  actor.Registry
	TypeReg   message.TypeRegistry
	Clock     func() time.Time
	Logger    behavior.Logger
	Metrics   behavior.Metrics
	State     behavior.StateStore
	// Forward is the relay transport seam (daemon-injected for
	// runtime_inbound_via_relay adapters); nil otherwise.
	Forward behavior.ExternalRequestFunc
	// Futures is the channel-level caller-side future hub; nil for pure
	// receiver adapters.
	Futures callerFutures
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

	// Publish type rows (installOne step 4/6). Strict mode: a non-nil
	// TypeDeclarations map MUST cover every Types entry.
	strict := decl.TypeDeclarations != nil
	if deps.TypeReg != nil {
		for _, t := range decl.Types {
			row := message.TypeRow{
				Type:           t,
				HandlerActorID: decl.ActorID,
				HandlerBinding: decl.Binding,
				MaxPendingMs:   decl.MaxPendingMs,
				AllowedKinds:   []message.Kind{message.KindEvent, message.KindRequest, message.KindResponse},
			}
			if td, ok := decl.TypeDeclarations[t]; ok {
				if len(td.AllowedKinds) > 0 {
					row.AllowedKinds = td.AllowedKinds
				}
			} else if strict {
				return InstallResult{}, fmt.Errorf("adapterhost: type %s missing TypeDeclaration (strict mode)", t)
			}
			if _, err := deps.TypeReg.Upsert(ctx, row); err != nil {
				return InstallResult{}, fmt.Errorf("adapterhost: type_registry upsert %s: %w", t, err)
			}
		}
	}

	a := &adapterActor{
		self:        decl.ActorID,
		module:      mod,
		declaration: decl,
		correlation: map[behavior.CorrelationKey]behavior.CorrelationEntry{},
		logger:      deps.Logger,
		metrics:     deps.Metrics,
		state:       deps.State,
		channelID:   deps.ChannelID,
		chain:       deps.Chain,
		lookup:      deps.Lookup,
		clock:       deps.Clock,
		forward:     deps.Forward,
		futures:     deps.Futures,
	}
	return InstallResult{ActorID: decl.ActorID, Declaration: decl, Actor: a}, nil
}
