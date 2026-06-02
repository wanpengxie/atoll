// Package channelkit is the channel template — the OTP application+supervisor
// analog sitting on top of runtime/actorrt. It ASSEMBLES a channel's固有 cells
// (the system actor) + the audience policy resolver + the supervision tree, and
// holds references to the engine pieces (store/harness/trigger) the deployment
// layer provides. It composes; it does not own. The supervision tree is
// MECHANISM (manages goroutine life) — never an actor; domain coordination is
// the system actor's job (don't put coordination back in the mechanism layer).
package channelkit

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/lib/policy"
	"github.com/wanpengxie/ActOS/lib/sysactor"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
)

// Channel is one assembled channel: its actorrt runtime (cell host) + system
// actor + policy resolver. The deployment layer (server channelhost) provisions
// the store/harness/trigger and Spawns the固有 cells.
type Channel struct {
	channelID channel.ID
	cells     *actorrt.Runtime
	system    *sysactor.SystemActor
	resolver  *policy.Resolver
}

// Config assembles a channel.
type Config struct {
	ChannelID channel.ID
	System    *sysactor.SystemActor
}

// New builds the channel, wiring the supervision tree (the Channel itself is the
// actorrt.Supervisor — see OnDeath) and spawning the固有 system cell.
func New(cfg Config) *Channel {
	c := &Channel{
		channelID: cfg.ChannelID,
		system:    cfg.System,
		resolver:  policy.New(),
	}
	c.cells = actorrt.New(actorrt.Config{Supervisor: c})
	if cfg.System != nil {
		c.cells.Spawn(actor.SystemActorID, cfg.System)
	}
	return c
}

// Cells exposes the runtime so the deployment layer spawns business cells.
func (c *Channel) Cells() *actorrt.Runtime { return c.cells }

// Resolver exposes the audience policy resolver.
func (c *Channel) Resolver() *policy.Resolver { return c.resolver }

// OnDeath implements actorrt.Supervisor: when a hosted cell dies abnormally, the
// supervision tree is the seam that materializes receiver_unavailable for the
// dead actor's in-flight requests (the substrate's only closure obligation —
// the death signal). v2 caller-scoped closure: the caller's pending timer also
// collapses independently; this routes the positive death signal to whoever is
// waiting. (Full pending-router wiring lands with the caller-side futureHub.)
func (c *Channel) OnDeath(ctx context.Context, sig actorrt.DeathSignal) {
	// Death observed positively: the substrate reports it; callers waiting on
	// sig.Actor collapse to receiver_unavailable. Routing to specific pending
	// senders is done by the caller-side closure (lib/behavior) once a death
	// notification is delivered. Here we ensure the dead cell is removed so a
	// replacement can be spawned.
	c.cells.Despawn(sig.Actor)
}
