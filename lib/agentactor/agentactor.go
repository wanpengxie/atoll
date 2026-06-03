// Package agentactor adapts a worker-session trigger into a kind=agent actor
// cell. Receive routes an inbound request to the injected trigger (serial on the
// cell goroutine); the worker's report is emitted back as the response by the
// daemon host that owns the worker mechanism. agentactor is a thin facade, NOT
// the worker host itself.
//
// NOTE: the v1 worker mechanism (runtime/workerhost) was removed by the
// substrate purification — the actor-host axis is now cell (in-process) / port
// (out-of-process connect-in). The concrete worker-session hosting (a
// port-backed subprocess) is re-derived at daemon implementation, pain-driven;
// agentactor stays the minimal request→trigger facade until then.
package agentactor

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/message"
)

// TriggerFunc launches/continues the worker session for an inbound request. The
// daemon host injects an implementation backed by its worker mechanism.
type TriggerFunc func(ctx context.Context, env *message.Envelope) error

// AgentActor is the worker-session facade: a cell that routes requests to its
// injected trigger.
type AgentActor struct {
	trigger TriggerFunc
}

// New constructs an agent actor cell over a worker-session trigger.
func New(trigger TriggerFunc) *AgentActor {
	return &AgentActor{trigger: trigger}
}

// Receive routes an inbound request to the worker session (serial on the cell
// goroutine). Non-requests are ignored at this seam.
func (a *AgentActor) Receive(ctx context.Context, env *message.Envelope) error {
	if env.Kind == message.KindRequest && a.trigger != nil {
		return a.trigger(ctx, env)
	}
	return nil
}
