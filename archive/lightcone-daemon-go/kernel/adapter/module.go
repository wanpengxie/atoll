package adapter

import (
	"context"

	"github.com/coagent-ai/daemon-go/kernel/message"
)

// Module is the v4 adapter framework F1 interface — a single adapter's
// entry point. Concrete adapters (xhs, feishu, github, …) implement
// this and are composed by the daemon (cmd/daemon).
//
// Handle is called by the daemon's scheduler when an envelope addressed
// to the adapter actor arrives. The adapter is responsible for:
//
//   - validating the envelope per its own type schema
//   - submitting the outbound side-effect (HTTP call / device frame /
//     in-process tool call) via Ctx
//   - calling Ctx.Respond with the terminal response envelope
type Module interface {
	Name() string
	Handle(ctx context.Context, env *message.Envelope) error
}

// Manager is the F1 registry of installed Modules.
type Manager interface {
	Register(m Module) error
	Lookup(name string) (Module, bool)
	Names() []string
}
