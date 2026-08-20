// Package driverproto defines the provider-neutral contract between the
// shared agent runtime and provider adapters.
package driverproto

import "context"

type Documentation struct {
	Description string
	SkillDoc    string
}

type Provider interface {
	Spec() ProviderSpec
	NewWorker(WorkerHost) (Worker, error)
}

// Worker owns every physical resource in one runtime generation.
type Worker interface {
	Open(context.Context, OpenRequest)
	Start(context.Context, StartRequest)
	Control(context.Context, ControlRequest)
	Retire()
	Reaped() <-chan struct{}
}

type ProviderSpec struct {
	Name             string
	Capabilities     map[string]bool
	Documentation    Documentation
	Selections       []TurnOptions
	DefaultSelection int
}

const (
	CapabilitySteer     = "steer"
	CapabilityInterrupt = "interrupt"
	CapabilityResume    = "resume"
	CapabilityFork      = "fork"
)

// Situation is who one agent is and where it sits. It is host context, never
// declaration config: composition knows the member id and channel, the model
// does not — every tool an agent has answers about OTHER actors, so without
// this it cannot tell its own facts from its sender's. Providers project it
// into the system prompt; nothing on the wire carries it.
type Situation struct {
	ActorID      string // this agent's full member id
	Kind         string // its actor kind, from the closed set
	Seed         string // its declaration id (agents are declaration-minted)
	Class        string // the class backing it, e.g. codex
	Channel      string // the channel it lives in
	DeviceName   string // the device hosting this agent
	WorkspaceDir string // absolute channel workspace path and child-process cwd
	IsCore       bool   // whether that channel is c0, the space registry channel
}
