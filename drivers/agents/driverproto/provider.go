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
