// Package driverproto defines the provider-neutral contract between the
// shared agent runtime and provider adapters.
package driverproto

import (
	"context"
	"fmt"
	"strings"
)

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
	// SelectionTitles are display metadata parallel to Selections (same index).
	// They ride NEXT TO TurnOptions, never inside it: options participate in
	// persistence and equality, titles never do — they only feed the
	// agent.select manifest schema (oneOf branch titles).
	SelectionTitles []SelectionTitle
}

// SelectionTitle is one selection's optional human names. Empty fields mean
// "show the raw value".
type SelectionTitle struct {
	Model  string
	Effort string
}

// ValidateSelections rejects a selections list the agent.select manifest
// schema cannot faithfully represent: blank fields, or a duplicate
// (model, effort) pair — a duplicate becomes two identical oneOf branches,
// and a fully valid submit then matches both and fails oneOf validation.
func ValidateSelections(selections []TurnOptions) error {
	seen := map[TurnOptions]struct{}{}
	for i, option := range selections {
		if strings.TrimSpace(option.Model) == "" || strings.TrimSpace(option.Effort) == "" {
			return fmt.Errorf("selections[%d]: model and effort must be non-empty", i)
		}
		if _, dup := seen[option]; dup {
			return fmt.Errorf("selections[%d]: duplicate (model, effort) pair %s/%s", i, option.Model, option.Effort)
		}
		seen[option] = struct{}{}
	}
	return nil
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
	DeviceID     string // server-assigned daemon installation hosting this agent
	DeviceName   string // canonical registry name used in readable file addresses
	DeviceLabel  string // optional local diagnostic label; never used for routing
	WorkspaceDir string // absolute channel workspace path and child-process cwd
	IsCore       bool   // whether that channel is c0, the space registry channel
}
