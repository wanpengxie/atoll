// Package driverproto defines the provider-neutral contract between the
// shared agent runtime and provider adapters.
package driverproto

import (
	"context"
	"fmt"
	"strings"
	"time"
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
	// persistence and equality, titles never do. Providers use them only when
	// they must construct the declaration fallback for agent.options.
	SelectionTitles []SelectionTitle
}

// SelectionTitle is one selection's optional human names. Empty fields mean
// "show the raw value".
type SelectionTitle struct {
	Model  string
	Effort string
}

// OptionsSnapshot is one provider worker generation's native option catalog.
// It is actor-local runtime data: the provider discovers it while opening and
// agent.options returns it. Declaration selections remain only the fallback
// used when native discovery is unavailable.
type OptionsSnapshot struct {
	Provider    string        `json:"provider"`
	Source      string        `json:"source"`
	GeneratedAt string        `json:"generated_at"`
	Models      []ModelOption `json:"models"`
	Default     TurnOptions   `json:"default"`
	Current     TurnOptions   `json:"current"`
	Client      ClientInfo    `json:"client"`
}

type ModelOption struct {
	Value       string         `json:"value"`
	Label       string         `json:"label,omitempty"`
	Description string         `json:"description,omitempty"`
	Efforts     []EffortOption `json:"efforts,omitempty"`
}

type EffortOption struct {
	Value       string `json:"value"`
	Label       string `json:"label,omitempty"`
	Description string `json:"description,omitempty"`
}

type ClientInfo struct {
	Name         string `json:"name"`
	Current      string `json:"current,omitempty"`
	Latest       string `json:"latest,omitempty"`
	UpdateStatus string `json:"update_status"`
}

const (
	OptionsSourceNative   = "native"
	OptionsSourceFallback = "fallback"
	UpdateCurrent         = "current"
	UpdateAvailable       = "available"
	UpdateUnknown         = "unknown"
)

// FallbackOptions groups the declaration's flat legal pairs into the same
// model-first shape native providers report.
func FallbackOptions(provider string, selections []TurnOptions, titles []SelectionTitle, defaultIndex int, current TurnOptions) OptionsSnapshot {
	models := make([]ModelOption, 0)
	index := map[string]int{}
	for i, pair := range selections {
		model := strings.TrimSpace(pair.Model)
		if model == "" {
			continue
		}
		mi, ok := index[model]
		if !ok {
			label := ""
			if i < len(titles) {
				label = titles[i].Model
			}
			mi = len(models)
			index[model] = mi
			models = append(models, ModelOption{Value: model, Label: label})
		}
		effort := strings.TrimSpace(pair.Effort)
		if effort == "" {
			continue
		}
		label := ""
		if i < len(titles) {
			label = titles[i].Effort
		}
		models[mi].Efforts = append(models[mi].Efforts, EffortOption{Value: effort, Label: label})
	}
	def := TurnOptions{}
	if defaultIndex >= 0 && defaultIndex < len(selections) {
		def = selections[defaultIndex]
	}
	if current.Model == "" {
		current = def
	}
	return OptionsSnapshot{
		Provider: provider, Source: OptionsSourceFallback, GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Models: models, Default: def, Current: current,
		Client: ClientInfo{Name: provider, UpdateStatus: UpdateUnknown},
	}
}

func CloneOptionsSnapshot(in OptionsSnapshot) OptionsSnapshot {
	out := in
	out.Models = make([]ModelOption, len(in.Models))
	for i, model := range in.Models {
		out.Models[i] = model
		out.Models[i].Efforts = append([]EffortOption(nil), model.Efforts...)
	}
	return out
}

func (s OptionsSnapshot) Accepts(options TurnOptions) bool {
	for _, model := range s.Models {
		if model.Value != options.Model {
			continue
		}
		if options.Effort == "" {
			return true
		}
		for _, effort := range model.Efforts {
			if effort.Value == options.Effort {
				return true
			}
		}
	}
	return false
}

// ValidateSelections rejects a fallback catalog with blank fields or a
// duplicate (model, effort) pair. The catalog is data returned by
// agent.options; it is deliberately absent from actor.describe's schema.
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
