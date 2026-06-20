// Package agent is the agent subsystem's public face. It registers the ONE
// "agent" actor class into the kind-neutral registry and owns a private
// looper-registry that engine providers (agent/provider/{kimi,claudecode})
// self-register into via init() (agent-spec §10.2 driver-registration).
//
// The engines are quarantined in agent/provider/*; this core never imports them
// — the same "brain-agnostic" discipline that keeps substrate blind to agent,
// repeated one scale down: agent core is blind to the specific engine. cmd
// blank-imports agent/all (the providers) to populate the looper-registry.
//
// Dependency direction (one-way): {actors/*, agent/, app} → registry → platform
// (substrate). agent never imports app; the engines live only under provider/*.
package agent

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/registry"
)

// LooperConstructor builds the actor decl for one engine, given the instance
// spec (its Config carries persona / knobs) + host context. Identical shape to
// registry.Constructor, so a provider's NewDecl satisfies it directly.
type LooperConstructor = registry.Constructor

// defaultLooper is the engine for an unset looper — the server fallback
// (agent:boost carries no agents row, so app passes an empty looper).
const defaultLooper = "go-kimi"

var (
	mu      sync.RWMutex
	loopers = map[string]LooperConstructor{}
)

// RegisterLooper records an engine under its looper key, called from a provider
// package's init(). A duplicate key is a programmer error (panic, like
// sql.Register). The agent core never calls a provider — registration is the
// only edge, and it points provider → agent (never the reverse).
func RegisterLooper(key string, c LooperConstructor) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := loopers[key]; dup {
		panic("agent: duplicate looper registration: " + key)
	}
	loopers[key] = c
}

func init() { registry.Register("agent", build) }

// build is the "agent" class Constructor. It peeks the looper key out of the
// opaque config (the DSN pattern: app packs agents.looper into Config; the
// engine sub-config ignores the extra key), then dispatches to the registered
// engine. An empty looper = the server fallback = go-kimi.
func build(spec registry.InstanceSpec, ctx registry.Deps) (platform.ActorDecl, error) {
	key := looperOf(spec.Config)
	if key == "" {
		key = defaultLooper
	}
	mu.RLock()
	c, ok := loopers[key]
	mu.RUnlock()
	if !ok {
		return platform.ActorDecl{}, fmt.Errorf("agent: unknown looper %q (registered: %v)", key, Loopers())
	}
	return c(spec, ctx)
}

// looperOf extracts the agent's engine selector from the opaque config blob.
// Exact-match only — there are no looper aliases (agents.looper is the canonical
// column; accepting variants would be premature flexibility with zero consumer).
func looperOf(cfg json.RawMessage) string {
	if len(cfg) == 0 {
		return ""
	}
	var probe struct {
		Looper string `json:"looper"`
	}
	_ = json.Unmarshal(cfg, &probe)
	return probe.Looper
}

// Loopers returns the registered looper keys, sorted (stable iteration).
func Loopers() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(loopers))
	for k := range loopers {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
