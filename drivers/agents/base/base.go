package base

import (
	"errors"
	"sort"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
)

type Config struct {
	NewRuntime      runtimeproto.Factory
	Runtime         runtimeproto.Spec
	RequestMaxCount int
	BufferMaxCount  int
	BufferMaxBytes  int
	BatchMaxCount   int
	ReceiptDeadline time.Duration
}

const (
	defaultBufferMaxCount = 128
	defaultBufferMaxBytes = 8 << 20
	defaultBatchMaxCount  = 32
)

type Definition = actorbase.Def

type definition struct {
	cfg      Config
	controls map[string]struct{}
}

func Def(doc string, cfg Config) (actorbase.Def, error) {
	if cfg.NewRuntime == nil {
		return actorbase.Def{}, errors.New("agent/base: Config.NewRuntime required")
	}
	if cfg.Runtime.Documentation.Description == "" {
		cfg.Runtime.Documentation.Description = doc
	}
	if cfg.Runtime.Documentation.SkillDoc == "" {
		cfg.Runtime.Documentation.SkillDoc = doc
	}
	d := definition{cfg: cfg, controls: map[string]struct{}{TypeAsk: {}, TypeCompact: {}, TypeSelect: {}, TypeContext: {}, TypeQueue: {}, TypeStop: {}}}
	if cfg.Runtime.Capabilities[runtimeproto.CapabilityFork] {
		d.controls[TypeFork] = struct{}{}
	}
	if cfg.Runtime.Capabilities[runtimeproto.CapabilitySteer] {
		d.controls[TypeSteer] = struct{}{}
	}
	if cfg.Runtime.Capabilities[runtimeproto.CapabilityInterrupt] {
		d.controls[TypeInterrupt] = struct{}{}
	}
	if d.cfg.BufferMaxCount <= 0 {
		d.cfg.BufferMaxCount = defaultBufferMaxCount
	}
	if d.cfg.RequestMaxCount <= 0 {
		d.cfg.RequestMaxCount = d.cfg.BufferMaxCount + 8
	}
	if d.cfg.BufferMaxBytes <= 0 {
		d.cfg.BufferMaxBytes = defaultBufferMaxBytes
	}
	if d.cfg.BatchMaxCount <= 0 {
		d.cfg.BatchMaxCount = defaultBatchMaxCount
	}
	if d.cfg.ReceiptDeadline <= 0 {
		d.cfg.ReceiptDeadline = cfg.Runtime.Bounds.ReceiptDeadline
	}
	if d.cfg.ReceiptDeadline <= 0 {
		d.cfg.ReceiptDeadline = 20 * time.Minute
	}
	return actorbase.Def{Manifest: Manifest("agent", cfg.Runtime.Capabilities), New: func() (actorbase.Proc, error) { return d.run, nil }}, nil
}

func Manifest(class string, capabilities map[string]bool) introspect.Manifest {
	words := map[string]introspect.WordSpec{}
	for _, name := range []string{TypeAsk, TypeFork, TypeSteer, TypeQueue, TypeInterrupt, TypeStop, TypeCompact, TypeSelect, TypeContext} {
		words[name] = introspect.WordSpec{Description: "standard agent request"}
	}
	return introspect.Manifest{Class: class, Interfaces: []string{"actor", "agent"}, Capabilities: cloneCapabilities(capabilities), Words: words}
}

func cloneCapabilities(in map[string]bool) map[string]bool {
	out := make(map[string]bool, len(in))
	for name, enabled := range in {
		out[name] = enabled
	}
	return out
}

const (
	TypeAsk       = "agent.ask"
	TypeFork      = "agent.fork"
	TypeCompact   = "agent.compact"
	TypeSelect    = "agent.select"
	TypeContext   = "agent.context"
	TypeSteer     = "agent.steer"
	TypeInterrupt = "agent.interrupt"
	TypeQueue     = "agent.queue"
	TypeStop      = "agent.stop"
)

func (d definition) supports(typ string) bool { _, ok := d.controls[typ]; return ok }

// accepted lists what this agent does take, sorted. A refusal that names only
// the rejected word leaves the sender to go looking; naming the alternatives
// ends the search in the refusal itself. This is not hypothetical: a web client
// shipped sending "human.text" to an agent, and the refusal said only that the
// agent did not support it — the word set it does support was one line away and
// went unsaid, so the mismatch had to be traced through the ledger instead.
//
// The set is per-instance, not per-class: steer is only present when the
// provider declares the capability, so a class-level list would over-promise.
func (d definition) accepted() []string {
	out := make([]string, 0, len(d.controls))
	for name := range d.controls {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
func actorKind() actor.Kind { return actor.KindAgent }
