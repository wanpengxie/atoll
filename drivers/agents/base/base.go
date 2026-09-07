package base

import (
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
)

type Config struct {
	NewRuntime       runtimeproto.Factory
	Runtime          runtimeproto.Spec
	OutputWorkspace  string
	OutputDeviceName string
	RequestMaxCount  int
	BufferMaxCount   int
	BufferMaxBytes   int
	BatchMaxCount    int
	ReceiptDeadline  time.Duration
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
	d := definition{cfg: cfg, controls: map[string]struct{}{TypeAsk: {}, TypeCompact: {}, TypeNew: {}, TypeSelect: {}, TypeOptions: {}, TypeContext: {}, TypeQueue: {}, TypeHold: {}, TypeUnhold: {}, TypeReplace: {}, TypeInterrupt: {}, TypeDismiss: {}}}
	if cfg.Runtime.Capabilities[runtimeproto.CapabilityFork] {
		d.controls[TypeFork] = struct{}{}
	}
	if cfg.Runtime.Capabilities[runtimeproto.CapabilitySteer] {
		d.controls[TypeSteer] = struct{}{}
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
	return actorbase.Def{Manifest: instanceManifest(cfg.Runtime), New: func() (actorbase.Proc, error) { return d.run, nil }}, nil
}

// instanceManifest is deliberately stable across provider versions. Dynamic
// model values belong to agent.options, not actor.describe's type schema.
func instanceManifest(spec runtimeproto.Spec) introspect.Manifest {
	// The class is the provider's name (actor.describe must answer the real
	// class); "agent" is only the fallback for spec-less test harnesses.
	class := spec.Name
	if class == "" {
		class = "agent"
	}
	return Manifest(class, spec.Capabilities)
}

func Manifest(class string, capabilities map[string]bool) introspect.Manifest {
	words := map[string]introspect.WordSpec{}
	for _, name := range []string{TypeAsk, TypeFork, TypeSteer, TypeQueue, TypeInterrupt, TypeHold, TypeUnhold, TypeReplace, TypeCompact, TypeNew, TypeSelect, TypeOptions, TypeContext} {
		words[name] = introspect.WordSpec{Description: "standard agent request"}
	}
	words[TypeHold] = introspect.WordSpec{Description: "freeze the agent wait queue, optionally interrupting an owned target for editing", InputSchema: json.RawMessage(`{"type":"object","properties":{"target":{"type":"string"},"duration_ms":{"type":"integer","minimum":1,"maximum":1800000}},"additionalProperties":false}`), ErrorCodes: []string{errorCASMismatch, "target_not_owned", "invalid_args", "busy"}}
	words[TypeUnhold] = introspect.WordSpec{Description: "idempotently release the agent wait queue", InputSchema: json.RawMessage(`{"type":"object","additionalProperties":false}`)}
	words[TypeReplace] = introspect.WordSpec{Description: "replace an owned buffered request in place", InputSchema: json.RawMessage(`{"type":"object","required":["target","old_text","new_text"],"properties":{"target":{"type":"string","minLength":1},"old_text":{"type":"string"},"new_text":{"type":"string"},"attachments":{"type":"array"}},"additionalProperties":false}`), ErrorCodes: []string{errorCASMismatch, "invalid_args", errorBaseCapacity, "target_not_owned"}}
	words[TypeInterrupt] = introspect.WordSpec{Description: "freeze the wait queue and interrupt the current turn when supported", InputSchema: json.RawMessage(`{"type":"object","additionalProperties":false}`), ErrorCodes: []string{"invalid_args", "busy"}}
	words[TypeSteer] = introspect.WordSpec{Description: "steer text into the active turn or insert owned buffered requests", InputSchema: json.RawMessage(`{"oneOf":[{"type":"object","required":["text"],"properties":{"text":{"type":"string"},"expected_turn_id":{"type":"string"}},"additionalProperties":false},{"type":"object","required":["target"],"properties":{"target":{"type":"string","minLength":1}},"additionalProperties":false},{"type":"object","required":["all"],"properties":{"all":{"const":true}},"additionalProperties":false}]}`), ErrorCodes: []string{errorCASMismatch, "target_not_owned", "invalid_args"}}
	words[TypeNew] = introspect.WordSpec{Description: "replace the provider conversation while preserving the actor, workspace and selected options", InputSchema: json.RawMessage(`{"type":"object","additionalProperties":false}`), ErrorCodes: []string{"invalid_args"}}
	words[TypeSelect] = introspect.WordSpec{Description: "switch this agent's current model and optional effort; call agent.options for the current legal values", InputSchema: json.RawMessage(`{"type":"object","required":["model"],"properties":{"model":{"type":"string","minLength":1},"effort":{"type":"string","minLength":1}},"additionalProperties":false}`), ErrorCodes: []string{"invalid_args", "superseded"}}
	words[TypeOptions] = introspect.WordSpec{Description: "return this agent incarnation's provider-discovered model catalog, current selection, and client update signal", InputSchema: json.RawMessage(`{"type":"object","additionalProperties":false}`), ErrorCodes: []string{"invalid_args", "busy", errorBaseCapacity, errorProviderFailed, errorProviderCrash, errorProviderTimeout}}
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
	TypeNew       = "agent.new"
	TypeSelect    = "agent.select"
	TypeOptions   = "agent.options"
	TypeContext   = "agent.context"
	TypeSteer     = "agent.steer"
	TypeInterrupt = "agent.interrupt"
	TypeHold      = "agent.hold"
	TypeUnhold    = "agent.unhold"
	TypeReplace   = "agent.replace"
	TypeQueue     = "agent.queue"
	// TypeDismiss drops a request that is still WAITING, answered by the actor
	// holding it. Cancelling your own request is the caller closing its own
	// account; dropping somebody else's cannot be, because a bystander is not
	// an authorized closure author — but the receiver always is, so the honest
	// shape is to ask the receiver to answer it rather than to reach around it.
	TypeDismiss     = "agent.dismiss"
	typeHoldExpired = "agent.hold_expired"
)

// supports gates intake. TypeTimerWake is accepted but deliberately NOT in the
// controls set: that set is also the advertised vocabulary (accepted(), and the
// refusal text that names it), and the alarm commission is one the loop sends
// ITSELF — advertising it would invite callers to send a word whose whole
// meaning is "this agent's own earlier intent came back".
func (d definition) supports(typ string) bool {
	if typ == TypeTimerWake {
		return true
	}
	_, ok := d.controls[typ]
	return ok
}

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
