package native

import (
	"encoding/json"
	"fmt"

	agentbase "github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/drivers/agents/workapi"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

func Manifest() introspect.Manifest {
	m := workapi.Manifest(Class, map[string]bool{
		workapi.CapabilityMultiWork:          true,
		workapi.CapabilityWorkTextSteer:      true,
		workapi.CapabilityTargetedInterrupt:  true,
		workapi.CapabilityAgentWideInterrupt: true,
		workapi.CapabilityBranchFromWork:     false,
	})
	ask := m.Words[workapi.TypeAsk]
	ask.Description = "Submit work in _context.session. Use the standard session=new parameter to fork a new branch. Receipt mode requires submission_key."
	ask.ErrorCodes = append(ask.ErrorCodes, "scope_required", "session_not_found")
	ask.Examples = []json.RawMessage{json.RawMessage(`{"text":"explain the failure"}`)}
	m.Words[workapi.TypeAsk] = ask
	steer := m.Words[workapi.TypeSteer]
	steer.InputSchema = json.RawMessage(workapi.SteerInputSchema)
	steer.Description = "Steer text or waiting target into one view's execution; all gathers only the caller's waiting requests in that view. Acceptance is confirmed by the execution; included=false is not model consumption. expected_turn_id is a compare-and-swap guard."
	steer.ErrorCodes = append(steer.ErrorCodes, "scope_required", "session_not_found", "busy", "cas_mismatch", "target_gone")
	m.Words[workapi.TypeSteer] = steer
	for word, description := range map[string]string{
		workapi.TypeReplace: "Replace a waiting target using old_text CAS; the replacement retains its sender identity and queue position.",
		workapi.TypeHold:    "Freeze one view for up to 30 minutes. Holding its current owner first interrupts and then requeues it for editing; effects are not rolled back.",
		workapi.TypeUnhold:  "Release a view's hold, restoring any prior interrupt freeze.",
	} {
		schema := `{"type":"object","properties":{"work_id":{"type":"string"},"target":{"type":"string"},"old_text":{"type":"string"},"new_text":{"type":"string"},"duration_ms":{"type":"integer","minimum":1,"maximum":1800000}},"additionalProperties":false}`
		var shape map[string]any
		_ = json.Unmarshal([]byte(schema), &shape)
		props := shape["properties"].(map[string]any)
		if word != workapi.TypeReplace {
			delete(props, "old_text")
			delete(props, "new_text")
		} else {
			delete(props, "duration_ms")
			shape["required"] = []string{"target", "old_text", "new_text"}
		}
		if word == workapi.TypeUnhold {
			delete(props, "target")
			delete(props, "duration_ms")
		}
		encoded, _ := json.Marshal(shape)
		schema = string(encoded)
		m.Words[word] = introspect.WordSpec{Description: description, InputSchema: json.RawMessage(schema), OutputSchema: json.RawMessage(workapi.ControlOutputSchema), ErrorCodes: []string{"invalid_args", "scope_required", "session_not_found", "work_not_found", "cas_mismatch", "target_not_owned", "busy"}}
	}
	for _, word := range []string{workapi.TypeSessionList, workapi.TypeSessionGet, workapi.TypeSessionRename, workapi.TypeSessionArchive, workapi.TypeSessionReset, workapi.TypeSessionSync} {
		m.Words[word] = introspect.WordSpec{Description: "Read or change session lifecycle state.", InputSchema: json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"},"from_session":{"type":"string"},"through":{"type":"string"}},"additionalProperties":false}`), OutputSchema: json.RawMessage(`{"type":"object"}`), ErrorCodes: []string{"scope_required", "session_not_found", "session_archived", "busy", "ledger_unavailable"}}
	}
	m.Capabilities["session_control"] = true
	m.Words[agentbase.TypeOptions] = introspect.WordSpec{Description: "Return the provider-discovered model catalog after configured model patterns are applied.", InputSchema: json.RawMessage(`{"type":"object","additionalProperties":false}`), OutputSchema: json.RawMessage(`{"type":"object"}`), ErrorCodes: []string{"invalid_args", "provider_failed"}}
	m.Words[agentbase.TypeSelect] = introspect.WordSpec{Description: "Persist this Agent's model and thinking selection.", InputSchema: json.RawMessage(`{"type":"object","required":["model"],"properties":{"model":{"type":"string","minLength":1},"effort":{"type":"string"}},"additionalProperties":false}`), OutputSchema: json.RawMessage(`{"type":"object"}`), ErrorCodes: []string{"invalid_args", "provider_failed", "ledger_unavailable"}}
	return m

}

func ValidateConfig(raw json.RawMessage) error { _, err := ParseConfig(raw); return err }

func New(spec registry.InstanceSpec, deps registry.Deps) (platform.ActorDecl, error) {
	if spec.ID == "" {
		return platform.ActorDecl{}, fmt.Errorf("native-agent: explicit instance id required")
	}
	if deps.ChannelID == "" {
		return platform.ActorDecl{}, fmt.Errorf("native-agent: channel required")
	}
	cfg, err := ParseConfig(spec.Config)
	if err != nil {
		return platform.ActorDecl{}, err
	}
	return platform.ActorDecl{ID: spec.ID, Kind: actor.KindAgent, Factory: platform.ActorFactory{Proc: actorbase.Def{
		Manifest: Manifest(), New: func() (actorbase.Proc, error) { return func(sys actorbase.Sys) error { return run(sys, cfg) }, nil },
	}}}, nil
}
