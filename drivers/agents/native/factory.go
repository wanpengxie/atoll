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

const (
	sessionListInputSchema    = `{"type":"object","additionalProperties":false}`
	sessionGetInputSchema     = `{"type":"object","properties":{"session":{"type":"string","minLength":1}},"additionalProperties":false}`
	sessionArchiveInputSchema = `{"type":"object","properties":{"session":{"type":"string","minLength":1}},"additionalProperties":false}`
	sessionResetInputSchema   = `{"type":"object","properties":{"session":{"type":"string","minLength":1}},"additionalProperties":false}`
	sessionRenameInputSchema  = `{"type":"object","required":["name"],"properties":{"session":{"type":"string","minLength":1},"name":{"type":"string","pattern":"\\S"}},"additionalProperties":false}`
	sessionSyncInputSchema    = `{"type":"object","required":["from_session"],"properties":{"session":{"type":"string","minLength":1},"from_session":{"type":"string","minLength":1},"after":{"type":"string"},"through":{"type":"string"}},"additionalProperties":false}`

	sessionProjectionOutputSchema = `{"type":"object","required":["session_id","holder","archived","active_turn","last_used","relation"],"properties":{"session_id":{"type":"string"},"holder":{"type":"string"},"archived":{"type":"boolean"},"active_turn":{"type":"string"},"last_used":{"type":"integer"},"relation":{"type":"object","required":["merge","merged","pending","skipped","synced"],"properties":{"merge":{"type":"string"},"merged":{"type":"array","items":{"type":"string"}},"pending":{"type":"boolean"},"skipped":{"type":"array","items":{"type":"string"}},"synced":{"type":"string"}},"additionalProperties":false}},"additionalProperties":false}`
	sessionListOutputSchema       = `{"type":"object","required":["sessions"],"properties":{"sessions":{"type":"array","items":` + sessionProjectionOutputSchema + `}},"additionalProperties":false}`
	sessionArchiveOutputSchema    = `{"type":"object","required":["disposition","session_id"],"properties":{"disposition":{"const":"archived"},"session_id":{"type":"string"}},"additionalProperties":false}`
	sessionResetOutputSchema      = `{"type":"object","required":["disposition","session_id"],"properties":{"disposition":{"const":"reset_requested"},"session_id":{"type":"string"}},"additionalProperties":false}`
	sessionSyncOutputSchema       = `{"type":"object","required":["disposition","session_id"],"properties":{"disposition":{"const":"sync_requested"},"session_id":{"type":"string"}},"additionalProperties":false}`
	sessionRenameOutputSchema     = `{"type":"object","required":["disposition","session_id","name"],"properties":{"disposition":{"const":"rename_requested"},"session_id":{"type":"string"},"name":{"type":"string"}},"additionalProperties":false}`
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
	ask.Description = "Submit work in _context.session. Use this Controller's session=new parameter to fork a new branch. Receipt mode requires submission_key. Work addresses, deduplication and controls belong to the current Controller process; after restart submit a new request."
	ask.ErrorCodes = append(ask.ErrorCodes, "scope_required", "session_not_found", "relation_history_limit_exceeded")
	ask.Examples = []json.RawMessage{json.RawMessage(`{"text":"explain the failure"}`)}
	m.Words[workapi.TypeAsk] = ask
	steer := m.Words[workapi.TypeSteer]
	steer.InputSchema = json.RawMessage(workapi.SteerInputSchema)
	steer.Description = "Steer text or waiting target into one session's execution; all gathers only the caller's waiting requests in that session. Acceptance is confirmed by the execution; included=false is not model consumption. expected_turn_id is a compare-and-swap guard."
	steer.ErrorCodes = append(steer.ErrorCodes, "scope_required", "session_not_found", "busy", "cas_mismatch", "target_gone")
	m.Words[workapi.TypeSteer] = steer
	for word, description := range map[string]string{
		workapi.TypeReplace: "Replace a waiting target using old_text CAS; the replacement retains its sender identity and queue position.",
		workapi.TypeHold:    "Freeze one session for up to 30 minutes. Holding its current owner first interrupts and then requeues it for editing; effects are not rolled back.",
		workapi.TypeUnhold:  "Release a session's hold, restoring any prior interrupt freeze.",
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
	m.Words[workapi.TypeSessionList] = introspect.WordSpec{
		Description: "List all sessions known to this Controller instance.",
		InputSchema: json.RawMessage(sessionListInputSchema), OutputSchema: json.RawMessage(sessionListOutputSchema),
		ErrorCodes: []string{"invalid_args", "ledger_unavailable", "relation_history_limit_exceeded"},
	}
	m.Words[workapi.TypeSessionGet] = introspect.WordSpec{
		Description: "Read one session selected by the inherited request context or the session parameter.",
		InputSchema: json.RawMessage(sessionGetInputSchema), OutputSchema: json.RawMessage(sessionProjectionOutputSchema),
		ErrorCodes: []string{"invalid_args", "scope_required", "session_not_found", "ledger_unavailable", "relation_history_limit_exceeded"},
	}
	m.Words[workapi.TypeSessionArchive] = introspect.WordSpec{
		Description: "Archive one session selected by the inherited request context or the session parameter.",
		InputSchema: json.RawMessage(sessionArchiveInputSchema), OutputSchema: json.RawMessage(sessionArchiveOutputSchema),
		ErrorCodes: []string{"invalid_args", "scope_required", "session_not_found", "session_archived", "ledger_unavailable"},
	}
	m.Words[workapi.TypeSessionReset] = introspect.WordSpec{
		Description: "Reset one idle session selected by the inherited request context or the session parameter.",
		InputSchema: json.RawMessage(sessionResetInputSchema), OutputSchema: json.RawMessage(sessionResetOutputSchema),
		ErrorCodes: []string{"invalid_args", "scope_required", "session_not_found", "session_archived", "busy", "ledger_unavailable"},
	}
	m.Words[workapi.TypeSessionRename] = introspect.WordSpec{
		Description: "Rename one session selected by the inherited request context or the session parameter.",
		InputSchema: json.RawMessage(sessionRenameInputSchema), OutputSchema: json.RawMessage(sessionRenameOutputSchema),
		ErrorCodes: []string{"invalid_args", "scope_required", "session_not_found", "session_archived", "ledger_unavailable"},
	}
	m.Words[workapi.TypeSessionSync] = introspect.WordSpec{
		Description: "Synchronize an idle session from a source session over the optional after/through boundary range.",
		InputSchema: json.RawMessage(sessionSyncInputSchema), OutputSchema: json.RawMessage(sessionSyncOutputSchema),
		ErrorCodes: []string{"invalid_args", "scope_required", "session_not_found", "session_archived", "busy", "ledger_unavailable"},
	}
	m.Capabilities["session_control"] = true
	m.Words[agentbase.TypeOptions] = introspect.WordSpec{Description: "Return the provider-discovered model catalog after configured model patterns are applied.", InputSchema: json.RawMessage(`{"type":"object","additionalProperties":false}`), OutputSchema: json.RawMessage(`{"type":"object"}`), ErrorCodes: []string{"invalid_args", "provider_failed"}}
	m.Words[agentbase.TypeSelect] = introspect.WordSpec{Description: "Persist this Agent's model and thinking selection.", InputSchema: json.RawMessage(`{"type":"object","required":["model"],"properties":{"model":{"type":"string","minLength":1},"effort":{"type":"string"}},"additionalProperties":false}`), OutputSchema: json.RawMessage(`{"type":"object"}`), ErrorCodes: []string{"invalid_args", "provider_failed", "ledger_unavailable"}}
	for _, word := range []string{workapi.TypeAsk, workapi.TypeStatus, workapi.TypeResult, workapi.TypeSteer, workapi.TypeReplace, workapi.TypeHold, workapi.TypeUnhold, workapi.TypeInterrupt} {
		spec := m.Words[word]
		var shape map[string]any
		if json.Unmarshal(spec.InputSchema, &shape) == nil {
			addSessionSelector(shape)
			spec.InputSchema, _ = json.Marshal(shape)
			m.Words[word] = spec
		}
	}
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

func addSessionSelector(shape map[string]any) {
	if variants, ok := shape["oneOf"].([]any); ok {
		for _, variant := range variants {
			if object, ok := variant.(map[string]any); ok {
				addSessionSelector(object)
			}
		}
		return
	}
	props, _ := shape["properties"].(map[string]any)
	if props == nil {
		props = map[string]any{}
		shape["properties"] = props
	}
	props["session"] = map[string]any{"type": "string", "minLength": 1}
}
