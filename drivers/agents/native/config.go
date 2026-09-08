package native

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const Class = "native-agent"

type Config struct {
	Loopers                 []string `json:"loopers"`
	ContextActor            string   `json:"context_actor"`
	LLMActor                string   `json:"llm_actor"`
	WorkspaceActor          string   `json:"workspace_actor,omitempty"`
	HostActor               string   `json:"host_actor,omitempty"`
	Model                   string   `json:"model,omitempty"`
	MaxOpenWorks            int      `json:"max_open_works,omitempty"`
	MaxAssignmentsPerLooper int      `json:"max_assignments_per_looper,omitempty"`
	MaxInputsPerWork        int      `json:"max_inputs_per_work,omitempty"`
	MaxOperationKeys        int      `json:"max_operation_keys,omitempty"`
	MaxTurns                int      `json:"max_turns,omitempty"`
}

func DefaultConfig() json.RawMessage {
	return json.RawMessage(`{"loopers":["agent-looper"],"context_actor":"agent-context","llm_actor":"pi-llm","workspace_actor":"pi-workspace","max_open_works":32,"max_assignments_per_looper":32,"max_inputs_per_work":128,"max_operation_keys":256,"max_turns":12}`)
}

func ParseConfig(raw json.RawMessage) (Config, error) {
	cfg := Config{Loopers: []string{"agent-looper"}, ContextActor: "agent-context", LLMActor: "pi-llm", WorkspaceActor: "pi-workspace", MaxOpenWorks: 32, MaxAssignmentsPerLooper: 32, MaxInputsPerWork: 128, MaxOperationKeys: 256, MaxTurns: 12}
	if len(raw) == 0 {
		return cfg, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("native-agent config: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Config{}, errors.New("native-agent config: multiple JSON values")
	}
	if len(cfg.Loopers) == 0 {
		return Config{}, errors.New("native-agent config: at least one looper is required")
	}
	seenLoopers := make(map[string]struct{}, len(cfg.Loopers))
	for _, target := range cfg.Loopers {
		if _, exists := seenLoopers[target]; exists {
			return Config{}, fmt.Errorf("native-agent config: duplicate looper %q", target)
		}
		seenLoopers[target] = struct{}{}
	}
	for _, target := range append(append([]string(nil), cfg.Loopers...), cfg.ContextActor, cfg.LLMActor) {
		if strings.TrimSpace(target) == "" || strings.TrimSpace(target) != target {
			return Config{}, errors.New("native-agent config: actor targets must be non-blank and trimmed")
		}
	}
	for name, target := range map[string]string{"workspace_actor": cfg.WorkspaceActor, "host_actor": cfg.HostActor} {
		if target != "" && strings.TrimSpace(target) != target {
			return Config{}, fmt.Errorf("native-agent config: %s must be trimmed", name)
		}
	}
	if cfg.MaxOpenWorks < 1 || cfg.MaxOpenWorks > 10000 {
		return Config{}, errors.New("native-agent config: max_open_works must be 1..10000")
	}
	if cfg.MaxAssignmentsPerLooper < 1 || cfg.MaxAssignmentsPerLooper > 10000 {
		return Config{}, errors.New("native-agent config: max_assignments_per_looper must be 1..10000")
	}
	if cfg.MaxInputsPerWork < 1 || cfg.MaxInputsPerWork > 10000 {
		return Config{}, errors.New("native-agent config: max_inputs_per_work must be 1..10000")
	}
	if cfg.MaxOperationKeys < 1 || cfg.MaxOperationKeys > 10000 {
		return Config{}, errors.New("native-agent config: max_operation_keys must be 1..10000")
	}
	if cfg.MaxTurns < 1 || cfg.MaxTurns > 128 {
		return Config{}, errors.New("native-agent config: max_turns must be 1..128")
	}
	return cfg, nil
}

const ConfigSchema = `{"type":"object","additionalProperties":false,"properties":{"loopers":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}},"context_actor":{"type":"string","minLength":1},"llm_actor":{"type":"string","minLength":1},"workspace_actor":{"type":"string"},"host_actor":{"description":"body-side channel Handle used to act through this Channel's Seat in its host","type":"string"},"model":{"type":"string"},"max_open_works":{"type":"integer","minimum":1,"maximum":10000},"max_assignments_per_looper":{"type":"integer","minimum":1,"maximum":10000},"max_inputs_per_work":{"type":"integer","minimum":1,"maximum":10000},"max_operation_keys":{"type":"integer","minimum":1,"maximum":10000},"max_turns":{"type":"integer","minimum":1,"maximum":128}}}`
