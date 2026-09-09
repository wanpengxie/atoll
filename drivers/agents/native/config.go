package native

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
)

const Class = "native-agent"

type Config struct {
	ContextActor            string        `json:"-"` // retired v4 field retained only for source compatibility
	ToolTimeoutMS           int64         `json:"tool_timeout_ms,omitempty"`
	ExecutionTimeoutMS      int64         `json:"execution_timeout_ms,omitempty"`
	Loopers                 []string      `json:"loopers"`
	LLMActor                string        `json:"llm_actor"`
	Prompt                  string        `json:"prompt,omitempty"`
	WorkspaceActor          string        `json:"workspace_actor,omitempty"`
	HostActor               string        `json:"host_actor,omitempty"`
	Model                   string        `json:"model,omitempty"`
	Models                  []string      `json:"models,omitempty"`
	DefaultModel            string        `json:"default_model,omitempty"`
	DefaultThinkingLevel    string        `json:"default_thinking_level,omitempty"`
	MaxOpenWorks            int           `json:"max_open_works,omitempty"`
	MaxAssignmentsPerLooper int           `json:"max_assignments_per_looper,omitempty"`
	MaxInputsPerWork        int           `json:"max_inputs_per_work,omitempty"`
	MaxOperationKeys        int           `json:"max_operation_keys,omitempty"`
	MaxTurns                int           `json:"max_turns,omitempty"`
	Tools                   []ToolConfig  `json:"tools,omitempty"`
	ToolsConfigured         bool          `json:"-"`
	ToolResultMaxLines      int           `json:"tool_result_max_lines,omitempty"`
	ToolResultMaxBytes      int           `json:"tool_result_max_bytes,omitempty"`
	ToolImageMaxBytes       int           `json:"tool_image_max_bytes,omitempty"`
	Compact                 CompactConfig `json:"compact,omitempty"`
	Archive                 ArchiveConfig `json:"archive,omitempty"`
}

type CompactConfig struct {
	ContextWindow    int    `json:"context_window,omitempty"`
	ReserveTokens    int    `json:"reserve_tokens,omitempty"`
	KeepRecentTokens int    `json:"keep_recent_tokens,omitempty"`
	Model            string `json:"model,omitempty"`
}

type ArchiveConfig struct {
	KeepAlive int   `json:"keep_alive,omitempty"`
	IdleMS    int64 `json:"idle_ms,omitempty"`
}

type ToolConfig struct {
	Name  string `json:"name"`
	Actor string `json:"actor"`
	Word  string `json:"word"`
}

var modelToolName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]{0,63}$`)

func DefaultConfig() json.RawMessage {
	return json.RawMessage(`{"loopers":["agent-looper"],"llm_actor":"pi-llm","workspace_actor":"pi-workspace","max_open_works":32,"max_assignments_per_looper":32,"max_inputs_per_work":128,"max_operation_keys":256,"max_turns":12,"tool_result_max_lines":2000,"tool_result_max_bytes":51200,"tool_image_max_bytes":3145728,"compact":{"context_window":128000,"reserve_tokens":16384,"keep_recent_tokens":20000},"archive":{"keep_alive":3,"idle_ms":3600000}}`)
}

func ParseConfig(raw json.RawMessage) (Config, error) {
	cfg := Config{Loopers: []string{"agent-looper"}, LLMActor: "pi-llm", WorkspaceActor: "pi-workspace", MaxOpenWorks: 32, MaxAssignmentsPerLooper: 32, MaxInputsPerWork: 128, MaxOperationKeys: 256, MaxTurns: 12, ToolResultMaxLines: 2000, ToolResultMaxBytes: 50 << 10, ToolImageMaxBytes: 3 << 20}
	cfg.ToolTimeoutMS = 120000
	cfg.ExecutionTimeoutMS = 1800000
	cfg.Compact = CompactConfig{ContextWindow: 128000, ReserveTokens: 16384, KeepRecentTokens: 20000}
	cfg.Archive = ArchiveConfig{KeepAlive: 3, IdleMS: 3600000}
	if len(raw) == 0 {
		return cfg, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("native-agent config: %w", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return Config{}, fmt.Errorf("native-agent config: %w", err)
	}
	if toolRaw, ok := fields["tools"]; ok {
		if string(toolRaw) == "null" {
			return Config{}, errors.New("native-agent config: tools must be an array, not null")
		}
		cfg.ToolsConfigured = true
	}
	if cfg.DefaultModel == "" {
		cfg.DefaultModel = cfg.Model
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
	for _, target := range append(append([]string(nil), cfg.Loopers...), cfg.LLMActor) {
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
	if cfg.ToolTimeoutMS < 1 || cfg.ToolTimeoutMS > 1800000 || cfg.ExecutionTimeoutMS < 1 || cfg.ExecutionTimeoutMS > 86400000 {
		return Config{}, errors.New("native-agent config: timeouts must be positive and within tool 30m/execution 24h limits")
	}
	seenTools := make(map[string]struct{}, len(cfg.Tools))
	for _, tool := range cfg.Tools {
		if !modelToolName.MatchString(tool.Name) {
			return Config{}, fmt.Errorf("native-agent config: invalid model tool name %q", tool.Name)
		}
		if strings.TrimSpace(tool.Actor) == "" || strings.TrimSpace(tool.Actor) != tool.Actor || strings.TrimSpace(tool.Word) == "" || strings.TrimSpace(tool.Word) != tool.Word {
			return Config{}, fmt.Errorf("native-agent config: tool %q actor and word must be non-blank and trimmed", tool.Name)
		}
		if _, ok := seenTools[tool.Name]; ok {
			return Config{}, fmt.Errorf("native-agent config: duplicate tool name %q", tool.Name)
		}
		seenTools[tool.Name] = struct{}{}
	}
	if cfg.ToolResultMaxLines < 1 || cfg.ToolResultMaxLines > 100000 || cfg.ToolResultMaxBytes < 1024 || cfg.ToolResultMaxBytes > 4<<20 || cfg.ToolImageMaxBytes < 1024 || cfg.ToolImageMaxBytes > 3<<20 {
		return Config{}, errors.New("native-agent config: tool result limits are outside their valid ranges")
	}
	if cfg.Compact.ContextWindow < 4096 || cfg.Compact.ReserveTokens < 0 || cfg.Compact.KeepRecentTokens < 1 || cfg.Compact.ReserveTokens+cfg.Compact.KeepRecentTokens >= cfg.Compact.ContextWindow {
		return Config{}, errors.New("native-agent config: compact token budget is invalid")
	}
	if cfg.Archive.KeepAlive < 0 || cfg.Archive.KeepAlive > maxSessions || cfg.Archive.IdleMS < 0 {
		return Config{}, errors.New("native-agent config: archive bounds are invalid")
	}
	return cfg, nil
}

const ConfigSchema = `{"type":"object","additionalProperties":false,"properties":{"loopers":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}},"llm_actor":{"type":"string","minLength":1},"workspace_actor":{"type":"string"},"host_actor":{"description":"body-side channel Handle used to act through this Channel's Seat in its host","type":"string"},"prompt":{"type":"string"},"model":{"type":"string"},"models":{"type":"array","items":{"type":"string","minLength":1}},"default_model":{"type":"string"},"default_thinking_level":{"type":"string"},"compact":{"type":"object","properties":{"context_window":{"type":"integer","minimum":4096},"reserve_tokens":{"type":"integer","minimum":0},"keep_recent_tokens":{"type":"integer","minimum":1},"model":{"type":"string"}},"additionalProperties":false},"archive":{"type":"object","properties":{"keep_alive":{"type":"integer","minimum":0},"idle_ms":{"type":"integer","minimum":0}},"additionalProperties":false},"max_open_works":{"type":"integer","minimum":1,"maximum":10000},"max_assignments_per_looper":{"type":"integer","minimum":1,"maximum":10000},"max_inputs_per_work":{"type":"integer","minimum":1,"maximum":10000},"max_operation_keys":{"type":"integer","minimum":1,"maximum":10000},"tool_timeout_ms":{"type":"integer","minimum":1,"maximum":1800000},"execution_timeout_ms":{"type":"integer","minimum":1,"maximum":86400000},"max_turns":{"type":"integer","minimum":1,"maximum":128},"tools":{"type":"array","items":{"type":"object","required":["name","actor","word"],"properties":{"name":{"type":"string","pattern":"^[A-Za-z_][A-Za-z0-9_-]{0,63}$"},"actor":{"type":"string","minLength":1},"word":{"type":"string","minLength":1}},"additionalProperties":false}},"tool_result_max_lines":{"type":"integer","minimum":1,"maximum":100000},"tool_result_max_bytes":{"type":"integer","minimum":1024,"maximum":4194304},"tool_image_max_bytes":{"type":"integer","minimum":1024,"maximum":3145728}}}`
