package codex

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

const (
	maxRPCLineBytes     = 8 << 20
	toolSummaryMaxChars = 4 << 10
)

type Config struct {
	WorkspaceDir   string
	Binary         string
	Logger         *slog.Logger
	processFactory processFactory
	Selections     []driverproto.TurnOptions
	Default        int
	// Prompt is the decl-authored static instruction block. It reaches the
	// model as thread/start developerInstructions (appended to codex's own
	// system prompt), so it is part of the thread from its first request.
	Prompt string
	// Home is the CODEX_HOME the app-server child runs under. Empty means
	// the codex default (~/.codex). See ResolveHome.
	Home string
	// Situation is who this agent is and where it sits. It reaches the model
	// as the system prompt's identity block and the tool guide's reach
	// paragraph. Composition fills it from the host context and the instance
	// spec; it is never decl config.
	Situation driverproto.Situation
}

type specConfig struct {
	Selections []struct {
		Model  string `json:"model"`
		Effort string `json:"effort"`
	} `json:"selections,omitempty"`
	Default int    `json:"default,omitempty"`
	Prompt  string `json:"prompt,omitempty"`
}

func ValidateConfig(raw json.RawMessage) error {
	if len(raw) == 0 {
		return nil
	}
	var c specConfig
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return err
	}
	if len(c.Selections) > 0 && (c.Default < 0 || c.Default >= len(c.Selections)) {
		return fmt.Errorf("codex: default selection index %d out of range", c.Default)
	}
	if len(c.Selections) == 0 && c.Default != 0 {
		return fmt.Errorf("codex: default selection requires selections")
	}
	return nil
}

func ParseConfig(raw json.RawMessage, workspace string, logger *slog.Logger) (Config, error) {
	if workspace == "" {
		return Config{}, errors.New("codex: daemon workspace required")
	}
	if err := ValidateConfig(raw); err != nil {
		return Config{}, err
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	var spec specConfig
	if len(raw) != 0 {
		_ = json.Unmarshal(raw, &spec)
	}
	selections := make([]driverproto.TurnOptions, len(spec.Selections))
	for i, option := range spec.Selections {
		selections[i] = driverproto.TurnOptions{Model: option.Model, Effort: option.Effort}
	}
	return Config{WorkspaceDir: workspace, Binary: "codex", Logger: logger, processFactory: spawnProcess, Selections: selections, Default: spec.Default, Prompt: spec.Prompt, Home: ResolveHome()}, nil
}

// ConfigSchema publishes what specConfig above accepts. Decoding is strict, so
// an undeclared field is a hard rejection; stating the fields forward is the
// only way a declaration author learns them without reading this file.
const ConfigSchema = `{
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "prompt": {"type": "string", "description": "static instruction block prepended to this agent's system prompt"},
    "selections": {
      "type": "array",
      "description": "model/effort options this agent may be switched between at runtime",
      "items": {"type": "object", "additionalProperties": false, "properties": {
        "model": {"type": "string"},
        "effort": {"type": "string"}
      }}
    },
    "default": {"type": "integer", "minimum": 0, "description": "index into selections; requires selections to be non-empty"}
  }
}`
