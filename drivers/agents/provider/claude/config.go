package claude

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

const (
	maxLineBytes    = 8 << 20
	summaryMaxChars = 4 << 10
	termGrace       = 3 * time.Second
)

type Config struct {
	WorkspaceDir   string
	Binary         string
	Model          string
	Logger         *slog.Logger
	processFactory processFactory
	Selections     []driverproto.TurnOptions
	Default        int
	// Prompt is the decl-authored static instruction block, passed as
	// --append-system-prompt so it rides on Claude Code's own system prompt.
	Prompt string
	// mcpConfig is a generation-owned anonymous pipe inherited as fd 3. It
	// carries the loopback MCP URL and bearer token without argv or disk.
	mcpConfig *os.File
	// Situation is who this agent is and where it sits. It reaches the model
	// as the system prompt's identity block and the tool guide's reach
	// paragraph. Composition fills it from the host context and the instance
	// spec; it is never decl config.
	Situation driverproto.Situation
}

type specConfig struct {
	Model      string `json:"model,omitempty"`
	Prompt     string `json:"prompt,omitempty"`
	Selections []struct {
		Model  string `json:"model"`
		Effort string `json:"effort"`
	} `json:"selections,omitempty"`
	Default int `json:"default,omitempty"`
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
		return fmt.Errorf("claude: default selection index %d out of range", c.Default)
	}
	if len(c.Selections) == 0 && c.Default != 0 {
		return fmt.Errorf("claude: default selection requires selections")
	}
	return nil
}

func ParseConfig(raw json.RawMessage, workspace string, logger *slog.Logger) (Config, error) {
	if workspace == "" {
		return Config{}, errors.New("claude: daemon workspace required")
	}
	if err := ValidateConfig(raw); err != nil {
		return Config{}, err
	}
	var spec specConfig
	if len(raw) != 0 {
		if err := json.Unmarshal(raw, &spec); err != nil {
			return Config{}, err
		}
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	selections := make([]driverproto.TurnOptions, len(spec.Selections))
	for i, option := range spec.Selections {
		selections[i] = driverproto.TurnOptions{Model: option.Model, Effort: option.Effort}
	}
	return Config{WorkspaceDir: workspace, Binary: "claude", Model: spec.Model, Logger: logger, processFactory: spawnProcess, Selections: selections, Default: spec.Default, Prompt: spec.Prompt}, nil
}

// ConfigSchema publishes what specConfig above accepts. Decoding is strict, so
// an undeclared field is a hard rejection; stating the fields forward is the
// only way a declaration author learns them without reading this file.
const ConfigSchema = `{
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "prompt": {"type": "string", "description": "static instruction block appended to this agent's system prompt"},
    "model": {"type": "string", "description": "model used when selections are not configured"},
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
