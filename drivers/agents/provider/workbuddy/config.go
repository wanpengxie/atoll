package workbuddy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

const maxRPCLineBytes = 8 << 20

type Config struct {
	WorkspaceDir   string
	Binary         string
	ConfigDir      string
	Logger         *slog.Logger
	Selections     []driverproto.TurnOptions
	Titles         []driverproto.SelectionTitle
	Default        int
	Prompt         string
	Situation      driverproto.Situation
	processFactory processFactory
}

type selectionConfig struct {
	Model       string `json:"model"`
	Effort      string `json:"effort"`
	ModelLabel  string `json:"model_label,omitempty"`
	EffortLabel string `json:"effort_label,omitempty"`
}

type specConfig struct {
	Binary     string            `json:"binary,omitempty"`
	ConfigDir  string            `json:"config_dir,omitempty"`
	Prompt     string            `json:"prompt,omitempty"`
	Selections []selectionConfig `json:"selections,omitempty"`
	Default    int               `json:"default"`
}

var defaultModels = []struct{ value, label string }{
	{"fast-model", "快速"}, {"balanced-model", "均衡"}, {"deep-model", "极致"},
	{"hy4-preview", "Hy4 preview"}, {"hy3", "Hy3"}, {"hy3-x", "Hy3 X"},
	{"deepseek-v4.1-flash", "DeepSeek V4.1 Flash"}, {"glm-5.3", "GLM 5.3"},
	{"glm-5.3-flash", "GLM 5.3 Flash"}, {"glm-5.2", "GLM 5.2"},
	{"glm-5.1", "GLM 5.1"}, {"glm-5v-turbo", "GLM 5V Turbo"},
	{"minimax-m3", "MiniMax M3"}, {"kimi-k3-1", "Kimi K3"},
	{"kimi-k2.7", "Kimi K2.7 Code"}, {"kimi-k2.6", "Kimi K2.6"},
	{"deepseek-v4-pro", "DeepSeek V4 Pro"},
}

var defaultEfforts = []struct{ value, label string }{
	{"enabled", "默认"}, {"minimal", "最小"}, {"low", "轻量"},
	{"medium", "中等"}, {"high", "高"}, {"xhigh", "超高"}, {"max", "极限"},
}

func DefaultConfig() json.RawMessage {
	spec := specConfig{}
	for _, model := range defaultModels {
		for _, effort := range defaultEfforts {
			spec.Selections = append(spec.Selections, selectionConfig{Model: model.value, ModelLabel: model.label, Effort: effort.value, EffortLabel: effort.label})
		}
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		panic("workbuddy: marshal default config: " + err.Error())
	}
	return raw
}

func (c specConfig) options() []driverproto.TurnOptions {
	out := make([]driverproto.TurnOptions, len(c.Selections))
	for i, option := range c.Selections {
		out[i] = driverproto.TurnOptions{Model: option.Model, Effort: option.Effort}
	}
	return out
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
		return fmt.Errorf("workbuddy: default selection index %d out of range", c.Default)
	}
	if len(c.Selections) == 0 && c.Default != 0 {
		return errors.New("workbuddy: default selection requires selections")
	}
	if err := driverproto.ValidateSelections(c.options()); err != nil {
		return fmt.Errorf("workbuddy: %w", err)
	}
	return nil
}

func ParseConfig(raw json.RawMessage, workspace string, logger *slog.Logger) (Config, error) {
	if workspace == "" {
		return Config{}, errors.New("workbuddy: daemon workspace required")
	}
	if err := ValidateConfig(raw); err != nil {
		return Config{}, err
	}
	var spec specConfig
	if len(raw) != 0 {
		_ = json.Unmarshal(raw, &spec)
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	binary := spec.Binary
	if binary == "" {
		bundled := "/Applications/WorkBuddy.app/Contents/Resources/app.asar.unpacked/cli/bin/codebuddy"
		if _, err := os.Stat(bundled); err == nil {
			binary = bundled
		} else {
			binary = "codebuddy"
		}
	}
	configDir := spec.ConfigDir
	if configDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return Config{}, err
		}
		configDir = filepath.Join(home, ".workbuddy")
	}
	titles := make([]driverproto.SelectionTitle, len(spec.Selections))
	for i, option := range spec.Selections {
		titles[i] = driverproto.SelectionTitle{Model: option.ModelLabel, Effort: option.EffortLabel}
	}
	return Config{WorkspaceDir: workspace, Binary: binary, ConfigDir: configDir, Logger: logger, Selections: spec.options(), Titles: titles, Default: spec.Default, Prompt: spec.Prompt, processFactory: spawnProcess}, nil
}

const ConfigSchema = `{
  "type":"object","additionalProperties":false,
  "properties":{
    "binary":{"type":"string","minLength":1,"description":"WorkBuddy/CodeBuddy CLI path"},
    "config_dir":{"type":"string","minLength":1,"description":"WorkBuddy CLI state and product-config directory"},
    "prompt":{"type":"string","description":"static instruction block appended to WorkBuddy's system prompt"},
    "selections":{"type":"array","description":"model/thought-level choices exposed by agent.select","items":{"type":"object","additionalProperties":false,"required":["model","effort"],"properties":{"model":{"type":"string","minLength":1},"effort":{"type":"string","minLength":1},"model_label":{"type":"string"},"effort_label":{"type":"string"}}}},
    "default":{"type":"integer","minimum":0}
  }
}`
