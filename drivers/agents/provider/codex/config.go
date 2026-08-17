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
}

type specConfig struct {
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
	return Config{WorkspaceDir: workspace, Binary: "codex", Logger: logger, processFactory: spawnProcess, Selections: selections, Default: spec.Default}, nil
}
