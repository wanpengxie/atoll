package claude

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"time"
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
}

type specConfig struct {
	Model string `json:"model,omitempty"`
}

func ValidateConfig(raw json.RawMessage) error {
	if len(raw) == 0 {
		return nil
	}
	var c specConfig
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(&c)
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
	return Config{WorkspaceDir: workspace, Binary: "claude", Model: spec.Model, Logger: logger, processFactory: spawnProcess}, nil
}
