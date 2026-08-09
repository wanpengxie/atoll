package codex

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
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
}

type specConfig struct{}

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
		return Config{}, errors.New("codex: daemon workspace required")
	}
	if err := ValidateConfig(raw); err != nil {
		return Config{}, err
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return Config{WorkspaceDir: workspace, Binary: "codex", Logger: logger, processFactory: spawnProcess}, nil
}
