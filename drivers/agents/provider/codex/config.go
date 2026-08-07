package codex

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"time"
)

const (
	initializeTimeout   = 15 * time.Second
	rpcTimeout          = 30 * time.Second
	maxRPCLineBytes     = 8 << 20
	inputMaxChars       = 1 << 20
	toolSummaryMaxChars = 4 << 10
)

type Config struct {
	ActorID        string
	WorkspaceDir   string
	Binary         string
	Logger         *slog.Logger
	processFactory processFactory
}

type specConfig struct{}

func parseConfig(raw json.RawMessage, workspace string, logger *slog.Logger) (Config, error) {
	if workspace == "" {
		return Config{}, errors.New("codex: daemon workspace required")
	}
	if len(raw) > 0 {
		var c specConfig
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&c); err != nil {
			return Config{}, err
		}
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return Config{WorkspaceDir: workspace, Binary: "codex", Logger: logger, processFactory: spawnProcess}, nil
}
