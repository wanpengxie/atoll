package channelmember

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type SeatConfig struct {
	Body channel.ID `json:"body"`
}
type Word struct {
	Schema      json.RawMessage   `json:"schema"`
	Description string            `json:"description,omitempty"`
	Examples    []json.RawMessage `json:"examples,omitempty"`
	Target      string            `json:"target"`
}
type HandleConfig struct {
	Host    channel.ID      `json:"host"`
	Words   map[string]Word `json:"words"`
	Drivers []string        `json:"drivers,omitempty"`
}

func ParseSeatConfig(raw json.RawMessage) (SeatConfig, error) {
	var cfg SeatConfig
	if err := actorbase.DecodeStrict(raw, &cfg); err != nil {
		return cfg, err
	}
	if cfg.Body == "" {
		return cfg, errors.New("body required")
	}
	return cfg, nil
}
func ParseHandleConfig(raw json.RawMessage) (HandleConfig, error) {
	var cfg HandleConfig
	if err := actorbase.DecodeStrict(raw, &cfg); err != nil {
		return cfg, err
	}
	if cfg.Host == "" {
		return cfg, errors.New("host required")
	}
	if cfg.Words == nil {
		return cfg, errors.New("words required (an empty object is allowed)")
	}
	for name, w := range cfg.Words {
		if name == "" || w.Target == "" || len(w.Schema) == 0 || !json.Valid(w.Schema) {
			return cfg, fmt.Errorf("word %q requires target and schema", name)
		}
	}
	return cfg, nil
}
func (cfg HandleConfig) ManifestWords() map[string]introspect.WordSpec {
	words := make(map[string]introspect.WordSpec, len(cfg.Words))
	for name, w := range cfg.Words {
		words[name] = introspect.WordSpec{InputSchema: w.Schema, Description: w.Description, Examples: w.Examples}
	}
	return words
}
