package channelmember

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/channel"
)

const (
	DefaultMaxConcurrency = 16
	DefaultQueueCapacity  = 64
	maxMaxConcurrency     = 64
	maxQueueCapacity      = 1024
)

type SeatConfig struct {
	Body           channel.ID `json:"body"`
	MaxConcurrency int        `json:"max_concurrency,omitempty"`
	QueueCapacity  int        `json:"queue_capacity,omitempty"`
}
type Word struct {
	Schema      json.RawMessage   `json:"schema"`
	Description string            `json:"description,omitempty"`
	Examples    []json.RawMessage `json:"examples,omitempty"`
	Target      string            `json:"target"`
}
type HandleConfig struct {
	Host           channel.ID      `json:"host"`
	Words          map[string]Word `json:"words"`
	Drivers        []string        `json:"drivers,omitempty"`
	MaxConcurrency int             `json:"max_concurrency,omitempty"`
	QueueCapacity  int             `json:"queue_capacity,omitempty"`
}

func ParseSeatConfig(raw json.RawMessage) (SeatConfig, error) {
	cfg := SeatConfig{MaxConcurrency: DefaultMaxConcurrency, QueueCapacity: DefaultQueueCapacity}
	if err := actorbase.DecodeStrict(raw, &cfg); err != nil {
		return cfg, err
	}
	if cfg.Body == "" {
		return cfg, errors.New("body required")
	}
	if _, _, err := forwardingLimits(cfg.MaxConcurrency, cfg.QueueCapacity); err != nil {
		return cfg, err
	}
	return cfg, nil
}
func ParseHandleConfig(raw json.RawMessage) (HandleConfig, error) {
	cfg := HandleConfig{MaxConcurrency: DefaultMaxConcurrency, QueueCapacity: DefaultQueueCapacity}
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
	if _, _, err := forwardingLimits(cfg.MaxConcurrency, cfg.QueueCapacity); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// forwardingLimits resolves the operational defaults for configs constructed
// in Go as well as configs decoded from JSON. Seat and Handle own this queue;
// the generic actor mailbox is not their overflow buffer.
func forwardingLimits(workers, backlog int) (int, int, error) {
	if workers == 0 {
		workers = DefaultMaxConcurrency
	}
	if backlog == 0 {
		backlog = DefaultQueueCapacity
	}
	if workers < 1 || workers > maxMaxConcurrency {
		return 0, 0, fmt.Errorf("max_concurrency must be 1..%d", maxMaxConcurrency)
	}
	if backlog < 1 || backlog > maxQueueCapacity {
		return 0, 0, fmt.Errorf("queue_capacity must be 1..%d", maxQueueCapacity)
	}
	return workers, backlog, nil
}
func (cfg HandleConfig) ManifestWords() map[string]introspect.WordSpec {
	words := make(map[string]introspect.WordSpec, len(cfg.Words))
	for name, w := range cfg.Words {
		words[name] = introspect.WordSpec{InputSchema: w.Schema, Description: w.Description, Examples: w.Examples}
	}
	return words
}
