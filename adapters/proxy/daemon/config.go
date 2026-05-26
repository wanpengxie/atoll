package daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/wanpengxie/ActOS/adapters/proxy/actorapi"
	"github.com/wanpengxie/ActOS/kernel/actor"
)

const (
	DefaultServerWS = "ws://localhost:8832"
	DefaultPort     = 10387
)

var DefaultEnabledActors = []actor.ActorID{"tool:kimi"}

type Config struct {
	APIKey        string                            `json:"api_key"`
	ServerWS      string                            `json:"server_ws"`
	Port          int                               `json:"port,omitempty"`
	EnabledActors []actor.ActorID                   `json:"enabled_actors,omitempty"`
	Actors        map[actor.ActorID]json.RawMessage `json:"actors,omitempty"`
	HostLabel     string                            `json:"host_label,omitempty"`
}

func DefaultConfigPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("proxy config home: %w", err)
	}
	return filepath.Join(home, ".coagent", "proxy", "config.json"), nil
}

func LoadConfig(path string) (Config, error) {
	if path == "" {
		var err error
		path, err = DefaultConfigPath()
		if err != nil {
			return Config{}, err
		}
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return Config{}, fmt.Errorf("proxy config decode %s: %w", path, err)
	}
	return cfg.Normalize(), nil
}

func WriteConfig(path string, cfg Config) error {
	if path == "" {
		var err error
		path, err = DefaultConfigPath()
		if err != nil {
			return err
		}
	}
	cfg = cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("proxy config encode: %w", err)
	}
	raw = append(raw, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("proxy config mkdir %s: %w", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return fmt.Errorf("proxy config write %s: %w", path, err)
	}
	return nil
}

func (c Config) Normalize() Config {
	c.APIKey = strings.TrimSpace(c.APIKey)
	c.ServerWS = strings.TrimSpace(c.ServerWS)
	c.HostLabel = strings.TrimSpace(c.HostLabel)
	if c.ServerWS == "" {
		c.ServerWS = DefaultServerWS
	}
	if c.Port == 0 {
		c.Port = DefaultPort
	}
	if len(c.EnabledActors) == 0 {
		c.EnabledActors = append([]actor.ActorID(nil), DefaultEnabledActors...)
	} else {
		seen := map[actor.ActorID]struct{}{}
		out := make([]actor.ActorID, 0, len(c.EnabledActors))
		for _, id := range c.EnabledActors {
			id = actor.ActorID(strings.TrimSpace(string(id)))
			if id == "" {
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
		c.EnabledActors = out
	}
	if c.Actors == nil {
		c.Actors = map[actor.ActorID]json.RawMessage{}
	}
	return c
}

func (c Config) Validate() error {
	c = c.Normalize()
	if c.APIKey == "" {
		return errors.New("proxy config: api_key required")
	}
	if c.ServerWS == "" {
		return errors.New("proxy config: server_ws required")
	}
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("proxy config: port %d out of range", c.Port)
	}
	if len(c.EnabledActors) == 0 {
		return errors.New("proxy config: enabled_actors required")
	}
	return nil
}

func (c Config) ModuleConfig(id actor.ActorID) actorapi.ModuleConfig {
	if len(c.Actors) == 0 {
		return actorapi.ModuleConfig{}
	}
	raw := c.Actors[id]
	if len(raw) == 0 {
		return actorapi.ModuleConfig{}
	}
	return actorapi.ModuleConfig{Raw: append(json.RawMessage(nil), raw...)}
}
