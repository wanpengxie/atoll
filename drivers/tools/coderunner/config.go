package coderunner

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
)

const defaultNode = "node"

var requirePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*(:[a-z0-9][a-z0-9._-]*)?$`)

type Config struct {
	Program  string   `json:"program,omitempty"`
	Requires []string `json:"requires,omitempty"`
	Node     string   `json:"node,omitempty"`
}

func parseConfig(raw json.RawMessage) (Config, error) {
	cfg := Config{Node: defaultNode}
	if len(raw) == 0 {
		return cfg, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("coderunner config: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Config{}, errors.New("coderunner config: multiple JSON values")
		}
		return Config{}, fmt.Errorf("coderunner config: %w", err)
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	if program, ok := fields["program"]; ok {
		var value string
		if json.Unmarshal(program, &value) != nil || strings.TrimSpace(value) == "" {
			return Config{}, errors.New("coderunner config: program must be a non-blank string")
		}
	}
	if requires, ok := fields["requires"]; ok {
		var value []string
		if json.Unmarshal(requires, &value) != nil || value == nil {
			return Config{}, errors.New("coderunner config: requires must be an array of strings")
		}
	}
	if cfg.Node == "" {
		cfg.Node = defaultNode
	}
	if err := validateRequires(cfg.Requires); err != nil {
		return Config{}, fmt.Errorf("coderunner config: %w", err)
	}
	return cfg, nil
}

func validateRequires(requires []string) error {
	for _, requirement := range requires {
		if requirement != "system" && !requirePattern.MatchString(requirement) {
			return fmt.Errorf("requires item %q does not match %s", requirement, requirePattern.String())
		}
	}
	return nil
}

const ConfigSchema = `{
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "program": {"type": "string", "minLength": 1},
    "requires": {"type": "array", "items": {"type": "string", "pattern": "^[a-z0-9][a-z0-9_-]*(:[a-z0-9][a-z0-9._-]*)?$"}},
    "node": {"type": "string"}
  }
}`
