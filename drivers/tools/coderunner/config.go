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
	// Node is the legacy way to pick the runtime: the node binary for the
	// embedded runner. Runtime supersedes it.
	Node string `json:"node,omitempty"`
	// Runtime is any process that speaks MCP client over its stdio and
	// imports the program from $ATOLL_PROGRAM (see code-runtime-mcp.md).
	// Absent → the embedded Node runner.
	Runtime *RuntimeConfig `json:"runtime,omitempty"`
}

// RuntimeConfig names the runtime process. Suffix is the program file's
// extension (".mjs" by default), so a runtime that imports by path sees the
// file kind it expects.
type RuntimeConfig struct {
	Command string   `json:"command"`
	Args    []string `json:"args,omitempty"`
	Suffix  string   `json:"suffix,omitempty"`
}

// runtime resolves the process to start for one run.
func (c Config) runtime() (command string, args []string, suffix string) {
	if c.Runtime != nil && strings.TrimSpace(c.Runtime.Command) != "" {
		suffix = c.Runtime.Suffix
		if suffix == "" {
			suffix = ".mjs"
		}
		return c.Runtime.Command, append([]string(nil), c.Runtime.Args...), suffix
	}
	node := c.Node
	if node == "" {
		node = defaultNode
	}
	return node, []string{"--input-type=module", "-e", runnerSource}, ".mjs"
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
	if cfg.Runtime != nil && strings.TrimSpace(cfg.Runtime.Command) == "" {
		return Config{}, errors.New("coderunner config: runtime.command must be a non-blank string")
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
    "node": {"type": "string"},
    "runtime": {
      "type": "object",
      "additionalProperties": false,
      "required": ["command"],
      "properties": {
        "command": {"type": "string", "minLength": 1},
        "args": {"type": "array", "items": {"type": "string"}},
        "suffix": {"type": "string"}
      }
    }
  }
}`
