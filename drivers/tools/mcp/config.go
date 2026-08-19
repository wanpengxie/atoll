package mcp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"regexp"
	"time"
)

const (
	transportStdio       = "stdio"
	transportHTTP        = "http"
	defaultCallTimeoutMS = int64(60_000)
)

var configNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

// Config contains only the facts needed to reach one external MCP server.
// Its tool surface is deliberately absent: the actor discovers that live.
type Config struct {
	Name          string            `json:"name"`
	Transport     string            `json:"transport"`
	Command       string            `json:"command,omitempty"`
	Args          []string          `json:"args,omitempty"`
	Cwd           string            `json:"cwd,omitempty"`
	Env           map[string]string `json:"env,omitempty"`
	URL           string            `json:"url,omitempty"`
	CallTimeoutMS int64             `json:"call_timeout_ms,omitempty"`
}

func parseConfig(raw json.RawMessage) (Config, error) {
	if len(raw) == 0 {
		return Config{}, errors.New("mcp config: transport required")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return Config{}, errors.New("mcp config: must be a JSON object")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("mcp config: %w", err)
	}
	if err := ensureEOF(dec); err != nil {
		return Config{}, fmt.Errorf("mcp config: %w", err)
	}

	has := func(name string) bool {
		_, ok := fields[name]
		return ok
	}
	if !has("call_timeout_ms") {
		cfg.CallTimeoutMS = defaultCallTimeoutMS
	} else if cfg.CallTimeoutMS <= 0 || cfg.CallTimeoutMS > math.MaxInt64/int64(time.Millisecond) {
		return Config{}, errors.New("mcp config: call_timeout_ms must be a positive millisecond duration")
	}
	if cfg.Name == "" {
		return Config{}, errors.New("mcp config: name required")
	}
	if !configNamePattern.MatchString(cfg.Name) {
		return Config{}, errors.New("mcp config: name must match [a-z0-9][a-z0-9_-]* and must not contain dots")
	}
	switch cfg.Transport {
	case transportStdio:
		if cfg.Command == "" {
			return Config{}, errors.New("mcp config: stdio transport requires command")
		}
		if has("url") {
			return Config{}, errors.New("mcp config: stdio transport forbids url")
		}
	case transportHTTP:
		if cfg.URL == "" {
			return Config{}, errors.New("mcp config: http transport requires url")
		}
		if has("command") || has("args") || has("cwd") || has("env") {
			return Config{}, errors.New("mcp config: http transport forbids command, args, cwd, and env")
		}
		parsed, err := url.Parse(cfg.URL)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return Config{}, errors.New("mcp config: url must be an absolute http or https URL")
		}
	default:
		return Config{}, fmt.Errorf("mcp config: transport must be %q or %q", transportStdio, transportHTTP)
	}
	return cfg, nil
}

func ensureEOF(dec *json.Decoder) error {
	var trailing any
	err := dec.Decode(&trailing)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("multiple JSON values")
	}
	return err
}

// ConfigSchema publishes what Config above accepts, for the same reason the
// strict decoder exists: an undeclared field is a hard rejection, so a caller
// needs the field list before writing one, not after.
const ConfigSchema = `{
  "type": "object",
  "additionalProperties": false,
  "required": ["name", "transport"],
  "properties": {
    "name": {"type": "string", "pattern": "^[a-z0-9][a-z0-9_-]*$"},
    "transport": {"type": "string", "enum": ["stdio", "http"]},
    "command": {"type": "string", "description": "stdio transport: the executable to run"},
    "args": {"type": "array", "items": {"type": "string"}},
    "cwd": {"type": "string"},
    "env": {"type": "object", "additionalProperties": {"type": "string"}},
    "url": {"type": "string", "description": "http transport: the server endpoint"},
    "call_timeout_ms": {"type": "integer", "minimum": 1}
  }
}`
