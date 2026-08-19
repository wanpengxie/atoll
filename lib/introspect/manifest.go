package introspect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Manifest is the one class/instance capability declaration projected by
// actor.describe. Dynamic is the instance half; it is never serialized.
type Manifest struct {
	Class        string                                             `json:"class"`
	Interfaces   []string                                           `json:"interfaces"`
	Capabilities map[string]bool                                    `json:"capabilities"`
	Words        map[string]WordSpec                                `json:"words"`
	Dynamic      func(context.Context) (map[string]WordSpec, error) `json:"-"`
}

// WordSpec describes only a request body and its application result.
type WordSpec struct {
	Description  string            `json:"description,omitempty"`
	InputSchema  json.RawMessage   `json:"input_schema,omitempty"`
	OutputSchema json.RawMessage   `json:"output_schema,omitempty"`
	ErrorCodes   []string          `json:"error_codes,omitempty"`
	Examples     []json.RawMessage `json:"examples,omitempty"`

	// Compatibility-only authoring fields. They are intentionally absent from
	// the wire projection and disappear as class declarations are migrated.
	AllowedKinds   []string        `json:"-"`
	MaxPendingMs   int64           `json:"-"`
	PayloadExample json.RawMessage `json:"-"`
	PayloadFields  []FieldDoc      `json:"-"`
	Notes          string          `json:"-"`
}

// TypeMeta is retained as a source alias while callers move to WordSpec; the
// actor.describe wire has only words.
type TypeMeta = WordSpec

type FieldDoc struct {
	Name        string `json:"name"`
	Required    bool   `json:"required,omitempty"`
	Description string `json:"description"`
	Example     any    `json:"example,omitempty"`
}

type ErrorDoc struct {
	Code        string `json:"code"`
	Description string `json:"description"`
	Recovery    string `json:"recovery,omitempty"`
}

var gateErrorCodes = map[string]bool{
	"bad_origin": true, "endpoint_not_found": true, "receiver_inactive": true,
	"forbidden": true, "channel_unavailable": true, "no_service_agent": true,
}

var standardInterfaces = map[string]bool{
	"actor": true, "agent": true, "human": true, "peer": true, "svcactor": true,
}

var reservedWordPrefixes = map[string]bool{
	"system": true, "actor": true, "agent": true, "human": true,
	"peer": true, "svcactor": true,
}

func ValidateManifest(m Manifest) error {
	if m.Class == "" {
		return errors.New("introspect: manifest class required")
	}
	owners := map[string]bool{}
	for _, name := range m.Interfaces {
		if !standardInterfaces[name] {
			return fmt.Errorf("introspect: unknown interface %q", name)
		}
		owners[name] = true
	}
	// system.* belongs to the membrane's class-level manifest. "membrane" is
	// not a reserved class name; this only identifies the one static owner of
	// that word prefix.
	if m.Class == "membrane" {
		owners["system"] = true
	}
	return validateWords(m.Words, owners, false)
}

func ValidateDynamicWords(static map[string]WordSpec, words map[string]WordSpec) error {
	return validateDynamicWords(static, words, true)
}

// ValidateProjectedWords checks a live proxy projection. Unlike an instance
// declaration, these words keep the remote owner's prefixes, so prefix
// ownership was already checked at the remote class/state write boundary.
func ValidateProjectedWords(static map[string]WordSpec, words map[string]WordSpec) error {
	return validateDynamicWords(static, words, false)
}

func validateDynamicWords(static map[string]WordSpec, words map[string]WordSpec, rejectReserved bool) error {
	for name := range words {
		if _, exists := static[name]; exists {
			return fmt.Errorf("introspect: dynamic word collides with class word %q", name)
		}
	}
	return validateWords(words, nil, rejectReserved)
}

func validateWords(words map[string]WordSpec, owners map[string]bool, dynamic bool) error {
	for name, spec := range words {
		parts := strings.Split(name, ".")
		if len(parts) < 2 || parts[0] == "" {
			return fmt.Errorf("introspect: invalid word %q", name)
		}
		if reservedWordPrefixes[parts[0]] && (dynamic || !owners[parts[0]]) {
			return fmt.Errorf("introspect: reserved word prefix %q", parts[0])
		}
		for _, code := range spec.ErrorCodes {
			if gateErrorCodes[code] {
				return fmt.Errorf("introspect: gate error code %q is reserved", code)
			}
		}
	}
	return nil
}

func CloneWords(in map[string]WordSpec) map[string]WordSpec {
	out := make(map[string]WordSpec, len(in))
	for name, spec := range in {
		out[name] = spec
	}
	return out
}

func ManifestFromLegacy(class string, interfaces []string, capabilities map[string]bool, d Describe) Manifest {
	words := d.Words
	if words == nil {
		words = d.Types
	}
	return Manifest{Class: class, Interfaces: interfaces, Capabilities: capabilities, Words: CloneWords(words)}
}
