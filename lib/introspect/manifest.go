package introspect

import (
	"context"
	"encoding/json"
	"fmt"
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
}

var gateErrorCodes = map[string]bool{
	"bad_origin": true, "endpoint_not_found": true, "receiver_inactive": true,
	"forbidden": true, "channel_unavailable": true, "no_service_agent": true,
}

func ValidateManifest(m Manifest) error {
	return validateWords(m.Words)
}

func ValidateDynamicWords(static map[string]WordSpec, words map[string]WordSpec) error {
	return validateDynamicWords(static, words)
}

// ValidateProjectedWords checks a live proxy projection with the same
// collision and error-code rules used for an instance declaration. Word
// prefixes are capability names, not an authorization boundary.
func ValidateProjectedWords(static map[string]WordSpec, words map[string]WordSpec) error {
	return validateDynamicWords(static, words)
}

func validateDynamicWords(static map[string]WordSpec, words map[string]WordSpec) error {
	for name := range words {
		if _, exists := static[name]; exists {
			return fmt.Errorf("introspect: dynamic word collides with class word %q", name)
		}
	}
	return validateWords(words)
}

func validateWords(words map[string]WordSpec) error {
	for _, spec := range words {
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
