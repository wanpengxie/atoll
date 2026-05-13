package harness

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// coreTypeSpec captures everything the harness needs to know about one
// L1 §1.1 core type to drive Step 0 / Step 5 / Step 6 / Step 8 without
// reaching into the type_registry. core types are hard-coded by spec
// (L1 §1.1: "core types 由 L0/L1 spec 持有，channel 不可禁用") and never
// live in type_registry rows — keeping them here makes the harness body
// self-contained.
//
// Fields:
//
//   - DefaultKind    : Step 0 normalize fill-in when caller omits `kind`.
//   - AllowOverride  : when false, callers MUST not supply a non-default
//     kind (L1 §1.1 "允许覆盖=❌"). When true, callers may
//     pass any of {event, request, response}.
//   - TerminalAlways : when kind=response, is_terminal is always true for
//     core types (no payload_status branch; L1 §10.2.1
//     "core type single-response").
//   - ValidatePayload: optional baseline payload check. For M1.3 baseline
//     every core type only requires payload to be a JSON
//     object (`{}` legal). Detailed structural schemas
//     (severity enum, path shape, …) are deferred to T15
//     view-sync work where they actually drive UI.
type coreTypeSpec struct {
	DefaultKind     v4types.Kind
	AllowOverride   bool
	TerminalAlways  bool
	ValidatePayload func(payload json.RawMessage) error
}

// coreTypes holds the L1 §1.1 closed set of 6 core types. The map is
// package-private; callers reach in through IsCoreType / CoreDefaultKind
// / coreAllowedKinds / etc. so the map definition stays the single
// source of truth.
var coreTypes = map[string]coreTypeSpec{
	"human.text": {
		DefaultKind:     v4types.KindEvent,
		AllowOverride:   true,
		TerminalAlways:  true,
		ValidatePayload: requireObjectPayload,
	},
	"agent.text": {
		DefaultKind:     v4types.KindEvent,
		AllowOverride:   true,
		TerminalAlways:  true,
		ValidatePayload: requireObjectPayload,
	},
	"system.event": {
		DefaultKind:     v4types.KindEvent,
		AllowOverride:   false,
		TerminalAlways:  true,
		ValidatePayload: requireObjectPayload,
	},
	"system.heartbeat": {
		DefaultKind:     v4types.KindEvent,
		AllowOverride:   false,
		TerminalAlways:  true,
		ValidatePayload: requireObjectPayload,
	},
	"file.created": {
		DefaultKind:     v4types.KindEvent,
		AllowOverride:   false,
		TerminalAlways:  true,
		ValidatePayload: requireObjectPayload,
	},
	"file.updated": {
		DefaultKind:     v4types.KindEvent,
		AllowOverride:   false,
		TerminalAlways:  true,
		ValidatePayload: requireObjectPayload,
	},
}

// CoreTypes returns the closed set of L1 §1.1 core type names. Order is
// stable across calls (alphabetical so tests can iterate deterministically).
func CoreTypes() []string {
	return []string{
		"agent.text",
		"file.created",
		"file.updated",
		"human.text",
		"system.event",
		"system.heartbeat",
	}
}

// IsCoreType reports whether t names one of the L1 §1.1 core types.
func IsCoreType(t string) bool {
	_, ok := coreTypes[t]
	return ok
}

// coreDefaultKind returns the spec'd default kind for core type t. Caller
// MUST gate on IsCoreType first; passing a non-core type yields the zero
// Kind (which is invalid).
func coreDefaultKind(t string) v4types.Kind {
	return coreTypes[t].DefaultKind
}

// coreAllowedKinds returns the kinds caller may supply for core type t.
// When the spec says "allow override" (`✅` in L1 §1.1) all three kinds
// are allowed; otherwise only the default kind is permitted.
func coreAllowedKinds(t string) []v4types.Kind {
	spec, ok := coreTypes[t]
	if !ok {
		return nil
	}
	if spec.AllowOverride {
		return []v4types.Kind{v4types.KindEvent, v4types.KindRequest, v4types.KindResponse}
	}
	return []v4types.Kind{spec.DefaultKind}
}

// validateCorePayload runs the minimal payload check for core type t.
// Returns nil if t is not a core type — callers should branch by
// IsCoreType before invoking this.
func validateCorePayload(t string, payload json.RawMessage) error {
	spec, ok := coreTypes[t]
	if !ok {
		return nil
	}
	if spec.ValidatePayload == nil {
		return nil
	}
	return spec.ValidatePayload(payload)
}

// coreTerminalAlways reports whether kind=response on this core type is
// always considered terminal (no payload_status branch). Per L1 §10.2.1
// every core type takes the "single-response" branch.
func coreTerminalAlways(t string) bool {
	spec, ok := coreTypes[t]
	if !ok {
		return false
	}
	return spec.TerminalAlways
}

// requireObjectPayload is the baseline core-type payload check (M1.3):
// `payload` must parse as a JSON object. An empty `{}` is legal
// (L0 §2.2 "`payload={}` legal"). Tighter per-type schemas are deferred
// to T15.
func requireObjectPayload(payload json.RawMessage) error {
	if len(payload) == 0 {
		return errors.New("payload required")
	}
	var probe any
	if err := json.Unmarshal(payload, &probe); err != nil {
		return fmt.Errorf("payload is not valid JSON: %w", err)
	}
	if _, ok := probe.(map[string]any); !ok {
		return errors.New("payload must be a JSON object")
	}
	return nil
}
