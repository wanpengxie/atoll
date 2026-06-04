// Package introspect is the standard self-answer convention every actor exposes:
// the reserved actor.* introspection queries plus their response shapes.
//
// It is NOT substrate. The substrate does not gate or enforce these — the
// generic harness sender-consistency step already prevents an actor from forging
// an answer about another actor (you can only emit envelopes as yourself), and
// actor.* is otherwise a plain type. These are well-known CONVENTION names (like
// HTTP "GET"): the set of names and response shapes for the actor.* self-answer
// protocol, owned by the stdlib and frozen. Changing a name or response field is
// a protocol-level convention revision.
package introspect

import (
	"context"
	"encoding/json"
)

// The reserved introspection queries — the standard questions any actor / the
// channel answers about itself.
const (
	// QueryDescribe — what can this actor do (its live API surface).
	QueryDescribe = "actor.describe"
	// QueryList — who is in this channel (membership ∧ presence).
	QueryList = "actor.list"
)

// NOTE: there is no actor.status query. "Is this actor serviceable right now"
// is not a queryable 存量 — it is the OUTCOME of send→terminal (the substrate
// presence-down edge materialises receiver_unavailable when the actor is gone). A
// status query could only answer a trivial constant available=true, which
// carries no truth — a half-built slice that misleads later readers. When a
// concrete adapter has non-trivial domain state worth surfacing proactively
// (e.g. an adapter with non-trivial login state), an optional Statuser self-answer is added
// additively (parallel to Describer below) — pain-driven, not pre-built.

// APIDescriptor describes one callable API, returned dynamically inside a
// Describe response. The actor is the sole authority on its own capability; a
// caller discovers it by asking the actor, live (via the Describer hook).
type APIDescriptor struct {
	// Name is the request envelope.type the API answers (e.g. "notes.publish").
	Name string `json:"name"`
	// Schema is the parameter schema for the request payload — a caller uses it
	// to construct a valid call. Concrete format is the actor's domain concern
	// (opaque here).
	Schema json.RawMessage `json:"schema,omitempty"`
	// Desc is a one-line description of what the API does.
	Desc string `json:"desc,omitempty"`
}

// Describe is the actor.describe response: the actor's identity plus its live
// API surface. APIs is nil for actors that expose no callable surface.
type Describe struct {
	Name    string          `json:"name"`
	Binding string          `json:"binding,omitempty"`
	APIs    []APIDescriptor `json:"apis,omitempty"`
}

// CatalogEntry is one row of the actor.list channel directory: membership
// (registry truth) ⋈ obs (volatile presence + uptime, read via Runtime.Stat). No
// readiness axis — whether an actor can service a request is the OUTCOME of
// send→terminal, not a field here.
type CatalogEntry struct {
	ID      string `json:"id"`
	Kind    string `json:"kind"`
	Binding string `json:"binding,omitempty"`
	Present bool   `json:"present"`
	// UptimeMs is the elapsed time since the substrate bound the live instance
	// (now - StartedAt), derived by the system actor from Runtime.Stat. 0 when
	// not present. Substrate-owned obs (the actor never self-reports it).
	UptimeMs int64 `json:"uptime_ms,omitempty"`
}

// Catalog is the actor.list response: the channel-wide directory.
type Catalog struct {
	Actors []CatalogEntry `json:"actors"`
}

// Describer is the OPTIONAL capability an actor implements to answer
// actor.describe with its live API surface. It is asked on the actor's own
// goroutine, so it reports CURRENT capability (e.g. only what it can do while
// logged in), never a predefined registry. Actors that don't implement it
// answer describe with their identity only.
type Describer interface {
	Describe(ctx context.Context) ([]APIDescriptor, error)
}

// BuildDescribe assembles the actor.describe answer for an actor identified by
// name, honouring the Describer convention: if impl implements Describer its live
// APIs are included; otherwise the answer is identity-only. This is the ONE
// standard way to serve actor.describe — every actor, and any generic host
// serving describe on behalf of arbitrary actors, routes through it, so the
// answer shape never drifts from the convention. Binding (an optional addressing
// attribute) is left for the caller to set when it has one.
func BuildDescribe(ctx context.Context, name string, impl any) (Describe, error) {
	d := Describe{Name: name}
	if dr, ok := impl.(Describer); ok {
		apis, err := dr.Describe(ctx)
		if err != nil {
			return Describe{}, err
		}
		d.APIs = apis
	}
	return d, nil
}
