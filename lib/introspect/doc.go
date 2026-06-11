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
//
// This package is the ONE home of the contract. Response shapes are defined
// here and ONLY here: actors construct these types (never parallel local
// structs — archtest enforces it), and lib/metatool binds them to the LLM tool
// surface without restating fields.
package introspect
