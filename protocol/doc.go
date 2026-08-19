// Package protocol is Atoll's data-only protocol contract layer.
//
// The layer has five subpackages with deliberately narrow dependency edges:
// actor, channel, and resource are independent leaves; message may depend only
// on actor and channel; access may depend only on actor and resource. Protocol
// packages own closed wire vocabularies and pure data transformations, never
// storage, runtime capabilities, transport bindings, or context-bearing seams.
//
// Runtime readers, placement and provisioning models, canonical configuration,
// and admission results live with their consumers outside this layer. These
// import directions are enforced as architectural invariants.
package protocol
