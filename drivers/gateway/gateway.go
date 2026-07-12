// Package gateway is the human ingress driver's home (design
// humancell-gateway-design-v2.md §5.3): the one thick component that swallows
// the external world's dirt — auth sessions, multi-tab arbitration, reconnect
// storms, cross-connector session aggregation, binding management — and
// standardises it into the channel frame protocol. It has ZERO channel
// write/action capability (reads are a controlled flow handle); the pen never
// leaves the wall.
//
// This is the S0 empty shell: the Start/Close lifecycle contract only. The
// session cross (user entry × channel arm × lane), connectors, per-identity
// slot wiring, frame interpreter and presence归一 land in S3/S4 — this file
// holds the seam so the umbrella package (drivers/) fence and the assembly
// root (cmd/*) have a stable target from day 0.
//
// Fence (archtest drivers_confinement_test.go): drivers/* may import only the
// lib/protocol/runtime + platform export faces + registry; nobody imports
// drivers/* except the assembly root cmd/*. gateway reaches app-side policy
// (routing) and platform-side membership events through injected seams the
// assembly root wires, never by importing app.
package gateway

// Gateway is the human ingress component (one per process). S0 shell: it holds
// no state yet — the session cross and its arms are built in S3.
type Gateway struct{}

// New constructs the (empty) gateway shell. Dependencies (the injected routing
// resolver, feed-stream handles, revocation source) arrive as S3 fills the
// component; the S0 constructor takes none so the assembly-root seam compiles
// from day 0.
func New() *Gateway { return &Gateway{} }

// Start brings the gateway up. S0 shell: no-op. S3 spins connectors, the read
// pump and the session accounting here.
func (g *Gateway) Start() error { return nil }

// Close tears the gateway down. Per the close ordering (design §5.5 / DoD-9),
// the gateway goes silent BEFORE Home — epoch invalidation → slot撤销 → arm
// seal cascade lands in S3/S4. S0 shell: no-op.
func (g *Gateway) Close() error { return nil }
