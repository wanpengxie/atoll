// Package policy is the channel's audience-resolution policy plane: it maps a
// request's intent to a concrete audience BEFORE the harness audience step. v1
// is the single-host star prototype (pass-through — the sender already names
// the receiver). This is the seam where future topology policy (membership /
// routing) plugs in. Mechanism manages the audience CONSTRAINT; policy manages
// its VALUE — they meet only at the audience field (P24).
package policy

import "github.com/wanpengxie/ActOS/kernel/message"

// Resolver resolves intent → audience. v1 prototype holds no state.
type Resolver struct{}

// New constructs the v1 resolver.
func New() *Resolver { return &Resolver{} }

// Resolve returns the audience for a request. v1 single-host star: as-is.
func (r *Resolver) Resolve(audience message.Audience) message.Audience { return audience }
