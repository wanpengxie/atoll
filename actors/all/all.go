// Package all blank-imports every in-tree, self-registering actor so ONE daemon
// binary packages them all (one process hosts all — not one process per actor).
// Each imported package's init() calls registry.Register; the daemon then
// builds instances from the catalog via registry.Build(class, spec, ctx).
//
// This list is hand-maintained: adding an in-tree actor = add one blank import
// line here. That is the earliest, simplest form and is correct at this scale.
// When hand-editing actually hurts (dozens of actors, frequent churn), the SAME
// list can be auto-generated from the actors/ directory by a codegen step — a
// pure-additive change (the file's shape is identical), so deferring it costs
// nothing. Building that codegen now would be premature abstraction.
//
// Third-party / out-of-tree actors do NOT touch this file: they are not compiled
// in, they connect in over the link as their own actor (see actor-integration-spec).
package all

import (
	_ "github.com/wanpengxie/atoll/actors/device"
	_ "github.com/wanpengxie/atoll/actors/echo"
	_ "github.com/wanpengxie/atoll/actors/kimi"
	_ "github.com/wanpengxie/atoll/actors/xhs"
)
