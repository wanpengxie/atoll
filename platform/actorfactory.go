package platform

import "github.com/wanpengxie/atoll/platform/internal/hostcommon"

// ActorFactory is the "def" every out-generation entry point speaks (spec
// actorbase-v1 §4 S3: every construction entry point resolves this one definition shape.
// Home, compute, and declarations all resolve to this ONE shape. Downstream
// packages never receive the raw caps-to-actor constructor representation; the caps→actor weld happens INSIDE
// hostcommon's Build, the single seam this period. This is an alias onto the
// shared representation in platform/internal/hostcommon — both host packages
// (home/compute) speak the SAME concrete type, so a factory built by one is
// legible to the other without conversion.
type ActorFactory = hostcommon.ActorFactory
