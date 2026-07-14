package platform

import (
	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/platform/internal/hostcommon"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// ActorFactory is the "def" every out-generation entry point speaks (spec
// actorbase-v1 §4 S3: _, _, _ = Home.SpawnIfAbsent(ctx,id,kind,def) / CapsFactoryBuilder /
// compute.Builder / ActorDecl all resolve to this ONE shape). It replaces the
// old bare `func(actorcaps.Caps) actorrt.Actor` — no downstream package ever
// names actorcaps.Caps to produce one; the caps→actor weld happens INSIDE
// hostcommon's Build, the single seam this period. This is an alias onto the
// shared representation in platform/internal/hostcommon — both host packages
// (home/compute) speak the SAME concrete type, so a factory built by one is
// legible to the other without conversion.
type ActorFactory = hostcommon.ActorFactory

// CapsFactory exposes the platform-only full-caps test seam. Production actor
// implementations use ActorFactory{Proc: ...}; tests that must inspect a welded
// capability bundle use this constructor without adding a second production
// factory representation.
func CapsFactory(f func(actorcaps.Caps) actorrt.Actor) ActorFactory {
	return hostcommon.CapsFactory(f)
}
