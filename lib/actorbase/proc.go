package actorbase

import "github.com/wanpengxie/atoll/lib/introspect"

// Proc is the process form itself (spec §1.6): entry = birth, return = death
// (nil → quiet/寿终, non-nil error → loud/横死), defer = resource release,
// local variables = the process's own memory (its stack). This one function
// signature is the entire generation contract — no second lifecycle hook
// exists beside it (mirrors Unix main / an Erlang spawn fun).
//
// A Proc MUST respond to its termination signals — sys.Recv() returning an
// error (ErrRecvDone) and sys.Life() being Done — by returning. This is a
// cooperative model with NO pre-emption: a Proc that ignores them makes Stop
// wait for it without bound (Stop joins the worker goroutine).
type Proc func(sys Sys) error

// Def is the registry entry one Proc constructor is registered under. New
// takes zero parameters BY DESIGN: spec/deps are captured by the
// registry.Constructor closure that produced New (the assembly chain is
// Constructor(spec,deps) → Def → one New() per incarnation → Proc), so this
// type itself never carries configuration. Manifest is the class declaration
// projected by the engine; actor code never handles actor.describe itself.
type Def struct {
	Manifest introspect.Manifest
	New      func() (Proc, error)
}
