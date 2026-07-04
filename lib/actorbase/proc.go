package actorbase

// Proc is the process form itself (spec §1.6): entry = birth, return = death
// (nil → quiet/寿终, non-nil error → loud/横死), defer = resource release,
// local variables = the process's own memory (its stack). This one function
// signature is the entire generation contract — no second lifecycle hook
// exists beside it (mirrors Unix main / an Erlang spawn fun).
type Proc func(sys Sys) error

// Def is the registry entry one Proc constructor is registered under. New
// takes zero parameters BY DESIGN: spec/deps are captured by the
// registry.Constructor closure that produced New (the assembly chain is
// Constructor(spec,deps) → Def → one New() per incarnation → Proc), so this
// type itself never carries configuration. Doc is the one declaration this
// contract keeps — introspection answers describe with it, so it earns its
// keep as data rather than as a second registration channel.
type Def struct {
	Doc string
	New func() (Proc, error)
}
