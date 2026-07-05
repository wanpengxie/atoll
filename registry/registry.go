package registry

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// Deps is the HOST CONTEXT a constructor builds against (which channel, the
// workspace root, the device identity, a logger). It is NOT instance identity —
// "which instance, with what config" comes from InstanceSpec. Splitting the two
// is the whole point: a class is a template; the host context + an InstanceSpec
// together produce one running instance.
type Deps struct {
	ChannelID    channel.ID // the channel this cell is scoped to
	WorkspaceDir string     // workspace root (device / agent situation facts)
	DeviceName   string     // device identity (for the device class's id)
	Logger       *slog.Logger
}

// InstanceSpec is one actor instance's deployment params: its id and opaque
// per-instance config. A class (actors/<x>) is a template; an instance =
// class + InstanceSpec.
//
//   - ID == "" → "use the class's default id" (the fat-daemon one-of-each case).
//   - ID != "" → instantiate this class under that id; the SAME class can be
//     instantiated many times under different ids (multi-agent falls out here).
//   - device ignores ID and derives it from the device identity (essence
//     singleton: id IS the resource).
type InstanceSpec struct {
	ID actor.ActorID
	// Config is THE per-instance config injection point — the app-rewire spec's
	// "ctx.Config" (K2=a/S8). It is NOT a new runtime surface; the constructor
	// closure captures it and hands it to the actor in BOTH forms: a Legacy
	// actor's constructor parses it and closes over the result in the
	// func(pen) closure; a Proc actor's constructor closes it into its Def
	// (Constructor(spec,deps) → Def → New() per incarnation; see
	// actorbase.Def's doc). Either way config rides the constructor, NOT the
	// capability bundle: it is an independent PARAMETER, never welded into
	// actorcaps.Caps (S-P16 红线; enforced by archtest.TestConfigNotInCaps).
	// A config change is an intent write on the composition row (改配置门) that
	// takes effect via Spawn-replace — a fresh incarnation over a fresh snapshot;
	// there is no live hot-read of Config.
	Config json.RawMessage
}

// Constructor builds one instance of a class: given the deployment spec (id +
// config) and the host context, it produces the ActorDecl the host spawns. The
// id comes from the spec, NOT baked into the constructor — so one class yields
// many instances. A bad config / missing creds is a hard error.
type Constructor func(spec InstanceSpec, ctx Deps) (platform.ActorDecl, error)

// ClassDecl is a class's directory-level declaration: the class-level facts
// knowable WITHOUT running the constructor (pre-Build), plus the constructor
// itself. Kind is the one such fact today — Admit needs it before any cell is
// built, and it was previously locked inside the Constructor's return value
// (unreachable pre-Build). Future class-level facts are additive fields here;
// the Register signature no longer changes when they arrive.
type ClassDecl struct {
	Kind actor.Kind
	New  Constructor
}

var (
	mu  sync.RWMutex
	reg = map[string]ClassDecl{}
)

// Register records a class's ClassDecl under its class key. Called from an
// actor package's init(); a duplicate class is a programmer error (panic, like
// sql.Register).
func Register(class string, d ClassDecl) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := reg[class]; dup {
		panic("registry: duplicate class registration: " + class)
	}
	reg[class] = d
}

// ClassKind returns a class's declared Kind (pure table lookup, no construction)
// and whether the class is registered. It is the pre-Build kind source Admit
// resolves against.
func ClassKind(class string) (actor.Kind, bool) {
	mu.RLock()
	defer mu.RUnlock()
	d, ok := reg[class]
	if !ok {
		return "", false
	}
	return d.Kind, true
}

// classes returns the registered class keys, sorted (stable iteration order).
// Unexported: the only legitimate reader is Build's own error path below —
// there is no discovery/catalog surface over the registry (see package doc).
func classes() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(reg))
	for c := range reg {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// Build instantiates one instance of class with the given spec + host context.
// Unknown class or a constructor error are returned to the caller (who asked for
// it explicitly).
func Build(class string, spec InstanceSpec, ctx Deps) (platform.ActorDecl, error) {
	mu.RLock()
	d, found := reg[class]
	mu.RUnlock()
	if !found {
		return platform.ActorDecl{}, fmt.Errorf("registry: unknown class %q (registered: %v)", class, classes())
	}
	decl, err := d.New(spec, ctx)
	if err != nil {
		return platform.ActorDecl{}, fmt.Errorf("registry: build %q: %w", class, err)
	}
	// Consistency guard: the constructed decl's Kind must match the class's
	// declared Kind — a drift means the directory declaration lies about what
	// the constructor produces. Fail loud rather than admit a mislabelled cell.
	if decl.Kind != d.Kind {
		return platform.ActorDecl{}, fmt.Errorf("registry: build %q: constructed kind %q ≠ declared kind %q", class, decl.Kind, d.Kind)
	}
	return decl, nil
}
