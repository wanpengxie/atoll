package registry

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
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
//     singleton: id IS the resource — see actor-instance-model §5.1).
type InstanceSpec struct {
	ID     actor.ActorID
	Config json.RawMessage
}

// Constructor builds one instance of a class: given the deployment spec (id +
// config) and the host context, it produces the ActorDecl the host spawns. The
// id comes from the spec, NOT baked into the constructor — so one class yields
// many instances. A bad config / missing creds is a hard error.
type Constructor func(spec InstanceSpec, ctx Deps) (platform.ActorDecl, error)

var (
	mu  sync.RWMutex
	reg = map[string]Constructor{}
)

// Register records a class's Constructor under its class key. Called from an
// actor package's init(); a duplicate class is a programmer error (panic, like
// sql.Register).
func Register(class string, c Constructor) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := reg[class]; dup {
		panic("actors/registry: duplicate class registration: " + class)
	}
	reg[class] = c
}

// Classes returns the registered class keys, sorted (stable iteration order).
func Classes() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(reg))
	for c := range reg {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// Has reports whether a class is registered.
func Has(class string) bool {
	mu.RLock()
	defer mu.RUnlock()
	_, ok := reg[class]
	return ok
}

// Build instantiates one instance of class with the given spec + host context.
// Unknown class or a constructor error are returned to the caller (who asked for
// it explicitly).
func Build(class string, spec InstanceSpec, ctx Deps) (platform.ActorDecl, error) {
	mu.RLock()
	c, found := reg[class]
	mu.RUnlock()
	if !found {
		return platform.ActorDecl{}, fmt.Errorf("actors/registry: unknown class %q (registered: %v)", class, Classes())
	}
	decl, err := c(spec, ctx)
	if err != nil {
		return platform.ActorDecl{}, fmt.Errorf("actors/registry: build %q: %w", class, err)
	}
	return decl, nil
}
