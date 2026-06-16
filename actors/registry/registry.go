package registry

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/channel"
)

// Deps bundles every input an actor might need to declare itself at the daemon
// composition root. Each Decl takes what it needs and ignores the rest — there is
// no "generic vs special" actor, just self-describing decls.
type Deps struct {
	ChannelID    channel.ID // from the --server URL ?channel= (a cell is channel-scoped)
	WorkspaceDir string     // workspace root for device + agent situation facts
	DeviceName   string     // device identity
	Logger       *slog.Logger
}

// Decl builds one actor's hosting declaration from deps. ok=false means the actor
// is NOT applicable in this daemon's context (e.g. an agent with no channel) — it
// is silently skipped by BuildAll and reported as an error only when explicitly
// requested by name. err is a hard build failure (bad config/creds).
type Decl func(Deps) (decl platform.ActorDecl, ok bool, err error)

var (
	mu  sync.RWMutex
	reg = map[string]Decl{}
)

// Register records an actor's Decl under name. Called from an actor package's
// init(); a duplicate name is a programmer error (panic, like sql.Register).
func Register(name string, d Decl) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := reg[name]; dup {
		panic("actors/registry: duplicate actor registration: " + name)
	}
	reg[name] = d
}

// Names returns the registered actor names, sorted (stable BuildAll order).
func Names() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(reg))
	for n := range reg {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Build builds one named actor. An unknown name or a not-applicable actor (ok
// =false) is an error here — the caller asked for it explicitly, so silence would
// hide a misconfiguration.
func Build(name string, deps Deps) (platform.ActorDecl, error) {
	mu.RLock()
	d, found := reg[name]
	mu.RUnlock()
	if !found {
		return platform.ActorDecl{}, fmt.Errorf("actors/registry: unknown actor %q (registered: %v)", name, Names())
	}
	decl, ok, err := d(deps)
	if err != nil {
		return platform.ActorDecl{}, fmt.Errorf("actors/registry: build %q: %w", name, err)
	}
	if !ok {
		return platform.ActorDecl{}, fmt.Errorf("actors/registry: actor %q not applicable in this context (e.g. missing channel)", name)
	}
	return decl, nil
}

// BuildAll builds every registered+applicable actor (the fat-daemon default: one
// binary packages all impls, hosts all that apply here). A not-applicable actor
// is skipped; a hard build error aborts.
func BuildAll(deps Deps) ([]platform.ActorDecl, error) {
	var out []platform.ActorDecl
	for _, name := range Names() {
		mu.RLock()
		d := reg[name]
		mu.RUnlock()
		decl, ok, err := d(deps)
		if err != nil {
			return nil, fmt.Errorf("actors/registry: build %q: %w", name, err)
		}
		if !ok {
			if deps.Logger != nil {
				deps.Logger.Debug("registry.skip", "actor", name, "reason", "not applicable in this context")
			}
			continue
		}
		out = append(out, decl)
	}
	return out, nil
}
