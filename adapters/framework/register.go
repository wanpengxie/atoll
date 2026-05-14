package framework

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/wanpengxie/ActOS/kernel/adapter"
)

// Deps is the dependency bundle a Factory receives. It is a strict
// subset of ManagerConfig — only the seams an adapter Module
// constructor actually needs. The daemon composition root builds Deps
// from its ManagerConfig once and reuses the same bundle for every
// factory call so adapters share infrastructure (HTTPClient pools,
// credential store, etc.).
//
// All fields are optional for tests; adapters MUST tolerate nils for
// the fields they don't use.
type Deps struct {
	HTTPClient      *HTTPClient
	CredentialStore CredentialStore
	StateStore      StateStore
	Logger          Logger
	Metrics         Metrics
	Tracer          Tracer
	Clock           func() time.Time
}

// DepsFromManagerConfig extracts the Deps subset from a fully-configured
// ManagerConfig. The daemon calls this once after applyDefaults.
func DepsFromManagerConfig(cfg ManagerConfig) Deps {
	cfg.applyDefaults()
	return Deps{
		HTTPClient:      cfg.HTTPClient,
		CredentialStore: cfg.CredentialStore,
		StateStore:      cfg.StateStore,
		Logger:          cfg.Logger,
		Metrics:         cfg.Metrics,
		Tracer:          cfg.Tracer,
		Clock:           cfg.Clock,
	}
}

// Factory is the constructor adapters publish into the package-level
// registry. It receives a Deps bundle so adapters can wire HTTPClient /
// CredentialStore / Logger without reaching into globals.
type Factory func(deps Deps) adapter.Module

var (
	registryMu      sync.RWMutex
	registryFactory = map[string]Factory{}
)

// Register publishes a Factory under name. Duplicate names panic — a
// programmer error that should fail loud at init time.
func Register(name string, f Factory) {
	if name == "" {
		panic("framework.Register: name required")
	}
	if f == nil {
		panic("framework.Register: factory nil")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, dup := registryFactory[name]; dup {
		panic(fmt.Sprintf("framework.Register: duplicate name %q", name))
	}
	registryFactory[name] = f
}

// RegisteredFactories returns a copy of the current registry map.
func RegisteredFactories() map[string]Factory {
	registryMu.RLock()
	defer registryMu.RUnlock()
	out := make(map[string]Factory, len(registryFactory))
	for k, v := range registryFactory {
		out[k] = v
	}
	return out
}

// RegisteredNames returns the sorted list of registered adapter names.
func RegisteredNames() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	out := make([]string, 0, len(registryFactory))
	for k := range registryFactory {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ResetRegistry clears the package-level registry. Tests use this to
// avoid cross-test contamination.
func ResetRegistry() {
	registryMu.Lock()
	defer registryMu.Unlock()
	registryFactory = map[string]Factory{}
}

// BuildRegistered returns Module instances for the requested names.
// Returns an error wrapping the first missing name.
func BuildRegistered(deps Deps, names ...string) ([]adapter.Module, error) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	out := make([]adapter.Module, 0, len(names))
	for _, n := range names {
		f, ok := registryFactory[n]
		if !ok {
			return nil, fmt.Errorf("framework.BuildRegistered: unknown adapter %q", n)
		}
		out = append(out, f(deps))
	}
	return out, nil
}

// ErrNoRegisteredModules is returned by BuildAllRegistered when the
// registry is empty.
var ErrNoRegisteredModules = errors.New("framework: no adapters registered")

// BuildAllRegistered returns Module instances for every registered
// factory, sorted by name for deterministic order.
func BuildAllRegistered(deps Deps) ([]adapter.Module, error) {
	names := RegisteredNames()
	if len(names) == 0 {
		return nil, ErrNoRegisteredModules
	}
	return BuildRegistered(deps, names...)
}
