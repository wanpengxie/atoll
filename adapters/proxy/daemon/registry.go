package daemon

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/wanpengxie/ActOS/adapters/proxy/actorapi"
	"github.com/wanpengxie/ActOS/kernel/actor"
)

type ModuleFactory func() (actorapi.ActorModule, error)

type Registry struct {
	mu        sync.RWMutex
	factories map[actor.ActorID]ModuleFactory
	modules   map[actor.ActorID]actorapi.ActorModule
}

type InitResult struct {
	ActorID actor.ActorID
	Module  actorapi.ActorModule
	Err     error
}

func NewRegistry() *Registry {
	return &Registry{
		factories: map[actor.ActorID]ModuleFactory{},
		modules:   map[actor.ActorID]actorapi.ActorModule{},
	}
}

func (r *Registry) Register(id actor.ActorID, factory ModuleFactory) error {
	if id == "" {
		return errors.New("proxy registry: actor id required")
	}
	if factory == nil {
		return fmt.Errorf("proxy registry: factory required for %s", id)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.factories[id]; ok {
		return fmt.Errorf("proxy registry: factory already registered for %s", id)
	}
	r.factories[id] = factory
	return nil
}

func (r *Registry) InitEnabled(ctx context.Context, cfg Config) ([]InitResult, error) {
	cfg = cfg.Normalize()
	results := make([]InitResult, 0, len(cfg.EnabledActors))
	for _, id := range cfg.EnabledActors {
		r.mu.RLock()
		factory := r.factories[id]
		r.mu.RUnlock()
		if factory == nil {
			results = append(results, InitResult{ActorID: id, Err: fmt.Errorf("proxy registry: no factory for %s", id)})
			continue
		}
		mod, err := factory()
		if err == nil && mod.ActorID() != id {
			err = fmt.Errorf("proxy registry: factory for %s returned module %s", id, mod.ActorID())
		}
		if err == nil {
			err = mod.Init(ctx, cfg.ModuleConfig(id))
		}
		res := InitResult{ActorID: id, Module: mod, Err: err}
		results = append(results, res)
		if err != nil {
			continue
		}
		r.mu.Lock()
		r.modules[id] = mod
		r.mu.Unlock()
	}
	if len(r.Modules()) == 0 {
		return results, errors.New("proxy registry: no enabled modules initialized")
	}
	return results, nil
}

func (r *Registry) Get(id actor.ActorID) (actorapi.ActorModule, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	mod, ok := r.modules[id]
	return mod, ok
}

func (r *Registry) Modules() []actorapi.ActorModule {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.modules))
	for id := range r.modules {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	out := make([]actorapi.ActorModule, 0, len(ids))
	for _, id := range ids {
		out = append(out, r.modules[actor.ActorID(id)])
	}
	return out
}

func (r *Registry) Shutdown(ctx context.Context) error {
	var errs []error
	for _, mod := range r.Modules() {
		if err := mod.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", mod.ActorID(), err))
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}
