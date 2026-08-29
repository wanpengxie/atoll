package engineboot

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/platform/peeractor"
	"github.com/wanpengxie/atoll/platform/svcactor"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	classregistry "github.com/wanpengxie/atoll/registry"
)

type assemblyResolver struct {
	registry  *lagoon.Registry
	registrar *lagoon.Registrar
	host      *channelhost.ChannelHost
	logger    *slog.Logger
}

func (r *assemblyResolver) BuildClass(ch channel.ID, id actor.ActorID, class string, config json.RawMessage) (platform.ActorFactory, bool) {
	switch class {
	case lagoon.ClassRegistrar:
		if ch != channelspec.C0ChannelID {
			return platform.ActorFactory{}, false
		}
		return platform.ActorFactory{Proc: lagoon.Def(r.registrar)}, true
	case lagoon.PeerActorClass:
		target, err := peeractor.ValidateConfig(config)
		if err != nil {
			return platform.ActorFactory{}, false
		}
		return platform.ActorFactory{Proc: peeractor.Def(peeractor.Deps{Caller: ch, Target: target, Seam: r.callPeer, Describe: r.describePeer})}, true
	}
	decl, err := classregistry.Build(class, classregistry.InstanceSpec{ID: id, Config: config}, classregistry.Deps{ChannelID: ch, Logger: r.logger})
	if err != nil {
		return platform.ActorFactory{}, false
	}
	return decl.Factory, true
}

func (r *assemblyResolver) BuildServiceClass(ch channel.ID, _ actor.ActorID, port *svcactor.Port, audit svcactor.Audit, members svcactor.Members) (platform.ActorFactory, bool) {
	if port == nil {
		return platform.ActorFactory{}, false
	}
	deps := svcactor.Deps{Port: port, Self: ch, Core: channelspec.C0ChannelID, Members: members, Audit: audit, Logger: r.logger}
	return platform.ActorFactory{Proc: svcactor.Def(deps)}, true
}

func (r *assemblyResolver) callPeer(ctx context.Context, caller, target channel.ID, req channel.Request, onProgress func(channel.Progress)) (channel.Result, error) {
	port, _, ok := r.host.AcquirePort(target)
	if !ok {
		return channel.Result{}, errors.New("target channel unavailable")
	}
	return port.Call(ctx, caller, req, onProgress)
}

func (r *assemblyResolver) Peer(ctx context.Context, caller, target channel.ID, req channel.Request, onProgress func(channel.Progress)) (channel.Result, error) {
	return r.callPeer(ctx, caller, target, req, onProgress)
}
func (r *assemblyResolver) describePeer(ctx context.Context, caller, target channel.ID, frame channel.Describe) (channel.Card, error) {
	port, _, ok := r.host.AcquirePort(target)
	if !ok {
		return channel.Card{}, errors.New("target channel unavailable")
	}
	return port.Describe(ctx, caller, frame)
}
func (r *assemblyResolver) ResolveDeclaration(ctx context.Context, ch channel.ID, id string) (channelspec.DeclarationFacts, error) {
	decl, ok, err := r.registry.GetDecl(ctx, id)
	if err != nil {
		return channelspec.DeclarationFacts{}, err
	}
	if !ok || decl.Status != regspec.DeclPresent {
		return channelspec.DeclarationFacts{}, channelspec.ErrDeclarationNotFound
	}
	config := append(json.RawMessage(nil), decl.Config...)
	overlays, err := r.registry.GetOverlays(ctx, ch)
	if err != nil {
		return channelspec.DeclarationFacts{}, err
	}
	for _, overlay := range overlays {
		if overlay.DeclID == id {
			config = append(json.RawMessage(nil), overlay.Config...)
			break
		}
	}
	return channelspec.DeclarationFacts{
		OwnerPrincipal: decl.Owner, Name: decl.Name, Description: decl.Description,
		Visibility: decl.Visibility, Class: decl.DefaultClass, Config: config, Singleton: decl.Singleton,
	}, nil
}

func (r *assemblyResolver) ResolveDeclarationCatalog(ctx context.Context, _ channel.ID, ids []string) (map[string]channelspec.DeclarationFacts, error) {
	wanted := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		wanted[id] = struct{}{}
	}
	rows, err := r.registry.ListDecls(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]channelspec.DeclarationFacts, len(wanted))
	for _, decl := range rows {
		if _, ok := wanted[decl.ID]; !ok {
			continue
		}
		out[decl.ID] = channelspec.DeclarationFacts{
			OwnerPrincipal: decl.Owner, Name: decl.Name, Description: decl.Description,
			Visibility: decl.Visibility, Class: decl.DefaultClass, Singleton: decl.Singleton,
		}
	}
	return out, nil
}

func (r *assemblyResolver) PrincipalKind(ctx context.Context, id string) (actor.Kind, bool, error) {
	principal, found, err := r.registry.GetPrincipal(ctx, id)
	if err != nil || !found || principal.Status != regspec.PrincipalPresent {
		return "", false, err
	}
	return principal.Kind, true, nil
}
func (r *assemblyResolver) ClassKind(_ context.Context, class string) (actor.Kind, bool, error) {
	switch class {
	case lagoon.PeerActorClass, lagoon.SvcActorClass:
		return actor.KindPeer, true, nil
	case lagoon.ClassRegistrar:
		return actor.KindSystem, true, nil
	}
	kind, ok := classregistry.ClassKind(class)
	return kind, ok, nil
}

func (r *assemblyResolver) ClassPlacement(_ context.Context, class string) (channelspec.PlacementKind, bool, error) {
	switch class {
	case lagoon.PeerActorClass, lagoon.SvcActorClass, lagoon.ClassRegistrar, "human":
		return channelspec.PlacementServer, true, nil
	}
	p, ok := classregistry.ClassPlacement(class)
	return p, ok, nil
}

func (r *assemblyResolver) AdmitIntroduction(ctx context.Context, holder channel.ID, facts channelspec.DeclarationFacts) error {
	if facts.Class != lagoon.PeerActorClass {
		return nil
	}
	target, err := peeractor.ValidateConfig(facts.Config)
	if err != nil {
		return channelspec.ErrDeclarationNotFound
	}
	row, ok, err := r.registry.GetChannelDesired(ctx, target)
	if err != nil {
		return err
	}
	if !ok || row.Status != regspec.ChannelPresent {
		return channelspec.ErrDeclarationNotFound
	}
	if holder == channelspec.C0ChannelID || holder == row.ParentID {
		return nil
	}
	if row.Serving == 0 {
		return channelspec.ErrTargetNotServing
	}
	return nil
}

func (r *assemblyResolver) ValidateConfig(class string, config json.RawMessage) error {
	if class == lagoon.PeerActorClass || class == lagoon.SvcActorClass || class == lagoon.ClassRegistrar {
		if class == lagoon.PeerActorClass {
			_, err := peeractor.ValidateConfig(config)
			return err
		}
		return nil
	}
	return classregistry.ValidateConfig(class, config)
}

func (r *assemblyResolver) ResolveConfig(class string, config json.RawMessage) (json.RawMessage, error) {
	if class == lagoon.PeerActorClass || class == lagoon.SvcActorClass || class == lagoon.ClassRegistrar {
		if err := r.ValidateConfig(class, config); err != nil {
			return nil, err
		}
		return append(json.RawMessage(nil), config...), nil
	}
	return classregistry.ResolveConfig(class, config)
}

func (r *assemblyResolver) LookupClassKind(class string) (actor.Kind, bool) {
	if class == lagoon.PeerActorClass || class == lagoon.SvcActorClass {
		return actor.KindPeer, true
	}
	if class == lagoon.ClassRegistrar {
		return actor.KindSystem, true
	}
	return classregistry.ClassKind(class)
}

// Classes and ClassConfigSchema answer what a declaration author needs before
// writing one. The platform-internal classes above are minted by genesis and
// never named in a template, so they are absent here for the same reason they
// are special-cased above: listing them would offer a choice that is not one.
func (r *assemblyResolver) Classes() []string { return classregistry.RegisteredClasses() }

func (r *assemblyResolver) ClassConfigSchema(class string) (json.RawMessage, bool) {
	return classregistry.ClassConfigSchema(class)
}

func (r *assemblyResolver) ClassDefaultConfig(class string) (json.RawMessage, bool) {
	return classregistry.ClassDefaultConfig(class)
}

func (r *assemblyResolver) LookupClassPlacement(class string) (channelspec.PlacementKind, bool) {
	switch class {
	case lagoon.PeerActorClass, lagoon.SvcActorClass, lagoon.ClassRegistrar, "human":
		return channelspec.PlacementServer, true
	}
	return classregistry.ClassPlacement(class)
}

type sourceFacts struct {
	host    channelhost.LocalHost
	genesis lagoon.GenesisSpec
}

func (f sourceFacts) ActorFacts(ctx context.Context, ch channel.ID, id actor.ActorID) (channelspec.ActorFacts, bool, error) {
	bundle, ok := f.host.Acquire(ch)
	if !ok {
		return channelspec.ActorFacts{}, false, errors.New("source channel unavailable")
	}
	return bundle.View().ActorFacts(ctx, id)
}

func (f sourceFacts) ActorsPlacedOn(ctx context.Context, ch channel.ID, deviceID string) ([]actor.ActorID, error) {
	bundle, ok := f.host.Acquire(ch)
	if !ok {
		return nil, errors.New("source channel unavailable")
	}
	reader, ok := bundle.View().(interface {
		ActorsPlacedOn(context.Context, string) ([]actor.ActorID, error)
	})
	if !ok {
		return nil, errors.New("source channel placement view unavailable")
	}
	return reader.ActorsPlacedOn(ctx, deviceID)
}

func (f sourceFacts) SystemGenesis(context.Context) (lagoon.GenesisSpec, bool, error) {
	return f.genesis, f.genesis.ChannelID != "", nil
}
