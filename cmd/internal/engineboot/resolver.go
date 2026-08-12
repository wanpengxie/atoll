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
	"github.com/wanpengxie/atoll/platform/spacetool"
	"github.com/wanpengxie/atoll/protocol"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	classregistry "github.com/wanpengxie/atoll/registry"
)

type assemblyResolver struct {
	registry  *lagoon.Registry
	registrar *lagoon.Registrar
	caller    lagoon.C0Caller
	logger    *slog.Logger
}

func (r *assemblyResolver) BuildClass(ch channel.ID, id actor.ActorID, class string, config json.RawMessage) (platform.ActorFactory, bool) {
	switch class {
	case lagoon.RegistrarClass:
		if ch != protocol.C0ChannelID {
			return platform.ActorFactory{}, false
		}
		return platform.ActorFactory{Proc: lagoon.Def(r.registrar)}, true
	case lagoon.SpaceToolClass:
		return platform.ActorFactory{Proc: spacetool.Def(r.caller)}, true
	}
	decl, err := classregistry.Build(class, classregistry.InstanceSpec{ID: id, Config: config}, classregistry.Deps{ChannelID: ch, Logger: r.logger})
	if err != nil {
		return platform.ActorFactory{}, false
	}
	return decl.Factory, true
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
	return channelspec.DeclarationFacts{OwnerPrincipal: decl.Owner, Visibility: decl.Visibility, Class: decl.DefaultClass, Config: config}, nil
}
func (r *assemblyResolver) ClassKind(_ context.Context, class string) (actor.Kind, bool, error) {
	switch class {
	case lagoon.SpaceToolClass, lagoon.RegistrarClass:
		return actor.KindTool, true, nil
	}
	kind, ok := classregistry.ClassKind(class)
	return kind, ok, nil
}

func (r *assemblyResolver) ValidateConfig(class string, config json.RawMessage) error {
	if class == lagoon.SpaceToolClass || class == lagoon.RegistrarClass {
		return nil
	}
	return classregistry.ValidateConfig(class, config)
}

func (r *assemblyResolver) LookupClassKind(class string) (actor.Kind, bool) {
	if class == lagoon.SpaceToolClass || class == lagoon.RegistrarClass {
		return actor.KindTool, true
	}
	return classregistry.ClassKind(class)
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

func (f sourceFacts) SystemGenesis(context.Context) (lagoon.GenesisSpec, bool, error) {
	return f.genesis, f.genesis.ChannelID != "", nil
}
