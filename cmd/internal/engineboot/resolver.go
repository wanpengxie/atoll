package engineboot

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/platform/peeractor"
	"github.com/wanpengxie/atoll/platform/peerproto"
	"github.com/wanpengxie/atoll/platform/svcactor"
	"github.com/wanpengxie/atoll/protocol"
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
	case lagoon.RegistrarClass:
		if ch != protocol.C0ChannelID {
			return platform.ActorFactory{}, false
		}
		return platform.ActorFactory{Proc: lagoon.Def(r.registrar)}, true
	case lagoon.PeerActorClass:
		target, err := peeractor.ValidateConfig(config)
		if err != nil {
			return platform.ActorFactory{}, false
		}
		return platform.ActorFactory{Proc: peeractor.Def(peeractor.Deps{Caller: ch, Target: target, Seam: r.callPeer, Card: r.card})}, true
	}
	decl, err := classregistry.Build(class, classregistry.InstanceSpec{ID: id, Config: config}, classregistry.Deps{ChannelID: ch, Logger: r.logger})
	if err != nil {
		return platform.ActorFactory{}, false
	}
	return decl.Factory, true
}

func (r *assemblyResolver) BuildServiceClass(ch channel.ID, _ actor.ActorID, port *svcactor.Port, audit svcactor.Audit) (platform.ActorFactory, bool) {
	if port == nil {
		return platform.ActorFactory{}, false
	}
	deps := svcactor.Deps{Port: port, Self: ch, Core: protocol.C0ChannelID, RegistrarClass: lagoon.RegistrarClass,
		Audit: audit, Logger: r.logger,
		Endpoints: func(ctx context.Context) ([]svcactor.Endpoint, error) {
			rows, err := r.registry.ListEndpoints(ctx, ch)
			out := make([]svcactor.Endpoint, len(rows))
			for i, row := range rows {
				out[i] = svcactor.Endpoint{Name: row.Name, Receiver: row.Receiver}
			}
			return out, err
		},
		Instances: func(ctx context.Context, decl string) ([]actor.ActorID, error) {
			bundle, ok := r.host.Acquire(ch)
			if !ok {
				return nil, errors.New("channel unavailable")
			}
			return bundle.View().DeclaredInstances(ctx, decl)
		},
		Parent: func(ctx context.Context) (channel.ID, error) {
			row, ok, err := r.registry.GetChannelDesired(ctx, ch)
			if err != nil {
				return "", err
			}
			if !ok {
				return "", errors.New("channel not found")
			}
			return row.ParentID, nil
		},
		ReceiverClass: func(ctx context.Context, decl string) (string, error) {
			row, ok, err := r.registry.GetDecl(ctx, decl)
			if err != nil {
				return "", err
			}
			if !ok {
				return "", errors.New("declaration not found")
			}
			return row.DefaultClass, nil
		},
		Card: func(ctx context.Context, caller channel.ID) (introspect.Describe, error) {
			return r.registry.Describe(ctx, ch, caller)
		},
	}
	return platform.ActorFactory{Proc: svcactor.Def(deps)}, true
}

func (r *assemblyResolver) callPeer(ctx context.Context, caller, target channel.ID, req peerproto.Request) (peerproto.Result, error) {
	port, _, ok := r.host.AcquirePort(target)
	if !ok {
		return peerproto.Result{}, errors.New("target channel unavailable")
	}
	return port.Call(ctx, caller, req)
}
func (r *assemblyResolver) card(ctx context.Context, target, caller channel.ID) (introspect.Describe, error) {
	return r.registry.Describe(ctx, target, caller)
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
		Visibility: decl.Visibility, Class: decl.DefaultClass, Config: config,
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
			Visibility: decl.Visibility, Class: decl.DefaultClass,
		}
	}
	return out, nil
}
func (r *assemblyResolver) ClassKind(_ context.Context, class string) (actor.Kind, bool, error) {
	switch class {
	case lagoon.PeerActorClass, lagoon.SvcActorClass, lagoon.RegistrarClass:
		return actor.KindTool, true, nil
	}
	kind, ok := classregistry.ClassKind(class)
	return kind, ok, nil
}

func (r *assemblyResolver) ClassPlacement(_ context.Context, class string) (channel.PlacementKind, bool, error) {
	switch class {
	case lagoon.PeerActorClass, lagoon.SvcActorClass, lagoon.RegistrarClass, "human":
		return channel.PlacementServer, true, nil
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
	if holder == protocol.C0ChannelID || holder == row.ParentID {
		return nil
	}
	if row.Serving == 0 {
		return channelspec.ErrTargetNotServing
	}
	return nil
}

func (r *assemblyResolver) ValidateConfig(class string, config json.RawMessage) error {
	if class == lagoon.PeerActorClass || class == lagoon.SvcActorClass || class == lagoon.RegistrarClass {
		if class == lagoon.PeerActorClass {
			_, err := peeractor.ValidateConfig(config)
			return err
		}
		return nil
	}
	return classregistry.ValidateConfig(class, config)
}

func (r *assemblyResolver) LookupClassKind(class string) (actor.Kind, bool) {
	if class == lagoon.PeerActorClass || class == lagoon.SvcActorClass || class == lagoon.RegistrarClass {
		return actor.KindTool, true
	}
	return classregistry.ClassKind(class)
}

func (r *assemblyResolver) LookupClassPlacement(class string) (channel.PlacementKind, bool) {
	switch class {
	case lagoon.PeerActorClass, lagoon.SvcActorClass, lagoon.RegistrarClass, "human":
		return channel.PlacementServer, true
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

func (f sourceFacts) DeclaredInstances(ctx context.Context, ch channel.ID, decl string) ([]actor.ActorID, error) {
	bundle, ok := f.host.Acquire(ch)
	if !ok {
		return nil, errors.New("source channel unavailable")
	}
	return bundle.View().DeclaredInstances(ctx, decl)
}

func (f sourceFacts) WaitChannelService(ctx context.Context, ch channel.ID) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, _, ok := f.host.AcquirePort(ch); ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (f sourceFacts) SystemGenesis(context.Context) (lagoon.GenesisSpec, bool, error) {
	return f.genesis, f.genesis.ChannelID != "", nil
}
