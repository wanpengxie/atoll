package home

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type routingResolver struct{}

func (routingResolver) BuildClass(_ channel.ID, _ actor.ActorID, class string, _ json.RawMessage) (platform.ActorFactory, bool) {
	if class != "routing-live" {
		return platform.ActorFactory{}, false
	}
	return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
		return func(sys actorbase.Sys) error {
			<-sys.Life().Done()
			return nil
		}, nil
	}}}, true
}

func openRoutingHome(t *testing.T, name string, declarations ...DeclareRequest) *Home {
	t.Helper()
	h, err := Open(Config{
		ChannelID: channel.ID(name), DBPath: filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver: routingResolver{}, ReconcileInterval: 10 * time.Millisecond, Bootstrap: true,
		IntroductionResolver:  inertIntroductionResolver{},
		BootstrapDeclarations: declarations,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	return h
}

func routingAgent(t *testing.T, h *Home, source, principal, class string, makeDefault bool) actor.ActorID {
	t.Helper()
	ids, err := h.View().DeclaredInstances(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Fatalf("bootstrap declaration %q missing: instances=%v", source, ids)
	}
	return ids[0]
}

func routingDeclaration(source, class string, makeDefault bool) DeclareRequest {
	return DeclareRequest{
		SourceDeclID: source, Kind: actor.KindAgent, Class: class,
		Placement: storespec.NewServerPlacement(), CreatedAt: time.Now().UnixMilli(), MakeDefault: makeDefault,
	}
}

func visibleWatermark(t *testing.T, h *Home) int64 {
	t.Helper()
	_, watermark, err := h.View().ReadVisibleAfterSeq(context.Background(), channel.Reader{
		Principal: "observer", Mode: channel.ReaderObserver,
	}, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	return watermark
}

func waitRoutingLive(t *testing.T, h *Home, id actor.ActorID) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, live := h.View().Stat(id); live {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("actor %s did not become live", id)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func writeUnaddressed(t *testing.T, h *Home, source actor.ActorID, id string) (*message.Envelope, harness.WriteResult, error) {
	t.Helper()
	env := &message.Envelope{ID: message.ID(id), TS: time.Now().UnixMilli(), Kind: message.KindRequest, Type: "routing.probe", Visibility: message.VisibilityPublic}
	result, err := h.minter.MintAdmitted(
		storespec.IdentityAdmission{ID: source, Kind: actor.KindAgent},
	).Write(context.Background(), env)
	return env, result, err
}

func TestRoutingResolverCoversAllMembraneCases(t *testing.T) {
	t.Run("live default", func(t *testing.T) {
		h := openRoutingHome(t, "routing-default",
			routingDeclaration("source", "missing", false),
			routingDeclaration("default", "routing-live", true),
		)
		source := routingAgent(t, h, "source", "source", "missing", false)
		target := routingAgent(t, h, "default", "default", "routing-live", true)
		waitRoutingLive(t, h, target)
		env, result, err := writeUnaddressed(t, h, source, "route-default")
		if err != nil || !result.Accepted() || env.Kind != message.KindRequest || len(env.Audience) != 1 || env.Audience[0] != target {
			t.Fatalf("default route env=%+v result=%+v err=%v", env, result, err)
		}
	})

	t.Run("boost fallback", func(t *testing.T) {
		h := openRoutingHome(t, "routing-boost",
			routingDeclaration("source", "missing", false),
			routingDeclaration(defaultRoutingAgentSource, "routing-live", false),
		)
		source := routingAgent(t, h, "source", "source", "missing", false)
		boost := routingAgent(t, h, defaultRoutingAgentSource, "boost", "routing-live", false)
		waitRoutingLive(t, h, boost)
		env, result, err := writeUnaddressed(t, h, source, "route-boost")
		if err != nil || !result.Accepted() || env.Kind != message.KindRequest || len(env.Audience) != 1 || env.Audience[0] != boost {
			t.Fatalf("boost route env=%+v result=%+v err=%v", env, result, err)
		}
	})

	// boost carries no protection gate, so it can simply be absent. With the
	// fallback terminus gone routing fails FAST with one fixed error — it never
	// silently swallows the message and never invents another destination.
	t.Run("boost missing refuses with the fixed error", func(t *testing.T) {
		h := openRoutingHome(t, "routing-no-boost",
			routingDeclaration("source", "missing", false),
		)
		source := routingAgent(t, h, "source", "source", "missing", false)
		if _, err := admitThroughSysOp(h, context.Background(), actor.KindHuman, "alice"); err != nil {
			t.Fatal(err)
		}
		before := visibleWatermark(t, h)
		env, result, err := writeUnaddressed(t, h, source, "route-no-boost")
		if !errors.Is(err, ErrBoostMissing) {
			t.Fatalf("boost-missing err=%v, want ErrBoostMissing", err)
		}
		if result.MessageID != "" || result.Seq != 0 || len(env.Audience) != 0 {
			t.Fatalf("boost-missing swallowed the message: env=%+v result=%+v", env, result)
		}
		if after := visibleWatermark(t, h); after != before {
			t.Fatalf("boost-missing appended: before=%d after=%d", before, after)
		}
	})

	// Terminal never clears the default pointer — it is channel configuration,
	// not a dead actor's belonging. A pointer at a deregistered actor reads as
	// UNCONFIGURED, so routing falls back to boost with no cleanup action of any
	// kind having run.
	t.Run("dangling default pointer falls back to boost", func(t *testing.T) {
		h := openRoutingHome(t, "routing-dangling",
			routingDeclaration("source", "missing", false),
			routingDeclaration("default", "routing-live", true),
			routingDeclaration(defaultRoutingAgentSource, "routing-live", false),
		)
		source := routingAgent(t, h, "source", "source", "missing", false)
		target := routingAgent(t, h, "default", "default", "routing-live", true)
		boost := routingAgent(t, h, defaultRoutingAgentSource, "boost", "routing-live", false)
		waitRoutingLive(t, h, target)
		waitRoutingLive(t, h, boost)
		if err := removeThroughSysOp(h, context.Background(), target); err != nil {
			t.Fatal(err)
		}
		// The pointer row still names the dead actor; nothing cleaned it up.
		pointed, hasPointer, err := h.View().DefaultAgent(context.Background())
		if err != nil || !hasPointer || pointed != target {
			t.Fatalf("default pointer=%q has=%v err=%v, want the dangling %q",
				pointed, hasPointer, err, target)
		}
		env, result, err := writeUnaddressed(t, h, source, "route-dangling")
		if err != nil || !result.Accepted() || env.Kind != message.KindRequest ||
			len(env.Audience) != 1 || env.Audience[0] != boost {
			t.Fatalf("dangling default env=%+v result=%+v err=%v", env, result, err)
		}
	})

	t.Run("configured default unavailable does not append", func(t *testing.T) {
		h := openRoutingHome(t, "routing-unavailable",
			routingDeclaration("source", "missing", false),
			routingDeclaration("default", "missing", true),
			routingDeclaration(defaultRoutingAgentSource, "routing-live", false),
		)
		source := routingAgent(t, h, "source", "source", "missing", false)
		_ = routingAgent(t, h, "default", "default", "missing", true)
		// Even a live boost must not override an explicitly configured default.
		boost := routingAgent(t, h, defaultRoutingAgentSource, "boost", "routing-live", false)
		waitRoutingLive(t, h, boost)
		before := visibleWatermark(t, h)
		_, result, writeErr := writeUnaddressed(t, h, source, "route-unavailable")
		if !errors.Is(writeErr, ErrRoutingUnavailable) || result.MessageID != "" || result.Seq != 0 {
			t.Fatalf("unavailable result=%+v err=%v", result, writeErr)
		}
		after := visibleWatermark(t, h)
		if after != before {
			t.Fatalf("unavailable request appended: before=%d after=%d", before, after)
		}
	})
}
