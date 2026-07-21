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

func openRoutingHome(t *testing.T, name string) *Home {
	t.Helper()
	h, err := Open(Config{
		ChannelID: channel.ID(name), DBPath: filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver: routingResolver{}, ReconcileInterval: 10 * time.Millisecond, Bootstrap: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	return h
}

func routingAgent(t *testing.T, h *Home, source, principal, class string, makeDefault bool) storespec.ActorControlRow {
	t.Helper()
	result, err := h.declare(context.Background(), DeclareRequest{
		SourceDeclID: source, Kind: actor.KindAgent, Class: class,
		Placement: storespec.NewServerPlacement(), CreatedAt: time.Now().UnixMilli(), MakeDefault: makeDefault,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result.Row
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

func writeUnaddressed(t *testing.T, h *Home, source storespec.ActorControlRow, id string) (*message.Envelope, harness.WriteResult, error) {
	t.Helper()
	env := &message.Envelope{ID: message.ID(id), TS: time.Now().UnixMilli(), Kind: message.KindRequest, Type: "routing.probe", Visibility: message.VisibilityPublic}
	result, err := h.minter.Mint(source.ID, source.Kind, h.channelID, source.CurrentDeclVersion).Write(context.Background(), env)
	return env, result, err
}

func TestRoutingResolverCoversAllMembraneCases(t *testing.T) {
	t.Run("live default", func(t *testing.T) {
		h := openRoutingHome(t, "routing-default")
		source := routingAgent(t, h, "source", "source", "missing", false)
		target := routingAgent(t, h, "default", "default", "routing-live", true)
		waitRoutingLive(t, h, target.ID)
		env, result, err := writeUnaddressed(t, h, source, "route-default")
		if err != nil || !result.Accepted() || env.Kind != message.KindRequest || len(env.Audience) != 1 || env.Audience[0] != target.ID {
			t.Fatalf("default route env=%+v result=%+v err=%v", env, result, err)
		}
	})

	t.Run("boost fallback", func(t *testing.T) {
		h := openRoutingHome(t, "routing-boost")
		source := routingAgent(t, h, "source", "source", "missing", false)
		boost := routingAgent(t, h, defaultRoutingAgentSource, "boost", "routing-live", false)
		waitRoutingLive(t, h, boost.ID)
		env, result, err := writeUnaddressed(t, h, source, "route-boost")
		if err != nil || !result.Accepted() || env.Kind != message.KindRequest || len(env.Audience) != 1 || env.Audience[0] != boost.ID {
			t.Fatalf("boost route env=%+v result=%+v err=%v", env, result, err)
		}
	})

	t.Run("human broadcast", func(t *testing.T) {
		h := openRoutingHome(t, "routing-broadcast")
		source := routingAgent(t, h, "source", "source", "missing", false)
		alice, err := h.admit(context.Background(), actor.KindHuman, "alice")
		if err != nil {
			t.Fatal(err)
		}
		bob, err := h.admit(context.Background(), actor.KindHuman, "bob")
		if err != nil {
			t.Fatal(err)
		}
		env, result, err := writeUnaddressed(t, h, source, "route-broadcast")
		if err != nil || !result.Accepted() || env.Kind != message.KindEvent || len(env.Audience) != 2 {
			t.Fatalf("broadcast env=%+v result=%+v err=%v", env, result, err)
		}
		seen := map[actor.ActorID]bool{}
		for _, id := range env.Audience {
			seen[id] = true
		}
		if !seen[alice] || !seen[bob] {
			t.Fatalf("broadcast audience=%v, want %s and %s", env.Audience, alice, bob)
		}
	})

	t.Run("configured default unavailable does not append", func(t *testing.T) {
		h := openRoutingHome(t, "routing-unavailable")
		source := routingAgent(t, h, "source", "source", "missing", false)
		_ = routingAgent(t, h, "default", "default", "missing", true)
		// Even a live boost must not override an explicitly configured default.
		boost := routingAgent(t, h, defaultRoutingAgentSource, "boost", "routing-live", false)
		waitRoutingLive(t, h, boost.ID)
		before, err := h.View().MaxSeq(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		_, result, writeErr := writeUnaddressed(t, h, source, "route-unavailable")
		if !errors.Is(writeErr, ErrRoutingUnavailable) || result.MessageID != "" || result.Seq != 0 {
			t.Fatalf("unavailable result=%+v err=%v", result, writeErr)
		}
		after, err := h.View().MaxSeq(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if after != before {
			t.Fatalf("unavailable request appended: before=%d after=%d", before, after)
		}
	})
}
