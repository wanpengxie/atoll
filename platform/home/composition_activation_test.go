package home

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type compositionActivationResolver struct {
	builds atomic.Int32
	fail   atomic.Bool
}

func (r *compositionActivationResolver) factory() platform.ActorFactory {
	return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
		r.builds.Add(1)
		return func(sys actorbase.Sys) error {
			<-sys.Life().Done()
			return nil
		}, nil
	}}}
}

func (r *compositionActivationResolver) ResolveComposition(_ context.Context, _ channel.ID, row storespec.CompositionRecord) (platform.ActorDecl, bool, error) {
	if r.fail.Load() {
		return platform.ActorDecl{}, false, errors.New("resolver unavailable")
	}
	if row.DeclID != "decl:probe" {
		return platform.ActorDecl{}, false, nil
	}
	return platform.ActorDecl{ID: row.InstanceID, Kind: actor.KindAgent, Factory: r.factory()}, true, nil
}

func (r *compositionActivationResolver) BuildClass(_ channel.ID, _ actor.ActorID, class string, _ json.RawMessage) (platform.ActorFactory, bool) {
	if class != "probe" {
		return platform.ActorFactory{}, false
	}
	return r.factory(), true
}

func TestCompositionActivationUsesCurrentResolverSnapshot(t *testing.T) {
	resolver := &compositionActivationResolver{}
	h, err := Open(Config{
		ChannelID:           "composition-activation",
		DBPath:              filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver: resolver,
		DaemonAuthority:     allowTestDaemonAuthority{},
		ReconcileInterval:   10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })

	record, _, _, err := h.IntroduceComposition(context.Background(), storespec.CompositionIntroduce{
		DeclID: "decl:probe", Principal: "probe", Class: "probe",
		Placement: storespec.PlacementServer, Kind: actor.KindAgent, At: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitHomeCondition(t, func() bool { return resolver.builds.Load() == 1 })
	first, ok := h.channel.Cells().CurrentIncarnation(record.InstanceID)
	if !ok {
		t.Fatal("composition member was not embodied")
	}

	// A resolver read failure keeps the last-known-good body; there is no
	// alternate desired source to cull or rebuild it from.
	resolver.fail.Store(true)
	h.pokeReconcile()
	time.Sleep(40 * time.Millisecond)
	if current, ok := h.channel.Cells().CurrentIncarnation(record.InstanceID); !ok || current != first {
		t.Fatal("resolver failure replaced or removed the last-known-good body")
	}

	resolver.fail.Store(false)
	if _, err := h.RestartInstanceDirect(context.Background(), record.InstanceID); err != nil {
		t.Fatal(err)
	}
	waitHomeCondition(t, func() bool { return resolver.builds.Load() == 2 })
	second, ok := h.channel.Cells().CurrentIncarnation(record.InstanceID)
	if !ok || second == first {
		t.Fatal("epoch restart did not replace the composition incarnation")
	}

	if err := h.RemoveInstance(context.Background(), record.InstanceID); err != nil {
		t.Fatal(err)
	}
	waitHomeCondition(t, func() bool {
		_, live := h.channel.Cells().CurrentIncarnation(record.InstanceID)
		return !live
	})
}

func waitHomeCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not converge")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
