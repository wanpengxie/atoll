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
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type compositionActivationResolver struct {
	builds atomic.Int32
	fail   atomic.Bool
}

type crashBackoffResolver struct{ births atomic.Int32 }

func (r *crashBackoffResolver) BuildClass(_ channel.ID, _ actor.ActorID, class string, _ json.RawMessage) (platform.ActorFactory, bool) {
	if class != "crash-backoff" {
		return platform.ActorFactory{}, false
	}
	return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
		birth := r.births.Add(1)
		return func(sys actorbase.Sys) error {
			for {
				_, err := sys.Recv()
				if err != nil {
					return err
				}
				if birth == 1 {
					panic("crash to exercise restart backoff")
				}
			}
		}, nil
	}}}, true
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

func (r *compositionActivationResolver) BuildClass(_ channel.ID, _ actor.ActorID, class string, _ json.RawMessage) (platform.ActorFactory, bool) {
	if r.fail.Load() || class != "probe" {
		return platform.ActorFactory{}, false
	}
	return r.factory(), true
}

// buildWindowResolver mutates durable identity truth from the real declaration
// resolver boundary. activateOne resolves the factory after its first registry
// read and before SpawnIfAbsent publishes the body, so the selected resolution
// call exercises the complete pre-read/build/post-read window without a
// production test hook.
type buildWindowResolver struct {
	onResolve func()
	resolveAt int32
	resolves  atomic.Int32
}

func (r *buildWindowResolver) factory() platform.ActorFactory {
	return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
		return func(sys actorbase.Sys) error {
			<-sys.Life().Done()
			return nil
		}, nil
	}}}
}

func (r *buildWindowResolver) BuildClass(_ channel.ID, _ actor.ActorID, class string, _ json.RawMessage) (platform.ActorFactory, bool) {
	if class != "build-window" {
		return platform.ActorFactory{}, false
	}
	if r.resolves.Add(1) == r.resolveAt && r.onResolve != nil {
		r.onResolve()
	}
	return r.factory(), true
}

func openBuildWindowHome(t *testing.T, name string, resolver *buildWindowResolver) *Home {
	t.Helper()
	h, err := Open(Config{
		ChannelID:           channel.ID(name),
		DBPath:              filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver: resolver,
		DaemonAuthority:     allowTestDaemonAuthority{},
		ReconcileInterval:   time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	// These tests drive the exact build window synchronously. Stop the background
	// level loop before installing mutable resolver hooks so a queued Declare poke
	// cannot race the hand-driven activation or consume the selected resolve call.
	h.reconcileStop()
	<-h.reconcileDone
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func introduceBuildWindowComposition(t *testing.T, h *Home, principal string) storespec.ActorControlRow {
	t.Helper()
	at := time.Now().UnixMilli()
	result, err := h.Declare(context.Background(), DeclareRequest{
		SourceDeclID: "decl:build-window", Principal: principal, Class: "build-window",
		Placement: storespec.NewServerPlacement(), Kind: actor.KindAgent, TIdle: 60_000, CreatedAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result.Row
}

func TestHomeReviverBuildWindowRemoveSelfUndoes(t *testing.T) {
	ctx := context.Background()
	resolver := &buildWindowResolver{resolveAt: 1}
	h := openBuildWindowHome(t, "reviver-build-window", resolver)
	record := introduceBuildWindowComposition(t, h, "reviver-build-window")
	if _, live := h.channel.Cells().CurrentIncarnation(record.ID); live {
		t.Fatal("precondition: composition was embodied before the test drove revival")
	}

	var removeErr error
	resolver.onResolve = func() { removeErr = h.Remove(ctx, record.ID) }
	err := (homeReviver{h: h}).EnsureLive(ctx, record.ID)
	if removeErr != nil {
		t.Fatalf("Remove inside build window: %v", removeErr)
	}
	var rejected schedule.ReviveRejected
	if !errors.As(err, &rejected) || rejected.Reason != "not_a_member" {
		t.Fatalf("EnsureLive after window Remove = %v, want not_a_member", err)
	}
	if resolver.resolves.Load() != 1 {
		t.Fatalf("factory resolutions = %d, want the post-lookup resolution", resolver.resolves.Load())
	}
	if _, live := h.channel.Cells().CurrentIncarnation(record.ID); live {
		t.Fatal("reviver left a body live after membership disappeared during build")
	}
}

func TestReconcileActivationBuildWindowRemoveSelfUndoes(t *testing.T) {
	ctx := context.Background()
	resolver := &buildWindowResolver{resolveAt: 1}
	h := openBuildWindowHome(t, "reconcile-build-window", resolver)
	record := introduceBuildWindowComposition(t, h, "reconcile-build-window")
	if _, live := h.channel.Cells().CurrentIncarnation(record.ID); live {
		t.Fatal("precondition: composition was embodied before the test drove reconcile")
	}
	if verdict, err := h.liveness.AcceptDelivery(record.ID, &message.Envelope{Kind: message.KindRequest}); verdict != transitionApplied || err != nil {
		t.Fatalf("mark request dirty: verdict=%v err=%v", verdict, err)
	}
	var removeErr error
	resolver.onResolve = func() { removeErr = h.Remove(ctx, record.ID) }
	h.reconcileActivation(ctx)
	if removeErr != nil {
		t.Fatalf("Remove inside build window: %v", removeErr)
	}
	if resolver.resolves.Load() == 0 {
		t.Fatal("factory was never resolved inside the build window")
	}
	if _, live := h.channel.Cells().CurrentIncarnation(record.ID); live {
		t.Fatal("reconcile left a body live after membership disappeared during build")
	}
}

func TestStaleFactoryShellIsAbortedAndCurrentVersionRebuilt(t *testing.T) {
	ctx := context.Background()
	resolver := &buildWindowResolver{resolveAt: 1}
	h := openBuildWindowHome(t, "stale-factory-version", resolver)
	record := introduceBuildWindowComposition(t, h, "stale-factory-version")
	edited, err := h.EditDeclaration(ctx, storespec.DeclEditBundle{
		ActorID: record.ID, Class: record.Class, Config: json.RawMessage(`{"version":2}`),
		Placement: record.Placement, TIdle: record.TIdle, SourceDeclID: record.SourceDeclID,
		CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil || edited.CurrentDeclVersion != 2 {
		t.Fatalf("edit=%+v err=%v", edited, err)
	}
	resolver.onResolve = func() {
		if _, applyErr := h.ApplyDeclaration(ctx, record.ID, 2); applyErr != nil {
			t.Errorf("apply inside build window: %v", applyErr)
		}
	}

	if err := (homeReviver{h: h}).EnsureLive(ctx, record.ID); err == nil {
		t.Fatal("stale build returned success")
	}
	if _, live := h.channel.Cells().CurrentIncarnation(record.ID); live {
		t.Fatal("version-1 shell survived version-2 apply")
	}
	resolver.onResolve = nil
	deadline := time.Now().Add(2 * time.Second)
	for {
		state, _ := h.liveness.stateForTest(record.ID)
		if state.occ == occRunning && state.version == 2 {
			break
		}
		_ = (homeReviver{h: h}).EnsureLive(ctx, record.ID)
		if time.Now().After(deadline) {
			t.Fatalf("current version did not rebuild; state=%+v", state)
		}
		time.Sleep(time.Millisecond)
	}
	state, ok := h.liveness.stateForTest(record.ID)
	if !ok || state.occ != occRunning || state.version != 2 {
		t.Fatalf("rebuilt liveness=%+v present=%v", state, ok)
	}
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

	result, err := h.Declare(context.Background(), DeclareRequest{
		SourceDeclID: "decl:probe", Principal: "probe", Class: "probe",
		Placement: storespec.NewServerPlacement(), Kind: actor.KindAgent, CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitHomeCondition(t, func() bool { return resolver.builds.Load() == 1 })
	record := result.Row
	first, ok := h.channel.Cells().CurrentIncarnation(record.ID)
	if !ok {
		t.Fatal("composition member was not embodied")
	}

	// A resolver read failure keeps the last-known-good body; there is no
	// alternate desired source to cull or rebuild it from.
	resolver.fail.Store(true)
	h.pokeReconcile()
	time.Sleep(40 * time.Millisecond)
	if current, ok := h.channel.Cells().CurrentIncarnation(record.ID); !ok || current != first {
		t.Fatal("resolver failure replaced or removed the last-known-good body")
	}

	resolver.fail.Store(false)
	if _, err := h.RestartInstanceDirect(context.Background(), record.ID); err != nil {
		t.Fatal(err)
	}
	waitHomeCondition(t, func() bool { return resolver.builds.Load() == 2 })
	second, ok := h.channel.Cells().CurrentIncarnation(record.ID)
	if !ok || second == first {
		t.Fatal("version restart did not replace the composition incarnation")
	}

	if err := h.RemoveInstance(context.Background(), record.ID); err != nil {
		t.Fatal(err)
	}
	waitHomeCondition(t, func() bool {
		_, live := h.channel.Cells().CurrentIncarnation(record.ID)
		return !live
	})
}

func TestCompositionConfigChangeAdvancesVersionAndRebuildsFromOneCommit(t *testing.T) {
	resolver := &compositionActivationResolver{}
	h, err := Open(Config{
		ChannelID:           "composition-config-rebuild",
		DBPath:              filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver: resolver,
		DaemonAuthority:     allowTestDaemonAuthority{},
		ReconcileInterval:   10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })

	result, err := h.Declare(context.Background(), DeclareRequest{
		SourceDeclID: "decl:probe", Principal: "probe", Class: "probe",
		Placement: storespec.NewServerPlacement(), Kind: actor.KindAgent, CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitHomeCondition(t, func() bool { return resolver.builds.Load() == 1 })
	record := result.Row
	first, ok := h.channel.Cells().CurrentIncarnation(record.ID)
	if !ok {
		t.Fatal("initial composition member was not embodied")
	}

	cfg := json.RawMessage(`{"tone":"brisk"}`)
	updated, err := h.Declare(context.Background(), DeclareRequest{
		SourceDeclID: "decl:probe", Principal: "probe", Class: "probe", Config: &cfg,
		Placement: storespec.NewServerPlacement(), Kind: actor.KindAgent, CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil || updated.Created || !updated.ConfigUpdated {
		t.Fatalf("config update: result=%+v err=%v", updated, err)
	}
	if updated.Row.CurrentDeclVersion != record.CurrentDeclVersion+1 || string(updated.Row.Config) != string(cfg) {
		t.Fatalf("config update record=%+v", updated)
	}
	waitHomeCondition(t, func() bool { return resolver.builds.Load() == 2 })
	second, ok := h.channel.Cells().CurrentIncarnation(record.ID)
	if !ok || second == first {
		t.Fatal("atomic config version did not replace the server incarnation")
	}
}

func TestInvoluntaryBodyCrashBacksOffThenAutomaticallyRebuilds(t *testing.T) {
	resolver := &crashBackoffResolver{}
	h, err := Open(Config{
		ChannelID: "crash-backoff", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver: resolver, DaemonAuthority: allowTestDaemonAuthority{},
		ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	ctx := context.Background()
	result, err := h.Declare(ctx, DeclareRequest{
		SourceDeclID: "decl:crash-backoff", Principal: "crash-backoff", Kind: actor.KindAgent,
		Class: "crash-backoff", Placement: storespec.NewServerPlacement(), CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitHomeCondition(t, func() bool {
		_, live := h.channel.Cells().CurrentIncarnation(result.Row.ID)
		return live && resolver.births.Load() == 1
	})
	now := time.Now().UnixMilli()
	expires := time.Now().Add(time.Minute).UnixMilli()
	write, err := h.systemPen.Write(ctx, &message.Envelope{
		ID: "crash-backoff-request", Kind: message.KindRequest, Type: "crash.run",
		Audience: message.Audience{result.Row.ID}, Visibility: message.VisibilitySystem,
		TS: now, TSReceived: now, ExpiresAt: &expires,
	})
	if err != nil || !write.Accepted() {
		t.Fatalf("trigger=(%+v,%v)", write, err)
	}
	waitHomeCondition(t, func() bool {
		state, _ := h.liveness.stateForTest(result.Row.ID)
		h.reviveMu.Lock()
		entry, held := h.reviveBackoff[result.Row.ID]
		h.reviveMu.Unlock()
		return state.occ == occNone && state.restart && held && entry.failures == 1
	})
	time.Sleep(150 * time.Millisecond)
	if got := resolver.births.Load(); got != 1 {
		t.Fatalf("crashed body rebuilt before initial backoff elapsed: births=%d", got)
	}
	waitHomeCondition(t, func() bool {
		_, live := h.channel.Cells().CurrentIncarnation(result.Row.ID)
		return live && resolver.births.Load() == 2
	})
	waitHomeCondition(t, func() bool {
		h.reviveMu.Lock()
		_, retained := h.reviveBackoff[result.Row.ID]
		h.reviveMu.Unlock()
		return !retained
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
