package home

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type relationEventResolver struct{}

func (relationEventResolver) ResolveDeclaration(context.Context, channel.ID, string) (channelspec.DeclarationFacts, error) {
	return channelspec.DeclarationFacts{
		OwnerPrincipal: "alice", Visibility: "public", Class: "relation-agent",
		Config: json.RawMessage(`{}`),
	}, nil
}
func (relationEventResolver) ClassKind(context.Context, string) (actor.Kind, bool, error) {
	return actor.KindAgent, true, nil
}

func TestRelationEventsComeFromCommitRootsAndOpenSnapshot(t *testing.T) {
	var mu sync.Mutex
	var batches [][]channelspec.RelationDelta
	cfg := completeHomeTestConfig(Config{
		ChannelID: "relation-events", DBPath: filepath.Join(t.TempDir(), "home.sqlite"),
		Bootstrap: true, IntroductionResolver: relationEventResolver{},
		OnRelationChange: func(_ channel.ID, deltas []channelspec.RelationDelta) {
			mu.Lock()
			batches = append(batches, append([]channelspec.RelationDelta(nil), deltas...))
			mu.Unlock()
		},
	})
	h, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Shutdown(h) })
	ctx := context.Background()

	admitted, err := SystemOps(h).Admit(ctx, channelspec.AdmitRequest{Principal: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	beforeAdmitReplay := len(batches)
	mu.Unlock()
	if replay, err := SystemOps(h).Admit(ctx, channelspec.AdmitRequest{Principal: "alice"}); err != nil {
		t.Fatal(err)
	} else if replay.Created {
		t.Fatal("admit replay reported a new birth")
	}
	mu.Lock()
	afterAdmitReplay := len(batches)
	mu.Unlock()
	if afterAdmitReplay != beforeAdmitReplay {
		t.Fatal("admit replay emitted a relation birth")
	}
	if _, err := SystemOps(h).AttachDaemon(ctx, channelspec.DaemonRequest{DaemonID: "daemon-a"}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	beforeAttachReplay := len(batches)
	mu.Unlock()
	if _, err := SystemOps(h).AttachDaemon(ctx, channelspec.DaemonRequest{DaemonID: "daemon-a"}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	afterAttachReplay := len(batches)
	mu.Unlock()
	if afterAttachReplay != beforeAttachReplay {
		t.Fatal("attach replay emitted a relation binding")
	}
	introduced, err := SystemOps(h).Introduce(ctx, channelspec.IntroduceRequest{
		DeclID: "decl-a", InitiatorActorID: admitted.ActorID,
	})
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	beforeIntroduceReplay := len(batches)
	mu.Unlock()
	if replay, err := SystemOps(h).Introduce(ctx, channelspec.IntroduceRequest{
		DeclID: "decl-a", InitiatorActorID: admitted.ActorID,
	}); err != nil {
		t.Fatal(err)
	} else if replay.Created {
		t.Fatal("introduce replay reported a new birth")
	}
	mu.Lock()
	afterIntroduceReplay := len(batches)
	mu.Unlock()
	if afterIntroduceReplay != beforeIntroduceReplay {
		t.Fatal("introduce replay emitted a relation birth")
	}
	mu.Lock()
	beforeZeroEdgeCommands := len(batches)
	mu.Unlock()
	if err := h.actors.Restart(ctx, actorctl.RestartRequest{ActorID: introduced.ActorID}); err != nil {
		t.Fatal(err)
	}
	if err := h.actors.ApplyDeclaration(ctx, actorctl.DeclarationChange{
		ActorID: introduced.ActorID,
		Definition: storespec.ActorDefinition{
			Class: "relation-agent", Config: json.RawMessage(`{"revision":2}`),
		},
	}); err != nil {
		t.Fatal(err)
	}
	var callerAttempt actorhost.AttemptKey
	desired, err := h.actors.PlanFor("server")
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range desired {
		if item.Actor() == admitted.ActorID {
			callerAttempt = item.Attempt()
			break
		}
	}
	if callerAttempt == "" {
		t.Fatal("admitted caller attempt not found")
	}
	if _, err := h.actors.Fork(ctx, actorctl.ForkRequest{
		CallerActorID: admitted.ActorID, CallerAttempt: callerAttempt,
		RequestID: message.ID("relation-zero-edge-fork"),
		Spec: actorcaps.ForkSpec{
			Kind: actor.KindAgent, Class: "relation-agent", Config: json.RawMessage(`{}`),
		},
	}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	afterZeroEdgeCommands := len(batches)
	mu.Unlock()
	if afterZeroEdgeCommands != beforeZeroEdgeCommands {
		t.Fatalf("Fork/Restart/ApplyDeclaration emitted relation batches: before=%d after=%d",
			beforeZeroEdgeCommands, afterZeroEdgeCommands)
	}
	if _, err := SystemOps(h).Remove(ctx, channelspec.RemoveRequest{
		Target: introduced.ActorID, InitiatorActorID: admitted.ActorID,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := SystemOps(h).DetachDaemon(ctx, channelspec.DaemonRequest{DaemonID: "daemon-a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := SystemOps(h).Remove(ctx, channelspec.RemoveRequest{
		Target: admitted.ActorID, InitiatorActorID: admitted.ActorID,
	}); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	var resetCount int
	got := map[channelspec.RelationKind][]channelspec.RelationDelta{}
	for _, batch := range batches {
		for _, delta := range batch {
			if delta.Reset {
				resetCount++
				continue
			}
			got[delta.Kind] = append(got[delta.Kind], delta)
		}
	}
	if resetCount != 1 {
		t.Fatalf("snapshot reset count=%d batches=%+v", resetCount, batches)
	}
	for _, kind := range []channelspec.RelationKind{
		channelspec.RelationJoined, channelspec.RelationIntroduced,
		channelspec.RelationInstanceRemoved, channelspec.RelationBound,
		channelspec.RelationUnbound, channelspec.RelationLeft,
	} {
		if len(got[kind]) != 1 {
			t.Fatalf("%s events=%+v", kind, got[kind])
		}
	}
	if got[channelspec.RelationJoined][0].Principal != "alice" ||
		got[channelspec.RelationJoined][0].ActorID != admitted.ActorID {
		t.Fatalf("joined payload=%+v", got[channelspec.RelationJoined])
	}
	if got[channelspec.RelationIntroduced][0].DeclID != "decl-a" ||
		got[channelspec.RelationIntroduced][0].ActorID != introduced.ActorID {
		t.Fatalf("introduced payload=%+v", got[channelspec.RelationIntroduced])
	}
	// Death deltas must carry the same identity axes as their birth twins:
	// the receiver deletes by actor_id, so a mistranslated EndedFact would
	// turn the delete into a silent no-op while a count-only check stays green.
	if got[channelspec.RelationLeft][0].Principal != "alice" ||
		got[channelspec.RelationLeft][0].ActorID != admitted.ActorID {
		t.Fatalf("left payload=%+v", got[channelspec.RelationLeft])
	}
	if got[channelspec.RelationInstanceRemoved][0].DeclID != "decl-a" ||
		got[channelspec.RelationInstanceRemoved][0].ActorID != introduced.ActorID {
		t.Fatalf("instance-removed payload=%+v", got[channelspec.RelationInstanceRemoved])
	}
	if got[channelspec.RelationBound][0].DaemonID != "daemon-a" ||
		got[channelspec.RelationUnbound][0].DaemonID != "daemon-a" {
		t.Fatalf("binding payloads=%+v / %+v", got[channelspec.RelationBound], got[channelspec.RelationUnbound])
	}
}
