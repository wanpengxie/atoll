package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func introduceAgent(t *testing.T, cs interface {
	IntroduceComposition(context.Context, storespec.CompositionIntroduce) (storespec.CompositionRecord, bool, bool, error)
}, principal string, at int64) storespec.CompositionRecord {
	t.Helper()
	r, created, _, err := cs.IntroduceComposition(context.Background(), storespec.CompositionIntroduce{
		DeclID: principal, Principal: principal, Class: "agent.test", Placement: storespec.PlacementServer,
		Kind: actor.KindAgent, At: at,
	})
	if err != nil || !created {
		t.Fatalf("IntroduceComposition: created=%v err=%v", created, err)
	}
	return r
}

func TestCompositionIntroduceAndRemoveAreRegistryAtomic(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	r := introduceAgent(t, cs.Composition, "decl-a", 100)
	if r.InstanceID == "" || r.DeclID != "decl-a" || r.Epoch != 0 {
		t.Fatalf("record = %+v", r)
	}
	member, ok, err := cs.Registry.Lookup(ctx, r.InstanceID)
	if err != nil || !ok || !member.IsActive() || member.Principal != "decl-a" {
		t.Fatalf("registry after introduce = %+v ok=%v err=%v", member, ok, err)
	}

	removed, err := cs.Composition.RemoveComposition(ctx, r.InstanceID, 200)
	if err != nil || !removed {
		t.Fatalf("RemoveComposition: removed=%v err=%v", removed, err)
	}
	if _, ok, err := cs.Composition.LookupComposition(ctx, r.InstanceID); err != nil || ok {
		t.Fatalf("composition survived remove: ok=%v err=%v", ok, err)
	}
	member, ok, err = cs.Registry.Lookup(ctx, r.InstanceID)
	if err != nil || !ok || member.IsActive() {
		t.Fatalf("registry not deregistered atomically: %+v ok=%v err=%v", member, ok, err)
	}
}

func TestCompositionRetryFreezesMetadataAndRepairsInactiveID(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	r := introduceAgent(t, cs.Composition, "decl-a", 100)
	cfg := `{"temperature":1}`
	got, created, changed, err := cs.Composition.IntroduceComposition(ctx, storespec.CompositionIntroduce{
		DeclID: "ignored", Principal: "decl-a", Class: "ignored", ConfigJSON: &cfg,
		Placement: storespec.PlacementDaemon, DesiredHost: "ignored", Kind: actor.KindAgent, At: 110,
	})
	if err != nil || created || !changed {
		t.Fatalf("retry: created=%v changed=%v err=%v", created, changed, err)
	}
	if got.InstanceID != r.InstanceID || got.Class != "agent.test" || got.Placement != storespec.PlacementServer || got.ConfigJSON != cfg {
		t.Fatalf("retry changed frozen metadata: %+v", got)
	}
	if err := cs.Membership.Deregister(ctx, r.InstanceID, 120); err != nil {
		t.Fatal(err)
	}
	repaired, created, _, err := cs.Composition.IntroduceComposition(ctx, storespec.CompositionIntroduce{
		DeclID: "decl-a", Principal: "decl-a", Class: "agent.test", Placement: storespec.PlacementServer,
		Kind: actor.KindAgent, At: 130,
	})
	if err != nil || created || repaired.InstanceID == r.InstanceID {
		t.Fatalf("repair = %+v created=%v err=%v", repaired, created, err)
	}
	member, ok, err := cs.Registry.Lookup(ctx, repaired.InstanceID)
	if err != nil || !ok || !member.IsActive() {
		t.Fatalf("repaired member = %+v ok=%v err=%v", member, ok, err)
	}
}

func TestCompositionDefaultAndRestartIdempotency(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	a := introduceAgent(t, cs.Composition, "decl-a", 100)
	b := introduceAgent(t, cs.Composition, "decl-b", 101)
	if err := cs.Composition.SetDefaultComposition(ctx, a.InstanceID); err != nil {
		t.Fatal(err)
	}
	if err := cs.Composition.SetDefaultComposition(ctx, b.InstanceID); err != nil {
		t.Fatal(err)
	}
	if id, ok, err := cs.Composition.DefaultComposition(ctx); err != nil || !ok || id != b.InstanceID {
		t.Fatalf("default = %q ok=%v err=%v", id, ok, err)
	}
	if epoch, err := cs.Composition.RestartComposition(ctx, b.InstanceID); err != nil || epoch != 1 {
		t.Fatalf("direct restart epoch=%d err=%v", epoch, err)
	}
	if epoch, applied, err := cs.Composition.ApplyRestartComposition(ctx, 7, b.InstanceID, 200); err != nil || !applied || epoch != 2 {
		t.Fatalf("job restart epoch=%d applied=%v err=%v", epoch, applied, err)
	}
	if epoch, applied, err := cs.Composition.ApplyRestartComposition(ctx, 7, b.InstanceID, 201); err != nil || applied || epoch != 2 {
		t.Fatalf("job replay epoch=%d applied=%v err=%v", epoch, applied, err)
	}
	if _, err := cs.Composition.RestartComposition(ctx, "missing"); !errors.Is(err, storespec.ErrCompositionNotFound) {
		t.Fatalf("missing restart err=%v", err)
	}
}

func TestCompositionInvalidIntroduceLeavesNoHalfRows(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	_, _, _, err := cs.Composition.IntroduceComposition(ctx, storespec.CompositionIntroduce{
		DeclID: "decl-a", Principal: "decl-a", Class: "agent.test", Placement: storespec.PlacementServer,
		DesiredHost: "daemon-a", Kind: actor.KindAgent, At: 100,
	})
	if err == nil {
		t.Fatal("invalid server desired_host accepted")
	}
	if rows, qerr := cs.Composition.ListComposition(ctx); qerr != nil || len(rows) != 0 {
		t.Fatalf("composition residue=%v err=%v", rows, qerr)
	}
	if members, qerr := cs.Registry.ListActive(ctx); qerr != nil || len(members) != 0 {
		t.Fatalf("registry residue=%v err=%v", members, qerr)
	}
}
