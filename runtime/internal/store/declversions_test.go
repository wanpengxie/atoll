package store_test

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func TestAdmitDeclaredWritesJoinedControlRowAtomically(t *testing.T) {
	cs := openTestChannel(t)
	ctx := context.Background()
	placement, err := storespec.NewDaemonPlacement("daemon-a")
	if err != nil {
		t.Fatal(err)
	}
	in := storespec.AdmitBundle{
		ID: "agent:a:1", Kind: actor.KindAgent,
		Binding: actor.BindingRuntimeInboundViaRelay, Class: "agent.test",
		Config: []byte(`{"model":"x"}`), Placement: placement,
		SourceDeclID: "decl-a", CreatedAt: 100,
	}
	got, err := cs.DeclAdmission.AdmitDeclared(ctx, in)
	if err != nil || !got.Created || got.ID != in.ID {
		t.Fatalf("AdmitDeclared = (%+v,%v)", got, err)
	}
	row, ok, err := cs.Declared.LookupDeclaredActive(ctx, in.ID)
	if err != nil || !ok {
		t.Fatalf("LookupDeclaredActive = (%+v,%v,%v)", row, ok, err)
	}
	if row.CurrentDeclVersion != 1 || row.Class != in.Class || string(row.Config) != string(in.Config) ||
		row.Placement != placement || row.SourceDeclID != in.SourceDeclID || row.Sponsor != actor.SystemActorID {
		t.Fatalf("joined row = %+v", row)
	}
	if rows, err := cs.Query.ReadAfterSeq(ctx, 0, 10); err != nil || len(rows) != 1 {
		t.Fatalf("birth mirrors = %d err=%v", len(rows), err)
	}

	// Same logical admission is an idempotent hit, not a second event.
	again, err := cs.DeclAdmission.AdmitDeclared(ctx, in)
	if err != nil || again.Created || again.ID != in.ID {
		t.Fatalf("idempotent AdmitDeclared = (%+v,%v)", again, err)
	}
	if rows, _ := cs.Query.ReadAfterSeq(ctx, 0, 10); len(rows) != 1 {
		t.Fatalf("idempotent retry appended %d mirrors", len(rows))
	}
}

func TestDeclarationSourceIsImmutableUniqueAmongActiveActors(t *testing.T) {
	cs := openTestChannel(t)
	ctx := context.Background()
	admit := func(id actor.ActorID, at int64) (storespec.DeclAdmissionResult, error) {
		return cs.DeclAdmission.AdmitDeclared(ctx, storespec.AdmitBundle{
			ID: id, Kind: actor.KindAgent, SourceDeclID: "decl:stable",
			Class: "agent", Placement: storespec.NewServerPlacement(), CreatedAt: at,
		})
	}
	first, err := admit("agent:first", 1)
	if err != nil || !first.Created {
		t.Fatalf("first admission = (%+v,%v)", first, err)
	}
	duplicate, err := admit("agent:second", 2)
	if err != nil || duplicate.Created || duplicate.ID != first.ID {
		t.Fatalf("active-source idempotency = (%+v,%v), want existing %q", duplicate, err, first.ID)
	}
	if _, err := cs.Cascade.EndCascade(ctx, storespec.CascadeBundle{IDs: []actor.ActorID{first.ID}, EndedAt: 3}); err != nil {
		t.Fatal(err)
	}
	reintroduced, err := admit("agent:second", 4)
	if err != nil || !reintroduced.Created || reintroduced.ID == first.ID {
		t.Fatalf("reintroduction = (%+v,%v)", reintroduced, err)
	}
	row, ok, err := cs.Declared.LookupDeclaredActive(ctx, reintroduced.ID)
	if err != nil || !ok || row.SourceDeclID != "decl:stable" || row.Principal != "" {
		t.Fatalf("reintroduced row = (%+v,%v,%v)", row, ok, err)
	}
}

func TestAdmitDeclaredNilConfigSurvivesReadPath(t *testing.T) {
	cs := openTestChannel(t)
	ctx := context.Background()
	in := storespec.AdmitBundle{
		ID: actor.SystemActorID, Kind: actor.KindSystem, Class: "system",
		Placement: storespec.NewServerPlacement(), CreatedAt: 1,
	}
	if _, err := cs.DeclAdmission.AdmitDeclared(ctx, in); err != nil {
		t.Fatal(err)
	}
	row, ok, err := cs.Declared.LookupDeclaredActive(ctx, actor.SystemActorID)
	if err != nil || !ok || row.Config != nil {
		t.Fatalf("nil config row = (%+v,%v,%v)", row, ok, err)
	}
	row.Config = append(row.Config, 'x')
	row2, _, _ := cs.Declared.LookupDeclaredActive(ctx, actor.SystemActorID)
	if row2.Config != nil {
		t.Fatalf("read mutation escaped into store row: %q", row2.Config)
	}
}
