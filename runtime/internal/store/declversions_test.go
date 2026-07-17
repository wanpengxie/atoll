package store_test

import (
	"context"
	"testing"
	"time"

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
		ID: "agent:a:1", Kind: actor.KindAgent, Principal: "a",
		Binding: actor.BindingRuntimeInboundViaRelay, Class: "agent.test",
		Config: []byte(`{"model":"x"}`), Placement: placement,
		TIdle: 3 * time.Second, SourceDeclID: "decl-a", CreatedAt: 100,
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
		row.Placement != placement || row.TIdle != in.TIdle || row.SourceDeclID != in.SourceDeclID || row.Sponsor != actor.SystemActorID {
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

func TestEditDeclaredMintsLatestWithoutMovingCurrent(t *testing.T) {
	cs := openTestChannel(t)
	ctx := context.Background()
	id := actor.ActorID("agent:edit:1")
	if _, err := cs.DeclAdmission.AdmitDeclared(ctx, storespec.AdmitBundle{
		ID: id, Kind: actor.KindAgent, Principal: "edit", Binding: actor.BindingRuntimeInboundViaRelay,
		Class: "agent.v1", Placement: storespec.NewServerPlacement(), CreatedAt: 1,
	}); err != nil {
		t.Fatal(err)
	}
	latest, err := cs.DeclVersions.EditDeclared(ctx, storespec.DeclEditBundle{
		ActorID: id, Class: "agent.v2", Config: nil, Placement: storespec.NewServerPlacement(),
		SourceDeclID: "source-v2", CreatedAt: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if latest.CurrentDeclVersion != 2 || latest.Config != nil || latest.SourceDeclID != "source-v2" {
		t.Fatalf("edited row = %+v", latest)
	}
	current, ok, err := cs.Declared.LookupDeclaredActive(ctx, id)
	if err != nil || !ok || current.CurrentDeclVersion != 1 || current.Class != "agent.v1" {
		t.Fatalf("current moved during edit: row=%+v ok=%v err=%v", current, ok, err)
	}
	gotLatest, ok, err := cs.Declared.LatestDeclaredVersion(ctx, id)
	if err != nil || !ok || gotLatest.CurrentDeclVersion != 2 || gotLatest.Class != "agent.v2" {
		t.Fatalf("latest = %+v ok=%v err=%v", gotLatest, ok, err)
	}
	applied, ok, err := cs.DeclVersions.ApplyDeclaredVersion(ctx, id, 2)
	if err != nil || !ok || applied.CurrentDeclVersion != 2 || applied.Config != nil {
		t.Fatalf("apply = %+v ok=%v err=%v", applied, ok, err)
	}
}
