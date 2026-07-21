package app

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestFinalizeDeliveryPersistsBytesAndSurvivesLostReceipt(t *testing.T) {
	ctx := context.Background()
	a := newBareAppForTest(t)
	a.admission = newAdmissionService(a)
	const declID = "decl-finalize"
	if _, err := a.db.Exec(`INSERT INTO actor_decls(id,name,owner,default_class,config_json,created_at,updated_at,visibility) VALUES (?,?,?,?,?,?,?,?)`, declID, "Finalize", "owner", "go-kimi", `{"model":"v2"}`, 1, 2, "public"); err != nil {
		t.Fatal(err)
	}
	v1, err := (channel.RenderedSnapshot{
		Class: "go-kimi", Config: json.RawMessage(`{"model":"v1"}`),
		Placement: channel.Placement{Kind: channel.PlacementDaemon, DesiredHost: "daemon-finalize"}, RenderSeq: 1,
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	chID := channel.ID("finalize-channel")
	spec := channelhost.ProvisionSpec{
		ChannelID: chID, Type: "group", OwnerPrincipal: "owner", CreatedAt: time.Now().UnixMilli(),
		GenesisDeclarations: []channelhost.GenesisDeclaration{{DeclID: declID, Kind: actor.KindAgent, Rendered: v1}},
	}
	openTestChannelForTest(t, a, chID, spec.GenesisDeclarations)
	bundle, ok := a.host.Acquire(chID)
	if !ok {
		t.Fatal("channel bundle unavailable")
	}
	job := provisionJob{OperationID: "lc:finalize", ChannelID: string(chID), Owner: "owner"}
	action, payload, found, err := a.createFinalizeDelivery(ctx, job, spec.GenesisDeclarations[0])
	if err != nil || !found || action != "apply" {
		t.Fatalf("create delivery=(%q,%s,%v,%v)", action, payload, found, err)
	}
	var beforeRef, beforePayload, beforeDigest string
	var beforeSeq int64
	if err := a.db.QueryRow(`SELECT ref,payload_json,request_digest,render_seq FROM channel_finalize_deliveries WHERE operation_id=? AND decl_id=?`, job.OperationID, declID).Scan(&beforeRef, &beforePayload, &beforeDigest, &beforeSeq); err != nil {
		t.Fatal(err)
	}
	// Commit in the channel, then lose the receipt before the realm ack update.
	var request channel.ApplyDeclVersionRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		t.Fatal(err)
	}
	applied, err := bundle.SysOp().ApplyDeclVersion(ctx, request)
	if err != nil || applied.Status != channel.ApplyApplied {
		t.Fatalf("first finalize apply=(%+v,%v) payload=%s", applied, err, payload)
	}
	rows, err := bundle.View().DeclaredBySource(ctx, declID)
	if err != nil || len(rows) != 1 || rows[0].CurrentDeclVersion != 2 {
		t.Fatalf("first apply was not published: rows=%+v err=%v", rows, err)
	}
	if err := a.finalizeProvision(ctx, job, spec, bundle); err != nil {
		t.Fatal(err)
	}
	var afterRef, afterPayload, afterDigest string
	var afterSeq int64
	var acked int64
	if err := a.db.QueryRow(`SELECT ref,payload_json,request_digest,render_seq,acked_at FROM channel_finalize_deliveries WHERE operation_id=? AND decl_id=?`, job.OperationID, declID).Scan(&afterRef, &afterPayload, &afterDigest, &afterSeq, &acked); err != nil {
		t.Fatal(err)
	}
	if beforeRef != afterRef || beforePayload != afterPayload || beforeDigest != afterDigest || beforeSeq != afterSeq || acked == 0 {
		t.Fatalf("delivery mutated across replay: before=(%s,%s,%s,%d) after=(%s,%s,%s,%d,%d)", beforeRef, beforePayload, beforeDigest, beforeSeq, afterRef, afterPayload, afterDigest, afterSeq, acked)
	}
	rows, err = bundle.View().DeclaredBySource(ctx, declID)
	if err != nil || len(rows) != 1 || rows[0].CurrentDeclVersion != 2 || string(rows[0].Config) != `{"model":"v2"}` {
		t.Fatalf("finalized rows=%+v err=%v", rows, err)
	}
	if err := a.finalizeProvision(ctx, job, spec, bundle); err != nil {
		t.Fatal(err)
	}
	rows, _ = bundle.View().DeclaredBySource(ctx, declID)
	if len(rows) != 1 || rows[0].CurrentDeclVersion != 2 {
		t.Fatalf("acked replay appended another version: %+v", rows)
	}
}
