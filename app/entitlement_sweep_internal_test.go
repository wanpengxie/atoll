package app

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// The sweep is the projection's third maintenance layer: it must insert rows
// for members no transaction recorded, delete rows no member backs, drop
// orphan rows for retired channels, and leave correct rows untouched.
func TestMembershipProjectionSweep(t *testing.T) {
	a, _ := newLifecycleTestApp(t, 0)
	ctx := context.Background()
	now := time.Now().UnixMilli()
	id := channel.ID(uuid.NewString())

	spec := channelhost.ProvisionSpec{ChannelID: id, Type: "group", OwnerPrincipal: "alice", CreatedAt: now}
	raw, _ := json.Marshal(spec)
	res, err := a.db.Exec(`INSERT INTO channel_provision_jobs(operation_id,channel_id,requested_by,name,type,owner_principal,spec_json,created_at) VALUES (?,?,?,?,?,?,?,?)`,
		"lc:sweep-test", id, "alice", "sweep-room", "group", "alice", string(raw), now)
	if err != nil {
		t.Fatal(err)
	}
	jobID, _ := res.LastInsertId()
	if err := a.runProvisionJob(ctx, jobID); err != nil {
		t.Fatalf("provision: %v", err)
	}

	bundle, ok := a.host.Acquire(id)
	if !ok {
		t.Fatal("channel not serving after provision")
	}
	// SysOp Admit writes membrane truth only — the projection row normally
	// comes from the admission service transaction, which this test bypasses
	// on purpose to create an under-grant.
	admitted, err := bundle.SysOp().Admit(ctx, channel.AdmitRequest{Ref: "adm:v1:sweep-test", Principal: "bob"})
	if err != nil {
		t.Fatalf("admit: %v", err)
	}

	// Ghost row: projected member the membrane does not know.
	if _, err := a.db.Exec(`INSERT INTO principal_channels(principal,channel_id,actor_id,updated_at) VALUES ('mallory',?,'ghost',?)`, string(id), now); err != nil {
		t.Fatal(err)
	}
	// Orphan row: channel that left the directory.
	if _, err := a.db.Exec(`INSERT INTO principal_channels(principal,channel_id,actor_id,updated_at) VALUES ('carol','gone','stale',?)`, now); err != nil {
		t.Fatal(err)
	}

	a.sweepMembershipProjection(ctx)

	rows, err := a.db.Query(`SELECT principal,channel_id,actor_id FROM principal_channels ORDER BY principal`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string][2]string{}
	for rows.Next() {
		var principal, ch, actorID string
		if err := rows.Scan(&principal, &ch, &actorID); err != nil {
			t.Fatal(err)
		}
		got[principal] = [2]string{ch, actorID}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("projection rows = %v, want exactly alice+bob", got)
	}
	if got["alice"][0] != string(id) {
		t.Fatalf("owner row missing or wrong: %v", got["alice"])
	}
	if got["bob"] != [2]string{string(id), string(admitted.ActorID)} {
		t.Fatalf("under-grant not repaired: %v", got["bob"])
	}
}
