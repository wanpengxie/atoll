package app

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/protocol/channel"
)

// A done edit promotes its pending overlay ONLY when the frame actually took
// effect: a stale outcome (superseded by a later seq) discards the pending
// value — promoting it would fork the realm's effective overlay from channel
// reality.
func TestEditFinishPromotesOnlyApplied(t *testing.T) {
	a, _ := newLifecycleTestApp(t, 0)
	ctx := context.Background()
	now := time.Now().UnixMilli()
	svc := newAdmissionService(a)

	run := func(applyStatus string) (config, pending any) {
		t.Helper()
		chID := uuid.NewString()
		opID := "adm:v1:promote-" + applyStatus
		if _, err := a.db.Exec(`INSERT INTO channel_decl_overlays(channel_id,decl_id,pending_config_json,pending_ref,updated_at) VALUES (?,?,?,?,?)`,
			chID, "decl:x", `{"v":"local"}`, opID, now); err != nil {
			t.Fatal(err)
		}
		if _, err := a.db.Exec(`INSERT INTO channel_admission_operations(operation_id,channel_id,op,requested_by,request_json,request_digest,created_at) VALUES (?,?,?,?,?,?,?)`,
			opID, chID, "edit", "alice", `{}`, "d:"+applyStatus, now); err != nil {
			t.Fatal(err)
		}
		record, found, err := svc.load(ctx, opID)
		if err != nil || !found {
			t.Fatalf("load: %v %v", found, err)
		}
		result, _ := json.Marshal(map[string]string{"status": applyStatus})
		if err := svc.finish(ctx, record, "done", result, ""); err != nil {
			t.Fatal(err)
		}
		if err := a.db.QueryRow(`SELECT config_json,pending_config_json FROM channel_decl_overlays WHERE channel_id=?`, chID).Scan(&config, &pending); err != nil {
			t.Fatal(err)
		}
		return config, pending
	}

	config, pending := run(string(channel.ApplyApplied))
	if config != `{"v":"local"}` || pending != nil {
		t.Fatalf("applied: config=%v pending=%v, want promoted+cleared", config, pending)
	}
	config, pending = run(string(channel.ApplyStale))
	if config != nil || pending != nil {
		t.Fatalf("stale: config=%v pending=%v, want discarded (never promoted)", config, pending)
	}
}
