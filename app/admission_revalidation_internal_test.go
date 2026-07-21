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

// Delivery acts on present realm state: a pending attach whose daemon was
// revoked while it waited must land rejected(daemon_not_found), never write a
// binding for a daemon the registry no longer knows.
func TestPendingAttachRejectsRevokedDaemon(t *testing.T) {
	a, _ := newLifecycleTestApp(t, 0)
	ctx := context.Background()
	now := time.Now().UnixMilli()
	id := channel.ID(uuid.NewString())

	spec := channelhost.ProvisionSpec{ChannelID: id, Type: "group", OwnerPrincipal: "alice", CreatedAt: now}
	raw, _ := json.Marshal(spec)
	res, err := a.db.Exec(`INSERT INTO channel_provision_jobs(operation_id,channel_id,requested_by,name,type,owner_principal,spec_json,created_at) VALUES (?,?,?,?,?,?,?,?)`,
		"lc:reval-test", id, "alice", "reval-room", "group", "alice", string(raw), now)
	if err != nil {
		t.Fatal(err)
	}
	jobID, _ := res.LastInsertId()
	if err := a.runProvisionJob(ctx, jobID); err != nil {
		t.Fatalf("provision: %v", err)
	}

	if _, err := a.db.Exec(`INSERT INTO users(id,email,password,created_at) VALUES ('alice','alice@example.com','x',?)`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := a.db.Exec(`INSERT INTO daemons(id,owner_id,name,api_key_hash,created_at) VALUES ('d1','alice','laptop','x',?)`, now); err != nil {
		t.Fatal(err)
	}

	svc := newAdmissionService(a)
	insertAttach := func(opID string) {
		t.Helper()
		request, _ := json.Marshal(channel.DaemonRequest{Ref: opID, DaemonID: "d1"})
		if _, err := a.db.Exec(`INSERT INTO channel_admission_operations(operation_id,channel_id,op,requested_by_principal,request_json,request_digest,created_at) VALUES (?,?,?,?,?,?,?)`,
			opID, string(id), "attach", "alice", string(request), "digest:"+opID, now); err != nil {
			t.Fatal(err)
		}
	}
	opStatus := func(opID string) (string, string) {
		t.Helper()
		var status, code string
		if err := a.db.QueryRow(`SELECT status,COALESCE(error_code,'') FROM channel_admission_operations WHERE operation_id=?`, opID).Scan(&status, &code); err != nil {
			t.Fatal(err)
		}
		return status, code
	}

	// Control: with the daemon registered, the pending attach delivers.
	insertAttach("adm:v1:reval-live")
	if err := svc.runOperation(ctx, "adm:v1:reval-live"); err != nil {
		t.Fatalf("live attach: %v", err)
	}
	if status, code := opStatus("adm:v1:reval-live"); status != "done" {
		t.Fatalf("live attach status=%s code=%s", status, code)
	}

	// The daemon is revoked while the second attach waits.
	insertAttach("adm:v1:reval-stale")
	if _, err := a.db.Exec(`DELETE FROM daemons WHERE id='d1'`); err != nil {
		t.Fatal(err)
	}
	_ = svc.runOperation(ctx, "adm:v1:reval-stale")
	status, code := opStatus("adm:v1:reval-stale")
	if status != "rejected" || code != string(admissionCodeDaemonNotFound) {
		t.Fatalf("stale attach status=%s code=%s, want rejected/daemon_not_found", status, code)
	}

	// Interleave shape: the present-state check runs INSIDE the channel critical
	// section. A revocation landing while the delivery is parked on the channel
	// lock is always observed — the check cannot act on an answer read before
	// the lock. (Whichever side wins the lock, the outcome converges: binding
	// swept by the revoke arm, or attach rejected on the current registry.)
	if _, err := a.db.Exec(`INSERT INTO daemons(id,owner_id,name,api_key_hash,created_at) VALUES ('d2','alice','desk','x',?)`, now); err != nil {
		t.Fatal(err)
	}
	request, _ := json.Marshal(channel.DaemonRequest{Ref: "adm:v1:reval-locked", DaemonID: "d2"})
	if _, err := a.db.Exec(`INSERT INTO channel_admission_operations(operation_id,channel_id,op,requested_by_principal,request_json,request_digest,created_at) VALUES (?,?,?,?,?,?,?)`,
		"adm:v1:reval-locked", string(id), "attach", "alice", string(request), "digest:locked", now); err != nil {
		t.Fatal(err)
	}
	release := a.channelLocks.lock(string(id))
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = svc.runOperation(context.Background(), "adm:v1:reval-locked")
	}()
	time.Sleep(50 * time.Millisecond) // let the delivery park on the channel lock
	if _, err := a.db.Exec(`DELETE FROM daemons WHERE id='d2'`); err != nil {
		t.Fatal(err)
	}
	release()
	<-done
	status, code = opStatus("adm:v1:reval-locked")
	if status != "rejected" || code != string(admissionCodeDaemonNotFound) {
		t.Fatalf("locked interleave status=%s code=%s, want rejected/daemon_not_found", status, code)
	}
}
