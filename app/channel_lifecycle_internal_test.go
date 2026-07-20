package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type countedHost struct {
	channelhost.LocalHost
	mu           sync.Mutex
	provisions   int
	destroys     int
	failDestroys int
}

func (h *countedHost) Provision(ctx context.Context, spec channelhost.ProvisionSpec) (channelhost.ProvisionReceipt, error) {
	h.mu.Lock()
	h.provisions++
	h.mu.Unlock()
	return h.LocalHost.Provision(ctx, spec)
}
func (h *countedHost) Destroy(ctx context.Context, id channel.ID) error {
	h.mu.Lock()
	h.destroys++
	if h.failDestroys > 0 {
		h.failDestroys--
		h.mu.Unlock()
		return errors.New("injected destroy failure")
	}
	h.mu.Unlock()
	return h.LocalHost.Destroy(ctx, id)
}

func newLifecycleTestApp(t *testing.T, failDestroys int) (*App, *countedHost) {
	t.Helper()
	root := t.TempDir()
	db, err := openTestAppDB(t, filepath.Join(root, "app.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	a := &App{db: db, logger: testLogger(), daemonLocks: newKeyedLockSet(), declLocks: newKeyedLockSet(), channelLocks: newKeyedLockSet()}
	real, err := channelhost.New(filepath.Join(root, "channels"), channelhost.HomeDeps{CompositionResolver: compositionResolver{app: a}, PlanProvider: appPlanProvider{app: a}, DaemonAuthority: appDaemonAuthority{app: a}, Operate: a.operateFace(), Logger: a.logger})
	if err != nil {
		t.Fatal(err)
	}
	wrapped := &countedHost{LocalHost: real, failDestroys: failDestroys}
	a.host = wrapped
	t.Cleanup(func() { _ = real.Close() })
	return a, wrapped
}

func testLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func TestPublishNameConflictCompensatesBeforeDead(t *testing.T) {
	a, host := newLifecycleTestApp(t, 3)
	ctx := context.Background()
	now := time.Now().UnixMilli()
	id := channel.ID(uuid.NewString())
	if _, err := a.db.Exec(`INSERT INTO channels(id,name,type,created_at,parent_id) VALUES ('winner','same','group',?,NULL)`, now); err != nil {
		t.Fatal(err)
	}
	spec := channelhost.ProvisionSpec{ChannelID: id, Type: "group", OwnerPrincipal: "owner", CreatedAt: now}
	raw, _ := json.Marshal(spec)
	res, err := a.db.Exec(`INSERT INTO channel_provision_jobs(operation_id,channel_id,requested_by,name,type,owner_principal,spec_json,created_at) VALUES (?,?,?,?,?,?,?,?)`, "lc:test", id, "owner", "same", "group", "owner", string(raw), now)
	if err != nil {
		t.Fatal(err)
	}
	jobID, _ := res.LastInsertId()
	for i := 0; i < 3; i++ {
		_ = a.runProvisionJob(ctx, jobID)
		var dead any
		if err := a.db.QueryRow(`SELECT dead_at FROM channel_provision_jobs WHERE job_id=?`, jobID).Scan(&dead); err != nil {
			t.Fatal(err)
		}
		if dead != nil {
			t.Fatalf("provision died before compensation closed on round %d", i)
		}
	}
	host.mu.Lock()
	provisions := host.provisions
	host.mu.Unlock()
	if provisions != 1 {
		t.Fatalf("Provision calls=%d want 1 during compensation retries", provisions)
	}
	if err := a.runProvisionJob(ctx, jobID); err != nil {
		t.Fatal(err)
	}
	var code string
	var dead int64
	if err := a.db.QueryRow(`SELECT error_code,dead_at FROM channel_provision_jobs WHERE job_id=?`, jobID).Scan(&code, &dead); err != nil {
		t.Fatal(err)
	}
	if code != "name_conflict" || dead == 0 {
		t.Fatalf("terminal=(%q,%d)", code, dead)
	}
	entries, err := a.host.Census(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.ChannelID == id {
			t.Fatalf("compensated channel remains in census: %v", entries)
		}
	}
}

func TestLifecycleClaimAlternatesKindsAndWrapsEachCursor(t *testing.T) {
	a, _ := newLifecycleTestApp(t, 0)
	now := time.Now().UnixMilli()
	for _, values := range []struct {
		table, operation, channel string
	}{
		{"channel_provision_jobs", "lc:p1", "p1"},
		{"channel_provision_jobs", "lc:p2", "p2"},
		{"channel_destroy_jobs", "lc:d1", "d1"},
		{"channel_destroy_jobs", "lc:d2", "d2"},
	} {
		if values.table == "channel_provision_jobs" {
			spec, _ := json.Marshal(channelhost.ProvisionSpec{ChannelID: channel.ID(values.channel), Type: "group", OwnerPrincipal: "owner", CreatedAt: now})
			if _, err := a.db.Exec(`INSERT INTO channel_provision_jobs(operation_id,channel_id,requested_by,name,type,owner_principal,spec_json,created_at) VALUES (?,?,?,?,?,?,?,?)`, values.operation, values.channel, "owner", values.channel, "group", "owner", string(spec), now); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, err := a.db.Exec(`INSERT INTO channel_destroy_jobs(operation_id,channel_id,requested_by,created_at) VALUES (?,?,?,?)`, values.operation, values.channel, "owner", now); err != nil {
			t.Fatal(err)
		}
	}
	w := newLifecycleWorker(a)
	got := make([]string, 0, 6)
	for range 6 {
		kind, id, ok := w.next()
		if !ok {
			t.Fatal("claim ring unexpectedly empty")
		}
		got = append(got, fmt.Sprintf("%s:%d", kind, id))
	}
	want := []string{"provision:1", "destroy:1", "provision:2", "destroy:2", "provision:1", "destroy:1"}
	if !slices.Equal(got, want) {
		t.Fatalf("claim order=%v want %v", got, want)
	}
}

func TestLifecycleClaimExcludesCompensatingProvision(t *testing.T) {
	a, _ := newLifecycleTestApp(t, 0)
	now := time.Now().UnixMilli()
	if _, err := a.db.Exec(`INSERT INTO channel_destroy_jobs(operation_id,channel_id,requested_by,created_at) VALUES ('lc:d','c','owner',?)`, now); err != nil {
		t.Fatal(err)
	}
	spec, _ := json.Marshal(channelhost.ProvisionSpec{ChannelID: "c", Type: "group", OwnerPrincipal: "owner", CreatedAt: now})
	if _, err := a.db.Exec(`INSERT INTO channel_provision_jobs(operation_id,channel_id,requested_by,name,type,owner_principal,spec_json,compensation_job_id,created_at) VALUES ('lc:p','c','owner','c','group','owner',?,1,?)`, string(spec), now); err != nil {
		t.Fatal(err)
	}
	w := newLifecycleWorker(a)
	kind, id, ok := w.next()
	if !ok || kind != "destroy" || id != 1 {
		t.Fatalf("claim=(%q,%d,%v), want destroy compensation", kind, id, ok)
	}
}
