package app

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	relationstore "github.com/wanpengxie/atoll/app/internal/relation"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type countedHost struct {
	channelhost.LocalHost
	mu                  sync.Mutex
	provisions          int
	destroys            int
	opens               int
	failDestroys        int
	destroyErr          error
	openErr             error
	beforeDestroyReturn func()
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
	if h.destroyErr != nil {
		err := h.destroyErr
		h.mu.Unlock()
		return err
	}
	if h.failDestroys > 0 {
		h.failDestroys--
		h.mu.Unlock()
		return errors.New("injected destroy failure")
	}
	beforeReturn := h.beforeDestroyReturn
	h.mu.Unlock()
	err := h.LocalHost.Destroy(ctx, id)
	if err == nil && beforeReturn != nil {
		beforeReturn()
	}
	return err
}

func (h *countedHost) Open(ctx context.Context, spec channelhost.OpenSpec) error {
	h.mu.Lock()
	h.opens++
	if h.openErr != nil {
		err := h.openErr
		h.mu.Unlock()
		return err
	}
	h.mu.Unlock()
	return h.LocalHost.Open(ctx, spec)
}

func newLifecycleTestApp(t *testing.T) (*App, *countedHost) {
	t.Helper()
	root := t.TempDir()
	db, err := openTestAppDB(t, filepath.Join(root, "app.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	a := &App{
		db: db, logger: slog.New(slog.DiscardHandler),
		daemonLocks: newKeyedLockSet(), channelLocks: newKeyedLockSet(),
	}
	a.relations = relationstore.New(db)
	real, err := channelhost.New(filepath.Join(root, "channels"), channelhost.HomeDeps{
		CompositionResolver:  compositionResolver{app: a},
		IntroductionResolver: compositionResolver{app: a},
		Logger:               a.logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	host := &countedHost{LocalHost: real}
	a.host = host
	t.Cleanup(func() { _ = real.Close(context.Background()) })
	return a, host
}

func desiredFixture(t *testing.T, name, owner string, parent *string) desiredChannel {
	t.Helper()
	id := channel.ID(uuid.NewString())
	now := time.Now().UnixMilli()
	spec := channelhost.ProvisionSpec{
		ChannelID: id, Type: "group", OwnerPrincipal: owner, CreatedAt: now,
	}
	if parent != nil {
		spec.Origin = &channelhost.Origin{
			ParentChannelID: channel.ID(*parent), InitiatorPrincipal: owner,
		}
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	return desiredChannel{
		ID: id, Name: name, Type: "group", Status: "present",
		Owner: owner, SpecJSON: string(raw), Created: now,
		Parent: nullableParent(parent),
	}
}

func insertDesired(t *testing.T, a *App, desired desiredChannel) {
	t.Helper()
	if _, err := a.db.Exec(`INSERT INTO channels(
		id,name,type,status,owner_principal,spec_json,created_at,parent_id)
		VALUES (?,?,?,?,?,?,?,?)`,
		desired.ID, desired.Name, desired.Type, desired.Status, desired.Owner,
		desired.SpecJSON, desired.Created, nullStringValue(desired.Parent)); err != nil {
		t.Fatal(err)
	}
}

func TestCreateAcceptancePredicateAndNameRelease(t *testing.T) {
	a, _ := newLifecycleTestApp(t)
	ctx := context.Background()
	first := desiredFixture(t, "same", "owner", nil)
	got, changed, conflict, parentMissing, err := a.acceptCreateChannel(ctx, first)
	if err != nil || !changed || conflict || parentMissing || got.ID != first.ID {
		t.Fatalf("first acceptance=(%+v,%v,%v,%v,%v)", got, changed, conflict, parentMissing, err)
	}
	replay := desiredFixture(t, "same", "owner", nil)
	got, changed, conflict, _, err = a.acceptCreateChannel(ctx, replay)
	if err != nil || changed || conflict || got.ID != first.ID {
		t.Fatalf("replay=(%+v,%v,%v,%v)", got, changed, conflict, err)
	}
	other := desiredFixture(t, "same", "other", nil)
	_, _, conflict, _, err = a.acceptCreateChannel(ctx, other)
	if err != nil || !conflict {
		t.Fatalf("different intent conflict=%v err=%v", conflict, err)
	}
	if _, err := a.db.Exec(`UPDATE channels SET status='retiring' WHERE id=?`, first.ID); err != nil {
		t.Fatal(err)
	}
	replacement := desiredFixture(t, "same", "owner", nil)
	got, changed, conflict, _, err = a.acceptCreateChannel(ctx, replacement)
	if err != nil || !changed || conflict || got.ID != replacement.ID {
		t.Fatalf("retiring name replacement=(%+v,%v,%v,%v)", got, changed, conflict, err)
	}
}

func TestCreateAcceptanceRejectsRetiringParent(t *testing.T) {
	a, _ := newLifecycleTestApp(t)
	parent := desiredFixture(t, "parent", "owner", nil)
	parent.Status = "retiring"
	insertDesired(t, a, parent)
	raw := string(parent.ID)
	child := desiredFixture(t, "child", "owner", &raw)
	_, changed, conflict, parentMissing, err := a.acceptCreateChannel(context.Background(), child)
	if err != nil || changed || conflict || !parentMissing {
		t.Fatalf("child acceptance=(%v,%v,%v,%v)", changed, conflict, parentMissing, err)
	}
}

func TestConcurrentCreateClassifiesWinner(t *testing.T) {
	for _, test := range []struct {
		name       string
		otherOwner string
		wantReplay bool
	}{
		{name: "same intent", otherOwner: "owner", wantReplay: true},
		{name: "different intent", otherOwner: "other", wantReplay: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			a, _ := newLifecycleTestApp(t)
			first := desiredFixture(t, "race", "owner", nil)
			second := desiredFixture(t, "race", test.otherOwner, nil)
			type result struct {
				changed, conflict bool
				err               error
			}
			start := make(chan struct{})
			results := make(chan result, 2)
			for _, desired := range []desiredChannel{first, second} {
				go func(d desiredChannel) {
					<-start
					a.createMu.Lock()
					_, changed, conflict, _, err := a.acceptCreateChannel(context.Background(), d)
					a.createMu.Unlock()
					results <- result{changed: changed, conflict: conflict, err: err}
				}(desired)
			}
			close(start)
			one, two := <-results, <-results
			if one.err != nil || two.err != nil {
				t.Fatalf("concurrent create errors: %v %v", one.err, two.err)
			}
			created := 0
			conflicts := 0
			replays := 0
			for _, got := range []result{one, two} {
				if got.changed {
					created++
				} else if got.conflict {
					conflicts++
				} else {
					replays++
				}
			}
			if created != 1 || (test.wantReplay && replays != 1) || (!test.wantReplay && conflicts != 1) {
				t.Fatalf("created=%d replay=%d conflicts=%d", created, replays, conflicts)
			}
		})
	}
}

func TestLifecycleConvergesPresentAndRetiring(t *testing.T) {
	a, host := newLifecycleTestApp(t)
	desired := desiredFixture(t, "lifecycle", "owner", nil)
	insertDesired(t, a, desired)
	a.convergeChannel(context.Background(), desired.ID)
	if _, ok := host.Acquire(desired.ID); !ok {
		t.Fatal("present desired row did not become serving")
	}
	if _, err := a.db.Exec(`UPDATE channels SET status='retiring' WHERE id=?`, desired.ID); err != nil {
		t.Fatal(err)
	}
	a.convergeChannel(context.Background(), desired.ID)
	var exists bool
	if err := a.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM channels WHERE id=?)`, desired.ID).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("retiring row survived confirmed physical destruction")
	}
}

func TestLifecycleRetiringDeleteIsConditional(t *testing.T) {
	a, host := newLifecycleTestApp(t)
	desired := desiredFixture(t, "conditional-retire", "owner", nil)
	desired.Status = "retiring"
	insertDesired(t, a, desired)
	host.beforeDestroyReturn = func() {
		if _, err := a.db.Exec(`UPDATE channels SET status='present' WHERE id=?`, desired.ID); err != nil {
			t.Errorf("restore desired status: %v", err)
		}
	}
	a.convergeChannel(context.Background(), desired.ID)
	var status string
	if err := a.db.QueryRow(`SELECT status FROM channels WHERE id=?`, desired.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "present" {
		t.Fatalf("conditional cleanup removed or rewrote a renewed desired row: status=%q", status)
	}
}

func TestLifecycleUnknownDestroyKeepsDesiredRow(t *testing.T) {
	a, host := newLifecycleTestApp(t)
	desired := desiredFixture(t, "unknown-destroy", "owner", nil)
	desired.Status = "retiring"
	insertDesired(t, a, desired)
	host.failDestroys = 1
	a.convergeChannel(context.Background(), desired.ID)
	var status string
	if err := a.db.QueryRow(`SELECT status FROM channels WHERE id=?`, desired.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "retiring" {
		t.Fatalf("status=%q", status)
	}
}

func TestLifecycleUnknownFailureRetriesOnLightScan(t *testing.T) {
	a, host := newLifecycleTestApp(t)
	desired := desiredFixture(t, "light-retry", "owner", nil)
	desired.Status = "retiring"
	insertDesired(t, a, desired)
	host.failDestroys = 1
	worker := newLifecycleWorker(a)
	a.lifecycle = worker
	worker.notify(desired.ID)
	worker.lightScan()
	time.Sleep(lifecycleTick + 25*time.Millisecond)
	worker.lightScan()
	var exists bool
	if err := a.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM channels WHERE id=?)`, desired.ID).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("unknown failure was not retried by the targeted light scan")
	}
}

func TestLifecyclePermanentDestroyStopsRetryAndKeepsRow(t *testing.T) {
	a, host := newLifecycleTestApp(t)
	desired := desiredFixture(t, "permanent-destroy", "owner", nil)
	desired.Status = "retiring"
	insertDesired(t, a, desired)
	host.destroyErr = channelhost.ErrInvalidChannelID
	a.lifecycle = newLifecycleWorker(a)
	a.convergeChannel(context.Background(), desired.ID)
	a.convergeChannel(context.Background(), desired.ID)
	host.mu.Lock()
	calls := host.destroys
	host.mu.Unlock()
	if calls != 1 {
		t.Fatalf("permanent destroy calls=%d want 1", calls)
	}
	var exists bool
	if err := a.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM channels WHERE id=?)`, desired.ID).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("permanent destroy failure deleted desired row")
	}
}

func TestLifecycleFullScanDestroysOrphanImage(t *testing.T) {
	a, host := newLifecycleTestApp(t)
	spec := desiredFixture(t, "orphan", "owner", nil)
	var provision channelhost.ProvisionSpec
	if err := json.Unmarshal([]byte(spec.SpecJSON), &provision); err != nil {
		t.Fatal(err)
	}
	if _, err := host.Provision(context.Background(), provision); err != nil {
		t.Fatal(err)
	}
	worker := newLifecycleWorker(a)
	a.lifecycle = worker
	worker.fullScan()
	entries, err := host.Census(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.ChannelID == spec.ID {
			t.Fatalf("orphan remains in census: %+v", entries)
		}
	}
}

// Open-first is the arm's data-safety order: a serving channel is never
// re-provisioned (Provision clears unpublished images behind its guards), so
// a revisit must observe via Open only.
func TestLifecycleServingChannelIsNeverReprovisioned(t *testing.T) {
	a, host := newLifecycleTestApp(t)
	desired := desiredFixture(t, "open-first", "owner", nil)
	insertDesired(t, a, desired)
	a.convergeChannel(context.Background(), desired.ID)
	host.mu.Lock()
	afterFirst := host.provisions
	host.mu.Unlock()
	if afterFirst != 1 {
		t.Fatalf("first convergence provisions=%d want 1", afterFirst)
	}
	a.convergeChannel(context.Background(), desired.ID)
	host.mu.Lock()
	afterSecond := host.provisions
	host.mu.Unlock()
	if afterSecond != afterFirst {
		t.Fatalf("serving channel was re-provisioned: %d -> %d", afterFirst, afterSecond)
	}
}

// Permanent open failures arrive Join-wrapped from the host, so the
// classification must hold through errors.Is: no provision attempt, no
// further retries, desired row kept for a human.
func TestLifecyclePermanentOpenFailureStopsRetryAndKeepsRow(t *testing.T) {
	a, host := newLifecycleTestApp(t)
	desired := desiredFixture(t, "permanent-open", "owner", nil)
	insertDesired(t, a, desired)
	host.openErr = errors.Join(channelhost.ErrSchemaIncompatible, errors.New("genesis type drift"))
	a.lifecycle = newLifecycleWorker(a)
	a.convergeChannel(context.Background(), desired.ID)
	a.convergeChannel(context.Background(), desired.ID)
	host.mu.Lock()
	opens, provisions := host.opens, host.provisions
	host.mu.Unlock()
	if provisions != 0 {
		t.Fatalf("permanent open failure still provisioned %d times (destructive-rebuild face)", provisions)
	}
	if opens != 1 {
		t.Fatalf("permanent open failure was retried: opens=%d want 1", opens)
	}
	// The discriminating assertion: a deferred (backoff) classification also
	// blocks the second converge within the tick window, so only the stopped
	// mark separates "permanent" from "will retry later".
	if !a.lifecycle.stopped(desired.ID) {
		t.Fatal("Join-wrapped ErrSchemaIncompatible was not classified permanent")
	}
	var exists bool
	if err := a.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM channels WHERE id=?)`, desired.ID).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("permanent open failure removed desired row")
	}
}
