package home

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type pullTestResolver struct {
	mu      sync.Mutex
	facts   channel.DeclarationFacts
	err     error
	block   chan struct{}
	entered chan struct{}
}

func (r *pullTestResolver) ResolveDeclaration(context.Context, channel.ID, string) (channel.DeclarationFacts, error) {
	r.mu.Lock()
	facts, err := r.facts, r.err
	block, entered := r.block, r.entered
	r.block, r.entered = nil, nil
	if entered != nil {
		close(entered)
	}
	r.mu.Unlock()
	if block != nil {
		<-block
	}
	return facts, err
}

func (r *pullTestResolver) ClassKind(_ context.Context, class string) (actor.Kind, bool, error) {
	if class == "pull-tool" {
		return actor.KindTool, true, nil
	}
	return actor.KindAgent, true, nil
}

func (r *pullTestResolver) DaemonFacts(context.Context, string) (channel.DaemonFacts, error) {
	return channel.DaemonFacts{}, nil
}

func (r *pullTestResolver) set(facts channel.DeclarationFacts, err error) {
	r.mu.Lock()
	r.facts, r.err = facts, err
	r.mu.Unlock()
}

func (r *pullTestResolver) blockNext() (<-chan struct{}, chan<- struct{}) {
	r.mu.Lock()
	r.block = make(chan struct{})
	r.entered = make(chan struct{})
	block, entered := r.block, r.entered
	r.mu.Unlock()
	return entered, block
}

func openPullTestHome(t *testing.T, resolver *pullTestResolver) *Home {
	h, _ := openPullTestHomeAt(t, resolver)
	return h
}

func openPullTestHomeAt(t *testing.T, resolver *pullTestResolver) (*Home, string) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
	h, err := Open(Config{
		ChannelID: "declaration-pull", DBPath: dbPath, Bootstrap: true,
		CompositionResolver: emptyCompositionResolver{}, IntroductionResolver: resolver, ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	h.reconcileStop()
	<-h.reconcileDone
	t.Cleanup(func() { _ = h.closeInternal("test") })
	return h, dbPath
}

func declarePullActor(t *testing.T, h *Home, source string, config string, placement storespec.Placement, idle time.Duration, createdAt int64) storespec.ActorControlRow {
	t.Helper()
	raw := json.RawMessage(config)
	result, err := h.declare(context.Background(), DeclareRequest{
		SourceDeclID: source, Kind: actor.KindAgent, Class: "pull-agent", Config: &raw,
		Placement: placement, TIdle: idle.Milliseconds(), CreatedAt: createdAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result.Row
}

func TestDeclarationPullConvergesAndPreservesChannelFields(t *testing.T) {
	ctx := context.Background()
	resolver := &pullTestResolver{facts: channel.DeclarationFacts{Class: "pull-agent", Config: json.RawMessage(`{"value":"a"}`)}}
	h := openPullTestHome(t, resolver)
	owner, err := h.admitChannelOwner(ctx, "owner")
	if err != nil {
		t.Fatal(err)
	}
	placement, _ := storespec.NewDaemonPlacement("daemon-a")
	row := declarePullActor(t, h, "decl-a", `{"value":"a"}`, placement, 7*time.Second, 10)

	resolver.set(channel.DeclarationFacts{Class: "pull-agent", Config: json.RawMessage(`{"value":"b"}`)}, nil)
	h.reconcileDeclarations(ctx)
	current, active, err := h.controlIndex.LookupActive(ctx, row.ID)
	if err != nil || !active || current.CurrentDeclVersion != 2 || string(current.Config) != `{"value":"b"}` || current.Placement != placement || current.TIdle != 7*time.Second {
		t.Fatalf("A→B row=(%+v,%v,%v)", current, active, err)
	}
	resolver.set(channel.DeclarationFacts{Class: "pull-agent", Config: json.RawMessage(`{"value":"a"}`)}, nil)
	h.reconcileDeclarations(ctx)
	current, _, _ = h.controlIndex.LookupActive(ctx, row.ID)
	if current.CurrentDeclVersion != 3 || string(current.Config) != `{"value":"a"}` {
		t.Fatalf("B→A row=%+v", current)
	}
	h.reconcileDeclarations(ctx)
	equal, _, _ := h.controlIndex.LookupActive(ctx, row.ID)
	if equal.CurrentDeclVersion != 3 {
		t.Fatalf("equal pull wrote version %d", equal.CurrentDeclVersion)
	}

	if human, active, err := h.controlIndex.LookupActive(ctx, owner); err != nil || !active || human.SourceDeclID != "" || human.CurrentDeclVersion != 1 {
		t.Fatalf("human was pulled: row=%+v active=%v err=%v", human, active, err)
	}
}

func TestDeclarationPullEqualRepublishesCommittedValue(t *testing.T) {
	ctx := context.Background()
	resolver := &pullTestResolver{facts: channel.DeclarationFacts{Class: "pull-agent", Config: json.RawMessage(`{"value":"b"}`)}}
	h := openPullTestHome(t, resolver)
	row := declarePullActor(t, h, "decl-republish", `{"value":"a"}`, storespec.NewServerPlacement(), 0, 10)

	request := struct {
		ActorID actor.ActorID   `json:"actor_id"`
		DeclID  string          `json:"decl_id"`
		Class   string          `json:"class"`
		Config  json.RawMessage `json:"config"`
	}{row.ID, "decl-republish", "pull-agent", json.RawMessage(`{"value":"b"}`)}
	meta, err := systemMeta("test:committed-before-publish", request)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := h.opEntry.sync.ApplyResolvedDeclaration(ctx, storespec.DeclarationSyncTx{
		SysOpMeta: meta, ActorID: row.ID, DeclID: request.DeclID, Class: request.Class, Config: request.Config,
	})
	if err != nil || committed.Status != storespec.DeclarationApplied {
		t.Fatalf("direct commit=(%+v,%v)", committed, err)
	}
	if stale, _, _ := h.controlIndex.LookupActive(ctx, row.ID); stale.CurrentDeclVersion != 1 {
		t.Fatalf("test did not preserve missed-publication image: %+v", stale)
	}

	equal, err := h.opEntry.applyResolvedDeclaration(ctx, row.ID, request.DeclID, request.Class, request.Config)
	if err != nil || equal.Status != storespec.DeclarationEqual {
		t.Fatalf("equal pull=(%+v,%v)", equal, err)
	}
	got, active, err := h.controlIndex.LookupActive(ctx, row.ID)
	if err != nil || !active || got.CurrentDeclVersion != 2 || string(got.Config) != `{"value":"b"}` {
		t.Fatalf("equal pull did not republish: row=%+v active=%v err=%v", got, active, err)
	}
}

func TestDeclarationPullSkipsResolverFailureAbsenceAndKindMismatch(t *testing.T) {
	ctx := context.Background()
	resolver := &pullTestResolver{facts: channel.DeclarationFacts{Class: "pull-agent", Config: json.RawMessage(`{"value":"a"}`)}}
	h := openPullTestHome(t, resolver)
	row := declarePullActor(t, h, "decl-skip", `{"value":"a"}`, storespec.NewServerPlacement(), 0, 10)

	for _, failure := range []error{errors.New("realm unavailable"), channel.ErrDeclarationNotFound} {
		resolver.set(channel.DeclarationFacts{}, failure)
		h.reconcileDeclarations(ctx)
		got, _, _ := h.controlIndex.LookupActive(ctx, row.ID)
		if got.CurrentDeclVersion != 1 {
			t.Fatalf("resolver failure %v mutated version to %d", failure, got.CurrentDeclVersion)
		}
	}
	resolver.set(channel.DeclarationFacts{Class: "pull-tool", Config: json.RawMessage(`{"value":"wrong-kind"}`)}, nil)
	h.reconcileDeclarations(ctx)
	got, _, _ := h.controlIndex.LookupActive(ctx, row.ID)
	if got.CurrentDeclVersion != 1 {
		t.Fatalf("kind mismatch mutated version to %d", got.CurrentDeclVersion)
	}
}

func TestDeclarationPullAttemptCannotCrossActorLifetime(t *testing.T) {
	ctx := context.Background()
	resolver := &pullTestResolver{facts: channel.DeclarationFacts{Class: "pull-agent", Config: json.RawMessage(`{"value":"a"}`)}}
	h := openPullTestHome(t, resolver)
	first := declarePullActor(t, h, "decl-aba", `{"value":"a"}`, storespec.NewServerPlacement(), 0, 10)
	resolver.set(channel.DeclarationFacts{Class: "pull-agent", Config: json.RawMessage(`{"value":"b"}`)}, nil)
	entered, release := resolver.blockNext()
	done := make(chan struct{})
	go func() {
		h.reconcileDeclarations(ctx)
		close(done)
	}()
	<-entered
	if err := removeThroughSysOp(h, ctx, first.ID); err != nil {
		t.Fatal(err)
	}
	second := declarePullActor(t, h, "decl-aba", `{"value":"a"}`, storespec.NewServerPlacement(), 0, 20)
	if second.ID == first.ID {
		t.Fatal("reintroduction reused ActorID")
	}
	close(release)
	<-done
	got, active, err := h.controlIndex.LookupActive(ctx, second.ID)
	if err != nil || !active || got.CurrentDeclVersion != 1 || string(got.Config) != `{"value":"a"}` {
		t.Fatalf("old attempt crossed lifetime: row=%+v active=%v err=%v", got, active, err)
	}
	h.reconcileDeclarations(ctx)
	got, _, _ = h.controlIndex.LookupActive(ctx, second.ID)
	if got.CurrentDeclVersion != 2 || string(got.Config) != `{"value":"b"}` {
		t.Fatalf("new lifetime did not converge: %+v", got)
	}
}

func TestDeclarationPullRecomposesLatestPlacementAndIdleInsideGate(t *testing.T) {
	ctx := context.Background()
	resolver := &pullTestResolver{facts: channel.DeclarationFacts{Class: "pull-agent", Config: json.RawMessage(`{"value":"a"}`)}}
	h, dbPath := openPullTestHomeAt(t, resolver)
	placementA, _ := storespec.NewDaemonPlacement("daemon-a")
	row := declarePullActor(t, h, "decl-fields", `{"value":"a"}`, placementA, time.Second, 10)
	resolver.set(channel.DeclarationFacts{Class: "pull-agent", Config: json.RawMessage(`{"value":"b"}`)}, nil)
	entered, release := resolver.blockNext()
	done := make(chan struct{})
	go func() {
		h.reconcileDeclarations(ctx)
		close(done)
	}()
	<-entered
	placementB, _ := storespec.NewDaemonPlacement("daemon-b")
	// Simulate an independent channel-owned placement/idle transaction while
	// realm resolution is in flight. The pull arm must re-read this current row
	// inside its gate and compose only class/config over it.
	db, err := sql.Open("sqlite", "file:"+dbPath+"?mode=rw&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err == nil {
		_, err = tx.ExecContext(ctx, `INSERT INTO actor_decl_versions
			(actor_id,version,class,config_json,placement,desired_host,t_idle_ms,created_at)
			SELECT actor_id,2,class,config_json,?,?,?,20 FROM actor_decl_versions
			WHERE actor_id=? AND version=1`, string(placementB.Kind), placementB.Host, int64(9000), string(row.ID))
	}
	if err == nil {
		_, err = tx.ExecContext(ctx, `UPDATE actor_registry SET current_decl_version=2 WHERE actor_id=?`, string(row.ID))
	}
	if err == nil {
		err = tx.Commit()
	} else if tx != nil {
		_ = tx.Rollback()
	}
	_ = db.Close()
	if err != nil {
		t.Fatal(err)
	}
	channelRow, active, err := h.cs.Declared.LookupDeclaredActive(ctx, row.ID)
	if err != nil || !active || !h.controlIndex.UpsertBatch([]controlEntry{{Row: channelRow, World: storespec.WorldDurable}}) {
		t.Fatalf("publish concurrent channel fields: row=%+v active=%v err=%v", channelRow, active, err)
	}
	close(release)
	<-done
	got, active, err := h.controlIndex.LookupActive(ctx, row.ID)
	if err != nil || !active || string(got.Config) != `{"value":"b"}` || got.Placement != placementB || got.TIdle != 9*time.Second {
		t.Fatalf("pull overwrote latest channel fields: row=%+v active=%v err=%v", got, active, err)
	}
}
