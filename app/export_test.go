package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// errTestChannelNotLoaded stands in for a torn-down home in the test seams below.
var errTestChannelNotLoaded = errors.New("app: channel not loaded")

func (a *App) ActorsForTest(chID channel.ID) ([]storespec.Record, error) {
	bundle, ok := a.host.Acquire(chID)
	if !ok {
		return nil, errTestChannelNotLoaded
	}
	return bundle.View().ListActors(context.Background())
}

func (a *App) MessagesForTest(chID channel.ID) ([]storespec.StoredRow, error) {
	bundle, ok := a.host.Acquire(chID)
	if !ok {
		return nil, errTestChannelNotLoaded
	}
	actors, err := bundle.View().ActiveActors(context.Background())
	if err != nil {
		return nil, err
	}
	for _, row := range actors {
		facts, found, err := bundle.View().ActorFacts(context.Background(), row.ID)
		if err != nil {
			return nil, err
		}
		if found && facts.Kind == actor.KindHuman {
			rows, _, err := bundle.View().ReadVisibleAfterSeq(context.Background(), channel.Reader{
				ActorID: row.ID, Mode: channel.ReaderMember,
			}, 0, 1000)
			return rows, err
		}
	}
	return nil, errors.New("app: test channel has no active human reader")
}

func (a *App) PresenceForTest(chID channel.ID, id actor.ActorID) (member, known, online bool, err error) {
	bundle, ok := a.host.Acquire(chID)
	if !ok {
		return false, false, false, errTestChannelNotLoaded
	}
	snapshot, err := bundle.View().Snapshot(context.Background(), id)
	if err != nil {
		return false, false, false, err
	}
	testimony, known := snapshot.L3[actorrt.ObsKind(introspect.ObsDevicePresence)]
	if !known {
		return snapshot.Member, false, false, nil
	}
	p, ok := introspect.ParseDevicePresence(testimony.Val)
	return snapshot.Member, ok, ok && p.Online, nil
}

func (a *App) PlanForDaemonForTest(chID channel.ID, daemonID string) ([]platform.PlanActor, error) {
	bundle, ok := a.host.Acquire(chID)
	if !ok {
		return nil, errTestChannelNotLoaded
	}
	return bundle.Daemon().PlanForDaemon(context.Background(), daemonID)
}

func (a *App) SetDeclarationOverlayForTest(chID channel.ID, sourceID string, config json.RawMessage) (actor.ActorID, int64, error) {
	bundle, ok := a.host.Acquire(chID)
	if !ok {
		return "", 0, errTestChannelNotLoaded
	}
	rows, err := bundle.View().DeclaredBySource(context.Background(), sourceID)
	if err != nil || len(rows) == 0 {
		return "", 0, err
	}
	row := rows[0]
	canonical, err := channel.CanonicalJSON(config)
	if err != nil {
		return row.ID, 0, err
	}
	if _, err := a.db.Exec(`INSERT INTO channel_decl_overlays(channel_id,decl_id,config_json,updated_at)
		VALUES(?,?,?,?) ON CONFLICT(channel_id,decl_id) DO UPDATE SET config_json=excluded.config_json,updated_at=excluded.updated_at`,
		string(chID), sourceID, string(canonical), time.Now().UnixMilli()); err != nil {
		return row.ID, 0, err
	}
	a.host.Poke(chID)
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, latest, err := bundle.View().DeclarationVersions(context.Background(), row.ID)
		if err != nil {
			return row.ID, 0, err
		}
		if string(latest.Config) == string(canonical) {
			return row.ID, latest.CurrentDeclVersion, nil
		}
		if time.Now().After(deadline) {
			return row.ID, latest.CurrentDeclVersion, errors.New("declaration overlay did not converge")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Handler exposes the assembled gin engine as an http.Handler so black-box
// (package app_test) tests can drive the whole server through httptest without
// binding a port. It lives in export_test.go on purpose: compiled only under
// `go test`, it is a test-only seam and never reaches the production API surface
// (production serves via Run). Routes stay owned by the app; callers get a
// read-only handler.
func (a *App) Handler() http.Handler {
	return a.engine
}

// DropHomeForTest closes the borrowed serving handle while retaining the realm
// directory row, reproducing a channel-unavailable image.
func (a *App) DropHomeForTest(chID channel.ID) {
	_ = a.host.Destroy(context.Background(), chID)
}

// AdmitForTest admits id as a durable declared identity in chID's Home
// primitive an introduce door writes). Since the membrane law (v1.8 问①) stopped
// daemon attach from minting membership, a daemon-hosted actor must be admitted
// BEFORE its daemon declares it — this test seam stands in for the introduce door
// the daemon-attach live tests bypass. Test-only.
func (a *App) AdmitForTest(chID string, id actor.ActorID, kind actor.Kind) (actor.ActorID, error) {
	bundle, ok := a.host.Acquire(channel.ID(chID))
	if !ok {
		return "", errTestChannelNotLoaded
	}
	principal := string(id)
	if i := strings.IndexByte(principal, ':'); i >= 0 {
		principal = principal[i+1:]
	}
	if kind != actor.KindHuman {
		return "", fmt.Errorf("test admission supports human identities only")
	}
	result, err := bundle.SysOp().Admit(context.Background(), channel.AdmitRequest{Ref: "test-admit:" + uuid.NewString(), Principal: principal})
	return result.ActorID, err
}

func (a *App) ComposeDaemonForTest(chID, principal, class, daemonID string, kind actor.Kind) (actor.ActorID, error) {
	bundle, ok := a.host.Acquire(channel.ID(chID))
	if !ok {
		return "", errTestChannelNotLoaded
	}
	owner, found, err := bundle.View().OwnerPrincipal(context.Background())
	if err != nil || !found {
		return "", err
	}
	now := time.Now().UnixMilli()
	_, err = a.db.ExecContext(context.Background(), `INSERT OR IGNORE INTO actor_decls(id,name,owner,default_class,created_at,updated_at,visibility) VALUES (?,?,?,?,?,?,?)`, principal, principal, owner, class, now, now, "public")
	if err != nil {
		return "", err
	}
	initiator, found, err := bundle.View().ResolvePrincipal(context.Background(), actor.KindHuman, owner)
	if err != nil || !found {
		return "", fmt.Errorf("resolve owner actor: found=%v err=%v", found, err)
	}
	result, err := bundle.SysOp().Introduce(context.Background(), channel.IntroduceRequest{
		Ref: "test-introduce:" + uuid.NewString(), DeclID: principal, InitiatorActorID: initiator,
	})
	if err == nil {
		facts, found, factsErr := bundle.View().ActorFacts(context.Background(), result.ActorID)
		if factsErr != nil || !found || facts.Kind != kind {
			return "", fmt.Errorf("introduced kind mismatch: facts=%+v found=%v err=%v", facts, found, factsErr)
		}
	}
	return result.ActorID, err
}

func (a *App) ResolvePrincipalForTest(chID string, kind actor.Kind, principal string) (actor.ActorID, error) {
	bundle, ok := a.host.Acquire(channel.ID(chID))
	if !ok {
		return "", errTestChannelNotLoaded
	}
	id, ok, err := bundle.View().ResolvePrincipal(context.Background(), kind, principal)
	if err != nil {
		return "", err
	}
	if ok {
		return id, nil
	}
	return "", fmt.Errorf("principal not found")
}

func (a *App) ResolveSourceForTest(chID, source string) (actor.ActorID, error) {
	bundle, ok := a.host.Acquire(channel.ID(chID))
	if !ok {
		return "", errTestChannelNotLoaded
	}
	row, found, err := bundle.View().DeclaredBySourceOne(context.Background(), source)
	if err != nil {
		return "", err
	}
	if found {
		return row.ID, nil
	}
	return "", fmt.Errorf("declaration source not found")
}

// WaitLiveForTest polls chID's home until id has a live embodiment (View.Stat) or
// the timeout elapses — the async-embodiment counterpart of the old synchronous
// spawn: since composition is embodied by the reconcile ring (Admit + poke → sweep),
// a test that needs a live default floor before sending must wait for the sweep.
// Test-only.
func (a *App) WaitLiveForTest(chID string, id actor.ActorID, timeout time.Duration) bool {
	bundle, ok := a.host.Acquire(channel.ID(chID))
	if !ok {
		return false
	}
	deadline := time.Now().Add(timeout)
	for {
		if _, live := bundle.View().Stat(id); live {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// CloseHomeForTest leaves the closed handle published in the app map so a
// post-commit daemon-obligation read deterministically returns ErrClosed.
func (a *App) CloseHomeForTest(chID channel.ID) error {
	if _, ok := a.host.Acquire(chID); !ok {
		return errTestChannelNotLoaded
	}
	return a.host.Destroy(context.Background(), chID)
}

func (a *App) RemoveRealmToolForTest(chID channel.ID) error {
	bundle, ok := a.host.Acquire(chID)
	if !ok {
		return errTestChannelNotLoaded
	}
	row, found, err := bundle.View().DeclaredBySourceOne(context.Background(), realmToolDeclID)
	if err != nil || !found {
		return err
	}
	owner, found, err := bundle.View().OwnerPrincipal(context.Background())
	if err != nil || !found {
		return err
	}
	initiator, found, err := bundle.View().ResolvePrincipal(context.Background(), actor.KindHuman, owner)
	if err != nil || !found {
		return err
	}
	_, err = bundle.SysOp().Remove(context.Background(), channel.RemoveRequest{
		Ref: "test-remove-realm-tool:" + uuid.NewString(), Target: row.ID, InitiatorActorID: initiator,
	})
	return err
}

// CreateHalfBuiltChannelForTest creates a published directory row plus its
// provision intent but no local image, modelling a crash window.
func (a *App) CreateHalfBuiltChannelForTest(ownerPrincipal, name string) (string, error) {
	chID := uuid.NewString()
	now := time.Now().UnixMilli()
	if _, err := a.db.ExecContext(context.Background(),
		`INSERT INTO channels (id,name,type,created_at,parent_id) VALUES (?,?,?,?,NULL)`,
		chID, name, "group", now); err != nil {
		return "", err
	}
	if _, err := a.db.ExecContext(context.Background(), `INSERT INTO channel_provision_jobs
		(operation_id,channel_id,requested_by,name,type,owner_principal,spec_json,published_at,created_at)
		VALUES (?,?,?,?,?,?,?,?,?)`, "lc:test:"+chID, chID, ownerPrincipal, name, "group", ownerPrincipal, `{}`, now, now); err != nil {
		return "", err
	}
	return chID, nil
}

// StatForTest reads id's L1 embodiment (View.Stat) on chID's home — the axis
// presence must stay orthogonal to (层2 link来去不碰层1: startedAt stable across a
// ws reconnect, live throughout). Test-only.
func (a *App) StatForTest(chID channel.ID, id actor.ActorID) (startedAt time.Time, live bool) {
	bundle, ok := a.host.Acquire(chID)
	if !ok {
		return time.Time{}, false
	}
	return bundle.View().Stat(id)
}

// SetBcryptCostForTest drops the password work factor for test fixtures —
// under -race a DefaultCost hash+compare burns ~1.7s of pure CPU per
// register+login, which was the app suite's dominant cost. Returns a restore
// func for cleanup. Test-only seam; production always runs DefaultCost.
func SetBcryptCostForTest(cost int) (restore func()) {
	prev := bcryptCost
	bcryptCost = cost
	return func() { bcryptCost = prev }
}
