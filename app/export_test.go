package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/app/contract"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// errTestChannelNotLoaded stands in for a torn-down home in the test seams below.
var errTestChannelNotLoaded = errors.New("app: channel not loaded")

func (a *App) DefaultAgentForTest(chID channel.ID) (actor.ActorID, bool, error) {
	bundle, ok := a.host.Acquire(chID)
	if !ok {
		return "", false, errTestChannelNotLoaded
	}
	return bundle.View().DefaultAgent(context.Background())
}

func (a *App) StableBootstrapCodexDeclarationForTest(owner string) (contract.Declaration, bool, error) {
	id := stableBootstrapCodexDeclID(owner)
	var d contract.Declaration
	var config string
	var deleted sql.NullInt64
	err := a.db.QueryRow(`SELECT id,name,owner,default_class,config_json,visibility,created_at,deleted_at FROM actor_decls WHERE id=?`, id).
		Scan(&d.ID, &d.Name, &d.Owner, &d.Class, &config, &d.Visibility, &d.CreatedAt, &deleted)
	if errors.Is(err, sql.ErrNoRows) {
		return contract.Declaration{}, false, nil
	}
	if err != nil {
		return contract.Declaration{}, false, err
	}
	return d, !deleted.Valid, nil
}

func (a *App) BootstrapCodexDeclarationCountForTest(owner string) (int, error) {
	var count int
	err := a.db.QueryRow(`SELECT COUNT(*) FROM actor_decls WHERE owner=? AND default_class='codex' AND deleted_at IS NULL`, owner).Scan(&count)
	return count, err
}

func declaredInstanceOneForTest(ctx context.Context, view channelhost.View, source string) (actor.ActorID, bool, error) {
	ids, err := view.DeclaredInstances(ctx, source)
	if err != nil || len(ids) == 0 {
		return "", false, err
	}
	return ids[0], true, nil
}

// DeclaredInstancesForTest asks the membrane's declaration-instance question.
func (a *App) DeclaredInstancesForTest(chID channel.ID, declID string) ([]actor.ActorID, error) {
	bundle, ok := a.host.Acquire(chID)
	if !ok {
		return nil, errTestChannelNotLoaded
	}
	return bundle.View().DeclaredInstances(context.Background(), declID)
}

// ActorFactsForTest asks the membrane's identity-fact question.
func (a *App) ActorFactsForTest(chID channel.ID, id actor.ActorID) (channelspec.ActorFacts, bool, error) {
	bundle, ok := a.host.Acquire(chID)
	if !ok {
		return channelspec.ActorFacts{}, false, errTestChannelNotLoaded
	}
	return bundle.View().ActorFacts(context.Background(), id)
}

func (a *App) DaemonBoundForTest(chID channel.ID, daemonID string) (bool, error) {
	bundle, ok := a.host.Acquire(chID)
	if !ok {
		return false, errTestChannelNotLoaded
	}
	return bundle.View().IsBound(context.Background(), daemonID)
}

// HumanRosterForTest asks the membrane's entitlement projection.
func (a *App) HumanRosterForTest(chID channel.ID) ([]channelspec.HumanRosterEntry, error) {
	bundle, ok := a.host.Acquire(chID)
	if !ok {
		return nil, errTestChannelNotLoaded
	}
	return bundle.View().HumanRoster(context.Background())
}

// ResolvedDeclarationForTest is the app-side half of declaration convergence:
// exactly what the runtime declaration pull loop reads for this channel and
// declaration. The runtime-side half (apply / equal-value no-op) is proven in
// platform/home, where the Controller projection lives.
func (a *App) ResolvedDeclarationForTest(
	ctx context.Context,
	chID channel.ID,
	declID string,
) (channelspec.DeclarationFacts, error) {
	return compositionResolver{app: a}.ResolveDeclaration(ctx, chID, declID)
}

func (a *App) MessagesForTest(chID channel.ID) ([]storespec.StoredRow, error) {
	bundle, ok := a.host.Acquire(chID)
	if !ok {
		return nil, errTestChannelNotLoaded
	}
	roster, err := bundle.View().HumanRoster(context.Background())
	if err != nil {
		return nil, err
	}
	for _, entry := range roster {
		rows, _, err := bundle.View().ReadVisibleAfterSeq(context.Background(), channel.Reader{
			ActorID: entry.ActorID, Mode: channel.ReaderMember,
		}, 0, 1000)
		return rows, err
	}
	return nil, errors.New("app: test channel has no active human reader")
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

// DropHomeForTest closes the borrowed serving handle while retaining the space
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
	result, err := bundle.SysOp().Admit(context.Background(), channelspec.AdmitRequest{Ref: "test-admit:" + uuid.NewString(), Principal: principal})
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
	initiator, found, err := bundle.View().ResolvePrincipal(context.Background(), owner)
	if err != nil || !found {
		return "", fmt.Errorf("resolve owner actor: found=%v err=%v", found, err)
	}
	result, err := bundle.SysOp().Introduce(context.Background(), channelspec.IntroduceRequest{
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

func (a *App) ResolvePrincipalForTest(chID string, principal string) (actor.ActorID, error) {
	bundle, ok := a.host.Acquire(channel.ID(chID))
	if !ok {
		return "", errTestChannelNotLoaded
	}
	id, ok, err := bundle.View().ResolvePrincipal(context.Background(), principal)
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
	id, found, err := declaredInstanceOneForTest(context.Background(), bundle.View(), source)
	if err != nil {
		return "", err
	}
	if found {
		return id, nil
	}
	return "", fmt.Errorf("declaration source not found")
}

// CloseHomeForTest leaves the closed handle published in the app map so a
// post-commit daemon-obligation read deterministically returns ErrClosed.
func (a *App) CloseHomeForTest(chID channel.ID) error {
	if _, ok := a.host.Acquire(chID); !ok {
		return errTestChannelNotLoaded
	}
	return a.host.Destroy(context.Background(), chID)
}

func (a *App) RemoveSpaceToolForTest(chID channel.ID) error {
	bundle, ok := a.host.Acquire(chID)
	if !ok {
		return errTestChannelNotLoaded
	}
	target, found, err := declaredInstanceOneForTest(context.Background(), bundle.View(), spaceToolDeclID)
	if err != nil || !found {
		return err
	}
	owner, found, err := bundle.View().OwnerPrincipal(context.Background())
	if err != nil || !found {
		return err
	}
	initiator, found, err := bundle.View().ResolvePrincipal(context.Background(), owner)
	if err != nil || !found {
		return err
	}
	_, err = bundle.SysOp().Remove(context.Background(), channelspec.RemoveRequest{
		Ref: "test-remove-space-tool:" + uuid.NewString(), Target: target, InitiatorActorID: initiator,
	})
	return err
}

// CreateHalfBuiltChannelForTest accepts a desired row without converging its
// local image, modelling the ordinary post-acceptance build window.
func (a *App) CreateHalfBuiltChannelForTest(ownerPrincipal, name string) (string, error) {
	chID := uuid.NewString()
	now := time.Now().UnixMilli()
	spec, err := json.Marshal(channelhost.ProvisionSpec{
		ChannelID: channel.ID(chID), Type: "group",
		OwnerPrincipal: ownerPrincipal, CreatedAt: now,
	})
	if err != nil {
		return "", err
	}
	if _, err := a.db.ExecContext(context.Background(),
		`INSERT INTO channels (id,name,type,status,owner_principal,spec_json,created_at,parent_id)
		VALUES (?,?,?,'present',?,?,?,NULL)`,
		chID, name, "group", ownerPrincipal, string(spec), now); err != nil {
		return "", err
	}
	return chID, nil
}

// CreateDaemonRowForTest mints an ordinary daemon row through the same core
// the API handler uses — provisioning tests use it to plant a name-colliding
// decoy device.
func (a *App) CreateDaemonRowForTest(ctx context.Context, ownerID, name string) (string, string, error) {
	return a.createDaemonRow(ctx, ownerID, name)
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
