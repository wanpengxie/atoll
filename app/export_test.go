package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// errTestChannelNotLoaded stands in for a torn-down home in the test seams below.
var errTestChannelNotLoaded = errors.New("app: channel not loaded")

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
func (a *App) ActorFactsForTest(chID channel.ID, id actor.ActorID) (channel.ActorFacts, bool, error) {
	bundle, ok := a.host.Acquire(chID)
	if !ok {
		return channel.ActorFacts{}, false, errTestChannelNotLoaded
	}
	return bundle.View().ActorFacts(context.Background(), id)
}

// HumanRosterForTest asks the membrane's entitlement projection.
func (a *App) HumanRosterForTest(chID channel.ID) ([]channel.HumanRosterEntry, error) {
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
) (channel.DeclarationFacts, error) {
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
	initiator, found, err := bundle.View().ResolvePrincipal(context.Background(), owner)
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

func (a *App) RemoveRealmToolForTest(chID channel.ID) error {
	bundle, ok := a.host.Acquire(chID)
	if !ok {
		return errTestChannelNotLoaded
	}
	target, found, err := declaredInstanceOneForTest(context.Background(), bundle.View(), realmToolDeclID)
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
	_, err = bundle.SysOp().Remove(context.Background(), channel.RemoveRequest{
		Ref: "test-remove-realm-tool:" + uuid.NewString(), Target: target, InitiatorActorID: initiator,
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

// SetBcryptCostForTest drops the password work factor for test fixtures —
// under -race a DefaultCost hash+compare burns ~1.7s of pure CPU per
// register+login, which was the app suite's dominant cost. Returns a restore
// func for cleanup. Test-only seam; production always runs DefaultCost.
func SetBcryptCostForTest(cost int) (restore func()) {
	prev := bcryptCost
	bcryptCost = cost
	return func() { bcryptCost = prev }
}
