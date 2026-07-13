package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	platformhome "github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// errTestChannelNotLoaded stands in for a torn-down home in the test seams below.
var errTestChannelNotLoaded = errors.New("app: channel not loaded")

// SetControlShimTimeoutForTest overrides the channel-control HTTP shim's bounded
// wait so a test can exercise the timeout branch (202+request_id) deterministically
// (a near-zero timeout times out before the async door reply can commit). Test-only.
func (a *App) SetControlShimTimeoutForTest(d time.Duration) {
	a.controlShimTimeout = d
}

// OperateFaceForTest exposes the app's channel-operate executor so black-box
// tests can drive the four control verbs directly (as the sysactor gate would
// after authorising the sender), without the not-yet-built message senders
// (S5b/shims). Test-only.
func (a *App) OperateFaceForTest() platformhome.OperateExecutor {
	return a.operateFace()
}

// errTestSeedAdmitFail / errTestRevokeFail are the forced failures the injected
// seams raise in place of a real persist, to drive the rollback paths.
var (
	errTestSeedAdmitFail = errors.New("app: forced seed admit failure (test)")
	errTestRevokeFail    = errors.New("app: forced revoke persist failure (test)")
)

// SetSeedAdmitFailForTest installs (v=true) or clears (v=false) the injected
// seeding-Admit failure hook so a test can drive the create-channel transactional
// rollback (close home + delete channel row + 5xx). Test-only.
func (a *App) SetSeedAdmitFailForTest(v bool) {
	if v {
		a.seedAdmitFailHook = func() error { return errTestSeedAdmitFail }
		return
	}
	a.seedAdmitFailHook = nil
}

// SetRevokeFailForTest installs (v=true) or clears (v=false) the injected
// revocation-persist failure hook so a test can prove the daemon-delete handler
// rolls back and returns 5xx (not a false ok) when revocation does not reach
// durable storage. Test-only.
func (a *App) SetRevokeFailForTest(v bool) {
	if v {
		a.revokeFailHook = func() error { return errTestRevokeFail }
		return
	}
	a.revokeFailHook = nil
}

// SetHomeCloseHookForTest installs the failpoint immediately after a Home is detached
// from a.homes and a.mu is released, before its potentially blocking Close. op is one
// of "app-close", "delete-channel", or "create-rollback". Test-only.
func (a *App) SetHomeCloseHookForTest(fn func(op string, chID channel.ID)) {
	a.homeCloseHook = fn
}

// SeedIntentRowForTest inserts a raw channel_actors intent row WITHOUT admitting
// its membership — reproducing the半失败 state (intent landed, Admit did not) an
// Introduce retry must heal, so a test can assert the retry Admits under the
// FROZEN row's class-kind, not the request's. Test-only.
func (a *App) SeedIntentRowForTest(chID, instanceID, class, placement string) error {
	principal := instanceID
	if i := strings.IndexByte(principal, ':'); i >= 0 {
		principal = principal[i+1:]
	}
	_, err := a.db.ExecContext(context.Background(),
		`INSERT INTO channel_actors (channel_id, instance_id, principal, class, placement) VALUES (?,?,?,?,?)`,
		chID, instanceID, principal, class, placement)
	return err
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

// DropHomeForTest removes chID's open Home from the app's home map WITHOUT
// deleting its channels-table directory row — reproducing the "present in the
// directory but its universe is not open" state (getHome==nil) that homeOrError
// answers with 503 (A-P8). Test-only.
func (a *App) DropHomeForTest(chID channel.ID) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.homes, chID)
}

// AdmitForTest admits id as a durable member of chID's home (the pure-membership
// primitive an introduce door writes). Since the membrane law (v1.8 问①) stopped
// daemon attach from minting membership, a daemon-hosted actor must be admitted
// BEFORE its daemon declares it — this test seam stands in for the introduce door
// the daemon-attach live tests bypass. Test-only.
func (a *App) AdmitForTest(chID string, id actor.ActorID, kind actor.Kind) (actor.ActorID, error) {
	home := a.getHome(channel.ID(chID))
	if home == nil {
		return "", errTestChannelNotLoaded
	}
	principal := string(id)
	if i := strings.IndexByte(principal, ':'); i >= 0 {
		principal = principal[i+1:]
	}
	return home.Admit(context.Background(), kind, principal)
}

func (a *App) ResolvePrincipalForTest(chID string, kind actor.Kind, principal string) (actor.ActorID, error) {
	home := a.getHome(channel.ID(chID))
	if home == nil {
		return "", errTestChannelNotLoaded
	}
	id, ok, err := home.ResolvePrincipal(context.Background(), kind, principal)
	if err != nil {
		return "", err
	}
	if ok {
		return id, nil
	}
	return "", fmt.Errorf("principal not found")
}

// WaitLiveForTest polls chID's home until id has a live embodiment (View.Stat) or
// the timeout elapses — the async-embodiment counterpart of the old synchronous
// spawn: since composition is embodied by the reconcile ring (Admit + poke → sweep),
// a test that needs a live default floor before sending must wait for the sweep.
// Test-only.
func (a *App) WaitLiveForTest(chID string, id actor.ActorID, timeout time.Duration) bool {
	home := a.getHome(channel.ID(chID))
	if home == nil {
		return false
	}
	deadline := time.Now().Add(timeout)
	for {
		if _, live := home.View().Stat(id); live {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// KillCellForTest kills id's live embodiment on chID's home (despawn + dereg) —
// the "brain went dead" event resolveRouting must answer with 503 when id is the
// channel's default agent. Test-only.
func (a *App) KillCellForTest(chID channel.ID, id actor.ActorID) error {
	home := a.getHome(chID)
	if home == nil {
		return errTestChannelNotLoaded
	}
	return home.Remove(context.Background(), id)
}

// CreateHalfBuiltChannelForTest reproduces the createChannel CRASH window: the
// app-db channels row + its seeded channel_actors intent are committed and the home
// is opened, but NO Admit ran — the creator + boost never became channel members
// (the process died between tx.Commit and the seeding Admits). The result is a valid
// directory entry over an EMPTY channel-db membership (only the intrinsic system
// actor Open seeds). Returns the new channel id. Test-only — proves half-built
// channels stay deletable and open with clear errors, never a panic. Test-only.
func (a *App) CreateHalfBuiltChannelForTest(wsID, name string) (string, error) {
	const legacyPlaceholderID = actor.ActorID("agent:boost")
	chID := uuid.NewString()
	dbPath := filepath.Join(a.channelDBDir, chID+".db")
	now := time.Now().UnixMilli()
	if _, err := a.db.ExecContext(context.Background(),
		`INSERT INTO channels (id, workspace_id, name, type, db_path, default_agent, created_at) VALUES (?,?,?,?,?,?,?)`,
		chID, wsID, name, "group", dbPath, string(legacyPlaceholderID), now); err != nil {
		return "", err
	}
	if _, err := a.db.ExecContext(context.Background(),
		`INSERT INTO channel_actors (channel_id, instance_id, class, placement) VALUES (?,?,?,?)`,
		chID, string(legacyPlaceholderID), defaultBoostClass, placementServer); err != nil {
		return "", err
	}
	if _, err := a.createHome(channel.ID(chID), dbPath); err != nil {
		return "", err
	}
	return chID, nil
}

// AddWorkspaceMemberForTest inserts userID into wsID's workspace roster so a
// second registered user can pass the ws/REST channel-access ACL (which gates on
// workspace membership). Test-only — the production join path is the invite flow.
func (a *App) AddWorkspaceMemberForTest(wsID, userID string) error {
	_, err := a.db.ExecContext(context.Background(),
		`INSERT OR IGNORE INTO workspace_members (workspace_id, user_id, role) VALUES (?,?,?)`,
		wsID, userID, "member")
	return err
}

// StatForTest reads id's L1 embodiment (View.Stat) on chID's home — the axis
// presence must stay orthogonal to (层2 link来去不碰层1: startedAt stable across a
// ws reconnect, live throughout). Test-only.
func (a *App) StatForTest(chID channel.ID, id actor.ActorID) (startedAt time.Time, live bool) {
	home := a.getHome(chID)
	if home == nil {
		return time.Time{}, false
	}
	return home.View().Stat(id)
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
