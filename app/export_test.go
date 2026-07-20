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
	platformhome "github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// errTestChannelNotLoaded stands in for a torn-down home in the test seams below.
var errTestChannelNotLoaded = errors.New("app: channel not loaded")

func (a *App) ActorsForTest(chID channel.ID) ([]storespec.Record, error) {
	h := a.getHome(chID)
	if h == nil {
		return nil, errTestChannelNotLoaded
	}
	return h.View().ListActors(context.Background())
}

func (a *App) MessagesForTest(chID channel.ID) ([]storespec.StoredRow, error) {
	h := a.getHome(chID)
	if h == nil {
		return nil, errTestChannelNotLoaded
	}
	return h.View().ReadAfterSeq(context.Background(), 0, 1000)
}

func (a *App) PresenceForTest(chID channel.ID, id actor.ActorID) (member, known, online bool, err error) {
	h := a.getHome(chID)
	if h == nil {
		return false, false, false, errTestChannelNotLoaded
	}
	snapshot, err := h.View().Snapshot(context.Background(), id)
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

// OperateFaceForTest exposes the app's channel-operate executor so black-box
// tests can drive the four control verbs directly (as the sysactor gate would
// after authorising the sender), without the not-yet-built message senders
// (S5b/HTTP adapters). Test-only.
func (a *App) OperateFaceForTest() platformhome.OperateExecutor {
	return a.operateFace()
}

func (a *App) StageDeclarationEditForTest(chID channel.ID, sourceID string, config json.RawMessage) (actor.ActorID, int64, error) {
	h := a.getHome(chID)
	if h == nil {
		return "", 0, errTestChannelNotLoaded
	}
	rows, err := h.DeclaredBySource(context.Background(), sourceID)
	if err != nil || len(rows) == 0 {
		return "", 0, err
	}
	row := rows[0]
	edited, err := h.EditDeclaration(context.Background(), storespec.DeclEditBundle{
		ActorID: row.ID, Class: row.Class, Config: config, Placement: row.Placement,
		TIdle: row.TIdle, SourceDeclID: row.SourceDeclID, CreatedAt: time.Now().UnixMilli(),
	})
	return row.ID, edited.CurrentDeclVersion, err
}

// LockDaemonForTest / LockDeclForTest expose only the keyed-lock barriers needed
// to prove composite acquisition order. They return the production lock's
// idempotent release closure and exist only in the test build.
func (a *App) LockDaemonForTest(id string) func() { return a.daemonLocks.lock(id) }
func (a *App) LockDeclForTest(id string) func()   { return a.declLocks.lock(id) }

// WaitDaemonLockRefsForTest waits until refs contenders (holder included) have
// registered for id. keyedLockSet increments refs before blocking on the entry,
// making this a deterministic barrier rather than a timeout-based lock-order
// guess.
func (a *App) WaitDaemonLockRefsForTest(id string, refs int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		a.daemonLocks.mu.Lock()
		entry := a.daemonLocks.m[id]
		got := 0
		if entry != nil {
			got = entry.refs
		}
		a.daemonLocks.mu.Unlock()
		if got >= refs {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
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
	h := a.getHome(chID)
	if h != nil {
		_ = h.Close()
	}
}

// AdmitForTest admits id as a durable declared identity in chID's Home
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

func (a *App) ComposeDaemonForTest(chID, principal, class, daemonID string, kind actor.Kind) (actor.ActorID, error) {
	h := a.getHome(channel.ID(chID))
	if h == nil {
		return "", errTestChannelNotLoaded
	}
	placement, err := storespec.NewDaemonPlacement(daemonID)
	if err != nil {
		return "", err
	}
	result, err := h.Declare(context.Background(), platformhome.DeclareRequest{
		SourceDeclID: "sys:test:" + principal, Principal: principal, Class: class,
		Placement: placement, Kind: kind, CreatedAt: time.Now().UnixMilli(),
	})
	return result.Row.ID, err
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

// CloseHomeForTest leaves the closed handle published in the app map so a
// post-commit daemon-obligation read deterministically returns ErrClosed.
func (a *App) CloseHomeForTest(chID channel.ID) error {
	h := a.getHome(chID)
	if h == nil {
		return errTestChannelNotLoaded
	}
	return h.Close()
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
