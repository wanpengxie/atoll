package link_test

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// forkedWorldRunAuthority is a minimal storespec.ActorAuthority that
// classifies exactly ONE actor id as WorldRun (the forked/"run 层" world) and
// answers CheckAuthor OK for the (id, BirthVersion=1) stamp — the exact stamp
// actorStateHandles.AdmitRun/newMemoryStateHandle weld for any run-world owner
// (runtime/accessdoor/memstate.go). Everything else (LookupActive/ListActive)
// is unreachable on the run-world Resolve path and stubbed honestly empty.
type forkedWorldRunAuthority struct{ forkedID actor.ActorID }

func (a forkedWorldRunAuthority) LookupActive(context.Context, actor.ActorID) (storespec.ActorControlRow, bool, error) {
	return storespec.ActorControlRow{}, false, nil
}
func (a forkedWorldRunAuthority) ListActive(context.Context) ([]storespec.ActorControlRow, error) {
	return nil, nil
}
func (a forkedWorldRunAuthority) WorldOf(_ context.Context, id actor.ActorID) (storespec.ActorWorld, bool, error) {
	if id == a.forkedID {
		return storespec.WorldRun, true, nil
	}
	return storespec.WorldDurable, false, nil
}
func (a forkedWorldRunAuthority) CheckAuthor(_ context.Context, stamp storespec.AuthorStamp) (storespec.AuthorVerdict, error) {
	if stamp.ID == a.forkedID && stamp.BirthVersion == 1 {
		return storespec.AuthorOK, nil
	}
	return storespec.AuthorNotMember, nil
}

// TestDaemonHostedForkedStateRoutesToRunLayerAgainstRealStore is the real-store
// companion to TestStateArmRoutesToActorScope above (which proves routing with
// a FAKE resolver only — no backing store at all). Here the
// accessdoor.StateHandleResolver is backed by cs.Access, the SAME real
// sqlite-backed durable AccessMinter runtime.OpenChannel hands production
// Home (runtime/storeopen.go) — not a fake. WorldOf classifies this one
// actor as WorldRun (the forked/daemon-hosted shape), so the write must land
// ONLY in the in-memory run layer — spec §2.7/red-line 10: "forked 行绝不落
// 表". The write crosses the REAL link wire (accept.go's accessInvocation →
// StateHandleResolver.Resolve, the daemon relay leg), and the assertions
// prove both promises: the durable actor_state SQL table stays at zero rows
// (checked via a SEPARATE read-only connection to the real db file, not a
// Home-internal shortcut), and the write reads back through the run layer —
// both over the same wire session and directly via the resolver.
func TestDaemonHostedForkedStateRoutesToRunLayerAgainstRealStore(t *testing.T) {
	ctx := context.Background()
	const forkedID = actor.ActorID("daemon-hosted-forked-child")

	dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
	cs, err := runtime.OpenChannel(ctx, channel.ID("state-relay-durable-store"), dbPath, runtime.OpenChannelOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	authority := forkedWorldRunAuthority{forkedID: forkedID}
	resolver, err := accessdoor.NewStateHandleResolver(authority, cs.Access)
	if err != nil {
		t.Fatalf("NewStateHandleResolver: %v", err)
	}
	if err := resolver.AdmitRun(forkedID); err != nil {
		t.Fatalf("AdmitRun: %v", err)
	}

	rt, _ := actorrt.New(actorrt.Config{Parent: ctx})
	t.Cleanup(rt.StopAll)

	r := &capsRig{access: &fakeAccessMinter{}, sched: &fakeScheduleMinter{}}
	r.acc = newTestAcceptor(t, link.Config{
		Minter: &stubMinter{}, Access: r.access, StateHandles: resolver, Schedule: r.sched,
		Runtime: rt, ChannelID: testChannelID, LeasePing: 5 * time.Second, LeaseTTL: 30 * time.Second,
	})
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.acc.Serve(w, req, "daemon-1")
	}))
	t.Cleanup(func() { _ = r.acc.Close(); r.srv.Close() })

	arms, d := dialArms(t, r, forkedID)
	defer func() { _ = d.Close() }()

	// The write crosses the real wire into accept.go's accessInvocation →
	// StateHandleResolver.Resolve leg — this IS the daemon-hosted relay path.
	if out, err := arms.State.Invoke(ctx, access.OpCreate, resource.ResourceID("relay-value"), []byte("run-body"), nil); err != nil || !out.Accepted() {
		t.Fatalf("state create over wire=(%+v,%v)", out, err)
	}

	// (a) durable actor_state: the REAL store's REAL table, zero rows — this
	// test never wrote State for any WorldDurable owner, and the forked owner
	// must never fall back to durable regardless.
	raw, err := sql.Open("sqlite", "file:"+dbPath+"?mode=ro")
	if err != nil {
		t.Fatalf("open durable db: %v", err)
	}
	defer raw.Close()
	var n int
	if err := raw.QueryRowContext(ctx, "SELECT COUNT(*) FROM actor_state").Scan(&n); err != nil {
		t.Fatalf("count actor_state: %v", err)
	}
	if n != 0 {
		t.Fatalf("actor_state durable rows = %d, want 0 (forked 行绝不落表)", n)
	}

	// (b) run-layer read-back — over the SAME wire session...
	if out, err := arms.State.Invoke(ctx, access.OpRead, resource.ResourceID("relay-value"), nil, nil); err != nil || string(out.Value) != "run-body" {
		t.Fatalf("state read back over wire=(%+v,%v), want run-body", out, err)
	}
	// ...and directly through the resolver (the shared run map, keyed by
	// actor id — proving the write is visible to ANY future incarnation of
	// this forked identity, not just this connection).
	runHandle, err := resolver.Resolve(ctx, storespec.AuthorStamp{ID: forkedID, BirthVersion: 1})
	if err != nil {
		t.Fatalf("resolve run handle: %v", err)
	}
	if out, err := runHandle.Invoke(ctx, access.OpRead, resource.ResourceID("relay-value"), nil, nil); err != nil || string(out.Value) != "run-body" {
		t.Fatalf("run-layer whitebox read=(%+v,%v), want run-body", out, err)
	}
}
