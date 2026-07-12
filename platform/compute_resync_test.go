package platform

// C6 / glue-v2 #7 (kubelet 两件套): the daemon reconcile ring's缩容边闭合.
//
// Two behaviour-semantic locks:
//   ① 削边同 tick 汇报 — a pure缩容 (a member leaving desired with nothing
//      newly missing) now triggers a full-set Reattach the SAME tick, so the
//      home's reconcileHost deregisters the fallen-out host row immediately
//      instead of滞留ing it until a later privileged扩容/reconnect.
//   ② 慢周期 resync 有界收敛 — a削章 dereg that fails home-side once leaves a
//      zombie row the poll loop can never re-fire (prevCurrent已 shrunk). The
//      periodic resync (unconditional full-set re-declaration) is the level-
//      triggered backstop that re-runs the diff and收敛s the zombie.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// idleProcDef is a test-only cell that just blocks on Recv — it exists only to
// be a live embodiment the home can Host-stamp and later deregister.
func idleProcDef() actorbase.Def {
	return actorbase.Def{
		Doc: "test-only idle cell — blocks on Recv, produces nothing",
		New: func() (actorbase.Proc, error) {
			return func(sys actorbase.Sys) error {
				for {
					if _, err := sys.Recv(); err != nil {
						return err
					}
				}
			}, nil
		},
	}
}

// multiBuilder resolves each declared id to its ActorFactory.
type multiBuilder map[actor.ActorID]ActorFactory

func (b multiBuilder) Lookup(id actor.ActorID) (ActorFactory, bool) {
	f, ok := b[id]
	return f, ok
}

// mutableDesired is a DesiredSource whose member set the test can shrink under
// the reconcile ring's feet.
type mutableDesired struct {
	mu      sync.Mutex
	members []actorrt.DesiredMember
}

func (d *mutableDesired) set(members []actorrt.DesiredMember) {
	d.mu.Lock()
	d.members = members
	d.mu.Unlock()
}

func (d *mutableDesired) Members(context.Context) ([]actorrt.DesiredMember, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]actorrt.DesiredMember, len(d.members))
	copy(out, d.members)
	return out, nil
}

// deregFailOnceMembership fails the FIRST removes-bearing ApplyMemberTransitions
// (reconcileHost's dereg) exactly once, then passes everything through — the
// injected account-vs-reality divergence #7's resync backstop must absorb.
type deregFailOnceMembership struct {
	storespec.MembershipControlPlane
	failed atomic.Bool
}

func (m *deregFailOnceMembership) ApplyMemberTransitions(ctx context.Context, adds []storespec.MemberActorAdd, removes []storespec.MemberActorRemove) error {
	if len(removes) > 0 && m.failed.CompareAndSwap(false, true) {
		return fmt.Errorf("injected: reconcileHost dereg fault (one-time)")
	}
	return m.MembershipControlPlane.ApplyMemberTransitions(ctx, adds, removes)
}

func openResyncHome(t *testing.T, name string, faults *homeFaults) *Home {
	t.Helper()
	h, err := openHome(HomeConfig{
		ChannelID: channel.ID("resync-" + name),
		DBPath:    filepath.Join(t.TempDir(), name+".sqlite"),
	}, faults)
	if err != nil {
		t.Fatalf("openHome: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func serveHome(t *testing.T, h *Home, daemonID string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.ServeAttach(w, r, daemonID)
	}))
	t.Cleanup(srv.Close)
	return "ws" + srv.URL[4:]
}

func runComputeAsync(t *testing.T, cfg ComputeConfig) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = RunCompute(ctx, cfg)
	}()
	t.Cleanup(func() { cancel(); <-done })
}

func waitHostActive(t *testing.T, h *Home, id actor.ActorID, host string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		rec, ok, err := h.cs.Registry.Lookup(context.Background(), id)
		if err != nil {
			t.Fatalf("Lookup(%s): %v", id, err)
		}
		if ok && rec.Host == host && rec.IsActive() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("actor %s never reached Host=%s active", id, host)
}

func waitInactive(t *testing.T, h *Home, id actor.ActorID) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		rec, ok, err := h.cs.Registry.Lookup(context.Background(), id)
		if err != nil {
			t.Fatalf("Lookup(%s): %v", id, err)
		}
		if ok && !rec.IsActive() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("actor %s never deregistered", id)
}

func assertActive(t *testing.T, h *Home, id actor.ActorID) {
	t.Helper()
	rec, ok, err := h.cs.Registry.Lookup(context.Background(), id)
	if err != nil || !ok || !rec.IsActive() {
		t.Fatalf("actor %s Lookup = %+v ok=%v err=%v, want active", id, rec, ok, err)
	}
}

// TestComputeReconcile_PureShrink_DeregistersHostRowSameTick locks ①: dropping a
// member from desired — with nothing newly missing — deregisters its home host
// row via the same-tick削边 Reattach. Resync is set an hour out, so the dereg can
// ONLY be the削边 trigger, never the periodic backstop.
func TestComputeReconcile_PureShrink_DeregistersHostRowSameTick(t *testing.T) {
	h := openResyncHome(t, "shrink", nil)
	ctx := context.Background()

	idA, err := h.Admit(ctx, actor.KindTool, "shrink-a")
	if err != nil {
		t.Fatalf("admit a: %v", err)
	}
	idB, err := h.Admit(ctx, actor.KindTool, "shrink-b")
	if err != nil {
		t.Fatalf("admit b: %v", err)
	}

	desired := &mutableDesired{members: []actorrt.DesiredMember{
		{ID: idA, Kind: actor.KindTool, Lifecycle: actorrt.LifecycleAlwaysOn},
		{ID: idB, Kind: actor.KindTool, Lifecycle: actorrt.LifecycleAlwaysOn},
	}}
	builder := multiBuilder{
		idA: {Proc: idleProcDef()},
		idB: {Proc: idleProcDef()},
	}

	runComputeAsync(t, ComputeConfig{
		ServerWS:  serveHome(t, h, "daemon-1"),
		ComputeID: "daemon-1",
		Desired:   desired,
		Builder:   builder,
		Poll:      40 * time.Millisecond,
		Resync:    time.Hour, // proves削边 alone deregs — resync cannot be the cause
	})

	waitHostActive(t, h, idA, "daemon-1")
	waitHostActive(t, h, idB, "daemon-1")

	// Pure缩容: drop A, keep B. Nothing is newly missing, so pre-#7 this Reattach
	// would never fire and A's host row would滞留 active indefinitely.
	desired.set([]actorrt.DesiredMember{{ID: idB, Kind: actor.KindTool, Lifecycle: actorrt.LifecycleAlwaysOn}})

	waitInactive(t, h, idA)
	assertActive(t, h, idB)
}

// TestComputeReconcile_ResyncConvergesAfterDeregFailure locks ②: after a削章
// dereg fails home-side once, the poll loop can never re-fire the diff
// (prevCurrent已 shrunk) — only the periodic resync re-declares the full set,
// re-runs reconcileHost, and收敛s the zombie row. Fast resync + a one-time
// injected ApplyMemberTransitions failure; the row must still reach inactive.
func TestComputeReconcile_ResyncConvergesAfterDeregFailure(t *testing.T) {
	fault := &deregFailOnceMembership{}
	h := openResyncHome(t, "resync", &homeFaults{
		wrapMembership: func(m storespec.MembershipControlPlane) storespec.MembershipControlPlane {
			fault.MembershipControlPlane = m
			return fault
		},
	})
	ctx := context.Background()

	idA, err := h.Admit(ctx, actor.KindTool, "resync-a")
	if err != nil {
		t.Fatalf("admit a: %v", err)
	}
	idB, err := h.Admit(ctx, actor.KindTool, "resync-b")
	if err != nil {
		t.Fatalf("admit b: %v", err)
	}

	desired := &mutableDesired{members: []actorrt.DesiredMember{
		{ID: idA, Kind: actor.KindTool, Lifecycle: actorrt.LifecycleAlwaysOn},
		{ID: idB, Kind: actor.KindTool, Lifecycle: actorrt.LifecycleAlwaysOn},
	}}
	builder := multiBuilder{
		idA: {Proc: idleProcDef()},
		idB: {Proc: idleProcDef()},
	}

	runComputeAsync(t, ComputeConfig{
		ServerWS:  serveHome(t, h, "daemon-1"),
		ComputeID: "daemon-1",
		Desired:   desired,
		Builder:   builder,
		Poll:      40 * time.Millisecond,
		Resync:    150 * time.Millisecond,
	})

	waitHostActive(t, h, idA, "daemon-1")
	waitHostActive(t, h, idB, "daemon-1")

	// Drop A: the削边 Reattach's reconcileHost dereg hits the injected one-time
	// fault, leaving A a zombie the poll loop will never revisit.
	desired.set([]actorrt.DesiredMember{{ID: idB, Kind: actor.KindTool, Lifecycle: actorrt.LifecycleAlwaysOn}})

	// Only the periodic resync's unconditional re-declaration can re-run the diff
	// and converge the zombie — without #7 半② this Lookup never goes inactive.
	waitInactive(t, h, idA)
	assertActive(t, h, idB)

	if !fault.failed.Load() {
		t.Fatal("injected dereg fault never fired — test is vacuous (resync path unproven)")
	}
}
