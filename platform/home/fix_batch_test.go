package home

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// A daemon-placed forked child must be born with the canonical relay binding —
// the attach handshake's fresh-check rejects a row whose Binding is not
// BindingRuntimeInboundViaRelay, so without it the child can never complete a
// real wire attach (the gap that kept the fork→daemon flagship path broken).
func TestDaemonPlacedForkedChildCarriesRelayBinding(t *testing.T) {
	h := openWhiteboxHome(t)
	ctx := context.Background()
	parent, err := h.admit(ctx, actor.KindHuman, "binding-parent")
	if err != nil {
		t.Fatal(err)
	}
	placement, _ := storespec.NewDaemonPlacement("daemon-b")
	remote, err := h.forkAdmission(ctx, parent, 1, actorrt.ForkSpec{
		Kind: actor.KindAgent, Class: "worker", Placement: &placement,
	}, "binding-remote")
	if err != nil {
		t.Fatal(err)
	}
	row, ok, err := h.controlIndex.LookupActive(ctx, remote)
	if err != nil || !ok {
		t.Fatalf("lookup daemon-placed child=(%v,%v)", ok, err)
	}
	if row.Binding != actor.BindingRuntimeInboundViaRelay {
		t.Fatalf("daemon-placed fork binding=%q, want %q", row.Binding, actor.BindingRuntimeInboundViaRelay)
	}

	local, err := h.forkAdmission(ctx, parent, 1, actorrt.ForkSpec{
		Kind: actor.KindAgent, Class: "worker",
	}, "binding-local")
	if err != nil {
		t.Fatal(err)
	}
	row, ok, err = h.controlIndex.LookupActive(ctx, local)
	if err != nil || !ok {
		t.Fatalf("lookup server-placed child=(%v,%v)", ok, err)
	}
	if row.Binding != "" {
		t.Fatalf("server-placed fork binding=%q, want empty", row.Binding)
	}
}

// Boot must rebuild the wake debt of a finite-idle receiver from its durable
// open requests (S6 write-path ⑥, the open-request predicate half): a request
// committed before a restart re-creates dirty on the next boot, so the
// receiver is re-activated to finish the work instead of waiting for its
// caller's deadline.
func TestBootRebuildsWakeDebtFromDurableOpenRequest(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
	resolver := &acceptanceResolver{}
	placement, _ := storespec.NewDaemonPlacement("daemon-wake")
	h1 := openAcceptanceHome(t, dbPath, "boot-wake-debt", resolver, time.Hour)
	decl, err := h1.declare(ctx, DeclareRequest{
		SourceDeclID: "decl:wake", Kind: actor.KindAgent,
		Class: "wake-worker", Placement: placement, TIdle: 60_000,
		CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	id := decl.Row.ID
	expires := time.Now().Add(time.Hour).UnixMilli()
	now := time.Now().UnixMilli()
	write, err := h1.systemPen.Write(ctx, &message.Envelope{
		ID: "boot-wake-request", Kind: message.KindRequest, Type: "resume.work",
		Audience: message.Audience{id}, ExpiresAt: &expires, Visibility: message.VisibilitySystem,
		TS: now, TSReceived: now,
	})
	if err != nil || !write.Accepted() {
		t.Fatalf("open request write=(%+v,%v)", write, err)
	}
	waitHomeCondition(t, func() bool {
		rows, qerr := h1.cs.Query.OpenRequestsForActor(ctx, id)
		return qerr == nil && len(rows) == 1
	})
	if err := h1.closeInternal("test"); err != nil {
		t.Fatal(err)
	}

	h2 := openAcceptanceHome(t, dbPath, "boot-wake-debt", resolver, time.Hour)
	t.Cleanup(func() { _ = h2.closeInternal("test") })
	standing, ok := h2.liveness.WakeStanding(id)
	if !ok || !standing.Dirty {
		t.Fatalf("boot wake standing=(%+v,%v), want dirty=true rebuilt from the durable open request", standing, ok)
	}
	// The rebuilt debt must actually drive attachment intent: the daemon plan
	// picks the receiver up on the next intent pass without any new input.
	h2.reconcileDaemonIntent(ctx)
	plan, err := h2.planForDaemon(ctx, "daemon-wake")
	if err != nil || len(plan) != 1 || plan[0].InstanceID != id {
		t.Fatalf("post-boot plan=%+v err=%v, want the woken receiver", plan, err)
	}
}
