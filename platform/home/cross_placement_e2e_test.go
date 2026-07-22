package home

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func TestCrossPlacementForkBothDirectionsWithRealLifecycleArms(t *testing.T) {
	ctx := context.Background()
	resolver := &acceptanceResolver{}
	h, err := Open(Config{
		ChannelID: "cross-placement", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver:  resolver,
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    5 * time.Millisecond, Bootstrap: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })

	// server → explicitly named daemon: drive the same incarnation-welded
	// LifecycleHandle a local actor receives, then prove only the target daemon's
	// plan projects the dirty child.
	serverParent, err := admitThroughSysOp(h, ctx, actor.KindHuman, "server-placement-parent")
	if err != nil {
		t.Fatal(err)
	}
	var serverParentInc actorrt.Incarnation
	waitHomeCondition(t, func() bool {
		serverParentInc, _ = h.channel.Cells().CurrentIncarnation(serverParent)
		return serverParentInc.ID() != ""
	})
	targetDaemon, _ := storespec.NewDaemonPlacement("daemon-target")
	serverLifecycle := newSpawnHandle(h, serverParentInc, 1, h.channel.Cells())
	daemonChild, err := serverLifecycle.Fork(ctx, actorrt.ForkSpec{
		Kind: actor.KindAgent, Class: "cross.daemon-child", NameHint: "to-daemon", Placement: &targetDaemon,
	})
	if err != nil {
		t.Fatal(err)
	}
	row, ok, err := h.controlIndex.LookupActive(ctx, daemonChild)
	if err != nil || !ok || row.Sponsor != serverParent || row.Placement != targetDaemon {
		t.Fatalf("server→daemon row=(%+v,%v,%v)", row, ok, err)
	}
	if plan, err := h.planForDaemon(ctx, "daemon-target"); err != nil || len(plan) != 0 {
		t.Fatalf("dormant server→daemon child plan=%+v err=%v", plan, err)
	}
	_, _ = h.liveness.AcceptDelivery(daemonChild, &message.Envelope{Kind: message.KindRequest})
	h.reconcileDaemonIntent(ctx)
	plan, err := h.planForDaemon(ctx, "daemon-target")
	if err != nil || len(plan) != 1 || plan[0].InstanceID != daemonChild || plan[0].EnsureTicket == "" {
		t.Fatalf("dirty server→daemon child plan=%+v err=%v", plan, err)
	}
	if other, _ := h.planForDaemon(ctx, "daemon-other"); len(other) != 0 {
		t.Fatalf("specified-host child leaked into another daemon plan: %+v", other)
	}

	// daemon → server: attach a real parent port using Home's plan ticket, then
	// invoke its remote Lifecycle arm. The spawn frame crosses the actual wire,
	// Home admits the run row, and the server reconcile builds its local body.
	daemonPlacement, _ := storespec.NewDaemonPlacement("daemon-source")
	if _, err := SystemOps(h).AttachDaemon(ctx, channelpkg.DaemonRequest{Ref: "cross-placement-bind", DaemonID: "daemon-source"}); err != nil {
		t.Fatal(err)
	}
	daemonParent, err := h.declare(ctx, DeclareRequest{
		SourceDeclID: "decl:daemon-parent", Kind: actor.KindAgent,
		Class: "cross.daemon-parent", Placement: daemonPlacement, CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	var parentPlan []link.Declaration
	waitHomeCondition(t, func() bool {
		rows, perr := h.planForDaemon(ctx, "daemon-source")
		if perr != nil || len(rows) != 1 || rows[0].InstanceID != daemonParent.Row.ID {
			return false
		}
		parentPlan = []link.Declaration{{
			ActorID: rows[0].InstanceID, Kind: rows[0].Kind, Binding: rows[0].Binding,
			Version: rows[0].Version, EnsureTicket: rows[0].EnsureTicket,
		}}
		return true
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		h.serveAttach(w, req, "daemon-source")
	}))
	t.Cleanup(srv.Close)
	dialer, err := link.Dial(ctx, "ws"+srv.URL[4:], parentPlan, link.DialConfig{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dialer.Close() })
	arms, err := dialer.OpenStream(ctx, daemonParent.Row.ID, parentPlan[0].Version, parentPlan[0].EnsureTicket, func(*message.Envelope) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	dialer.StartStream(daemonParent.Row.ID)
	server := storespec.NewServerPlacement()
	serverChild, err := arms.Lifecycle.Fork(ctx, actorrt.ForkSpec{
		Kind: actor.KindAgent, Class: "cross.server-child", NameHint: "to-server", Placement: &server,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	expires := time.Now().Add(time.Minute).UnixMilli()
	write, err := h.systemPen.Write(ctx, &message.Envelope{
		ID: "cross-placement-server-child-work", Kind: message.KindRequest, Type: "cross.work",
		Audience: message.Audience{serverChild}, Visibility: message.VisibilitySystem,
		TS: now, TSReceived: now, ExpiresAt: &expires,
	})
	if err != nil || !write.Accepted() {
		t.Fatalf("server child wake=(%+v,%v)", write, err)
	}
	waitHomeCondition(t, func() bool {
		row, active, _ := h.controlIndex.LookupActive(ctx, serverChild)
		_, live := h.channel.Cells().CurrentIncarnation(serverChild)
		return active && row.Sponsor == daemonParent.Row.ID && row.Placement == server && live
	})
}
