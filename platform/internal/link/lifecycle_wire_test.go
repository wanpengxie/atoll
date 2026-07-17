package link_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func TestDaemonLifecycleForkCrossesWireWithFullSpecAndTicksPlan(t *testing.T) {
	rt, _ := actorrt.New(actorrt.Config{})
	t.Cleanup(rt.StopAll)
	auth := newTestAuthorities()
	var calls atomic.Int32
	got := make(chan actorrt.ForkSpec, 1)
	acc := newTestAcceptor(t, link.Config{
		Minter: &stubMinter{}, Runtime: rt, ChannelID: testChannelID,
		Declarations: auth, Authority: auth, DaemonAuthority: auth, PortIndex: auth,
		ActorLock: func(actor.ActorID) func() { return func() {} },
		SpawnRequest: func(_ context.Context, inc actorrt.Incarnation, version int64, nonce string, spec actorrt.ForkSpec) (actor.ActorID, error) {
			wantPlacement, _ := storespec.NewDaemonPlacement("daemon-target")
			if inc.ID() != "agent:remote-parent" || version != 7 || nonce == "" || spec.Placement == nil || *spec.Placement != wantPlacement {
				t.Fatalf("spawn weld inc=%s version=%d nonce=%q placement=%+v", inc.ID(), version, nonce, spec.Placement)
			}
			calls.Add(1)
			got <- spec
			return "agent:remote-parent/worker-fixed", nil
		},
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { acc.Serve(w, req, "daemon-a") }))
	t.Cleanup(func() { _ = acc.Close(); srv.Close() })
	var planTicks atomic.Int32
	d, err := link.Dial(context.Background(), "ws"+srv.URL[4:], []link.Declaration{{
		ActorID: "agent:remote-parent", Kind: actor.KindAgent,
		Binding: actor.BindingRuntimeInboundViaRelay, Version: 7,
	}}, link.DialConfig{PlanChanged: func() { planTicks.Add(1) }}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	arms, err := d.OpenStream(context.Background(), "agent:remote-parent", 7, "", func(*message.Envelope) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	d.StartStream("agent:remote-parent")
	targetDaemon, _ := storespec.NewDaemonPlacement("daemon-target")
	spec := actorrt.ForkSpec{Kind: actor.KindTool, Class: "worker", NameHint: "probe", Config: []byte(`{"x":1}`), Placement: &targetDaemon}
	child, err := arms.Lifecycle.Fork(context.Background(), spec)
	if err != nil || child != "agent:remote-parent/worker-fixed" {
		t.Fatalf("wire Fork=(%q,%v)", child, err)
	}
	select {
	case wire := <-got:
		if wire.Kind != spec.Kind || wire.Class != spec.Class || wire.NameHint != spec.NameHint || string(wire.Config) != string(spec.Config) {
			t.Fatalf("wire spec=%+v want=%+v", wire, spec)
		}
	case <-time.After(time.Second):
		t.Fatal("spawn request did not cross wire")
	}
	if calls.Load() != 1 || planTicks.Load() == 0 {
		t.Fatalf("calls=%d planTicks=%d", calls.Load(), planTicks.Load())
	}
}
