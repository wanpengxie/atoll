package platform_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/actors/echo"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// actorbase-spec-v1.md §5 DoD⑦: "同一份 Def cell/port 两宿主跑(已知不对称两个:
// fork=ErrUnsupported、daemon caller cancel 信号半降级)". This file is the
// dual-host proof: echo.Def() — the SAME Def platform/echo_actorbase_test.go
// already runs over the cell host (Home.Spawn) — is spawned a SECOND time
// here over a real wire, daemon (port) host, using the identical assembly
// seam (actorbase.New) production code takes (platform.compute.buildOne
// wires this exact same NewLiveArms(rb, inc, host)+Hooks{} pair; this test
// only substitutes a hand-rolled daemon-side runtime for computeRing's
// reconcile loop so it can wire a SINGLE known decl deterministically).

// dualHostDaemon is the minimal test-local daemon side: an actorrt.Runtime
// dispatching inbound envelopes to whichever cell OpenStream installed —
// mirrors cancel_request_test.go's cancelDaemonHost (the existing port-host
// test precedent this file follows).
type dualHostDaemon struct {
	rt  *actorrt.Runtime
	del actorrt.Deliverer
}

func newDualHostDaemon() *dualHostDaemon {
	rt, del := actorrt.New(actorrt.Config{})
	return &dualHostDaemon{rt: rt, del: del}
}

func (h *dualHostDaemon) dispatch(target actor.ActorID, env *message.Envelope) error {
	_, err := h.del.Deliver([]actor.ActorID{target}, env)
	return err
}

// spawnActorbaseOverWire installs def as a daemon-hosted actorbase Proc: it
// dials ch's real /compute attach endpoint, OpenStreams id, and spawns
// actorbase.New(link.NewLiveArms(...), actorbase.Hooks{}, def) over the
// daemon's own actorrt.Runtime — the exact production wiring
// platform/compute.go's buildOne performs (daemon Hooks{}: Canceller nil,
// spec §3's known cancel-upstream gap).
func spawnActorbaseOverWire(t *testing.T, ch *platform.Home, id actor.ActorID, def actorbase.Def) (*link.Dialer, *dualHostDaemon) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ch.ServeAttach(w, r, "daemon-dualhost")
	}))
	t.Cleanup(srv.Close)
	wsURL := "ws" + srv.URL[4:]

	d, err := link.Dial(context.Background(), wsURL, "daemon-dualhost",
		[]link.Declaration{{ActorID: id, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	host := newDualHostDaemon()
	t.Cleanup(host.rt.StopAll)

	arms, err := d.OpenStream(id, func(env *message.Envelope) error {
		return host.dispatch(id, env)
	}, func(requestID message.ID) { host.rt.CancelRequest(id, requestID) })
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	rb := link.NewRebindableArms(arms)
	_, _, _ = host.rt.SpawnIfAbsent(id, actor.KindTool, func(inc actorrt.Incarnation) actorrt.Actor {
		// The SAME two lines platform/compute.go's buildOne runs in production
		// (link.NewLiveArms + actorbase.Hooks{}) — daemon Canceller stays nil.
		return actorbase.New(link.NewLiveArms(rb, inc, host.rt), actorbase.Hooks{}, def)
	})
	d.Start()
	return d, host
}

// TestActorbaseDef_RunsOverBothCellAndPortHosts is DoD⑦: echo.Def() closes
// the identical call->reply loop over the daemon (port) host that
// TestEcho_CallReplyClosesThroughRealCellHost (platform/echo_actorbase_test.go)
// already proves over the cell host — same Def, both hosts.
func TestActorbaseDef_RunsOverBothCellAndPortHosts(t *testing.T) {
	ch := newClosureHome(t)

	callerID := actor.ActorID("user:dualhost-caller")
	echoID := actor.ActorID("tool:dualhost-echo")
	registerActor(t, ch, echoID, actor.KindTool)
	callerPen := spawnWithPen(t, ch, callerID, actor.KindHuman)

	spawnActorbaseOverWire(t, ch, echoID, echo.Def())

	reqID := writeRequest(t, callerPen, echoID, echo.TypeSay, nil)
	term := pollForTerminal(t, ch, reqID, 5*time.Second)

	if term.Sender.ID != echoID {
		t.Fatalf("terminal sender.id=%s, want %s", term.Sender.ID, echoID)
	}
	var payload struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(term.Payload, &payload); err != nil {
		t.Fatalf("unmarshal terminal payload: %v (raw=%s)", err, term.Payload)
	}
	if payload.Status != "completed" {
		t.Fatalf("terminal payload.status=%q, want completed", payload.Status)
	}
}

// forkProbeDef is a tiny inline Proc (not a named actors/ package — this is a
// throwaway spec-conformance probe, not a product actor) whose only job is to
// call sys.Fork and report what it got back, so the daemon-hosted known-gap
// (spec §3/§1.2: "daemon 宿主返 ErrUnsupported") is asserted through a REAL
// wire-hosted Sys, not just the engine-internal unit test
// (TestEngine_ForkAndDespawnChildReturnErrUnsupportedWhenSpawnNil).
const typeForkProbe = "dualhost.fork_probe"

func forkProbeDef() actorbase.Def {
	return actorbase.Def{
		Doc: "test-only: replies with the sys.Fork() error string (spec §3 daemon known-gap probe)",
		New: func() (actorbase.Proc, error) {
			return func(sys actorbase.Sys) error {
				for {
					msg, err := sys.Recv()
					if err != nil {
						return err
					}
					if msg.Type != typeForkProbe {
						continue
					}
					_, forkErr := sys.Fork("probe", "child", nil)
					_, _ = sys.Reply(msg, map[string]string{"fork_err": fmt.Sprint(forkErr)})
				}
			}, nil
		},
	}
}

// TestActorbaseDef_DaemonHostForkReturnsErrUnsupported is DoD⑦'s second named
// asymmetry (the first, cancel, is already unit-tested by
// TestEngine_PendingCancelSelfClosesAndSkipsCancellerWhenNil at the ledger
// level): a daemon-hosted Sys.Fork must answer actorbase.ErrUnsupported
// through the real wire-hosted assembly, not silently succeed or panic.
func TestActorbaseDef_DaemonHostForkReturnsErrUnsupported(t *testing.T) {
	ch := newClosureHome(t)

	callerID := actor.ActorID("user:dualhost-fork-caller")
	probeID := actor.ActorID("tool:dualhost-fork-probe")
	registerActor(t, ch, probeID, actor.KindTool)
	callerPen := spawnWithPen(t, ch, callerID, actor.KindHuman)

	spawnActorbaseOverWire(t, ch, probeID, forkProbeDef())

	reqID := writeRequest(t, callerPen, probeID, typeForkProbe, nil)
	term := pollForTerminal(t, ch, reqID, 5*time.Second)

	var payload struct {
		ForkErr string `json:"fork_err"`
	}
	if err := json.Unmarshal(term.Payload, &payload); err != nil {
		t.Fatalf("unmarshal terminal payload: %v (raw=%s)", err, term.Payload)
	}
	if payload.ForkErr != actorbase.ErrUnsupported.Error() {
		t.Fatalf("terminal payload.fork_err=%q, want %q (actorbase.ErrUnsupported)", payload.ForkErr, actorbase.ErrUnsupported.Error())
	}
}
