package home_test

// F-2 / S5 DoD: a daemon-placed actor's Restart must TRULY rebuild it — the old
// daemon-side embodiment dies and a NEW one is re-built by the daemon's own
// reconcile ring. This is the daemon-placement twin of the server-placed
// TestDeclsAPI restart path (app/actor_decls_e2e_test.go only covers server
// placement); F-2 removed the placement='server' filter so Restart is
// placement-neutral, and this test proves the daemon half of that neutrality
// end-to-end over a REAL wire and the REAL production reconcile ring.
//
// The chain under test (all production surface):
//   Home.Restart(id) → channel.Cells().DespawnID(id) → home port signalDespawn
//   → KindDespawn frame over the yamux link → daemon DialConfig.DespawnLocal
//   → daemon rt.DespawnID → the cell's execution arm ends → next computeRing
//   reconcile tick sees the liveness gap → buildOne re-embodies a fresh cell.
//
// "Old dies + new rebuilds" is observed BLACK-BOX via a per-embodiment token:
// the probe Def mints a fresh uuid inside New() (which the actorbase engine
// runs once per built cell), so a reply carrying a DIFFERENT token after Restart
// proves a genuinely new Proc instance ran — not the old object quiesced and
// reused. The Desired source is STATIC across the whole test (the member never
// leaves desired), so the rebuild is proven to come purely from the reconcile
// ring healing the liveness gap, not from any membership churn.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/compute"
	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
)

const typeRestartProbe = "restart_probe.ping"

// restartProbeDef replies to every typeRestartProbe request with the token this
// embodiment minted at construction. New() runs once per built cell, so the
// token is a fingerprint of the embodiment: a changed token == a rebuild.
func restartProbeDef() actorbase.Def {
	return actorbase.Def{
		Doc: "test-only: replies with a per-embodiment token (fresh per New()) — a rebuild changes it",
		New: func() (actorbase.Proc, error) {
			token := uuid.NewString()
			return func(sys actorbase.Sys) error {
				for {
					msg, err := sys.Recv()
					if err != nil {
						return err
					}
					if msg.Type != typeRestartProbe {
						continue
					}
					if _, rerr := sys.Reply(msg, map[string]string{"token": token}); rerr != nil {
						return rerr
					}
				}
			}, nil
		},
	}
}

// staticDesired is a fixed desired-source: the one member is always-on and never
// leaves the set — so a rebuild can only be the reconcile ring healing a
// liveness gap, never a membership transition.
type staticDesired struct{ members []actorrt.DesiredMember }

func (s staticDesired) Members(context.Context) ([]actorrt.DesiredMember, error) {
	return s.members, nil
}

// oneActorBuilder resolves exactly one id to a Proc factory.
type oneActorBuilder struct {
	id actor.ActorID
	f  platform.ActorFactory
}

func (b oneActorBuilder) Lookup(id actor.ActorID) (platform.ActorFactory, bool) {
	if id == b.id {
		return b.f, true
	}
	return platform.ActorFactory{}, false
}

// awaitToken repeatedly sends a probe ping to target and returns the token from
// the first reply (matched by parent_id to THAT ping) whose token is non-empty
// and != reject. A reply lacking a token (e.g. a receiver_unavailable terminal
// while the port is momentarily gone mid-rebuild), a token == reject (the old
// embodiment answering just before it dies), or no reply at all, each just
// drive another ping until overall elapses. reject="" accepts any non-empty
// token.
func awaitToken(t *testing.T, ch *home.Home, pen harness.Pen, target actor.ActorID, reject string, overall time.Duration) string {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(overall)
	for time.Now().Before(deadline) {
		reqID := writeRequest(t, pen, target, typeRestartProbe, nil)
		sub := time.Now().Add(600 * time.Millisecond)
		for time.Now().Before(sub) {
			rows, err := ch.View().ReadAfterSeq(ctx, 0, 4000)
			if err != nil {
				t.Fatalf("ReadAfterSeq: %v", err)
			}
			responded := false
			for _, row := range rows {
				if row.Envelope.Kind == message.KindResponse && row.Envelope.ParentID == reqID {
					responded = true
					if tok := tokenOf(row.Envelope.Payload); tok != "" && tok != reject {
						return tok
					}
				}
			}
			if responded {
				break // this ping was answered (empty/rejected token) — send a fresh one
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	t.Fatalf("no acceptable probe token (reject=%q) within %v", reject, overall)
	return ""
}

func tokenOf(payload json.RawMessage) string {
	var p struct {
		Token string `json:"token"`
	}
	_ = json.Unmarshal(payload, &p)
	return p.Token
}

// TestRestartDaemonPlacedActor_RebuildsAcrossWire is the F-2 daemon-placement
// rebuild proof.
func TestRestartDaemonPlacedActor_RebuildsAcrossWire(t *testing.T) {
	ch := newClosureHome(t)

	callerID := actor.ActorID("user:restart-caller")
	agentID := actor.ActorID("tool:restart-probe")
	// The daemon may only declare an already-admitted id (handleAttach drops
	// orphans), so admit it home-side first; registerActor rewrites agentID to
	// the minted id.
	registerActor(t, ch, &agentID, actor.KindTool)
	callerPen := spawnWithPen(t, ch, &callerID, actor.KindHuman)

	// The daemon: a REAL compute.Run reconcile ring over a real wire to
	// this home, hosting agentID with the probe Def. Static desired + a fast
	// poll so the rebuild lands promptly.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ch.ServeAttach(w, r, "daemon-restart-rebuild")
	}))
	t.Cleanup(srv.Close)
	wsURL := "ws" + srv.URL[4:]

	desired := staticDesired{members: []actorrt.DesiredMember{{ID: agentID, Kind: actor.KindTool, Lifecycle: actorrt.LifecycleAlwaysOn}}}
	builder := oneActorBuilder{id: agentID, f: platform.ActorFactory{Proc: restartProbeDef()}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	computeDone := make(chan struct{})
	go func() {
		defer close(computeDone)
		_ = compute.Run(ctx, compute.Config{
			ServerWS:  wsURL,
			ComputeID: "daemon-restart-rebuild",
			Desired:   desired,
			Builder:   builder,
			Poll:      50 * time.Millisecond,
		})
	}()
	t.Cleanup(func() { cancel(); <-computeDone })

	// Token #1: the initially-built daemon cell.
	token1 := awaitToken(t, ch, callerPen, agentID, "", 10*time.Second)

	// The production restart entry point — placement-neutral (F-2): for a
	// daemon-hosted row this must cross the wire and kill the remote cell.
	if err := ch.Restart(context.Background(), agentID); err != nil {
		t.Fatalf("Home.Restart: %v", err)
	}

	// Token #2: whatever the reconcile ring rebuilt. It MUST differ from token1
	// — a fresh New() ran, i.e. the old embodiment truly died and a new one was
	// built (not the same object reused).
	token2 := awaitToken(t, ch, callerPen, agentID, token1, 15*time.Second)
	if token2 == token1 || token2 == "" {
		t.Fatalf("after Restart, probe token=%q (before=%q) — daemon-placed Restart did not rebuild the embodiment", token2, token1)
	}
}
