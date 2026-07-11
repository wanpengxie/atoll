package platform_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// TestActorStatus_AgentThroughGate proves actor.status is a real channel query:
// an ordinary agent calls the system actor through its welded pen and receives
// the same presence snapshot used by the out-of-band View.
func TestActorStatus_AgentThroughGate(t *testing.T) {
	h := newClosureHome(t)
	id := actor.ActorID("agent:presence-caller")
	registerActor(t, h, &id, actor.KindAgent)
	answers := make(chan introspect.Status, 1)
	factory := platform.CapsFactory(func(caps actorcaps.Caps) actorrt.Actor {
		return actorbase.New(caps, actorbase.Hooks{}, actorbase.Def{
			Doc: "presence status e2e caller",
			New: func() (actorbase.Proc, error) {
				return func(sys actorbase.Sys) error {
					pending, err := sys.Call(actor.SystemActorID, introspect.QueryStatus, introspect.StatusRequest{ActorID: string(id)})
					if err != nil {
						return err
					}
					msg, err := pending.Wait(sys.Life(), 5*time.Second)
					if err != nil {
						return err
					}
					var status introspect.Status
					if err := json.Unmarshal(msg.Payload, &status); err != nil {
						return err
					}
					answers <- status
					<-sys.Life().Done()
					return nil
				}, nil
			},
		})
	})
	if _, err := platform.SpawnForTest(h, id, actor.KindAgent, factory); err != nil {
		t.Fatalf("spawn caller: %v", err)
	}
	select {
	case status := <-answers:
		if status.ActorID != string(id) || !status.Member || !status.Present {
			t.Fatalf("status=%+v", status)
		}
		snapshot, err := h.View().Snapshot(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.Member != status.Member || snapshot.L1Present != status.Present {
			t.Fatalf("gate=%+v view=%+v", status, snapshot)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("agent did not receive actor.status response through the channel gate")
	}
}
