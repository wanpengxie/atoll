package harness

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

type gateAuthority struct{ rows map[actor.ActorID]int64 }

func (a gateAuthority) IsActive(_ context.Context, id actor.ActorID) (bool, error) {
	_, ok := a.rows[id]
	return ok, nil
}

func TestStepReceiverGateClosure(t *testing.T) {
	authority := gateAuthority{rows: map[actor.ActorID]int64{"author": 2, "receiver": 1}}
	step := newStepReceiverGate(Deps{Presence: authority})
	run := func(kind message.Kind, audience ...actor.ActorID) outcome {
		out, err := step.Run(context.Background(), &message.Envelope{Kind: kind, Audience: audience})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	if got := run(message.KindRequest, "missing"); got.RejectReason != HarnessReceiverNotMember {
		t.Fatalf("missing receiver = %+v", got)
	}
	if got := run(message.KindRequest, "receiver"); !got.Continue() {
		t.Fatalf("active request = %+v", got)
	}
	// Responses are allowed to close an obligation after the receiver identity
	// has ended; events are admitted once and inactive audience members are
	// filtered by delivery rather than rejecting the truth write.
	if got := run(message.KindResponse, "missing"); !got.Continue() {
		t.Fatalf("response exemption = %+v", got)
	}
	if got := run(message.KindEvent, "missing"); !got.Continue() {
		t.Fatalf("event audience filtering = %+v", got)
	}
}
