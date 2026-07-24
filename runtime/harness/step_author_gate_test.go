package harness

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type gateAuthority struct{ rows map[actor.ActorID]int64 }

func (a gateAuthority) LookupActive(_ context.Context, id actor.ActorID) (storespec.ActorControlRow, bool, error) {
	v, ok := a.rows[id]
	return storespec.ActorControlRow{ID: id, CurrentDeclVersion: v}, ok, nil
}
func (a gateAuthority) ListActive(context.Context) ([]storespec.ActorControlRow, error) {
	return nil, nil
}
func (a gateAuthority) CheckAuthor(_ context.Context, stamp storespec.AuthorStamp) (storespec.AuthorVerdict, error) {
	_, ok := a.rows[stamp.ID]
	if !ok {
		return storespec.AuthorNotMember, nil
	}
	return storespec.AuthorOK, nil
}

func TestStepAuthorGateActiveAuthorAndReceiverClosure(t *testing.T) {
	authority := gateAuthority{rows: map[actor.ActorID]int64{"author": 2, "receiver": 1}}
	step := newStepAuthorGate(Deps{Authority: authority})
	run := func(stamp caller, kind message.Kind, audience ...actor.ActorID) outcome {
		out, err := step.Run(ctxWithCaller(context.Background(), stamp), &message.Envelope{Kind: kind, Audience: audience})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	if got := run(caller{actorID: "missing"}, message.KindEvent, "missing"); got.RejectReason != HarnessAuthorNotMember {
		t.Fatalf("missing author = %+v", got)
	}
	if got := run(caller{actorID: "author"}, message.KindRequest, "missing"); got.RejectReason != HarnessReceiverNotMember {
		t.Fatalf("missing receiver = %+v", got)
	}
	if got := run(caller{actorID: "author"}, message.KindRequest, "receiver"); !got.Continue() {
		t.Fatalf("active request = %+v", got)
	}
	// Responses are allowed to close an obligation after the receiver identity
	// has ended; events are admitted once and inactive audience members are
	// filtered by delivery rather than rejecting the truth write.
	if got := run(caller{actorID: "author"}, message.KindResponse, "missing"); !got.Continue() {
		t.Fatalf("response exemption = %+v", got)
	}
	if got := run(caller{actorID: "author"}, message.KindEvent, "missing"); !got.Continue() {
		t.Fatalf("event audience filtering = %+v", got)
	}
}
