package harness

import (
	"context"
	"testing"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

func TestInWorkerBus_HappyPath(t *testing.T) {
	f := newFixture(t)
	res, err := InWorkerBus(context.Background(), f.deps, validEvent(), validCallerCtx())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.OK {
		t.Fatalf("expected OK result, got error %+v", res.Error)
	}
	if res.Value == nil || res.Value.ID != "msg-1" {
		t.Fatalf("unexpected value: %+v", res.Value)
	}
}

func TestInWorkerBus_RejectIsResultErrNotException(t *testing.T) {
	f := newFixture(t)
	env := validEvent()
	env.Sender.ID = "bob" // sender mismatch
	res, err := InWorkerBus(context.Background(), f.deps, env, validCallerCtx())
	if err != nil {
		t.Fatalf("rejects must NOT surface as error (L2 §3.6.2): %v", err)
	}
	if res.OK {
		t.Fatalf("expected OK=false")
	}
	if res.Error == nil || res.Error.Reason != v4types.HarnessSenderMismatch {
		t.Fatalf("expected sender_mismatch reject, got %+v", res.Error)
	}
	if !res.IsReject() {
		t.Fatalf("IsReject should return true")
	}
}
