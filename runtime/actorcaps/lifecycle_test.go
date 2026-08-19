package actorcaps

import (
	"context"
	"testing"
)

type lifecycleShape struct{}

func (lifecycleShape) EndSelf(context.Context, EndSelfRequest) error { return nil }

func TestLifecycleHandleIsEndSelfOnly(t *testing.T) {
	var handle LifecycleHandle = lifecycleShape{}
	if err := handle.EndSelf(context.Background(), EndSelfRequest{Reason: "done"}); err != nil {
		t.Fatal(err)
	}
}
