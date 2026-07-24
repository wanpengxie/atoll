package actorctl

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCommandOwnerQuiesceTimeoutDoesNotForgetInFlightOwnership(t *testing.T) {
	var owner commandOwner
	release, err := owner.begin()
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := owner.quiesce(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Quiesce error=%v, want deadline exceeded", err)
	}
	if _, err := owner.begin(); !errors.Is(err, ErrChannelClosing) {
		t.Fatalf("sealed owner admitted a new command: %v", err)
	}

	release()
	retryCtx, retryCancel := context.WithTimeout(context.Background(), time.Second)
	defer retryCancel()
	if err := owner.quiesce(retryCtx); err != nil {
		t.Fatalf("retry Quiesce after command completion: %v", err)
	}

	expired, expire := context.WithCancel(context.Background())
	expire()
	if err := owner.quiesce(expired); err != nil {
		t.Fatalf("drained owner regressed to caller timeout: %v", err)
	}
}
