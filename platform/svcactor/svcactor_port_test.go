package svcactor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestPortCallAfterCloseReturnsChannelUnavailable(t *testing.T) {
	port := NewPort()
	port.Close()
	result, err := port.Call(context.Background(), "caller", channel.Request{}, nil)
	if err != nil || result.Fail == nil || result.Fail.Code != "channel_unavailable" {
		t.Fatalf("close result=%+v err=%v", result, err)
	}
}

func TestPortWithoutActorHonorsCallerContextAndRemainsUsable(t *testing.T) {
	port := NewPort()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := port.Call(ctx, "caller", channel.Request{}, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("call error=%v", err)
	}
	served := make(chan struct{})
	go func() {
		req, err := port.receive(context.Background())
		if err == nil {
			req.done <- channel.Result{Body: []byte(`{"ok":true}`)}
		}
		close(served)
	}()
	result, err := port.Call(context.Background(), "caller", channel.Request{}, nil)
	if err != nil || string(result.Body) != `{"ok":true}` {
		t.Fatalf("reused port result=%+v err=%v", result, err)
	}
	<-served
}
