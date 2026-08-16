package svcactor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/peerproto"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestPortCloseReleasesQueuedRequestWithChannelClosed(t *testing.T) {
	port := NewPort()
	done := make(chan peerproto.Result, 1)
	go func() {
		result, _ := port.Call(context.Background(), "caller", peerproto.Request{})
		done <- result
	}()
	time.Sleep(10 * time.Millisecond)
	port.Close()
	select {
	case result := <-done:
		if result.Fail == nil || result.Fail.Code != "channel_closed" {
			t.Fatalf("close result=%+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("queued call remained blocked after close")
	}
}

func TestPortCloseReleasesReceivedPendingRequestWithChannelClosed(t *testing.T) {
	port := NewPort()
	received := make(chan struct{})
	go func() {
		_, err := port.receive(context.Background())
		if err == nil {
			close(received)
		}
	}()
	done := make(chan peerproto.Result, 1)
	go func() {
		result, _ := port.Call(context.Background(), "caller", peerproto.Request{})
		done <- result
	}()
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("request was not received")
	}
	port.Close()
	select {
	case result := <-done:
		if result.Fail == nil || result.Fail.Code != "channel_closed" {
			t.Fatalf("close result=%+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("pending call remained blocked after close")
	}
}

func TestPortWithoutSvcactorWaitsForCallerContextAndRemainsUsable(t *testing.T) {
	port := NewPort()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := port.Call(ctx, "caller", peerproto.Request{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("call error=%v", err)
	}

	served := make(chan struct{})
	go func() {
		req, err := port.receive(context.Background())
		if err == nil {
			req.done <- peerproto.Result{Body: []byte(`{"ok":true}`)}
		}
		close(served)
	}()
	result, err := port.Call(context.Background(), channel.ID("caller"), peerproto.Request{})
	if err != nil || string(result.Body) != `{"ok":true}` {
		t.Fatalf("reused port result=%+v err=%v", result, err)
	}
	<-served
}
