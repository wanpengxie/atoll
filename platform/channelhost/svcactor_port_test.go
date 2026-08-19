package channelhost

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestAcquirePortDisappearsAtomicallyWhenEntryIsDestroyed(t *testing.T) {
	ctx := context.Background()
	host := newTestHost(t)
	id := channel.ID("port-destroy")
	if err := host.provisionGenesis(ctx, genesisSpec(id), "c0.port-destroy"); err != nil {
		t.Fatal(err)
	}
	if err := host.Open(ctx, OpenSpec{ChannelID: id, ChannelName: "c0.port-destroy", ExpectedType: "group"}); err != nil {
		t.Fatal(err)
	}
	port, generation, ok := host.AcquirePort(id)
	if !ok || port == nil || generation == 0 {
		t.Fatalf("port=%p generation=%d ok=%v", port, generation, ok)
	}
	done := make(chan channel.Result, 1)
	go func() {
		result, _ := port.Call(context.Background(), "caller", channel.Request{}, nil)
		done <- result
	}()
	if err := host.Destroy(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := host.AcquirePort(id); ok {
		t.Fatal("destroyed entry still exposed a port")
	}
	select {
	case result := <-done:
		if result.Fail == nil || result.Fail.Code != "channel_unavailable" {
			t.Fatalf("queued result=%+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("destroy did not release queued port request")
	}
}

func TestReopenedChannelPublishesNewPortAndClosesOldGeneration(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	id := channel.ID("port-reopen")
	first, err := New(root, testBindings{}, HomeDeps{CompositionResolver: testResolver{}, IntroductionResolver: testResolver{}, RegistryBindings: testBindings{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.provisionGenesis(ctx, genesisSpec(id), "c0.port-reopen"); err != nil {
		t.Fatal(err)
	}
	if err := first.Open(ctx, OpenSpec{ChannelID: id, ChannelName: "c0.port-reopen", ExpectedType: "group"}); err != nil {
		t.Fatal(err)
	}
	oldPort, _, _ := first.AcquirePort(id)
	if err := first.Close(ctx); err != nil {
		t.Fatal(err)
	}
	closed, err := oldPort.Call(ctx, "caller", channel.Request{}, nil)
	if err != nil || closed.Fail == nil || closed.Fail.Code != "channel_unavailable" {
		t.Fatalf("old port result=%+v err=%v", closed, err)
	}

	second, err := New(root, testBindings{}, HomeDeps{CompositionResolver: testResolver{}, IntroductionResolver: testResolver{}, RegistryBindings: testBindings{}})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close(ctx)
	if err := second.Open(ctx, OpenSpec{ChannelID: id, ChannelName: "c0.port-reopen", ExpectedType: "group"}); err != nil {
		t.Fatal(err)
	}
	newPort, _, ok := second.AcquirePort(id)
	if !ok || newPort == nil || newPort == oldPort {
		t.Fatalf("new port=%p old=%p ok=%v", newPort, oldPort, ok)
	}
}
