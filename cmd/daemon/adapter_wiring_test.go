package main

import (
	"context"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
	rtharness "github.com/wanpengxie/ActOS/runtime/harness"
)

type recordingCallerChain struct {
	caller rtharness.CallerContext
}

func (c *recordingCallerChain) Write(ctx context.Context, env *message.Envelope) (khar.WriteResult, error) {
	c.caller = rtharness.CallerFromCtx(ctx)
	return khar.WriteResult{MessageID: env.ID, Seq: 1}, nil
}

func TestAdapterCallerChainStampsSystemSender(t *testing.T) {
	inner := &recordingCallerChain{}
	chain := &adapterCallerChain{
		inner:     inner,
		channelID: channel.ID("channel:test"),
	}
	env := &message.Envelope{
		ID:        "response:req:hash",
		ChannelID: "channel:test",
		Sender: message.Sender{
			Kind: actor.KindSystem,
			ID:   actor.SystemActorID,
		},
		Kind:       message.KindResponse,
		Type:       "xhs.publish",
		Visibility: message.VisibilityPrivate,
	}

	if _, err := chain.Write(context.Background(), env); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if inner.caller.ActorID != actor.SystemActorID {
		t.Fatalf("caller actor=%s want system", inner.caller.ActorID)
	}
	if inner.caller.ChannelID != "channel:test" {
		t.Fatalf("caller channel=%s want channel:test", inner.caller.ChannelID)
	}
	if !inner.caller.AllowProvidedSenderKind {
		t.Fatal("caller AllowProvidedSenderKind=false want true")
	}
}
