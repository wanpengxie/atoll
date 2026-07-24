package home

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func TestLookupForkReturnsReceiptWithoutRestoringRunWorldChild(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cs, err := runtime.OpenChannel(
		ctx,
		"fork-receipt",
		filepath.Join(t.TempDir(), "channel.sqlite"),
		runtime.OpenChannelOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	caller := actor.ActorID("agent:caller")
	child := actor.ActorID("agent:child")
	request := message.ID("fork-request")
	_, err = cs.SysOps.ForkActor(ctx, storespec.ForkTx{
		SysOpMeta: storespec.SysOpMeta{
			Anchor:        forkAnchor(caller, request),
			RequestDigest: "receipt-only",
			Source:        storespec.SysOpSourceMember,
			Sender:        caller,
		},
		Child: storespec.ActorControlRow{
			ID: child, Sponsor: caller, Kind: actor.KindAgent, Class: "test",
			CurrentDeclVersion: 1,
			Placement:          storespec.NewServerPlacement(),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	store := newHomeActorStore(channel.ID("fork-receipt"), cs, nil, nil)
	got, found, err := store.LookupFork(ctx, caller, request)
	if err != nil || !found || got != child {
		t.Fatalf("LookupFork = (%q,%v,%v), want (%q,true,nil)", got, found, err, child)
	}
	if len(store.runRows) != 0 {
		t.Fatalf("durable receipt restored run-world rows: %#v", store.runRows)
	}
	if _, active, err := store.LookupActive(ctx, child); err != nil || active {
		t.Fatalf("receipt child active after restart read-back: active=%v err=%v", active, err)
	}
}
