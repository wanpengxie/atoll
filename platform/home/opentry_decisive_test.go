package home

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/sysactor"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// Rejected member words leave nothing behind. With the receipt ledger gone
// there is no anchor, no digest and no started/completed pair — a rejection
// simply returns its typed error, so repeating the same garbage grows neither
// the value ledger nor the message log.
func TestMemberWordRejectionsLeaveNoLedger(t *testing.T) {
	h, err := Open(Config{
		ChannelID:            "member-noise",
		DBPath:               filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver:  routingResolver{},
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		Bootstrap:            true,
		BootstrapDeclarations: []DeclareRequest{{
			SourceDeclID: "decl:probe", Class: "routing-live",
			Placement: storespec.NewServerPlacement(), Kind: actor.KindAgent,
			CreatedAt: time.Now().UnixMilli(),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	ctx := context.Background()

	declared, found, err := h.View().DeclaredBySourceOne(ctx, "decl:probe")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("bootstrap actor missing")
	}
	sender := declared.ID

	before, err := h.cs.Query.MaxSeq(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Malformed payload: rejected at the entrance.
	raw := json.RawMessage(`{"instance_id":""}`)
	for range 3 {
		if _, err := h.opEntry.Execute(ctx, sysactor.TypeRestartActor,
			sysactor.OperateRequest{ChannelID: h.channelID, Sender: sender, Anchor: "op-msg-bad", Payload: raw}); err == nil {
			t.Fatal("malformed restart unexpectedly succeeded")
		}
	}

	// Absent target: rejected by the ledger verdict.
	encoded, _ := json.Marshal(map[string]string{"instance_id": "agent:absent:1"})
	for range 3 {
		if _, err := h.opEntry.Execute(ctx, sysactor.TypeRestartActor,
			sysactor.OperateRequest{ChannelID: h.channelID, Sender: sender, Anchor: "op-msg-absent", Payload: encoded}); err == nil {
			t.Fatal("absent-target restart unexpectedly succeeded")
		}
	}

	after, err := h.cs.Query.MaxSeq(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("rejections appended %d message rows; a rejection must leave nothing", after-before)
	}
}
