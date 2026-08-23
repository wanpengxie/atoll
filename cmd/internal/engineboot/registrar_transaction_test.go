package engineboot

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestRegistrarTransactionCreatesOneChannelAndPostsOneMaterializationIntent(t *testing.T) {
	_, _, core, registrar := newProtocolDeliveryRig(t)
	child := createdChannelID(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{
		"name": "transaction-child", "initial_actor_ids": []any{currentMemberID(t, core, channelspec.RootPrincipalID)},
	}))
	rows, _, err := core.View().ReadVisibleAfterSeq(context.Background(), 0, 512)
	if err != nil {
		t.Fatal(err)
	}
	posts := 0
	for _, stored := range rows {
		if stored.Envelope.Kind != message.KindRequest || stored.Envelope.Type != message.TypeSystemMemberCreate || stored.Envelope.Sender.ID != registrar {
			continue
		}
		var payload map[string]map[string]string
		_ = json.Unmarshal(stored.Envelope.Payload, &payload)
		if payload["body"]["decl_id"] != string(child) {
			continue
		}
		posts++
		if len(stored.Envelope.Audience) != 1 || stored.Envelope.Audience[0] != "system" {
			t.Fatalf("member.create audience=%v", stored.Envelope.Audience)
		}
	}
	if posts != 1 {
		t.Fatalf("registrar member.create posts=%d", posts)
	}
}
