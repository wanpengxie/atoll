package home

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/platform/internal/sysactor"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

func TestMemberCloneCreatesAnotherIdentityAndNarratesForkOrigin(t *testing.T) {
	const decl = "clone-source"
	h := openRoutingHome(t, "clone-channel", routingDeclaration(decl, "routing-live"))
	parent := routingAgent(t, h, decl)
	value, err := h.opEntry.Execute(context.Background(), sysactor.TypeMemberCreate, sysactor.OperateRequest{
		ChannelID: h.channelID,
		Caller:    harness.Caller{Channel: h.channelID, Actor: parent},
		Anchor:    "clone-request",
		Payload:   json.RawMessage(`{"decl_id":"clone-source"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	child := value.(map[string]any)["member"].(actor.ActorID)
	if child == parent {
		t.Fatalf("clone reused parent %q", parent)
	}
	members, err := rosterMembersForSource(context.Background(), h.View(), decl)
	if err != nil || len(members) != 2 {
		t.Fatalf("members=%v err=%v", members, err)
	}
	rows, err := h.query.ReadAfterSeq(context.Background(), 0, 128)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range rows {
		if row.Envelope.Type != message.TypeSystemMemberCreated {
			continue
		}
		var payload struct {
			Member string `json:"member"`
			By     struct {
				ForkOf string `json:"fork_of"`
			} `json:"by"`
		}
		_ = json.Unmarshal(row.Envelope.Payload, &payload)
		if payload.Member == string(child) && payload.By.ForkOf == string(parent) {
			found = true
		}
	}
	if !found {
		t.Fatalf("member.created fork narration missing for child=%v parent=%s", child, parent)
	}
}
