package engineboot

import (
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestRegistrarRoutingPolicyRejectsWrongDoorAndReservedClass(t *testing.T) {
	_, _, core, registrar := newProtocolDeliveryRig(t)
	for name, test := range map[string]struct {
		word    lagoon.Word
		payload any
		code    lagoon.ErrorCode
	}{
		"principal create outside lobby": {lagoon.WordPrincipalCreate, map[string]any{"id": "bad", "email": "bad@example.test", "secret_hash": "hash"}, lagoon.CodePermissionDenied},
		"overlay for another channel":    {lagoon.WordActorOverlaySet, map[string]any{"decl_id": "missing", "channel_id": channelspec.LobbyChannelID, "config": map[string]any{}}, lagoon.CodePermissionDenied},
		"system class declaration":       {lagoon.WordActorTemplateCreate, map[string]any{"id": "bad-peer", "name": "bad", "class": "peeractor", "visibility": "private", "config": map[string]any{}}, lagoon.CodeReserved},
	} {
		t.Run(name, func(t *testing.T) {
			terminal := decodeTerminal(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(test.word), test.payload))
			if terminal.Status != message.StatusFailed || terminal.ErrorCode != string(test.code) {
				t.Fatalf("terminal=%+v", terminal)
			}
		})
	}

	describe := callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, "actor.describe", map[string]any{})
	var card struct {
		Status string                     `json:"status"`
		Words  map[string]json.RawMessage `json:"words"`
	}
	if err := json.Unmarshal(describe, &card); err != nil || card.Status != message.StatusCompleted || len(card.Words) != 0 {
		t.Fatalf("registrar describe=%s err=%v", describe, err)
	}
}
