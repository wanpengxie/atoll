package engineboot

import (
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/subjectgate"
)

func TestHumanCellListsThePrincipalsLiveGatewaySessions(t *testing.T) {
	eng, _, core, _ := newProtocolDeliveryRig(t)
	web, err := eng.gateway.Attach(channelspec.RootPrincipalID, nil)
	if err != nil {
		t.Fatal(err)
	}
	web.SetLabel("Mac Chrome")
	phone, err := eng.gateway.Attach(channelspec.RootPrincipalID, nil)
	if err != nil {
		t.Fatal(err)
	}
	phone.SetLabel("Android Chrome")
	t.Cleanup(func() {
		web.Close()
		phone.Close()
	})

	human := currentMemberID(t, core, channelspec.RootPrincipalID)
	var reply struct {
		Status   string                  `json:"status"`
		Sessions []platform.HumanSession `json:"sessions"`
	}
	decode := func(raw json.RawMessage) {
		t.Helper()
		if err := json.Unmarshal(raw, &reply); err != nil {
			t.Fatal(err)
		}
		if reply.Status != "completed" {
			t.Fatalf("terminal=%s", raw)
		}
	}
	decode(callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, human, subjectgate.WordUISessionList, map[string]any{}))
	if len(reply.Sessions) != 2 {
		t.Fatalf("sessions=%v, want both live clients", reply.Sessions)
	}
	seen := map[string]string{}
	for _, session := range reply.Sessions {
		seen[session.ID] = session.Label
	}
	if seen[web.ID()] != "Mac Chrome" || seen[phone.ID()] != "Android Chrome" {
		t.Fatalf("sessions=%v", reply.Sessions)
	}

	phone.Close()
	reply.Sessions = nil
	decode(callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, human, subjectgate.WordUISessionList, map[string]any{}))
	if len(reply.Sessions) != 1 || reply.Sessions[0].ID != web.ID() {
		t.Fatalf("disconnected phone remained visible: %v", reply.Sessions)
	}
}
