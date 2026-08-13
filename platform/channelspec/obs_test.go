package channelspec

import (
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
)

func TestObsRosterProjectionGoldenJSON(t *testing.T) {
	row := ObsRosterRow{
		ID: "tool:a", Kind: actor.KindTool, DeclID: "decl-a", Bound: true,
		Device: DeviceState{Kind: DeviceKnown, Online: true, ReceivedAt: 8},
	}
	raw, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	const golden = `{"id":"tool:a","kind":"tool","decl_id":"decl-a"}`
	if string(raw) != golden {
		t.Fatalf("golden mismatch: got=%s want=%s", raw, golden)
	}
}

func TestSystemRosterProjectionOmitsEmptyDeclaration(t *testing.T) {
	raw, _ := json.Marshal(ObsRosterRow{ID: actor.SystemActorID, Kind: actor.KindSystem})
	if string(raw) != `{"id":"system","kind":"system"}` {
		t.Fatalf("system projection=%s", raw)
	}
}
