package lagoon

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/protocol/actor"
)

func TestObsProjectionGoldenJSON(t *testing.T) {
	tests := []struct {
		name   string
		value  any
		golden string
	}{
		{
			"channel",
			ObsChannelRow{ID: "child", ParentID: "c0", Name: "child", QualifiedName: "c0.child", Type: "group", Status: regspec.ChannelPresent, OwnerPrincipal: "root", CreatedAt: 1},
			`{"id":"child","parent_id":"c0","name":"child","qualified_name":"c0.child","type":"group","status":"present","owner_principal":"root","created_at":1}`,
		},
		{
			"principal",
			ObsPrincipalRow{ID: "alice", Kind: actor.KindHuman, Email: "alice@example.test", DisplayName: "Alice", Status: regspec.PrincipalPresent, CreatedAt: 2},
			`{"id":"alice","kind":"human","email":"alice@example.test","display_name":"Alice","status":"present","created_at":2}`,
		},
		{
			"daemon",
			ObsDaemonRow{ID: "daemon-a", OwnerPrincipal: "alice", Name: "desk", Status: regspec.DevicePresent, CreatedAt: 3},
			`{"id":"daemon-a","owner_principal":"alice","name":"desk","status":"present","created_at":3}`,
		},
		{
			"decl",
			ObsDeclRow{ID: "decl-a", Name: "A", Owner: "alice", DefaultClass: "echo", Config: json.RawMessage(`{"x":1}`), Status: regspec.DeclPresent, Visibility: "private", CreatedAt: 4, UpdatedAt: 5},
			`{"id":"decl-a","name":"A","owner":"alice","default_class":"echo","config":{"x":1},"status":"present","visibility":"private","created_at":4,"updated_at":5}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw, err := json.Marshal(test.value)
			if err != nil {
				t.Fatal(err)
			}
			if string(raw) != test.golden {
				t.Fatalf("golden mismatch\n got: %s\nwant: %s", raw, test.golden)
			}
		})
	}
}

func TestObsDaemonProjectionHasNoSecretField(t *testing.T) {
	typ := reflect.TypeOf(ObsDaemonRow{})
	for i := 0; i < typ.NumField(); i++ {
		name := strings.ToLower(typ.Field(i).Name + " " + typ.Field(i).Tag.Get("json"))
		if strings.Contains(name, "key") || strings.Contains(name, "secret") || strings.Contains(name, "credential") {
			t.Fatalf("daemon projection contains secret-shaped field %s", typ.Field(i).Name)
		}
	}
}

func TestChannelProjectionDropsGenesisSpecButKeepsQualifiedName(t *testing.T) {
	projected := projectObsChannel(regspec.ChannelRow{
		ID: "child", ParentID: "c0", Name: "child", QualifiedName: "c0.child",
		Type: "group", Status: regspec.ChannelPresent, OwnerPrincipal: "root",
		Spec: json.RawMessage(`{"secret_shape":"not observable"}`), CreatedAt: 7,
	})
	raw, _ := json.Marshal(projected)
	if strings.Contains(string(raw), "spec") || !strings.Contains(string(raw), `"qualified_name":"c0.child"`) {
		t.Fatalf("channel projection=%s", raw)
	}
}
