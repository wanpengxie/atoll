package engineboot

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/svcactor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestFrozenRecipePersistsRenderedDeclarationsOverlayAndServiceCard(t *testing.T) {
	eng, channelDir, core, registrar := newProtocolDeliveryRig(t)
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordActorTemplateCreate), map[string]any{
		"id": "recipe-echo", "name": "recipe-echo", "class": "echo", "visibility": "public", "config": map[string]any{"max_seconds": 1},
	}), nil)
	invalidProfile := decodeTerminal(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{
		"name": "bad-service", "recipe": map[string]any{"profile": map[string]any{"svc_agent": "missing"}},
	}))
	if invalidProfile.Status != message.StatusFailed || invalidProfile.ErrorCode != string(lagoon.CodeInvalidArgs) {
		t.Fatalf("invalid service profile=%+v", invalidProfile)
	}

	child := createdChannelID(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{
		"name": "frozen-recipe", "recipe": map[string]any{
			"declarations": []any{map[string]any{"decl_id": "recipe-echo", "config": map[string]any{"max_seconds": 2}}},
			"profile":      map[string]any{"svc_agent": nil, "endpoints": map[string]any{"echo.say": map[string]any{"receiver": "recipe-echo"}}},
		},
	}))
	row, found, err := eng.registry.GetChannelDesired(context.Background(), child)
	if err != nil || !found {
		t.Fatalf("child row found=%v err=%v", found, err)
	}
	var spec lagoon.GenesisSpec
	if err := json.Unmarshal(row.Spec, &spec); err != nil {
		t.Fatal(err)
	}
	foundRendered := false
	for _, declaration := range spec.Declarations {
		if declaration.DeclID == "recipe-echo" {
			foundRendered = string(declaration.Rendered.Config) == `{"max_seconds":2}`
		}
	}
	if !foundRendered {
		t.Fatalf("frozen declarations=%+v", spec.Declarations)
	}
	overlays, err := eng.registry.GetOverlays(context.Background(), child)
	if err != nil || len(overlays) != 1 || string(overlays[0].Config) != `{"max_seconds":2}` {
		t.Fatalf("overlays=%+v err=%v", overlays, err)
	}
	bundle := waitBundle(t, eng, child)
	roster, err := bundle.View().Roster(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	recipeMember := ""
	for _, member := range roster {
		if member.DeclID == "recipe-echo" {
			recipeMember = string(member.ID)
		}
	}
	if parts := strings.Split(recipeMember, ":"); len(parts) != 3 || parts[0] != "tool" {
		t.Fatalf("recipe member=%q roster=%+v", recipeMember, roster)
	}
	port, _, ok := eng.host.AcquirePort(child)
	if !ok {
		t.Fatal("child service port unavailable")
	}
	describeCtx, stopDescribe := context.WithTimeout(context.Background(), 5*time.Second)
	card, err := port.Describe(describeCtx, channelspec.C0ChannelID, channel.Describe{From: channel.DescribeFrom{Channel: channelspec.C0ChannelID}})
	stopDescribe()
	if err != nil || len(card.Words["echo.say"]) == 0 {
		t.Fatalf("peer describe card=%+v err=%v roster=%+v", card, err, roster)
	}
	childPath, err := channelhost.DBPath(channelDir, child)
	if err != nil {
		t.Fatal(err)
	}
	u := &url.URL{Scheme: "file", Path: childPath}
	db, err := sql.Open("sqlite", u.String()+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var serviceRaw []byte
	var service struct {
		Table svcactor.ServiceTable `json:"table"`
		Card  channel.Card          `json:"card"`
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		serviceRaw = nil
		service = struct {
			Table svcactor.ServiceTable `json:"table"`
			Card  channel.Card          `json:"card"`
		}{}
		err = db.QueryRow(`SELECT bytes FROM actor_state WHERE resource_id=?`, svcactor.ServiceStateKey).Scan(&serviceRaw)
		if err == nil {
			err = json.Unmarshal(serviceRaw, &service)
		}
		if err == nil && len(service.Card.Words["echo.say"]) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("service card did not materialize: raw=%s state=%+v err=%v", serviceRaw, service, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := json.Unmarshal(serviceRaw, &service); err != nil || string(service.Table.Endpoints["echo.say"]) != recipeMember || service.Table.SvcAgent != nil || len(service.Card.Words["echo.say"]) == 0 {
		t.Fatalf("service state=%+v raw=%s err=%v", service, serviceRaw, err)
	}
}
