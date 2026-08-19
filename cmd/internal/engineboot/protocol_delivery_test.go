package engineboot

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/wanpengxie/atoll/drivers/tools/echo"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/svcactor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestRegistrarPolicyAndFrozenRecipeDelivery(t *testing.T) {
	channelDir := filepath.Join(t.TempDir(), "channels")
	eng, err := Boot(Config{ChannelDBDir: channelDir, Addr: "127.0.0.1:0", RootPassword: "test-root-password", OpenRegistration: true}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(channelspec.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarDeclID)

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

	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordActorTemplateCreate), map[string]any{
		"id": "recipe-echo", "name": "Recipe Echo", "class": "echo", "visibility": "public", "config": map[string]any{"max_seconds": 1},
	}), nil)
	invalidProfile := decodeTerminal(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{
		"name": "bad-service", "recipe": map[string]any{"profile": map[string]any{"svc_agent": "missing"}},
	}))
	if invalidProfile.Status != message.StatusFailed || invalidProfile.ErrorCode != string(lagoon.CodeInvalidArgs) {
		t.Fatalf("invalid service profile=%+v", invalidProfile)
	}

	raw := callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{
		"name": "frozen-recipe", "recipe": map[string]any{
			"declarations": []any{map[string]any{"decl_id": "recipe-echo", "config": map[string]any{"max_seconds": 2}}},
			"profile":      map[string]any{"svc_agent": nil, "endpoints": map[string]any{"echo.say": map[string]any{"receiver": "recipe-echo"}}},
		},
	})
	var terminal map[string]json.RawMessage
	if err := json.Unmarshal(raw, &terminal); err != nil || len(terminal) != 2 {
		t.Fatalf("system channel-create terminal=%s err=%v", raw, err)
	}
	var value map[string]json.RawMessage
	if err := json.Unmarshal(terminal["value"], &value); err != nil || len(value) != 1 {
		t.Fatalf("system channel-create value=%s err=%v", terminal["value"], err)
	}
	var child string
	_ = json.Unmarshal(value["channel_id"], &child)
	row, found, err := eng.registry.GetChannelDesired(context.Background(), channel.ID(child))
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
	overlays, err := eng.registry.GetOverlays(context.Background(), channel.ID(child))
	if err != nil || len(overlays) != 1 || string(overlays[0].Config) != `{"max_seconds":2}` {
		t.Fatalf("overlays=%+v err=%v", overlays, err)
	}
	bundle := waitBundle(t, eng, channel.ID(child))
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
	childPath, err := channelhost.DBPath(channelDir, channel.ID(child))
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
	if err := db.QueryRow(`SELECT bytes FROM actor_state WHERE resource_id=?`, svcactor.ServiceStateKey).Scan(&serviceRaw); err != nil {
		t.Fatal(err)
	}
	var table svcactor.ServiceTable
	if err := json.Unmarshal(serviceRaw, &table); err != nil || string(table.Endpoints["echo.say"]) != recipeMember || table.SvcAgent != nil {
		t.Fatalf("service table=%+v raw=%s err=%v", table, serviceRaw, err)
	}

	rows, _, err := core.View().ReadVisibleAfterSeq(context.Background(), 0, 512)
	if err != nil {
		t.Fatal(err)
	}
	posts := 0
	for _, stored := range rows {
		if stored.Envelope.Kind == message.KindRequest && stored.Envelope.Type == message.TypeSystemMemberCreate && stored.Envelope.Sender.ID == registrar {
			var payload map[string]map[string]string
			_ = json.Unmarshal(stored.Envelope.Payload, &payload)
			if payload["body"]["decl_id"] == child {
				posts++
				if len(stored.Envelope.Audience) != 1 || stored.Envelope.Audience[0] != "system" {
					t.Fatalf("member.create audience=%v", stored.Envelope.Audience)
				}
			}
		}
	}
	if posts != 1 {
		t.Fatalf("registrar member.create posts=%d", posts)
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
