package engineboot

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/url"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/protocol"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestRecipeMemberConfigsAreAppliedByOverlayBeforeSvcactorIntroduction(t *testing.T) {
	channelDir := filepath.Join(t.TempDir(), "channels")
	eng, err := Boot(Config{ChannelDBDir: channelDir, Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	for _, declaration := range []map[string]any{
		{"id": "recipe-overridden", "name": "Recipe Overridden", "class": svcactorTestReceiverClass, "config": map[string]any{"marker": "DEFAULT"}, "visibility": "public"},
		{"id": "recipe-default", "name": "Recipe Default", "class": svcactorTestReceiverClass, "config": map[string]any{"marker": "DEFAULT"}, "visibility": "public"},
	} {
		terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordDeclRegister), declaration), nil)
	}
	var created struct {
		ID         channel.ID `json:"id"`
		Introduced struct {
			Members []struct {
				DeclID string `json:"decl_id"`
				Result any    `json:"result"`
			} `json:"members"`
		} `json:"introduced"`
	}
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{
		"name": "recipe-introduction", "overrides": map[string]any{"declarations": []any{
			map[string]any{"decl_id": "recipe-overridden", "config": map[string]any{"marker": "RECIPE-OVERRIDE"}},
			map[string]any{"decl_id": "recipe-default"},
		}},
	}), &created)
	if len(created.Introduced.Members) != 2 || created.Introduced.Members[0].DeclID != "recipe-overridden" || created.Introduced.Members[0].Result != "ok" || created.Introduced.Members[1].DeclID != "recipe-default" || created.Introduced.Members[1].Result != "ok" {
		t.Fatalf("introduced members=%+v", created.Introduced.Members)
	}
	bundle := waitBundle(t, eng, created.ID)
	for _, declID := range []string{"recipe-overridden", "recipe-default"} {
		if ids, err := bundle.View().DeclaredInstances(context.Background(), declID); err != nil || len(ids) != 1 {
			t.Fatalf("recipe member %s ids=%v err=%v", declID, ids, err)
		}
	}

	dbPath, err := channelhost.DBPath(channelDir, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	u := &url.URL{Scheme: "file", Path: dbPath}
	db, err := sql.Open("sqlite", u.String()+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	configs := map[string]string{}
	rows, err := db.Query(`SELECT source_decl_id,config_json FROM actor_registry WHERE source_decl_id IN ('recipe-overridden','recipe-default') AND deregistered_at IS NULL`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var declID, config string
		if err := rows.Scan(&declID, &config); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		configs[declID] = config
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if configs["recipe-overridden"] != `{"marker":"RECIPE-OVERRIDE"}` || configs["recipe-default"] != `{"marker":"DEFAULT"}` {
		t.Fatalf("recipe configs=%v", configs)
	}
	overlays, err := eng.registry.GetOverlays(context.Background(), created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(overlays) != 1 || overlays[0].DeclID != "recipe-overridden" || string(overlays[0].Config) != `{"marker":"RECIPE-OVERRIDE"}` {
		t.Fatalf("recipe overlays=%+v", overlays)
	}
	var requests, replies, audits int
	if err := db.QueryRow(`SELECT COUNT(*) FROM messages WHERE type='channel.introduce_actor' AND kind='request' AND sender_id LIKE 'tool:atoll-internal:svcactor:%'`).Scan(&requests); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM messages WHERE type='channel.introduce_actor' AND kind='response'`).Scan(&replies); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM messages WHERE type='svcactor.inbound' AND visibility=?`, message.VisibilitySystem).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if requests == 0 || replies == 0 || audits == 0 {
		t.Fatalf("introduction ledger requests=%d replies=%d audits=%d", requests, replies, audits)
	}
}

func TestRecipePeerServingFailureIsReportedWithoutKillingChannel(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	var closed lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{
		"name": "closed-recipe-target", "overrides": map[string]any{"profile": map[string]any{"serving": 0}},
	}), &closed)
	peerDecl := lagoon.PeerActorDeclPrefix + string(closed.ID)
	var raw struct {
		ID         string `json:"id"`
		Introduced struct {
			Members []struct {
				DeclID string          `json:"decl_id"`
				Result json.RawMessage `json:"result"`
			} `json:"members"`
		} `json:"introduced"`
	}
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{
		"name": "recipe-holder", "overrides": map[string]any{"declarations": []any{map[string]any{"decl_id": peerDecl}}},
	}), &raw)
	if raw.ID == "" || len(raw.Introduced.Members) != 1 || raw.Introduced.Members[0].DeclID != peerDecl {
		t.Fatalf("create result=%+v", raw)
	}
	var failed map[string]string
	if err := json.Unmarshal(raw.Introduced.Members[0].Result, &failed); err != nil || failed["error_code"] != "forbidden" {
		t.Fatalf("member result=%s decoded=%v err=%v", raw.Introduced.Members[0].Result, failed, err)
	}
	if _, ok := eng.host.Acquire(channel.ID(raw.ID)); !ok {
		t.Fatal("channel died after recipe introduction failure")
	}
	holder := waitBundle(t, eng, channel.ID(raw.ID))
	root := principalActorID(t, holder, protocol.RootPrincipalID)
	rows, _, err := holder.View().ReadVisibleAfterSeq(context.Background(), channel.Reader{ActorID: root, Mode: channel.ReaderMember}, 0, 1024)
	if err != nil {
		t.Fatal(err)
	}
	svc := onlyDecl(t, holder, lagoon.SvcActorDeclID)
	var requestID message.ID
	forbiddenReply := false
	for _, row := range rows {
		if row.Envelope.Type != "channel.introduce_actor" {
			continue
		}
		if row.Envelope.Kind == message.KindRequest && row.Envelope.Sender.ID == svc {
			var payload struct {
				DeclID string `json:"decl_id"`
			}
			if json.Unmarshal(row.Envelope.Payload, &payload) == nil && payload.DeclID == peerDecl {
				requestID = row.Envelope.ID
			}
		}
		if row.Envelope.Kind == message.KindResponse && requestID != "" && row.Envelope.ParentID == requestID {
			var terminal struct {
				Status    string `json:"status"`
				ErrorCode string `json:"error_code"`
			}
			forbiddenReply = json.Unmarshal(row.Envelope.Payload, &terminal) == nil && terminal.Status == message.StatusFailed && terminal.ErrorCode == "forbidden"
		}
	}
	if requestID == "" || !forbiddenReply {
		t.Fatalf("holder ledger request=%s forbidden_reply=%v", requestID, forbiddenReply)
	}
}

func TestChannelRecipeProjectionForksPeerDeclarationsWithoutConfig(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	create := func(payload any) lagoon.ChannelCreateReply {
		t.Helper()
		var created lagoon.ChannelCreateReply
		terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), payload), &created)
		return created
	}
	target := create(map[string]any{"name": "recipe-peer-target"})
	peerDecl := lagoon.PeerActorDeclPrefix + string(target.ID)
	holder := create(map[string]any{"name": "recipe-peer-holder", "overrides": map[string]any{"declarations": []any{map[string]any{"decl_id": peerDecl}}}})
	var view regspec.ChannelRow
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelGet), map[string]any{"channel_id": holder.ID}), &view)
	if view.Recipe == nil || len(view.Recipe.Declarations) != 1 || view.Recipe.Declarations[0].DeclID != peerDecl || view.Recipe.Declarations[0].Config != nil {
		t.Fatalf("peer recipe=%+v", view.Recipe)
	}
	fork := create(map[string]any{"name": "recipe-peer-fork", "overrides": view.Recipe})
	if len(fork.Introduced.Members) != 1 || fork.Introduced.Members[0].DeclID != peerDecl || fork.Introduced.Members[0].Result != "ok" {
		t.Fatalf("fork introduction=%+v", fork.Introduced.Members)
	}
}

func TestChannelRecipesRejectSystemDeclarationsPeerConfigAndDuplicateIDs(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordDeclRegister), map[string]any{
		"id": "ordinary-recipe", "name": "Ordinary Recipe", "class": svcactorTestReceiverClass, "config": map[string]any{}, "visibility": "public",
	}), nil)
	var target lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "recipe-validation-target"}), &target)
	peerDecl := lagoon.PeerActorDeclPrefix + string(target.ID)
	cases := []struct {
		name         string
		declarations []any
	}{
		{name: "svcactor id", declarations: []any{map[string]any{"decl_id": lagoon.SvcActorDeclID}}},
		{name: "coreactor id", declarations: []any{map[string]any{"decl_id": lagoon.CoreActorDeclID}}},
		{name: "registrar id", declarations: []any{map[string]any{"decl_id": lagoon.RegistrarSeatDeclID}}},
		{name: "peer config", declarations: []any{map[string]any{"decl_id": peerDecl, "config": map[string]any{"channel": target.ID}}}},
		{name: "duplicate id", declarations: []any{map[string]any{"decl_id": "ordinary-recipe"}, map[string]any{"decl_id": "ordinary-recipe"}}},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := map[string]any{"declarations": tc.declarations}
			template := decodeTerminal(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelTemplateRegister), map[string]any{"id": "invalid-template-" + strconv.Itoa(i), "name": "Invalid", "visibility": "public", "body": body}))
			if template.Status != message.StatusFailed || template.ErrorCode != string(lagoon.CodeInvalidArgs) {
				t.Fatalf("template terminal=%+v", template)
			}
			created := decodeTerminal(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "invalid-create-" + strconv.Itoa(i), "overrides": body}))
			if created.Status != message.StatusFailed || created.ErrorCode != string(lagoon.CodeInvalidArgs) {
				t.Fatalf("create terminal=%+v", created)
			}
		})
	}
}
