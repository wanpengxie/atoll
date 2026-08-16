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
	"github.com/wanpengxie/atoll/protocol"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestRecipeMembersAreIntroducedThroughTargetSvcactorAfterGenesis(t *testing.T) {
	channelDir := filepath.Join(t.TempDir(), "channels")
	eng, err := Boot(Config{ChannelDBDir: channelDir, Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordDeclRegister), map[string]any{
		"id": "recipe-member", "name": "Recipe Member", "class": svcactorTestReceiverClass, "config": map[string]any{}, "visibility": "public",
	}), nil)
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
		"name": "recipe-introduction", "overrides": map[string]any{"declarations": []any{map[string]any{"decl_id": "recipe-member"}}},
	}), &created)
	if len(created.Introduced.Members) != 1 || created.Introduced.Members[0].DeclID != "recipe-member" || created.Introduced.Members[0].Result != "ok" {
		t.Fatalf("introduced members=%+v", created.Introduced.Members)
	}
	bundle := waitBundle(t, eng, created.ID)
	if ids, err := bundle.View().DeclaredInstances(context.Background(), "recipe-member"); err != nil || len(ids) != 1 {
		t.Fatalf("recipe member ids=%v err=%v", ids, err)
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
