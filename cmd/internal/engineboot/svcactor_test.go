package engineboot

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/drivers/tools/echo"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/peerproto"
	"github.com/wanpengxie/atoll/protocol"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	classregistry "github.com/wanpengxie/atoll/registry"
)

const svcactorTestReceiverClass = "engineboot-test-receiver"

func init() {
	classregistry.Register(svcactorTestReceiverClass, classregistry.ClassDecl{
		Kind: actor.KindTool, Placement: channel.PlacementServer,
		New: func(spec classregistry.InstanceSpec, _ classregistry.Deps) (platform.ActorDecl, error) {
			return platform.ActorDecl{ID: spec.ID, Kind: actor.KindTool, Factory: platform.ActorFactory{Proc: echo.Def(echo.Config{})}}, nil
		},
	})
}

func TestSvcactorCrossChannelLedgerAndAuditChain(t *testing.T) {
	channelDir := filepath.Join(t.TempDir(), "channels")
	eng, err := Boot(Config{ChannelDBDir: channelDir, Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordDeclRegister), map[string]any{
		"id": "echo-chain", "name": "Echo Chain", "class": svcactorTestReceiverClass, "config": map[string]any{}, "visibility": "public",
	}), nil)
	var source, target lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "chain-source"}), &source)
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{
		"name": "chain-target", "overrides": map[string]any{
			"declarations": []any{map[string]any{"decl_id": "echo-chain"}},
			"profile":      map[string]any{"endpoints": map[string]any{"echo.say": map[string]any{"description": "echo", "receiver": "echo-chain"}}},
		},
	}), &target)
	sourceBundle := waitBundle(t, eng, source.ID)
	targetBundle := waitBundle(t, eng, target.ID)
	peerDecl := "peer:" + string(target.ID)
	if terminal := decodeTerminal(t, callMember(t, source.ID, sourceBundle, protocol.RootPrincipalID, actor.SystemActorID, "channel.introduce_actor", map[string]any{"kind": "tool", "decl_id": peerDecl})); terminal.Status != message.StatusCompleted {
		t.Fatalf("introduce peer=%+v", terminal)
	}
	peer := onlyDecl(t, sourceBundle, peerDecl)
	payload := map[string]any{"text": "byte-equivalent", "n": 7}
	terminal := decodeTerminal(t, callMember(t, source.ID, sourceBundle, protocol.RootPrincipalID, peer, "echo.say", payload))
	if terminal.Status != message.StatusCompleted {
		t.Fatalf("echo terminal=%+v", terminal)
	}
	forbidden := decodeTerminal(t, callMember(t, source.ID, sourceBundle, protocol.RootPrincipalID, peer, "channel.remove_actor", map[string]any{"instance_id": "anything"}))
	if forbidden.Status != message.StatusFailed || forbidden.ErrorCode != "forbidden" {
		t.Fatalf("foreign management=%+v", forbidden)
	}
	wantPayload, _ := json.Marshal(payload)

	sourceRoot := principalActorID(t, sourceBundle, protocol.RootPrincipalID)
	sourceRows, _, err := sourceBundle.View().ReadVisibleAfterSeq(context.Background(), channel.Reader{ActorID: sourceRoot, Mode: channel.ReaderMember}, 0, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var sourceRequest message.Envelope
	for _, row := range sourceRows {
		if row.Envelope.Kind == message.KindRequest && row.Envelope.Type == "echo.say" && row.Envelope.Sender.ID == sourceRoot {
			sourceRequest = row.Envelope
		}
		if row.Envelope.Type == "svcactor.inbound" {
			t.Fatal("source ledger gained a svcactor audit event")
		}
	}
	if sourceRequest.ID == "" || string(sourceRequest.Payload) != string(wantPayload) {
		t.Fatalf("source request=%+v payload=%s want=%s", sourceRequest, sourceRequest.Payload, wantPayload)
	}

	targetRoot := principalActorID(t, targetBundle, protocol.RootPrincipalID)
	targetRows, _, err := targetBundle.View().ReadVisibleAfterSeq(context.Background(), channel.Reader{ActorID: targetRoot, Mode: channel.ReaderMember}, 0, 2048)
	if err != nil {
		t.Fatal(err)
	}
	svc := onlyDecl(t, targetBundle, lagoon.SvcActorDeclID)
	var localRequest message.Envelope
	var audit struct {
		Origin         peerproto.Origin `json:"origin"`
		Type           string           `json:"type"`
		LocalRequestID message.ID       `json:"local_request_id"`
	}
	for _, row := range targetRows {
		switch {
		case row.Envelope.Kind == message.KindRequest && row.Envelope.Type == "echo.say" && row.Envelope.Sender.ID == svc:
			localRequest = row.Envelope
		}
		if row.Envelope.Type == "channel.remove_actor" {
			t.Fatal("forbidden management request landed in target ledger")
		}
	}
	if localRequest.ID == "" || string(localRequest.Payload) != string(sourceRequest.Payload) {
		t.Fatalf("local request=%+v payload=%s source=%s", localRequest, localRequest.Payload, sourceRequest.Payload)
	}
	targetPath, err := channelhost.DBPath(channelDir, target.ID)
	if err != nil {
		t.Fatal(err)
	}
	u := &url.URL{Scheme: "file", Path: targetPath}
	db, err := sql.Open("sqlite", u.String()+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var auditRaw, auditVisibility string
	if err := db.QueryRow(`SELECT payload,visibility FROM messages WHERE type='svcactor.inbound' ORDER BY seq DESC LIMIT 1`).Scan(&auditRaw, &auditVisibility); err != nil {
		rows, _ := db.Query(`SELECT seq,kind,type,sender_id,payload FROM messages ORDER BY seq`)
		defer rows.Close()
		var seen []map[string]any
		for rows.Next() {
			var seq int64
			var kind, typ, sender, payload string
			_ = rows.Scan(&seq, &kind, &typ, &sender, &payload)
			seen = append(seen, map[string]any{"seq": seq, "kind": kind, "type": typ, "sender": sender, "payload": payload})
		}
		t.Fatalf("audit query: %v messages=%v", err, seen)
	}
	if err := json.Unmarshal([]byte(auditRaw), &audit); err != nil {
		t.Fatal(err)
	}
	if auditVisibility != string(message.VisibilitySystem) {
		t.Fatalf("audit visibility=%q", auditVisibility)
	}
	wantOrigin := peerproto.Origin{Channel: source.ID, Actor: sourceRoot, RequestID: sourceRequest.ID}
	if audit.Origin != wantOrigin || audit.Type != "echo.say" || audit.LocalRequestID != localRequest.ID {
		t.Fatalf("audit=%+v wantOrigin=%+v local=%s", audit, wantOrigin, localRequest.ID)
	}
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelProfileSet), map[string]any{
		"channel_id": target.ID, "description": "closed to new peers", "serving": 0,
		"endpoints": map[string]any{"echo.say": map[string]any{"description": "echo", "receiver": "echo-chain"}},
	}), nil)
	if terminal := decodeTerminal(t, callMember(t, source.ID, sourceBundle, protocol.RootPrincipalID, peer, "echo.say", payload)); terminal.Status != message.StatusCompleted {
		t.Fatalf("existing peer after serving=0 terminal=%+v", terminal)
	}
}

func TestChildCanIntroduceParentPeerForReverseResult(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordDeclRegister), map[string]any{
		"id": "echo-parent", "name": "Echo Parent", "class": svcactorTestReceiverClass, "config": map[string]any{}, "visibility": "public",
	}), nil)
	var parent lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{
		"name": "reverse-parent", "overrides": map[string]any{
			"declarations": []any{map[string]any{"decl_id": "echo-parent"}},
			"profile":      map[string]any{"endpoints": map[string]any{"echo.say": map[string]any{"receiver": "echo-parent"}}},
		},
	}), &parent)
	parentBundle := waitBundle(t, eng, parent.ID)
	parentCore := onlyDecl(t, parentBundle, lagoon.CoreActorDeclID)
	var child lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, parent.ID, parentBundle, protocol.RootPrincipalID, parentCore, string(lagoon.WordChannelCreate), map[string]any{"name": "reverse-child"}), &child)
	childBundle := waitBundle(t, eng, child.ID)
	parentPeerDecl := "peer:" + string(parent.ID)
	if ids, err := childBundle.View().DeclaredInstances(context.Background(), parentPeerDecl); err != nil || len(ids) != 0 {
		t.Fatalf("child was born holding parent ids=%v err=%v", ids, err)
	}
	if terminal := decodeTerminal(t, callMember(t, child.ID, childBundle, protocol.RootPrincipalID, actor.SystemActorID, "channel.introduce_actor", map[string]any{"kind": "tool", "decl_id": parentPeerDecl})); terminal.Status != message.StatusCompleted {
		t.Fatalf("reverse peer introduce=%+v", terminal)
	}
	parentPeer := onlyDecl(t, childBundle, parentPeerDecl)
	if terminal := decodeTerminal(t, callMember(t, child.ID, childBundle, protocol.RootPrincipalID, parentPeer, "echo.say", map[string]any{"result": "done"})); terminal.Status != message.StatusCompleted {
		t.Fatalf("reverse result=%+v", terminal)
	}
}

func TestParentCreatesTwentyChildrenAndCallsBusinessEndpoint(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordDeclRegister), map[string]any{
		"id": "echo-twenty", "name": "Echo Twenty", "class": svcactorTestReceiverClass, "config": map[string]any{}, "visibility": "public",
	}), nil)
	var parent lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "twenty-parent"}), &parent)
	parentBundle := waitBundle(t, eng, parent.ID)
	parentCore := onlyDecl(t, parentBundle, lagoon.CoreActorDeclID)
	children := make([]lagoon.ChannelCreateReply, 20)
	for i := range children {
		terminalValue(t, callMember(t, parent.ID, parentBundle, protocol.RootPrincipalID, parentCore, string(lagoon.WordChannelCreate), map[string]any{
			"name": fmt.Sprintf("sub-%d", i), "overrides": map[string]any{
				"declarations": []any{map[string]any{"decl_id": "echo-twenty"}},
				"profile":      map[string]any{"endpoints": map[string]any{"echo.say": map[string]any{"receiver": "echo-twenty"}}},
			},
		}), &children[i])
		peerDecl := "peer:" + string(children[i].ID)
		if ids, _ := core.View().DeclaredInstances(context.Background(), peerDecl); len(ids) != 1 {
			t.Fatalf("child %d core peer=%v", i, ids)
		}
		if ids, _ := parentBundle.View().DeclaredInstances(context.Background(), peerDecl); len(ids) != 1 {
			t.Fatalf("child %d parent peer=%v", i, ids)
		}
	}
	target := children[3]
	targetBundle := waitBundle(t, eng, target.ID)
	if ids, _ := targetBundle.View().DeclaredInstances(context.Background(), "peer:"+string(parent.ID)); len(ids) != 0 {
		t.Fatalf("child held parent at birth: %v", ids)
	}
	peer := onlyDecl(t, parentBundle, "peer:"+string(target.ID))
	if terminal := decodeTerminal(t, callMember(t, parent.ID, parentBundle, protocol.RootPrincipalID, peer, "echo.say", map[string]any{"task": "run"})); terminal.Status != message.StatusCompleted {
		t.Fatalf("sub-3 business call=%+v", terminal)
	}
}

func TestUnrelatedAliceChannelCanCallBusinessButNotManagement(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordDeclRegister), map[string]any{
		"id": "echo-legal", "name": "Echo Legal", "class": svcactorTestReceiverClass, "config": map[string]any{}, "visibility": "public",
	}), nil)
	var target lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{
		"name": "legal-target", "overrides": map[string]any{
			"declarations": []any{map[string]any{"decl_id": "echo-legal"}},
			"profile":      map[string]any{"endpoints": map[string]any{"echo.say": map[string]any{"receiver": "echo-legal"}}},
		},
	}), &target)

	req := httptest.NewRequest(http.MethodPost, "/api/identity/register", bytes.NewBufferString(`{"id":"legal-alice","email":"legal-alice@example.test","password":"secret"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	eng.handler.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("register alice status=%d body=%s", w.Code, w.Body.String())
	}
	channels, err := eng.registry.ListPresentChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var aliceHomeID channel.ID
	for _, row := range channels {
		if row.OwnerPrincipal == "legal-alice" && row.ParentID == protocol.C0ChannelID {
			aliceHomeID = row.ID
		}
	}
	aliceHome := waitBundle(t, eng, aliceHomeID)
	aliceCore := onlyDecl(t, aliceHome, lagoon.CoreActorDeclID)
	var legal lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, aliceHomeID, aliceHome, "legal-alice", aliceCore, string(lagoon.WordChannelCreate), map[string]any{"name": "legal"}), &legal)
	legalBundle := waitBundle(t, eng, legal.ID)
	peerDecl := "peer:" + string(target.ID)
	if terminal := decodeTerminal(t, callMember(t, legal.ID, legalBundle, "legal-alice", actor.SystemActorID, "channel.introduce_actor", map[string]any{"kind": "tool", "decl_id": peerDecl})); terminal.Status != message.StatusCompleted {
		t.Fatalf("legal introduce=%+v", terminal)
	}
	peer := onlyDecl(t, legalBundle, peerDecl)
	if terminal := decodeTerminal(t, callMember(t, legal.ID, legalBundle, "legal-alice", peer, "echo.say", map[string]any{"legal": true})); terminal.Status != message.StatusCompleted {
		t.Fatalf("legal business=%+v", terminal)
	}
	if terminal := decodeTerminal(t, callMember(t, legal.ID, legalBundle, "legal-alice", peer, "channel.restart_actor", map[string]any{"instance_id": "anything"})); terminal.Status != message.StatusFailed || terminal.ErrorCode != "forbidden" {
		t.Fatalf("legal management=%+v", terminal)
	}
}

func principalActorID(t *testing.T, bundle channelhost.Bundle, principal string) actor.ActorID {
	t.Helper()
	id, found, err := bundle.View().ResolvePrincipal(context.Background(), principal)
	if err != nil || !found {
		t.Fatalf("principal %s id=%q found=%v err=%v", principal, id, found, err)
	}
	return id
}
