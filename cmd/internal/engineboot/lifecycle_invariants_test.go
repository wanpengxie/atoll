package engineboot

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

type terminalShape struct {
	Status    string          `json:"status"`
	ErrorCode string          `json:"error_code"`
	Detail    string          `json:"detail"`
	Value     json.RawMessage `json:"value"`
}

func decodeTerminal(t *testing.T, raw json.RawMessage) terminalShape {
	t.Helper()
	var terminal terminalShape
	if err := json.Unmarshal(raw, &terminal); err != nil {
		t.Fatal(err)
	}
	return terminal
}

func TestLifecycleProtectsSystemAndFoundationActors(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	for _, decl := range []string{lagoon.SvcActorDeclID, lagoon.RegistrarSeatDeclID, "peer:" + string(protocol.LobbyChannelID)} {
		id := onlyDecl(t, core, decl)
		for _, word := range []string{"channel.remove_actor", "channel.restart_actor"} {
			terminal := decodeTerminal(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, actor.SystemActorID, word, map[string]any{"instance_id": id}))
			if terminal.Status != message.StatusFailed || terminal.ErrorCode != "protected_actor" {
				t.Fatalf("decl=%s word=%s terminal=%+v", decl, word, terminal)
			}
		}
	}

	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	var a, b lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "peer-holder-a"}), &a)
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "peer-target-b"}), &b)
	homeA := waitBundle(t, eng, a.ID)
	for _, decl := range []string{lagoon.SvcActorDeclID, lagoon.CoreActorDeclID} {
		id := onlyDecl(t, homeA, decl)
		for _, word := range []string{"channel.remove_actor", "channel.restart_actor"} {
			terminal := decodeTerminal(t, callMember(t, a.ID, homeA, protocol.RootPrincipalID, actor.SystemActorID, word, map[string]any{"instance_id": id}))
			if terminal.Status != message.StatusFailed || terminal.ErrorCode != "protected_actor" {
				t.Fatalf("non-c0 decl=%s word=%s terminal=%+v", decl, word, terminal)
			}
		}
	}
	coreactor := onlyDecl(t, homeA, lagoon.CoreActorDeclID)
	var child lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, a.ID, homeA, protocol.RootPrincipalID, coreactor, string(lagoon.WordChannelCreate), map[string]any{"name": "foundation-child"}), &child)
	childPeer := onlyDecl(t, homeA, lagoon.PeerActorDeclPrefix+string(child.ID))
	for _, word := range []string{"channel.remove_actor", "channel.restart_actor"} {
		terminal := decodeTerminal(t, callMember(t, a.ID, homeA, protocol.RootPrincipalID, actor.SystemActorID, word, map[string]any{"instance_id": childPeer}))
		if terminal.Status != message.StatusFailed || terminal.ErrorCode != "protected_actor" {
			t.Fatalf("parent foundation word=%s terminal=%+v", word, terminal)
		}
	}
	peerDecl := "peer:" + string(b.ID)
	introduced := decodeTerminal(t, callMember(t, a.ID, homeA, protocol.RootPrincipalID, actor.SystemActorID, "channel.introduce_actor", map[string]any{"kind": "tool", "decl_id": peerDecl}))
	if introduced.Status != message.StatusCompleted {
		t.Fatalf("peer introduction=%+v", introduced)
	}
	peer := onlyDecl(t, homeA, peerDecl)
	removed := decodeTerminal(t, callMember(t, a.ID, homeA, protocol.RootPrincipalID, actor.SystemActorID, "channel.remove_actor", map[string]any{"instance_id": peer}))
	if removed.Status != message.StatusCompleted {
		t.Fatalf("peer removal=%+v", removed)
	}
}

func TestRetireCommitsThenRemovesCoreAndParentPeersIdempotently(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	var parent lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "retire-parent"}), &parent)
	parentHome := waitBundle(t, eng, parent.ID)
	coreactor := onlyDecl(t, parentHome, lagoon.CoreActorDeclID)
	var child lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, parent.ID, parentHome, protocol.RootPrincipalID, coreactor, string(lagoon.WordChannelCreate), map[string]any{"name": "retire-child"}), &child)
	peerDecl := "peer:" + string(child.ID)
	if ids, _ := core.View().DeclaredInstances(context.Background(), peerDecl); len(ids) != 1 {
		t.Fatalf("core peer before retire=%v", ids)
	}
	if ids, _ := parentHome.View().DeclaredInstances(context.Background(), peerDecl); len(ids) != 1 {
		t.Fatalf("parent peer before retire=%v", ids)
	}
	for attempt := 0; attempt < 2; attempt++ {
		terminal := decodeTerminal(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelRetire), map[string]any{"channel_id": child.ID}))
		if terminal.Status != message.StatusCompleted {
			t.Fatalf("retire attempt %d=%+v", attempt, terminal)
		}
		var value lagoon.ChannelRetireReply
		if err := json.Unmarshal(terminal.Value, &value); err != nil || value.Status != "retired" {
			t.Fatalf("retire value=%s err=%v", terminal.Value, err)
		}
	}
	if ids, _ := core.View().DeclaredInstances(context.Background(), peerDecl); len(ids) != 0 {
		t.Fatalf("core peer after retire=%v", ids)
	}
	if ids, _ := parentHome.View().DeclaredInstances(context.Background(), peerDecl); len(ids) != 0 {
		t.Fatalf("parent peer after retire=%v", ids)
	}
	decl, ok, err := eng.registry.GetDecl(context.Background(), peerDecl)
	if err != nil || !ok || decl.Status != "revoked" {
		t.Fatalf("peer template=%+v ok=%v err=%v", decl, ok, err)
	}
}

func TestCoreSourceReplacesRootSuperuserForRetire(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	var rootHome lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "root"}), &rootHome)
	rootBundle := waitBundle(t, eng, rootHome.ID)

	req := httptest.NewRequest(http.MethodPost, "/api/identity/register", bytes.NewBufferString(`{"id":"alice","email":"alice-retire@example.test","password":"secret"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	eng.handler.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("register status=%d body=%s", w.Code, w.Body.String())
	}
	channels, err := eng.registry.ListPresentChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var aliceHome channel.ID
	for _, row := range channels {
		if row.OwnerPrincipal == "alice" {
			aliceHome = row.ID
		}
	}
	if aliceHome == "" {
		t.Fatal("alice home missing")
	}
	rootCoreactor := onlyDecl(t, rootBundle, lagoon.CoreActorDeclID)
	denied := decodeTerminal(t, callMember(t, rootHome.ID, rootBundle, protocol.RootPrincipalID, rootCoreactor, string(lagoon.WordChannelRetire), map[string]any{"channel_id": aliceHome}))
	if denied.Status != message.StatusFailed || denied.ErrorCode != string(lagoon.CodePermissionDenied) {
		t.Fatalf("root home retire=%+v", denied)
	}
	allowed := decodeTerminal(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelRetire), map[string]any{"channel_id": aliceHome}))
	if allowed.Status != message.StatusCompleted {
		t.Fatalf("core retire=%+v", allowed)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := eng.host.Acquire(aliceHome); !ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("retired alice home remained serving")
}

func TestCorePeerManagementRunsThroughTargetSvcactorAndSysactor(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordDeclRegister), map[string]any{
		"id": "remove-me", "name": "Remove Me", "class": "echo", "config": map[string]any{}, "visibility": "public",
	}), nil)
	var target lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{
		"name": "managed-target", "overrides": map[string]any{"declarations": []any{map[string]any{"decl_id": "remove-me"}}},
	}), &target)
	targetBundle := waitBundle(t, eng, target.ID)
	member := onlyDecl(t, targetBundle, "remove-me")
	peer := onlyDecl(t, core, "peer:"+string(target.ID))
	terminal := decodeTerminal(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, peer, "channel.remove_actor", map[string]any{"instance_id": member}))
	if terminal.Status != message.StatusCompleted {
		t.Fatalf("remove terminal=%+v", terminal)
	}
	if ids, err := targetBundle.View().DeclaredInstances(context.Background(), "remove-me"); err != nil || len(ids) != 0 {
		t.Fatalf("removed ids=%v err=%v", ids, err)
	}
	targetRoot := principalActorID(t, targetBundle, protocol.RootPrincipalID)
	rows, _, err := targetBundle.View().ReadVisibleAfterSeq(context.Background(), channel.Reader{ActorID: targetRoot, Mode: channel.ReaderMember}, 0, 1024)
	if err != nil {
		t.Fatal(err)
	}
	svc := onlyDecl(t, targetBundle, lagoon.SvcActorDeclID)
	var requestID message.ID
	var replied bool
	for _, row := range rows {
		if row.Envelope.Type != "channel.remove_actor" {
			continue
		}
		if row.Envelope.Kind == message.KindRequest && row.Envelope.Sender.ID == svc && row.Envelope.Audience.Contains(actor.SystemActorID) {
			requestID = row.Envelope.ID
		}
		if row.Envelope.Kind == message.KindResponse && row.Envelope.ParentID == requestID {
			replied = true
		}
	}
	if requestID == "" || !replied {
		t.Fatalf("target management ledger request=%s replied=%v", requestID, replied)
	}
}
