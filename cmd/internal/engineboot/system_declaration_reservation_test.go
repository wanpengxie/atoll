package engineboot

import (
	"context"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestSystemDeclarationsRejectOverlayEditAndRevokeWithoutRetargeting(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	var parent lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "reserved-parent"}), &parent)
	parentBundle := waitBundle(t, eng, parent.ID)
	coreactor := onlyDecl(t, parentBundle, lagoon.CoreActorDeclID)
	var child lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, parent.ID, parentBundle, protocol.RootPrincipalID, coreactor, string(lagoon.WordChannelCreate), map[string]any{"name": "reserved-child"}), &child)
	peerDecl := lagoon.PeerActorDeclPrefix + string(child.ID)
	peer := onlyDecl(t, parentBundle, peerDecl)

	card := func(target string) introspect.Describe {
		t.Helper()
		raw := callMember(t, parent.ID, parentBundle, protocol.RootPrincipalID, actor.ActorID(target), introspect.QueryDescribe, map[string]any{})
		var out introspect.Describe
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	beforeCore, beforePeer := card(string(coreactor)), card(string(peer))
	for _, declID := range []string{lagoon.CoreActorDeclID, peerDecl} {
		for _, word := range []lagoon.Word{lagoon.WordOverlaySet, lagoon.WordOverlayClear} {
			payload := map[string]any{"decl_id": declID, "channel_id": parent.ID}
			if word == lagoon.WordOverlaySet {
				payload["config"] = map[string]any{"channel": "wrong-target"}
			}
			terminal := decodeTerminal(t, callMember(t, parent.ID, parentBundle, protocol.RootPrincipalID, coreactor, string(word), payload))
			if terminal.Status != message.StatusFailed || terminal.ErrorCode != string(lagoon.CodeReserved) {
				t.Fatalf("decl=%s word=%s terminal=%+v", declID, word, terminal)
			}
		}
	}
	if after := card(string(coreactor)); after.ActorID != beforeCore.ActorID {
		t.Fatalf("coreactor retargeted before=%s after=%s", beforeCore.ActorID, after.ActorID)
	}
	if after := card(string(peer)); after.ActorID != beforePeer.ActorID {
		t.Fatalf("peeractor retargeted before=%s after=%s", beforePeer.ActorID, after.ActorID)
	}

	for _, declID := range []string{lagoon.SvcActorDeclID, lagoon.CoreActorDeclID, lagoon.RegistrarSeatDeclID, peerDecl} {
		for _, word := range []lagoon.Word{lagoon.WordDeclEdit, lagoon.WordDeclRevoke} {
			terminal := decodeTerminal(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(word), map[string]any{"id": declID, "name": "changed"}))
			if terminal.Status != message.StatusFailed || terminal.ErrorCode != string(lagoon.CodeReserved) {
				t.Fatalf("decl=%s word=%s terminal=%+v", declID, word, terminal)
			}
		}
	}
}
