package engineboot

import (
	"context"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol"
)

func TestCoreactorAndChannelDescribeShareTheSameC0Card(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	var direct introspect.Describe
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelDescribe), map[string]any{"channel_id": protocol.C0ChannelID}), &direct)
	if len(direct.Types) != len(lagoon.WriteWords)+len(lagoon.ReadWords) {
		t.Fatalf("c0 types=%d", len(direct.Types))
	}

	var home lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "card-home"}), &home)
	bundle := waitBundle(t, eng, home.ID)
	coreactor := onlyDecl(t, bundle, lagoon.CoreActorDeclID)
	var through introspect.Describe
	raw := callMember(t, home.ID, bundle, protocol.RootPrincipalID, coreactor, introspect.QueryDescribe, map[string]any{})
	if err := json.Unmarshal(raw, &through); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(direct, through) {
		a, _ := json.Marshal(direct)
		b, _ := json.Marshal(through)
		t.Fatalf("direct=%s through=%s", a, b)
	}
}

func TestPeeractorCardShowsManagementOnlyToCoreOrParent(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	var target, legal lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "card-target"}), &target)
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "card-legal"}), &legal)

	var privileged introspect.Describe
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelDescribe), map[string]any{"channel_id": target.ID}), &privileged)
	for _, word := range []string{"channel.introduce_actor", "channel.remove_actor", "channel.restart_actor"} {
		if _, ok := privileged.Types[word]; !ok {
			t.Fatalf("core card omitted %s: %+v", word, privileged.Types)
		}
	}

	legalBundle := waitBundle(t, eng, legal.ID)
	peerDecl := "peer:" + string(target.ID)
	terminal := decodeTerminal(t, callMember(t, legal.ID, legalBundle, protocol.RootPrincipalID, "system", "channel.introduce_actor", map[string]any{"kind": "tool", "decl_id": peerDecl}))
	if terminal.Status != "completed" {
		t.Fatalf("introduce peer=%+v", terminal)
	}
	peer := onlyDecl(t, legalBundle, peerDecl)
	var ordinary introspect.Describe
	raw := callMember(t, legal.ID, legalBundle, protocol.RootPrincipalID, peer, introspect.QueryDescribe, map[string]any{})
	if err := json.Unmarshal(raw, &ordinary); err != nil {
		t.Fatal(err)
	}
	for _, word := range []string{"channel.introduce_actor", "channel.remove_actor", "channel.restart_actor"} {
		if _, ok := ordinary.Types[word]; ok {
			t.Fatalf("ordinary card exposed %s: %+v", word, ordinary.Types)
		}
	}
}
