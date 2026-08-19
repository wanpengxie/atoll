package engineboot

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestEveryRegistrarWordReachesItsHandlerThroughDeclaredRoutes(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(channelspec.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarDeclID)
	var home lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "word-routing-home"}), &home)
	bundle := waitBundle(t, eng, home.ChannelID)
	words := append(append([]lagoon.Word{}, lagoon.WriteWords[:]...), lagoon.ReadWords[:]...)
	if len(words) != 29 {
		t.Fatalf("registrar word inventory=%d want=29", len(words))
	}
	seenTemplates := 0
	routes := []struct {
		name   string
		ch     channel.ID
		bundle channelhost.Bundle
		target actor.ActorID
	}{
		{name: "membrane", ch: home.ChannelID, bundle: bundle, target: actor.SystemActorID},
		{name: "c0-direct", ch: channelspec.C0ChannelID, bundle: core, target: registrar},
	}
	for _, word := range words {
		for _, route := range routes {
			t.Run(route.name+"/"+string(word), func(t *testing.T) {
				terminal := decodeTerminal(t, callMember(t, route.ch, route.bundle, channelspec.RootPrincipalID, route.target, string(word), map[string]any{}))
				if terminal.ErrorCode == string(lagoon.CodeResultUnknown) || strings.Contains(terminal.Detail, "unknown registrar word") {
					t.Fatalf("word did not reach handler: %+v", terminal)
				}
			})
		}
		if strings.HasPrefix(string(word), "system.channel.template.") {
			seenTemplates++
		}
	}
	if seenTemplates != 5 {
		t.Fatalf("channel.template word inventory=%d want=5", seenTemplates)
	}
}
