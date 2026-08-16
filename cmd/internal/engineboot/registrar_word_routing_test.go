package engineboot

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol"
)

func TestEveryRegistrarWordReachesItsHandlerThroughCoreactor(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	var home lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "word-routing-home"}), &home)
	bundle := waitBundle(t, eng, home.ID)
	coreactor := onlyDecl(t, bundle, lagoon.CoreActorDeclID)
	words := append(append([]lagoon.Word{}, lagoon.WriteWords[:]...), lagoon.ReadWords[:]...)
	if len(words) != 28 {
		t.Fatalf("registrar word inventory=%d want=28", len(words))
	}
	seenTemplates := 0
	for _, word := range words {
		t.Run(string(word), func(t *testing.T) {
			terminal := decodeTerminal(t, callMember(t, home.ID, bundle, protocol.RootPrincipalID, coreactor, string(word), map[string]any{}))
			if terminal.ErrorCode == string(lagoon.CodeResultUnknown) || strings.Contains(terminal.Detail, "unknown registrar word") {
				t.Fatalf("word did not reach handler: %+v", terminal)
			}
		})
		if strings.HasPrefix(string(word), "channel.template.") {
			seenTemplates++
		}
	}
	if seenTemplates != 5 {
		t.Fatalf("channel.template word inventory=%d want=5", seenTemplates)
	}
}
