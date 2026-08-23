package engineboot

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestProfileSetAuthorizesTargetOrC0AndRejectsC0Target(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(channelspec.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarDeclID)
	var target, other lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "profile-target", "initial_actor_ids": []any{currentMemberID(t, core, channelspec.RootPrincipalID)}}), &target)
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "profile-other", "initial_actor_ids": []any{currentMemberID(t, core, channelspec.RootPrincipalID)}}), &other)
	targetBundle := waitBundle(t, eng, target.ChannelID)
	otherBundle := waitBundle(t, eng, other.ChannelID)
	payload := map[string]any{"channel_id": target.ChannelID, "description": "from target", "serving": 1}
	if terminal := decodeTerminal(t, callMember(t, target.ChannelID, targetBundle, channelspec.RootPrincipalID, "system", string(lagoon.WordChannelSet), payload)); terminal.Status != "completed" {
		t.Fatalf("target profile set=%+v", terminal)
	}
	if terminal := decodeTerminal(t, callMember(t, other.ChannelID, otherBundle, channelspec.RootPrincipalID, "system", string(lagoon.WordChannelSet), payload)); terminal.Status != "failed" || terminal.ErrorCode != string(lagoon.CodePermissionDenied) {
		t.Fatalf("other profile set=%+v", terminal)
	}
	payload["description"] = "from core"
	if terminal := decodeTerminal(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelSet), payload)); terminal.Status != "completed" {
		t.Fatalf("core profile set=%+v", terminal)
	}
	if terminal := decodeTerminal(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelSet), map[string]any{"channel_id": channelspec.C0ChannelID, "description": "no", "serving": 1})); terminal.Status != "failed" || terminal.ErrorCode != string(lagoon.CodeReserved) {
		t.Fatalf("c0 profile set=%+v", terminal)
	}
	if terminal := decodeTerminal(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelSet), map[string]any{"channel_id": target.ChannelID, "description": "missing serving"})); terminal.Status != "failed" || terminal.ErrorCode != string(lagoon.CodeInvalidArgs) {
		t.Fatalf("missing serving profile set=%+v", terminal)
	}
}

func waitBundle(t *testing.T, eng *Engine, id channel.ID) channelhost.Bundle {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if bundle, ok := eng.host.Acquire(id); ok {
			return bundle
		}
		if time.Now().After(deadline) {
			t.Fatalf("channel %s not serving", id)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
