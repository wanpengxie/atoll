package engineboot

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestChannelRetireRejectsLiveChildrenAndPreservesParent(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(channelspec.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarDeclID)
	var parent lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "live-parent", "initial_actor_ids": []any{currentMemberID(t, core, channelspec.RootPrincipalID)}}), &parent)
	parentBundle := waitBundle(t, eng, parent.ChannelID)
	var child lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, parent.ChannelID, parentBundle, channelspec.RootPrincipalID, "system", string(lagoon.WordChannelCreate), map[string]any{"name": "live-child", "initial_actor_ids": []any{currentMemberID(t, parentBundle, channelspec.RootPrincipalID)}}), &child)
	retired := decodeTerminal(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelDelete), map[string]any{"channel_id": parent.ChannelID}))
	if retired.Status != message.StatusFailed || retired.ErrorCode != string(lagoon.CodeConflictExists) {
		t.Fatalf("retire parent terminal=%+v", retired)
	}
	row, ok, err := eng.registry.GetChannelDesired(context.Background(), parent.ChannelID)
	if err != nil || !ok || row.Status != "present" {
		t.Fatalf("parent row=%+v ok=%v err=%v", row, ok, err)
	}
}

func TestChannelNameReplayReturnsExistingButRetiredNameRemainsReserved(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(channelspec.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarDeclID)
	create := func() lagoon.ChannelCreateReply {
		t.Helper()
		var out lagoon.ChannelCreateReply
		terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "reserved-name", "initial_actor_ids": []any{currentMemberID(t, core, channelspec.RootPrincipalID)}}), &out)
		return out
	}
	first, replay := create(), create()
	if replay.ChannelID != first.ChannelID {
		t.Fatalf("same-name replay first=%+v replay=%+v", first, replay)
	}
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelDelete), map[string]any{"channel_id": first.ChannelID}), nil)
	conflict := decodeTerminal(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "reserved-name", "initial_actor_ids": []any{currentMemberID(t, core, channelspec.RootPrincipalID)}}))
	if conflict.Status != message.StatusFailed || conflict.ErrorCode != string(lagoon.CodeConflictExists) {
		t.Fatalf("retired-name create=%+v", conflict)
	}
}
