package engineboot

import (
	"context"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"testing"

	_ "github.com/wanpengxie/atoll/drivers/tools/echo"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func newProtocolDeliveryRig(t *testing.T) (*Engine, string, channelhost.Bundle, actor.ActorID) {
	t.Helper()
	channelDir := filepath.Join(t.TempDir(), "channels")
	eng, err := Boot(Config{ChannelDBDir: channelDir, Addr: "127.0.0.1:0", RootPassword: "test-root-password", OpenRegistration: true}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close(context.Background()) })
	core, _ := eng.host.Acquire(channelspec.C0ChannelID)
	return eng, channelDir, core, onlyDecl(t, core, lagoon.RegistrarDeclID)
}

func createdChannelID(t *testing.T, raw []byte) channel.ID {
	t.Helper()
	var terminal map[string]json.RawMessage
	if err := json.Unmarshal(raw, &terminal); err != nil || len(terminal) != 2 {
		t.Fatalf("system channel-create terminal=%s err=%v", raw, err)
	}
	var value map[string]json.RawMessage
	if err := json.Unmarshal(terminal["value"], &value); err != nil || len(value) != 1 {
		t.Fatalf("system channel-create value=%s err=%v", terminal["value"], err)
	}
	var child channel.ID
	if err := json.Unmarshal(value["channel_id"], &child); err != nil || child == "" {
		t.Fatalf("channel_id=%s err=%v", value["channel_id"], err)
	}
	return child
}
