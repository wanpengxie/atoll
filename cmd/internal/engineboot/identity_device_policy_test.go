package engineboot

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestCredentialSetAcceptsHumansAndRejectsAgentPrincipals(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(channelspec.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarDeclID)
	var human lagoon.CredentialReply
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordCredentialSet), map[string]any{"principal_id": channelspec.RootPrincipalID, "secret_hash": "replacement"}), &human)
	if human.PrincipalID != channelspec.RootPrincipalID || human.Status != regspec.CredentialActive {
		t.Fatalf("human credential=%+v", human)
	}
	agent := decodeTerminal(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordCredentialSet), map[string]any{"principal_id": channelspec.StewardPrincipalID, "secret_hash": "forbidden"}))
	if agent.Status != message.StatusFailed || agent.ErrorCode != string(lagoon.CodePermissionDenied) {
		t.Fatalf("agent credential=%+v", agent)
	}
}

func TestLocalDeviceReservationsAndRetiredBindingsUseEffectiveStatus(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(channelspec.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarDeclID)
	localRetire := decodeTerminal(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordDeviceDelete), map[string]any{"device_id": channelspec.LocalDeviceID}))
	if localRetire.Status != message.StatusFailed || localRetire.ErrorCode != string(lagoon.CodeReserved) {
		t.Fatalf("local retire=%+v", localRetire)
	}
	localDetach := decodeTerminal(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordDeviceDetach), map[string]any{"channel_id": channelspec.C0ChannelID, "device_id": channelspec.LocalDeviceID}))
	if localDetach.Status != message.StatusFailed || localDetach.ErrorCode != string(lagoon.CodeReserved) {
		t.Fatalf("local detach=%+v", localDetach)
	}

	var device regspec.DeviceRow
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordDeviceCreate), map[string]any{"name": "retiring-device"}), &device)
	var created lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "device-binding-home"}), &created)
	bundle := waitBundle(t, eng, created.ChannelID)
	terminalValue(t, callMember(t, created.ChannelID, bundle, channelspec.RootPrincipalID, "system", string(lagoon.WordDeviceAttach), map[string]any{"channel_id": created.ChannelID, "device_id": device.ID}), nil)
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordDeviceDelete), map[string]any{"device_id": device.ID}), nil)
	bound, err := eng.registry.ListBoundDeviceIDs(context.Background(), created.ChannelID)
	if err != nil || len(bound) != 1 || bound[0] != channelspec.LocalDeviceID {
		t.Fatalf("effective bindings=%v err=%v", bound, err)
	}
	if yes, err := eng.registry.IsBound(context.Background(), created.ChannelID, device.ID); err != nil || yes {
		t.Fatalf("retired device remained effective bound=%v err=%v", yes, err)
	}
}
