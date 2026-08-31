package engineboot

import (
	"context"
	"encoding/json"
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
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "device-binding-home", "initial_actor_ids": []any{currentMemberID(t, core, channelspec.RootPrincipalID)}}), &created)
	createdRow, found, err := eng.registry.GetChannelDesired(context.Background(), created.ChannelID)
	if err != nil || !found || createdRow.DefaultStorageDeviceID != channelspec.LocalDeviceID {
		t.Fatalf("default channel storage row=%+v found=%v err=%v", createdRow, found, err)
	}
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

func TestChannelTemplateMaterializesDefaultStorageInItsOwnBody(t *testing.T) {
	_, _, core, registrar := newProtocolDeliveryRig(t)
	var row regspec.ChannelTemplateRow
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelTemplateCreate), map[string]any{
		"id": "default-storage", "name": "Default storage", "body": map[string]any{"declarations": []any{}},
	}), &row)
	var body regspec.TemplateBody
	if err := json.Unmarshal(row.Body, &body); err != nil {
		t.Fatal(err)
	}
	if body.Profile == nil || body.Profile.DefaultStorageDeviceID == nil || *body.Profile.DefaultStorageDeviceID != channelspec.LocalDeviceID {
		t.Fatalf("materialized template body=%s", row.Body)
	}
}

func TestChannelDefaultStorageIsConfiguredIndependentlyFromDaemonOrdering(t *testing.T) {
	eng, _, core, registrar := newProtocolDeliveryRig(t)

	var remote regspec.DeviceRow
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordDeviceCreate), map[string]any{"name": "mac-storage"}), &remote)
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordActorTemplateCreate), map[string]any{
		"id": "remote-storage-seat", "name": "remote-storage-seat", "class": "codex", "visibility": "public", "config": map[string]any{},
	}), nil)
	child := createdChannelID(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{
		"name":              "configured-storage",
		"initial_actor_ids": []any{currentMemberID(t, core, channelspec.RootPrincipalID)},
		"recipe": map[string]any{
			"declarations": []any{map[string]any{"decl_id": "remote-storage-seat", "desired_host": remote.ID}},
			"profile":      map[string]any{"default_storage_device_id": remote.ID},
		},
	}))
	bundle := waitBundle(t, eng, child)
	row, found, err := eng.registry.GetChannelDesired(context.Background(), child)
	if err != nil || !found || row.DefaultStorageDeviceID != remote.ID {
		t.Fatalf("configured channel row=%+v found=%v err=%v", row, found, err)
	}
	bound, err := eng.registry.ListBoundDeviceIDs(context.Background(), child)
	if err != nil || len(bound) != 2 {
		t.Fatalf("configured channel bindings=%v err=%v", bound, err)
	}
	var visible []regspec.ChannelDeviceRow
	terminalValue(t, callMember(t, child, bundle, channelspec.RootPrincipalID, "system", string(lagoon.WordChannelDeviceList), map[string]any{}), &visible)
	if len(visible) != 2 || visible[0].ChannelID != child || visible[1].ChannelID != child {
		t.Fatalf("channel device list=%+v", visible)
	}

	blocked := decodeTerminal(t, callMember(t, child, bundle, channelspec.RootPrincipalID, "system", string(lagoon.WordDeviceDetach), map[string]any{"channel_id": child, "device_id": remote.ID}))
	if blocked.Status != message.StatusFailed || blocked.ErrorCode != string(lagoon.CodeReserved) {
		t.Fatalf("detaching configured storage=%+v", blocked)
	}
	blocked = decodeTerminal(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordDeviceDelete), map[string]any{"device_id": remote.ID}))
	if blocked.Status != message.StatusFailed || blocked.ErrorCode != string(lagoon.CodeReserved) {
		t.Fatalf("retiring configured storage=%+v", blocked)
	}

	var updated regspec.ChannelRow
	terminalValue(t, callMember(t, child, bundle, channelspec.RootPrincipalID, "system", string(lagoon.WordChannelSet), map[string]any{
		"channel_id": child, "description": "", "serving": 1, "default_storage_device_id": channelspec.LocalDeviceID,
	}), &updated)
	if updated.DefaultStorageDeviceID != channelspec.LocalDeviceID {
		t.Fatalf("updated channel storage=%q", updated.DefaultStorageDeviceID)
	}
	blocked = decodeTerminal(t, callMember(t, child, bundle, channelspec.RootPrincipalID, "system", string(lagoon.WordDeviceDetach), map[string]any{"channel_id": child, "device_id": remote.ID}))
	if blocked.Status != message.StatusFailed || blocked.ErrorCode != string(lagoon.CodeReserved) {
		t.Fatalf("detaching actor placement=%+v", blocked)
	}
	terminalValue(t, callMember(t, child, bundle, channelspec.RootPrincipalID, "system", "system.member.delete", map[string]any{"decl_id": "remote-storage-seat"}), nil)
	terminalValue(t, callMember(t, child, bundle, channelspec.RootPrincipalID, "system", string(lagoon.WordDeviceDetach), map[string]any{"channel_id": child, "device_id": remote.ID}), nil)
}
