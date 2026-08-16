package engineboot

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/protocol"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestCredentialSetAcceptsHumansAndRejectsAgentPrincipals(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	var human lagoon.CredentialReply
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordCredentialSet), map[string]any{"principal_id": protocol.RootPrincipalID, "secret_hash": "replacement"}), &human)
	if human.PrincipalID != protocol.RootPrincipalID || human.Status != regspec.CredentialActive {
		t.Fatalf("human credential=%+v", human)
	}
	agent := decodeTerminal(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordCredentialSet), map[string]any{"principal_id": protocol.StewardPrincipalID, "secret_hash": "forbidden"}))
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
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	localRetire := decodeTerminal(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordDeviceRetire), map[string]any{"device_id": protocol.LocalDeviceID}))
	if localRetire.Status != message.StatusFailed || localRetire.ErrorCode != string(lagoon.CodeReserved) {
		t.Fatalf("local retire=%+v", localRetire)
	}
	localDetach := decodeTerminal(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordDeviceDetach), map[string]any{"channel_id": protocol.C0ChannelID, "device_id": protocol.LocalDeviceID}))
	if localDetach.Status != message.StatusFailed || localDetach.ErrorCode != string(lagoon.CodeReserved) {
		t.Fatalf("local detach=%+v", localDetach)
	}

	var device regspec.DeviceRow
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordDeviceMint), map[string]any{"name": "retiring-device"}), &device)
	var created lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "device-binding-home"}), &created)
	bundle := waitBundle(t, eng, created.ID)
	coreactor := onlyDecl(t, bundle, lagoon.CoreActorDeclID)
	terminalValue(t, callMember(t, created.ID, bundle, protocol.RootPrincipalID, coreactor, string(lagoon.WordDeviceAttach), map[string]any{"channel_id": created.ID, "device_id": device.ID}), nil)
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordDeviceRetire), map[string]any{"device_id": device.ID}), nil)
	bound, err := eng.registry.ListBoundDeviceIDs(context.Background(), created.ID)
	if err != nil || len(bound) != 1 || bound[0] != protocol.LocalDeviceID {
		t.Fatalf("effective bindings=%v err=%v", bound, err)
	}
	if yes, err := eng.registry.IsBound(context.Background(), created.ID, device.ID); err != nil || yes {
		t.Fatalf("retired device remained effective bound=%v err=%v", yes, err)
	}
}

func TestDeviceNamesAndClaimsAreValidatedAgainstStoredIdentity(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	call := func(word lagoon.Word, payload any) terminalShape {
		t.Helper()
		return decodeTerminal(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(word), payload))
	}
	var first regspec.DeviceRow
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordDeviceMint), map[string]any{"name": "unique-box"}), &first)
	if duplicate := call(lagoon.WordDeviceMint, map[string]any{"name": "unique-box"}); duplicate.Status != message.StatusFailed || duplicate.ErrorCode != string(lagoon.CodeConflictExists) {
		t.Fatalf("duplicate mint=%+v", duplicate)
	}
	if mismatch := call(lagoon.WordDeviceClaim, map[string]any{"device_id": first.ID, "name": "wrong-box"}); mismatch.Status != message.StatusFailed || mismatch.ErrorCode != string(lagoon.CodeConflictExists) {
		t.Fatalf("mismatched claim=%+v", mismatch)
	}
	if nameless := call(lagoon.WordDeviceClaim, map[string]any{"device_id": "missing"}); nameless.Status != message.StatusFailed || nameless.ErrorCode != string(lagoon.CodeInvalidArgs) {
		t.Fatalf("nameless claim=%+v", nameless)
	}
	var claimed regspec.DeviceRow
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordDeviceClaim), map[string]any{"device_id": "missing", "name": "claimed-box"}), &claimed)
	if claimed.ID == "" || claimed.Name != "claimed-box" {
		t.Fatalf("claimed device=%+v", claimed)
	}
}
