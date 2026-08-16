package engineboot

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestServingGateAppliesOnlyToNewPeerIntroduction(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	var parent lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "serving-parent"}), &parent)
	parentHome := waitBundle(t, eng, parent.ID)
	coreactor := onlyDecl(t, parentHome, lagoon.CoreActorDeclID)
	var child lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, parent.ID, parentHome, protocol.RootPrincipalID, coreactor, string(lagoon.WordChannelCreate), map[string]any{
		"name": "closed-child", "overrides": map[string]any{"declarations": []any{}, "profile": map[string]any{"serving": 0}},
	}), &child)

	resolver := &assemblyResolver{registry: eng.registry}
	facts := channelspec.DeclarationFacts{Class: lagoon.PeerActorClass, Config: json.RawMessage(`{"channel":"` + string(child.ID) + `"}`)}
	if err := resolver.AdmitIntroduction(context.Background(), protocol.C0ChannelID, facts); err != nil {
		t.Fatalf("core introduction: %v", err)
	}
	if err := resolver.AdmitIntroduction(context.Background(), parent.ID, facts); err != nil {
		t.Fatalf("parent introduction: %v", err)
	}
	if err := resolver.AdmitIntroduction(context.Background(), channel.ID("unrelated"), facts); !errors.Is(err, channelspec.ErrTargetNotServing) {
		t.Fatalf("unrelated introduction error=%v", err)
	}
	if ids, err := parentHome.View().DeclaredInstances(context.Background(), "peer:"+string(child.ID)); err != nil || len(ids) != 1 {
		t.Fatalf("parent retained foundation peer ids=%v err=%v", ids, err)
	}

	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelProfileSet), map[string]any{
		"channel_id": child.ID, "description": "open", "serving": 1, "endpoints": map[string]any{},
	}), nil)
	if err := resolver.AdmitIntroduction(context.Background(), channel.ID("unrelated"), facts); err != nil {
		t.Fatalf("reopened introduction: %v", err)
	}
}

func TestProfileSetAuthorizesTargetOrCoreAndRejectsCoreTarget(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	var target, other lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "profile-target"}), &target)
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "profile-other"}), &other)
	targetBundle := waitBundle(t, eng, target.ID)
	otherBundle := waitBundle(t, eng, other.ID)
	targetCore := onlyDecl(t, targetBundle, lagoon.CoreActorDeclID)
	otherCore := onlyDecl(t, otherBundle, lagoon.CoreActorDeclID)
	payload := map[string]any{"channel_id": target.ID, "description": "from target", "serving": 1, "endpoints": map[string]any{}}
	if terminal := decodeTerminal(t, callMember(t, target.ID, targetBundle, protocol.RootPrincipalID, targetCore, string(lagoon.WordChannelProfileSet), payload)); terminal.Status != "completed" {
		t.Fatalf("target profile set=%+v", terminal)
	}
	if terminal := decodeTerminal(t, callMember(t, other.ID, otherBundle, protocol.RootPrincipalID, otherCore, string(lagoon.WordChannelProfileSet), payload)); terminal.Status != "failed" || terminal.ErrorCode != string(lagoon.CodePermissionDenied) {
		t.Fatalf("other profile set=%+v", terminal)
	}
	payload["description"] = "from core"
	if terminal := decodeTerminal(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelProfileSet), payload)); terminal.Status != "completed" {
		t.Fatalf("core profile set=%+v", terminal)
	}
	if terminal := decodeTerminal(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelProfileSet), map[string]any{"channel_id": protocol.C0ChannelID, "description": "no", "serving": 1, "endpoints": map[string]any{}})); terminal.Status != "failed" || terminal.ErrorCode != string(lagoon.CodeReserved) {
		t.Fatalf("c0 profile set=%+v", terminal)
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
