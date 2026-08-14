package home

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/internal/sysactor"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type kindResolver struct{}

func (kindResolver) ResolveDeclaration(_ context.Context, _ channel.ID, id string) (channelspec.DeclarationFacts, error) {
	switch id {
	case "agent-decl":
		return channelspec.DeclarationFacts{OwnerPrincipal: "alice", Name: "Agent", Visibility: "public", Class: "agent-class"}, nil
	case "tool-decl":
		return channelspec.DeclarationFacts{OwnerPrincipal: "alice", Name: "Public Tool", Visibility: "public", Class: "tool-class"}, nil
	case "private-tool-decl":
		return channelspec.DeclarationFacts{OwnerPrincipal: "alice", Name: "Private Tool", Description: "Alice's private tool.", Visibility: "private", Class: "tool-class"}, nil
	default:
		return channelspec.DeclarationFacts{}, channelspec.ErrDeclarationNotFound
	}
}
func (kindResolver) ClassKind(_ context.Context, class string) (actor.Kind, bool, error) {
	switch class {
	case "agent-class":
		return actor.KindAgent, true, nil
	case "tool-class":
		return actor.KindTool, true, nil
	default:
		return "", false, nil
	}
}

type oneBindingReader struct{}

func (oneBindingReader) IsBound(context.Context, channel.ID, string) (bool, error) { return true, nil }
func (oneBindingReader) ListBoundDeviceIDs(context.Context, channel.ID) ([]string, error) {
	return []string{"device-1"}, nil
}

func openKindHome(t *testing.T) (*Home, actor.ActorID) {
	t.Helper()
	h, err := Open(Config{
		ChannelID: "kind-test", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver: emptyCompositionResolver{}, IntroductionResolver: kindResolver{},
		RegistryBindings: oneBindingReader{}, ReconcileInterval: time.Hour,
		Bootstrap: true, BootstrapOwnerPrincipal: "alice",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	alice, found, err := h.View().ResolvePrincipal(context.Background(), "alice")
	if err != nil || !found {
		t.Fatalf("resolve alice: found=%v err=%v", found, err)
	}
	return h, alice
}

func TestIntroduceAllowsUnknownFieldsButRejectsTrailingDocument(t *testing.T) {
	h, _ := openKindHome(t)
	for _, tc := range []struct {
		name    string
		payload json.RawMessage
		wantErr bool
	}{
		{name: "unknown field", payload: json.RawMessage(`{"kind":"human","principal":"future-human","future_option":true}`)},
		{name: "trailing document", payload: json.RawMessage(`{"kind":"human","principal":"second-human"} {}`), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.opEntry.Execute(context.Background(), sysactor.TypeIntroduceActor, sysactor.OperateRequest{Payload: tc.payload})
			if (err != nil) != tc.wantErr {
				t.Fatalf("Execute error=%v wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

func executeIntroduce(h *Home, sender actor.ActorID, payload string) (any, error) {
	return h.opEntry.Execute(context.Background(), sysactor.TypeIntroduceActor, sysactor.OperateRequest{
		ChannelID: h.channelID, Sender: sender, Anchor: "kind-test", Payload: json.RawMessage(payload),
	})
}

func requireBadPayload(t *testing.T, err error) {
	t.Helper()
	var opErr *sysactor.OperateError
	if !errors.As(err, &opErr) || opErr.Code != string(channelspec.ErrCodeBadPayload) {
		t.Fatalf("want bad_payload, got %v", err)
	}
}

func TestIntroducePayloadThreeKinds(t *testing.T) {
	t.Run("human uses principal without declaration", func(t *testing.T) {
		h, alice := openKindHome(t)
		value, err := executeIntroduce(h, alice, `{"kind":"human","principal":"bob"}`)
		if err != nil || value == nil {
			t.Fatalf("value=%v err=%v", value, err)
		}
		if _, found, err := h.View().ResolvePrincipal(context.Background(), "bob"); err != nil || !found {
			t.Fatalf("bob found=%v err=%v", found, err)
		}
	})
	t.Run("human rejects declaration", func(t *testing.T) {
		h, alice := openKindHome(t)
		_, err := executeIntroduce(h, alice, `{"kind":"human","principal":"bob","decl_id":"agent-decl"}`)
		requireBadPayload(t, err)
	})
	t.Run("agent requires declaration", func(t *testing.T) {
		h, alice := openKindHome(t)
		_, err := executeIntroduce(h, alice, `{"kind":"agent","principal":"steward"}`)
		requireBadPayload(t, err)
	})
	t.Run("tool rejects principal", func(t *testing.T) {
		h, alice := openKindHome(t)
		_, err := executeIntroduce(h, alice, `{"kind":"tool","decl_id":"tool-decl","principal":"wrong"}`)
		requireBadPayload(t, err)
	})
	t.Run("asserted kind must match class before mutation", func(t *testing.T) {
		h, alice := openKindHome(t)
		_, err := executeIntroduce(h, alice, `{"kind":"tool","decl_id":"agent-decl"}`)
		requireBadPayload(t, err)
		if ids, listErr := h.View().DeclaredInstances(context.Background(), "agent-decl"); listErr != nil || len(ids) != 0 {
			t.Fatalf("mismatched kind mutated registry: ids=%v err=%v", ids, listErr)
		}
	})
	t.Run("agent may own principal", func(t *testing.T) {
		h, alice := openKindHome(t)
		value, err := executeIntroduce(h, alice, `{"kind":"agent","decl_id":"agent-decl","principal":"steward"}`)
		if err != nil {
			t.Fatal(err)
		}
		id := value.(map[string]any)["instance_id"].(actor.ActorID)
		facts, found, err := h.View().ActorFacts(context.Background(), id)
		if err != nil || !found || facts.Kind != actor.KindAgent || facts.Principal != "steward" {
			t.Fatalf("facts=%+v found=%v err=%v", facts, found, err)
		}
	})
	t.Run("tool uses declaration without principal", func(t *testing.T) {
		h, alice := openKindHome(t)
		value, err := executeIntroduce(h, alice, `{"kind":"tool","decl_id":"tool-decl"}`)
		if err != nil || value == nil {
			t.Fatalf("value=%v err=%v", value, err)
		}
	})
}

func TestPrivateIntroductionUsesRegistrarAttributionRule(t *testing.T) {
	t.Run("agent without principal falls back to source declaration owner", func(t *testing.T) {
		h, alice := openKindHome(t)
		value, err := executeIntroduce(h, alice, `{"kind":"agent","decl_id":"agent-decl"}`)
		if err != nil {
			t.Fatal(err)
		}
		agentID := value.(map[string]any)["instance_id"].(actor.ActorID)
		privateValue, err := executeIntroduce(h, agentID, `{"kind":"tool","decl_id":"private-tool-decl"}`)
		if err != nil {
			t.Fatalf("unprincipaled agent could not introduce its owner's private declaration: %v", err)
		}
		privateID := privateValue.(map[string]any)["instance_id"].(actor.ActorID)
		roster, err := h.View().Roster(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range roster {
			if row.ID == privateID {
				if row.Name != "Private Tool" || row.Description != "Alice's private tool." {
					t.Fatalf("obs roster row=%+v", row)
				}
				return
			}
		}
		t.Fatalf("obs roster omitted %s", privateID)
	})

	t.Run("explicit mismatched principal wins and is rejected", func(t *testing.T) {
		h, alice := openKindHome(t)
		value, err := executeIntroduce(h, alice, `{"kind":"agent","decl_id":"agent-decl","principal":"mallory"}`)
		if err != nil {
			t.Fatal(err)
		}
		agentID := value.(map[string]any)["instance_id"].(actor.ActorID)
		_, err = executeIntroduce(h, agentID, `{"kind":"tool","decl_id":"private-tool-decl"}`)
		var operateErr *sysactor.OperateError
		if !errors.As(err, &operateErr) || operateErr.Code != string(channelspec.ErrCodeForbidden) {
			t.Fatalf("mismatched principal error=%v", err)
		}
	})

	t.Run("tool falls back to source declaration owner", func(t *testing.T) {
		h, alice := openKindHome(t)
		value, err := executeIntroduce(h, alice, `{"kind":"tool","decl_id":"tool-decl"}`)
		if err != nil {
			t.Fatal(err)
		}
		toolID := value.(map[string]any)["instance_id"].(actor.ActorID)
		if _, err := executeIntroduce(h, toolID, `{"kind":"tool","decl_id":"private-tool-decl"}`); err != nil {
			t.Fatalf("tool could not introduce its owner's private declaration: %v", err)
		}
	})
}
