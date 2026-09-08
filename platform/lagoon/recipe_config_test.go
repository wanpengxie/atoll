package lagoon_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform/boot"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

type recipeCatalog struct{ lagoon.ClassCatalog }

func (recipeCatalog) ValidateConfig(string, json.RawMessage) error { return nil }
func (recipeCatalog) LookupClassKind(class string) (actor.Kind, bool) {
	switch class {
	case "channel-seat":
		return actor.KindChannel, true
	case lagoon.PeerActorClass:
		return actor.KindPeer, true
	default:
		return actor.KindTool, true
	}
}
func (recipeCatalog) LookupClassPlacement(string) (channelspec.PlacementKind, bool) {
	return channelspec.PlacementServer, true
}
func (recipeCatalog) ResolveConfig(_ string, raw json.RawMessage) (json.RawMessage, error) {
	fields := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &fields); err != nil {
			return nil, err
		}
	}
	// A binding changes the input, which in turn changes a derived default.
	if host, ok := fields["host"]; ok {
		fields["resolved_host"] = host
	}
	return json.Marshal(fields)
}

func TestRecipeConfigMaterializationAndRegistryOverrides(t *testing.T) {
	ctx := context.Background()
	installed, err := boot.Ensure(ctx, boot.Config{ChannelDir: filepath.Join(t.TempDir(), "channels"), RootPassword: "test-password", ResolveClassConfig: registry.ResolveDefaultConfig})
	if err != nil {
		t.Fatal(err)
	}
	r, err := lagoon.Open(installed.RegistryDBPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	registrar := lagoon.NewRegistrar(r, cutFacts{}, recipeCatalog{})
	run := func(word lagoon.Word, payload any) *cutSys {
		t.Helper()
		proc, err := lagoon.Def(registrar).New()
		if err != nil {
			t.Fatal(err)
		}
		sys := &cutSys{msgs: []actorbase.Msg{cutMessage(word, payload)}}
		_ = proc(sys)
		return sys
	}
	if sys := run(lagoon.WordActorTemplateCreate, map[string]any{"id": "bound-tool", "name": "bound-tool", "class": "test-tool", "visibility": "public", "config": map[string]any{"host": "old"}}); sys.code != "" {
		t.Fatalf("register: %s", sys.code)
	}
	var bodyID string
	for i, mode := range []string{"bindings", "config", "both"} {
		item := map[string]any{"decl_id": "bound-tool"}
		if mode != "config" {
			item["bindings"] = map[string]string{"host": "parent_channel_id"}
		}
		if mode != "bindings" {
			item["config"] = map[string]any{"host": "inline"}
		}
		sys := run(lagoon.WordChannelCreate, map[string]any{"name": fmt.Sprintf("bound-%d", i), "recipe": map[string]any{"declarations": []any{item}}, "initial_seats": []any{}})
		if sys.code != "" {
			t.Fatalf("%s create: %s", mode, sys.code)
		}
		var created lagoon.ChannelCreateReply
		if err := sys.reply.DecodeValue(&created); err != nil {
			t.Fatal(err)
		}
		bodyID = string(created.ChannelID)
		row, found, err := r.GetChannelDesired(ctx, created.ChannelID)
		if err != nil || !found {
			t.Fatalf("row: %v %v", found, err)
		}
		var spec lagoon.GenesisSpec
		if err := json.Unmarshal(row.Spec, &spec); err != nil {
			t.Fatal(err)
		}
		overlays, err := r.GetOverlays(ctx, created.ChannelID)
		if err != nil || len(overlays) != 1 {
			t.Fatalf("overlays: %+v %v", overlays, err)
		}
		matched := false
		for _, decl := range spec.Declarations {
			if decl.DeclID != "bound-tool" {
				continue
			}
			matched = true
			if string(decl.Rendered.Config) != string(overlays[0].Config) {
				t.Fatalf("%s genesis=%s overlay=%s", mode, decl.Rendered.Config, overlays[0].Config)
			}
			var config map[string]any
			if err := json.Unmarshal(overlays[0].Config, &config); err != nil {
				t.Fatal(err)
			}
			want := string(channelspec.C0ChannelID)
			if mode == "config" {
				want = "inline"
			}
			if config["host"] != want || config["resolved_host"] != want {
				t.Fatalf("%s final config=%s", mode, overlays[0].Config)
			}
		}
		if !matched {
			t.Fatal("missing genesis declaration")
		}
	}
	for _, declID := range []string{bodyID, "seat:" + bodyID} {
		for i, override := range []map[string]any{{"config": map[string]any{"body": "wrong"}}, {"bindings": map[string]string{"body": "parent_channel_id"}}} {
			item := map[string]any{"decl_id": declID}
			for k, v := range override {
				item[k] = v
			}
			recipe := map[string]any{"declarations": []any{item}}
			for _, word := range []lagoon.Word{lagoon.WordChannelCreate, lagoon.WordChannelTemplateCreate} {
				payload := map[string]any{"name": fmt.Sprintf("invalid-%d", i), "recipe": recipe, "initial_seats": []any{}}
				if word == lagoon.WordChannelTemplateCreate {
					payload = map[string]any{"id": "invalid-template", "name": "invalid-template", "body": recipe}
				}
				if sys := run(word, payload); sys.code != "invalid_args" {
					t.Fatalf("%s %s override accepted: code=%s", word, declID, sys.code)
				}
			}
		}
	}
	for i, declID := range []string{bodyID, "seat:" + bodyID} {
		sys := run(lagoon.WordChannelCreate, map[string]any{"name": fmt.Sprintf("plain-%d", i), "recipe": map[string]any{"declarations": []any{map[string]any{"decl_id": declID}}}, "initial_seats": []any{}})
		if sys.code != "" {
			t.Fatalf("unmodified %s rejected: %s", declID, sys.code)
		}
	}
}
