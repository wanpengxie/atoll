package typeinstall_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	rtharness "github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/store"
	"github.com/wanpengxie/ActOS/runtime/typeinstall"
)

func TestServiceInstallTypeEmitsMirrorEvent(t *testing.T) {
	ctx := context.Background()
	chID := channel.ID("ch-typeinstall")
	svc, reg, msgs := newServiceFixture(t, chID, 12345)

	row := adapter.TypeRow{
		Type:               "xhs.publish",
		HandlerActorID:     "tool:xhs-adapter",
		HandlerBinding:     actor.BindingEmbedded,
		MaxPendingMs:       60_000,
		AllowedKinds:       []message.Kind{message.KindRequest, message.KindResponse},
		TerminalConvention: adapter.TerminalPayloadStatus,
	}
	if _, err := svc.InstallType(ctx, row); err != nil {
		t.Fatalf("InstallType: %v", err)
	}
	if _, ok, err := reg.Lookup(ctx, "xhs.publish"); err != nil || !ok {
		t.Fatalf("registry lookup ok=%v err=%v", ok, err)
	}

	env, ok, err := msgs.FindByID(ctx, chID, "system.type.installed:xhs.publish:12345")
	if err != nil || !ok {
		t.Fatalf("FindByID mirror ok=%v err=%v", ok, err)
	}
	if env.Type != "system.type.installed" || env.Kind != message.KindEvent {
		t.Fatalf("mirror envelope type=%s kind=%s", env.Type, env.Kind)
	}
	if env.Sender.ID != actor.SystemActorID || env.Sender.Kind != actor.KindSystem {
		t.Fatalf("mirror sender=%+v", env.Sender)
	}
	if env.Visibility != message.VisibilitySystem || !env.Audience.IsWildcard() {
		t.Fatalf("mirror visibility=%s audience=%v", env.Visibility, env.Audience)
	}

	var payload map[string]any
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["type"] != "xhs.publish" ||
		payload["handler_actor_id"] != "tool:xhs-adapter" ||
		payload["handler_binding"] != "embedded" ||
		payload["mutation_kind"] != "create" ||
		payload["installed_at"].(float64) != 12345 ||
		payload["max_pending_ms"].(float64) != 60000 {
		t.Fatalf("payload=%v", payload)
	}
	gotKinds := payload["allowed_kinds"].([]any)
	if len(gotKinds) != 2 || gotKinds[0] != "request" || gotKinds[1] != "response" {
		t.Fatalf("allowed_kinds=%v", gotKinds)
	}
}

func TestServiceInstallTypeRejectsReservedNamespace(t *testing.T) {
	ctx := context.Background()
	svc, reg, _ := newServiceFixture(t, "ch-typeinstall", 12345)
	_, err := svc.InstallType(ctx, adapter.TypeRow{
		Type:           "system.foo",
		HandlerActorID: "tool:xhs-adapter",
		HandlerBinding: actor.BindingEmbedded,
		MaxPendingMs:   60_000,
		AllowedKinds:   []message.Kind{message.KindEvent},
	})
	var ie *typeinstall.Error
	if !errors.As(err, &ie) || ie.Reason != message.InstallTypeRegistryReservedNamespace {
		t.Fatalf("expected type_registry_reserved_namespace, got %v", err)
	}
	if _, ok, err := reg.Lookup(ctx, "system.foo"); err != nil || ok {
		t.Fatalf("reserved row written ok=%v err=%v", ok, err)
	}
}

func newServiceFixture(t *testing.T, chID channel.ID, now int64) (*typeinstall.Service, *store.TypeRegistry, *store.Messages) {
	t.Helper()
	ctx := context.Background()
	db, err := store.OpenChannel(ctx, filepath.Join(t.TempDir(), "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	actors := store.NewActorRegistry(db)
	for _, rec := range []actorreg.Record{
		{ID: actor.SystemActorID, Kind: actor.KindSystem, CreatedAt: now},
		{ID: "tool:xhs-adapter", Kind: actor.KindTool, Binding: actor.BindingEmbedded, CreatedAt: now},
	} {
		if err := actors.Insert(ctx, rec); err != nil {
			t.Fatalf("insert actor %s: %v", rec.ID, err)
		}
	}

	reg := store.NewTypeRegistry(db, func() int64 { return now })
	msgs := store.NewMessages(db)
	chain, err := rtharness.New(rtharness.Deps{
		ChannelID:     chID,
		ActorRegistry: actors,
		TypeRegistry:  reg.HarnessView(),
		Log:           msgs,
		NowMs:         func() int64 { return now },
	})
	if err != nil {
		t.Fatalf("harness.New: %v", err)
	}
	svc, err := typeinstall.New(typeinstall.Config{
		ChannelID:     chID,
		ActorRegistry: actors,
		TypeRegistry:  reg,
		HarnessChain:  chain,
		NowFn:         func() int64 { return now },
	})
	if err != nil {
		t.Fatalf("typeinstall.New: %v", err)
	}
	return svc, reg, msgs
}
